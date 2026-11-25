//! The gRPC WorkflowService the Temporal SDKs talk to. Unlisted methods answer "unimplemented".

use std::sync::Arc;
use std::time::Duration as StdDuration;

use tonic::{Request, Response, Status};

use crate::engine::activity::ActEnd;
use crate::engine::namespace::{ALLOW_DEFAULT, ALLOW_START};
use crate::engine::update::{Reached, UPDATE_LONG_POLL_MS, wait_stage};
use crate::engine::visibility::HistTok;
use crate::engine::{Engine, HISTORY_LONG_POLL_MS, LONG_POLL_MS, QKind, QueryOutcome, poll_timeout_ms};
use crate::proto::temporal::api::workflowservice::v1::workflow_service_server::WorkflowService;
use crate::util::*;

pub const SERVER_VERSION: &str = "1.29.0";

pub struct Svc {
    pub engine: Arc<Engine>,
}

type Res<T> = Result<Response<T>, Status>;

fn grpc_timeout<T>(r: &Request<T>) -> Option<StdDuration> {
    let v = r.metadata().get("grpc-timeout")?.to_str().ok()?;
    let (num, unit) = v.split_at(v.len().checked_sub(1)?);
    let n: u64 = num.parse().ok()?;
    Some(match unit {
        "H" => StdDuration::from_secs(n * 3600),
        "M" => StdDuration::from_secs(n * 60),
        "S" => StdDuration::from_secs(n),
        "m" => StdDuration::from_millis(n),
        "u" => StdDuration::from_micros(n),
        "n" => StdDuration::from_nanos(n),
        _ => return None,
    })
}

fn exec_of(e: &Option<common::WorkflowExecution>) -> (String, String) {
    e.as_ref()
        .map(|x| (x.workflow_id.clone(), x.run_id.clone()))
        .unwrap_or_default()
}

struct PreparedUpdate {
    exec: common::WorkflowExecution,
    update_id: String,
    stage: enums::UpdateWorkflowExecutionLifecycleStage,
    request: update_pb::Request,
}

/// Go's prepareUpdateWorkflowRequest: check the request, give it an update id if it has none,
/// and work out the stage to wait for.
fn prepare_update(r: &mut wsv1::UpdateWorkflowExecutionRequest) -> R<PreparedUpdate> {
    use enums::UpdateWorkflowExecutionLifecycleStage as S;
    let (wid, rid) = valid_execution(&r.workflow_execution)?;
    let upd = r.request.get_or_insert_with(Default::default);
    let Some(meta) = upd.meta.as_mut() else {
        return Err(invalid("Update meta is not set on request."));
    };
    if meta.update_id.len() > 1000 {
        return Err(invalid("UpdateId length exceeds limit."));
    }
    if meta.update_id.is_empty() {
        meta.update_id = new_id();
    }
    let update_id = meta.update_id.clone();
    match upd.input.as_ref() {
        None => return Err(invalid("Update input is not set on request.")),
        Some(i) if i.name.is_empty() => return Err(invalid("Update name is not set on request.")),
        _ => {}
    }
    let stage = match r.wait_policy.as_ref().map(|w| w.lifecycle_stage).unwrap_or(0) {
        0 => S::Completed,
        v => S::try_from(v).unwrap_or(S::Completed),
    };
    if stage == S::Admitted {
        return Err(Status::permission_denied(
            "UpdateWorkflowExecution issued asynchronously and waiting on update admitted is not supported.",
        ));
    }
    Ok(PreparedUpdate {
        exec: common::WorkflowExecution {
            workflow_id: wid,
            run_id: rid,
        },
        update_id,
        stage,
        request: upd.clone(),
    })
}

fn update_response(
    ns: &str,
    wid: String,
    rid: String,
    update_id: String,
    reached: Reached,
) -> wsv1::UpdateWorkflowExecutionResponse {
    let link = update_link(ns, &wid, &rid, &reached);
    wsv1::UpdateWorkflowExecutionResponse {
        update_ref: Some(update_pb::UpdateRef {
            workflow_execution: Some(common::WorkflowExecution {
                workflow_id: wid,
                run_id: rid,
            }),
            update_id,
        }),
        outcome: reached.outcome,
        stage: reached.stage as i32,
        link,
        ..Default::default()
    }
}

/// The link a start response carries: the run's started event.
fn started_link(ns: &str, wid: &str, rid: &str) -> common::Link {
    use common::link;
    common::Link {
        variant: Some(link::Variant::WorkflowEvent(link::WorkflowEvent {
            namespace: ns.to_string(),
            workflow_id: wid.to_string(),
            run_id: rid.to_string(),
            reference: Some(link::workflow_event::Reference::EventRef(
                link::workflow_event::EventReference {
                    event_id: 1,
                    event_type: enums::EventType::WorkflowExecutionStarted as i32,
                },
            )),
        })),
    }
}

/// Go's MultiOperationExecution error: the first real failure's code, and a status per operation
/// (OK, the failure, or "aborted" for operations that didn't run because of it).
fn multi_op_error(errs: [Option<Status>; 2]) -> Status {
    use errordetails::multi_operation_execution_failure::OperationStatus;
    let mut code = None;
    let statuses = errs
        .iter()
        .map(|e| match e {
            None => OperationStatus::default(),
            Some(st) => {
                if code.is_none() && st.code() != tonic::Code::Aborted {
                    code = Some(st.code());
                }
                let details = if st.details().is_empty() {
                    vec![]
                } else {
                    <RpcStatus as prost::Message>::decode(st.details())
                        .map(|s| s.details)
                        .unwrap_or_default()
                };
                OperationStatus {
                    code: st.code() as i32,
                    message: st.message().to_string(),
                    details,
                }
            }
        })
        .collect();
    status_with_detail(
        code.unwrap_or(tonic::Code::Aborted),
        "Update-with-Start could not be executed.",
        "temporal.api.errordetails.v1.MultiOperationExecutionFailure",
        &errordetails::MultiOperationExecutionFailure { statuses },
    )
}

/// A moment past the caller's own deadline: like the Go server, an update call that runs out of
/// the caller's time isn't answered (the caller has already given up).
fn hard_deadline<T>(r: &Request<T>) -> Option<tokio::time::Instant> {
    grpc_timeout(r).map(|d| tokio::time::Instant::now() + d + StdDuration::from_millis(500))
}

/// The link an update response carries: the accepted event, or the workflow for a rejection.
fn update_link(ns: &str, wid: &str, rid: &str, reached: &Reached) -> Option<common::Link> {
    use common::link;
    use enums::UpdateWorkflowExecutionLifecycleStage as S;
    if reached.stage != S::Accepted && reached.stage != S::Completed {
        return None;
    }
    let variant = if reached.accepted_event_id == 0 {
        link::Variant::Workflow(link::Workflow {
            namespace: ns.to_string(),
            workflow_id: wid.to_string(),
            run_id: rid.to_string(),
            reason: "Update rejected".into(),
        })
    } else {
        link::Variant::WorkflowEvent(link::WorkflowEvent {
            namespace: ns.to_string(),
            workflow_id: wid.to_string(),
            run_id: rid.to_string(),
            reference: Some(link::workflow_event::Reference::EventRef(
                link::workflow_event::EventReference {
                    event_id: reached.accepted_event_id,
                    event_type: enums::EventType::WorkflowExecutionUpdateAccepted as i32,
                },
            )),
        })
    };
    Some(common::Link { variant: Some(variant) })
}

/// Go's validateExecution: the execution is set, has a workflow id and a well-formed run id.
fn valid_execution(e: &Option<common::WorkflowExecution>) -> R<(String, String)> {
    let e = e.as_ref().ok_or_else(|| invalid("Execution is not set on request."))?;
    if e.workflow_id.is_empty() {
        return Err(invalid("WorkflowId is not set on request."));
    }
    if !e.run_id.is_empty() && uuid::Uuid::parse_str(&e.run_id).is_err() {
        return Err(invalid("Invalid RunId."));
    }
    Ok((e.workflow_id.clone(), e.run_id.clone()))
}

/// Check an UpdateWorkflowExecutionOptions mask against WorkflowExecutionOptions (like
/// fieldmaskpb.New) and turn its paths into the camelCase names Go's ParseFieldMask gives.
fn options_mask_fields(paths: &[String]) -> R<Vec<String>> {
    const SUB: &[(&str, &[&str])] = &[
        (
            "versioning_override",
            &["behavior", "deployment", "pinned_version", "pinned", "auto_upgrade"],
        ),
        ("priority", &["priority_key", "fairness_key", "fairness_weight"]),
        (
            "time_skipping_config",
            &[
                "enabled",
                "fast_forward_config",
                "disable_propagation",
                "max_session_skip_count",
            ],
        ),
    ];
    let camel = |s: &str| {
        let mut out = String::new();
        let mut up = false;
        for ch in s.chars() {
            if ch == '_' {
                up = true;
            } else if up {
                out.extend(ch.to_uppercase());
                up = false;
            } else {
                out.push(ch);
            }
        }
        out
    };
    let mut out = Vec::new();
    for p in paths {
        let mut parts = p.split('.');
        let top = parts.next().unwrap_or("");
        let ok = match SUB.iter().find(|(name, _)| *name == top) {
            None => false,
            Some((_, subs)) => match parts.next() {
                None => true,
                Some(sub) => subs.contains(&sub) && (top != "priority" || parts.next().is_none()),
            },
        };
        if !ok {
            return Err(invalid(format!(
                "error parsing UpdateMask: proto: invalid path {p:?} for message \"temporal.api.workflow.v1.WorkflowExecutionOptions\""
            )));
        }
        out.push(p.split('.').map(camel).collect::<Vec<_>>().join("."));
    }
    Ok(out)
}

/// History page size in batches: 256 by default and at most (Go server).
fn history_page_size(requested: i32) -> usize {
    const MAX: i32 = 256;
    if requested <= 0 || requested > MAX {
        MAX as usize
    } else {
        requested as usize
    }
}

fn act_token(t: &[u8]) -> R<TaskToken> {
    let tok = TaskToken::decode(t)?;
    if tok.k != "act" {
        return Err(invalid("Not an activity task token."));
    }
    Ok(tok)
}

impl Svc {
    /// The namespace exists and is in a state that allows the operation.
    fn check_ns(&self, ns: &str, allowed: &[enums::NamespaceState]) -> R<()> {
        self.engine.lock().check_ns(ns, allowed)
    }

    /// Same, for operations that carry the namespace in their task token.
    fn check_token_ns(&self, token: &[u8]) -> R<()> {
        let t = TaskToken::decode(token)?;
        self.check_ns(&t.ns, ALLOW_DEFAULT)
    }
}

#[tonic::async_trait]
impl WorkflowService for Svc {
    async fn get_system_info(&self, _r: Request<wsv1::GetSystemInfoRequest>) -> Res<wsv1::GetSystemInfoResponse> {
        Ok(Response::new(wsv1::GetSystemInfoResponse {
            server_version: SERVER_VERSION.into(),
            capabilities: Some(wsv1::get_system_info_response::Capabilities {
                signal_and_query_header: true,
                internal_error_differentiation: true,
                activity_failure_include_heartbeat: true,
                supports_schedules: false,
                encoded_failure_attributes: true,
                build_id_based_versioning: false,
                upsert_memo: true,
                eager_workflow_start: false,
                sdk_metadata: true,
                count_group_by_execution_status: false,
                nexus: false,
                ..Default::default()
            }),
        }))
    }

    async fn get_cluster_info(&self, _r: Request<wsv1::GetClusterInfoRequest>) -> Res<wsv1::GetClusterInfoResponse> {
        Ok(Response::new(wsv1::GetClusterInfoResponse {
            server_version: SERVER_VERSION.into(),
            cluster_id: "rustify-temporal".into(),
            cluster_name: "active".into(),
            history_shard_count: 1,
            persistence_store: "sqlite".into(),
            visibility_store: "sqlite".into(),
            initial_failover_version: 1,
            failover_version_increment: 10,
            ..Default::default()
        }))
    }

    // ---------------- namespaces ----------------

    async fn register_namespace(
        &self,
        r: Request<wsv1::RegisterNamespaceRequest>,
    ) -> Res<wsv1::RegisterNamespaceResponse> {
        let r = r.into_inner();
        self.engine.op(|i| i.register_namespace(r))?;
        Ok(Response::new(wsv1::RegisterNamespaceResponse {}))
    }

    async fn describe_namespace(
        &self,
        r: Request<wsv1::DescribeNamespaceRequest>,
    ) -> Res<wsv1::DescribeNamespaceResponse> {
        let r = r.into_inner();
        Ok(Response::new(
            self.engine.lock().describe_namespace(&r.namespace, &r.id)?,
        ))
    }

    async fn list_namespaces(&self, r: Request<wsv1::ListNamespacesRequest>) -> Res<wsv1::ListNamespacesResponse> {
        let r = r.into_inner();
        Ok(Response::new(self.engine.lock().list_namespaces(&r)?))
    }

    async fn deprecate_namespace(
        &self,
        r: Request<wsv1::DeprecateNamespaceRequest>,
    ) -> Res<wsv1::DeprecateNamespaceResponse> {
        let r = r.into_inner();
        self.engine.op(|i| i.deprecate_namespace(&r.namespace))?;
        Ok(Response::new(wsv1::DeprecateNamespaceResponse::default()))
    }

    async fn update_namespace(&self, r: Request<wsv1::UpdateNamespaceRequest>) -> Res<wsv1::UpdateNamespaceResponse> {
        let r = r.into_inner();
        self.check_ns(&r.namespace, ALLOW_DEFAULT)?;
        Ok(Response::new(self.engine.op(|i| i.update_namespace(r))?))
    }

    // ---------------- starting and changing runs ----------------

    async fn start_workflow_execution(
        &self,
        r: Request<wsv1::StartWorkflowExecutionRequest>,
    ) -> Res<wsv1::StartWorkflowExecutionResponse> {
        let r = r.into_inner();
        self.check_ns(&r.namespace, ALLOW_START)?;
        let ns = r.namespace.clone();
        let wid = r.workflow_id.clone();
        let (run_id, started, first) = self.engine.op(|i| {
            let (rid, started) = i.start_workflow(r)?;
            let first = i.first_run_id(&(ns.clone(), rid.clone()));
            Ok((rid, started, first))
        })?;
        Ok(Response::new(wsv1::StartWorkflowExecutionResponse {
            link: Some(started_link(&ns, &wid, &run_id)),
            run_id,
            started,
            status: enums::WorkflowExecutionStatus::Running as i32,
            first_execution_run_id: first,
            ..Default::default()
        }))
    }

    async fn signal_workflow_execution(
        &self,
        r: Request<wsv1::SignalWorkflowExecutionRequest>,
    ) -> Res<wsv1::SignalWorkflowExecutionResponse> {
        let r = r.into_inner();
        self.check_ns(&r.namespace, ALLOW_DEFAULT)?;
        let (wid, rid) = exec_of(&r.workflow_execution);
        self.engine.op(|i| {
            let key = i.resolve(&r.namespace, &wid, &rid)?;
            i.signal_key(
                &key,
                &r.signal_name,
                r.input,
                r.header,
                &r.identity,
                &r.request_id,
                None,
            )
        })?;
        Ok(Response::new(wsv1::SignalWorkflowExecutionResponse::default()))
    }

    async fn signal_with_start_workflow_execution(
        &self,
        r: Request<wsv1::SignalWithStartWorkflowExecutionRequest>,
    ) -> Res<wsv1::SignalWithStartWorkflowExecutionResponse> {
        let r = r.into_inner();
        self.check_ns(&r.namespace, ALLOW_START)?;
        let (run_id, started) = self.engine.op(|i| i.signal_with_start(r))?;
        Ok(Response::new(wsv1::SignalWithStartWorkflowExecutionResponse {
            run_id,
            started,
            ..Default::default()
        }))
    }

    async fn terminate_workflow_execution(
        &self,
        r: Request<wsv1::TerminateWorkflowExecutionRequest>,
    ) -> Res<wsv1::TerminateWorkflowExecutionResponse> {
        let r = r.into_inner();
        self.check_ns(&r.namespace, ALLOW_DEFAULT)?;
        let (wid, rid) = exec_of(&r.workflow_execution);
        self.engine.op(|i| {
            let key = i.resolve(&r.namespace, &wid, &rid)?;
            i.terminate_key(&key, &r.reason, r.details, &r.identity)
        })?;
        Ok(Response::new(wsv1::TerminateWorkflowExecutionResponse::default()))
    }

    async fn request_cancel_workflow_execution(
        &self,
        r: Request<wsv1::RequestCancelWorkflowExecutionRequest>,
    ) -> Res<wsv1::RequestCancelWorkflowExecutionResponse> {
        let r = r.into_inner();
        self.check_ns(&r.namespace, ALLOW_DEFAULT)?;
        let (wid, rid) = exec_of(&r.workflow_execution);
        self.engine.op(|i| {
            let key = i.resolve(&r.namespace, &wid, &rid)?;
            i.request_cancel_key(&key, &r.reason, &r.identity, None)
        })?;
        Ok(Response::new(wsv1::RequestCancelWorkflowExecutionResponse::default()))
    }

    async fn delete_workflow_execution(
        &self,
        r: Request<wsv1::DeleteWorkflowExecutionRequest>,
    ) -> Res<wsv1::DeleteWorkflowExecutionResponse> {
        let r = r.into_inner();
        let (wid, rid) = valid_execution(&r.workflow_execution)?;
        self.check_ns(&r.namespace, ALLOW_DEFAULT)?;
        self.engine.op(|i| {
            let key = i.resolve(&r.namespace, &wid, &rid)?;
            i.delete_execution(&key)
        })?;
        Ok(Response::new(wsv1::DeleteWorkflowExecutionResponse::default()))
    }

    async fn execute_multi_operation(
        &self,
        req: Request<wsv1::ExecuteMultiOperationRequest>,
    ) -> Res<wsv1::ExecuteMultiOperationResponse> {
        use wsv1::execute_multi_operation_request::operation::Operation as Op;
        use wsv1::execute_multi_operation_response::{Response as OpResp, response::Response as RespKind};
        let hard = hard_deadline(&req);
        let r = req.into_inner();
        self.check_ns(&r.namespace, ALLOW_START)?;
        let bad_shape = || invalid("Operations have to be exactly [Start, Update].");
        if r.operations.len() != 2 {
            return Err(bad_shape());
        }
        let mut ops = r.operations.into_iter().map(|o| o.operation);
        let (Some(Op::StartWorkflow(mut start)), Some(Op::UpdateWorkflow(mut upd))) =
            (ops.next().flatten(), ops.next().flatten())
        else {
            return Err(bad_shape());
        };
        // Check both operations first; each failure is reported on its own operation.
        let start_check = (|| {
            if !start.namespace.is_empty() && start.namespace != r.namespace {
                return Err(invalid("Operation namespace did not match request's namespace."));
            }
            start.namespace = r.namespace.clone();
            if !start.cron_schedule.is_empty() {
                return Err(invalid("CronSchedule is not allowed."));
            }
            if start.request_eager_execution {
                return Err(invalid("RequestEagerExecution is not supported."));
            }
            if dur_ms(&start.workflow_start_delay) > 0 {
                return Err(invalid("WorkflowStartDelay is not supported."));
            }
            if start.request_id.is_empty() {
                start.request_id = new_id();
            }
            Ok(())
        })();
        let upd_check = (|| {
            if !upd.namespace.is_empty() && upd.namespace != r.namespace {
                return Err(invalid("Operation namespace did not match request's namespace."));
            }
            upd.namespace = r.namespace.clone();
            let p = prepare_update(&mut upd)?;
            if !upd.first_execution_run_id.is_empty() {
                return Err(invalid("FirstExecutionRunId is not allowed."));
            }
            if !p.exec.run_id.is_empty() {
                return Err(invalid("RunId is not allowed."));
            }
            if start_check.is_ok() && p.exec.workflow_id != start.workflow_id {
                return Err(invalid("WorkflowId is not consistent with previous operation(s)."));
            }
            Ok(p)
        })();
        let prepared = match (start_check, upd_check) {
            (Ok(()), Ok(p)) => p,
            (s, u) => {
                return Err(multi_op_error([
                    Some(s.err().unwrap_or_else(multi_op_aborted)),
                    Some(u.err().unwrap_or_else(multi_op_aborted)),
                ]));
            }
        };
        let ns = r.namespace.clone();
        let wid = start.workflow_id.clone();
        // One retry when the update was cut short by the workflow closing (Go server).
        let mut retried = false;
        loop {
            let got = self
                .engine
                .op(|i| Ok(i.update_with_start(start.clone(), prepared.request.clone())))?;
            let (run, h) = match got {
                Ok(x) => x,
                Err(errs) => return Err(multi_op_error(errs)),
            };
            match wait_stage(h.rx, prepared.stage, UPDATE_LONG_POLL_MS, hard).await {
                Ok(reached) => {
                    let start_resp = wsv1::StartWorkflowExecutionResponse {
                        run_id: run.run_id.clone(),
                        started: run.started,
                        status: run.status,
                        first_execution_run_id: if run.started {
                            run.first_run_id.clone()
                        } else {
                            String::new()
                        },
                        link: run.link.then(|| started_link(&ns, &wid, &run.run_id)),
                        ..Default::default()
                    };
                    let upd_resp = update_response(&ns, h.workflow_id, h.run_id, prepared.update_id.clone(), reached);
                    return Ok(Response::new(wsv1::ExecuteMultiOperationResponse {
                        responses: vec![
                            OpResp {
                                response: Some(RespKind::StartWorkflow(start_resp)),
                            },
                            OpResp {
                                response: Some(RespKind::UpdateWorkflow(upd_resp)),
                            },
                        ],
                    }));
                }
                Err(e) => {
                    let closed = e.code() == tonic::Code::NotFound
                        && e.message() == "workflow update was aborted by closing workflow";
                    if closed && !run.started {
                        if !retried {
                            retried = true;
                            continue;
                        }
                        return Err(multi_op_error([
                            Some(multi_op_aborted()),
                            Some(Status::aborted(e.message().to_string())),
                        ]));
                    }
                    let start_err = if run.started { None } else { Some(multi_op_aborted()) };
                    return Err(multi_op_error([start_err, Some(e)]));
                }
            }
        }
    }

    async fn update_workflow_execution_options(
        &self,
        r: Request<wsv1::UpdateWorkflowExecutionOptionsRequest>,
    ) -> Res<wsv1::UpdateWorkflowExecutionOptionsResponse> {
        let r = r.into_inner();
        let (wid, rid) = exec_of(&r.workflow_execution);
        if wid.is_empty() {
            return Err(invalid("WorkflowId is not set on request."));
        }
        let mask = r
            .update_mask
            .as_ref()
            .ok_or_else(|| invalid("UpdateMask is required"))?;
        let opts = r
            .workflow_execution_options
            .as_ref()
            .ok_or_else(|| invalid("WorkflowExecutionOptions is required"))?;
        let fields = options_mask_fields(&mask.paths)?;
        if let Some(p) = &opts.priority {
            if p.priority_key < 0 {
                return Err(invalid("priority key can't be negative"));
            }
            if p.fairness_key.len() > 64 {
                return Err(invalid("fairness key length exceeds limit"));
            }
            if p.fairness_weight < 0.0 {
                return Err(invalid("must be greater than zero"));
            }
        }
        if opts.time_skipping_config.is_some() {
            return Err(Status::permission_denied(
                "workflow time skipping is not enabled for the namespace",
            ));
        }
        self.check_ns(&r.namespace, ALLOW_DEFAULT)?;
        let merged = self.engine.op(|i| {
            let key = i.resolve(&r.namespace, &wid, &rid)?;
            i.update_options(&key, opts, &fields, &r.identity)
        })?;
        // The Go frontend answers with the merged options only (no update time).
        Ok(Response::new(wsv1::UpdateWorkflowExecutionOptionsResponse {
            workflow_execution_options: Some(merged),
            ..Default::default()
        }))
    }

    async fn pause_workflow_execution(
        &self,
        r: Request<wsv1::PauseWorkflowExecutionRequest>,
    ) -> Res<wsv1::PauseWorkflowExecutionResponse> {
        // Off by default on the Go server (frontend.WorkflowPauseEnabled).
        Err(Status::unimplemented(format!(
            "workflow pause is not enabled for namespace: {}",
            r.get_ref().namespace
        )))
    }

    async fn unpause_workflow_execution(
        &self,
        r: Request<wsv1::UnpauseWorkflowExecutionRequest>,
    ) -> Res<wsv1::UnpauseWorkflowExecutionResponse> {
        let r = r.into_inner();
        for (v, what) in [
            (&r.reason, "reason"),
            (&r.request_id, "request id"),
            (&r.identity, "identity"),
        ] {
            if v.len() > 1000 {
                return Err(invalid(format!("{what} is too long.")));
            }
        }
        self.check_ns(&r.namespace, ALLOW_DEFAULT)?;
        let mut g = self.engine.lock();
        let key = g.resolve(&r.namespace, &r.workflow_id, &r.run_id)?;
        let (_, open) = g.run_progress(&key)?;
        if !open {
            return Err(wf_completed_error());
        }
        // Workflows can't be paused (see PauseWorkflowExecution), so none is ever paused.
        Err(Status::failed_precondition("workflow is not paused."))
    }

    async fn reset_workflow_execution(
        &self,
        r: Request<wsv1::ResetWorkflowExecutionRequest>,
    ) -> Res<wsv1::ResetWorkflowExecutionResponse> {
        let r = r.into_inner();
        if r.request_id.is_empty() {
            return Err(invalid("RequestId is not set on request."));
        }
        if r.request_id.len() > 1000 {
            return Err(invalid("RequestId length exceeds limit."));
        }
        valid_execution(&r.workflow_execution)?;
        if enums::ResetReapplyType::try_from(r.reset_reapply_type).is_err() {
            return Err(Status::internal(format!(
                "unknown reset reapply type: {}",
                r.reset_reapply_type
            )));
        }
        for op in &r.post_reset_operations {
            use workflow_pb::post_reset_operation::Variant;
            if let Some(Variant::UpdateWorkflowOptions(o)) = &op.variant
                && o.workflow_execution_options
                    .as_ref()
                    .is_some_and(|x| x.time_skipping_config.is_some())
            {
                return Err(Status::permission_denied(
                    "workflow time skipping is not enabled for the namespace",
                ));
            }
        }
        self.check_ns(&r.namespace, ALLOW_DEFAULT)?;
        let run_id = self.engine.op(|i| i.reset_workflow(r))?;
        Ok(Response::new(wsv1::ResetWorkflowExecutionResponse { run_id }))
    }

    // ---------------- workflow updates ----------------

    async fn update_workflow_execution(
        &self,
        req: Request<wsv1::UpdateWorkflowExecutionRequest>,
    ) -> Res<wsv1::UpdateWorkflowExecutionResponse> {
        let hard = hard_deadline(&req);
        let mut r = req.into_inner();
        self.check_ns(&r.namespace, ALLOW_DEFAULT)?;
        let PreparedUpdate {
            exec,
            update_id,
            stage,
            request: upd,
        } = prepare_update(&mut r)?;
        // An update the server dropped before the workflow saw it is admitted again (the Go
        // server retries these internally).
        let started = tokio::time::Instant::now();
        let mut backoff = 50;
        loop {
            let h = self.engine.op(|i| {
                let key = i.resolve(&r.namespace, &exec.workflow_id, &exec.run_id)?;
                i.update_workflow(&key, &r.first_execution_run_id, upd.clone())
            })?;
            match wait_stage(h.rx, stage, UPDATE_LONG_POLL_MS, hard).await {
                Err(e)
                    if e.code() == tonic::Code::Unavailable
                        && started.elapsed() < StdDuration::from_millis(UPDATE_LONG_POLL_MS as u64)
                        && hard.is_none_or(|h| tokio::time::Instant::now() + std_dur(backoff) < h) =>
                {
                    tokio::time::sleep(std_dur(backoff)).await;
                    backoff = (backoff * 2).min(1000);
                }
                Err(e) => return Err(e),
                Ok(reached) => {
                    return Ok(Response::new(update_response(
                        &r.namespace,
                        h.workflow_id,
                        h.run_id,
                        update_id,
                        reached,
                    )));
                }
            }
        }
    }

    async fn poll_workflow_execution_update(
        &self,
        req: Request<wsv1::PollWorkflowExecutionUpdateRequest>,
    ) -> Res<wsv1::PollWorkflowExecutionUpdateResponse> {
        use enums::UpdateWorkflowExecutionLifecycleStage as S;
        let hard = hard_deadline(&req);
        let r = req.into_inner();
        let uref = r
            .update_ref
            .clone()
            .ok_or_else(|| invalid("UpdateRef is not set on request."))?;
        self.check_ns(&r.namespace, ALLOW_DEFAULT)?;
        let stage =
            S::try_from(r.wait_policy.as_ref().map(|w| w.lifecycle_stage).unwrap_or(0)).unwrap_or(S::Unspecified);
        let (wid, rid) = exec_of(&uref.workflow_execution);
        let h = self.engine.op(|i| {
            let key = i.resolve(&r.namespace, &wid, &rid)?;
            i.poll_update(&key, &uref.update_id)
        })?;
        let reached = wait_stage(h.rx, stage, UPDATE_LONG_POLL_MS, hard).await?;
        Ok(Response::new(wsv1::PollWorkflowExecutionUpdateResponse {
            outcome: reached.outcome,
            stage: reached.stage as i32,
            update_ref: Some(update_pb::UpdateRef {
                workflow_execution: Some(common::WorkflowExecution {
                    workflow_id: h.workflow_id,
                    run_id: h.run_id,
                }),
                update_id: uref.update_id,
            }),
            ..Default::default()
        }))
    }

    // ---------------- reading runs ----------------

    async fn get_workflow_execution_history(
        &self,
        req: Request<wsv1::GetWorkflowExecutionHistoryRequest>,
    ) -> Res<wsv1::GetWorkflowExecutionHistoryResponse> {
        let timeout = poll_timeout_ms(grpc_timeout(&req), HISTORY_LONG_POLL_MS);
        let r = req.into_inner();
        self.check_ns(&r.namespace, ALLOW_DEFAULT)?;
        let (wid, rid) = valid_execution(&r.execution)?;
        let page = history_page_size(r.maximum_page_size);
        let close_only = r.history_event_filter_type == enums::HistoryEventFilterType::CloseEvent as i32;
        let long_poll = r.wait_new_event;
        let resp = |events: Vec<history::HistoryEvent>, token: Vec<u8>| {
            Response::new(wsv1::GetWorkflowExecutionHistoryResponse {
                history: Some(history::History { events }),
                next_page_token: token,
                ..Default::default()
            })
        };
        // Where to continue: a fresh snapshot of the run, or the caller's token.
        let (key, mut tok, has_token) = if r.next_page_token.is_empty() {
            let mut g = self.engine.lock();
            let key = g.resolve(&r.namespace, &wid, &rid)?;
            let tok = g.history_start(&key)?;
            (key, tok, false)
        } else {
            let tok = HistTok::decode(&r.next_page_token)?;
            if !rid.is_empty() && rid != tok.r {
                return Err(invalid("RunId in the request does not match the NextPageToken."));
            }
            let mut g = self.engine.lock();
            g.resolve(&r.namespace, &wid, &tok.r)?;
            ((r.namespace.clone(), tok.r.clone()), tok, true)
        };
        // Wait until the run has progress worth reporting (Go's GetOrPollWorkflowMutableState).
        let wait_for = |engine: &Arc<crate::engine::Engine>, key: crate::engine::Key, after: Option<i64>| {
            let engine = engine.clone();
            async move {
                let got = engine
                    .long_poll(
                        timeout,
                        |i| i.notify_of_run(&key),
                        |i| {
                            let (next, open) = i.run_progress(&key)?;
                            let moved = match after {
                                Some(n) => next > n,
                                None => false,
                            };
                            Ok((moved || !open).then_some((next, open)))
                        },
                    )
                    .await?;
                match got {
                    Some(v) => Ok::<_, Status>(v),
                    None => engine.lock().run_progress(&key),
                }
            }
        };
        if close_only {
            if tok.run && long_poll {
                let (_, open) = wait_for(&self.engine, key.clone(), None).await?;
                tok.run = open;
            }
            if !tok.run {
                let ev = self.engine.lock().close_event(&key)?;
                return Ok(resp(ev.into_iter().collect(), vec![]));
            }
            return Ok(resp(vec![], if long_poll { tok.encode() } else { vec![] }));
        }
        if has_token && tok.c == 0 && long_poll && tok.run {
            let (next, open) = wait_for(&self.engine, key.clone(), Some(tok.n)).await?;
            tok.f = tok.n;
            tok.n = next;
            tok.run = open;
        }
        let events = if tok.f >= tok.n {
            if !tok.run {
                return Ok(resp(vec![], vec![]));
            }
            vec![]
        } else {
            self.engine.lock().history_read(&key, &mut tok, page)?
        };
        let token = if tok.c == 0 && (!tok.run || !long_poll) {
            vec![]
        } else {
            tok.encode()
        };
        Ok(resp(events, token))
    }

    async fn get_workflow_execution_history_reverse(
        &self,
        req: Request<wsv1::GetWorkflowExecutionHistoryReverseRequest>,
    ) -> Res<wsv1::GetWorkflowExecutionHistoryReverseResponse> {
        let r = req.into_inner();
        self.check_ns(&r.namespace, ALLOW_DEFAULT)?;
        let (wid, rid) = valid_execution(&r.execution)?;
        let page = history_page_size(r.maximum_page_size);
        let mut g = self.engine.lock();
        let (key, mut tok) = if r.next_page_token.is_empty() {
            let key = g.resolve(&r.namespace, &wid, &rid)?;
            let mut tok = g.history_start(&key)?;
            tok.n = 0;
            (key, tok)
        } else {
            let tok = HistTok::decode(&r.next_page_token)?;
            if !rid.is_empty() && rid != tok.r {
                return Err(invalid("RunId in the request does not match the NextPageToken."));
            }
            g.resolve(&r.namespace, &wid, &tok.r)?;
            ((r.namespace.clone(), tok.r.clone()), tok)
        };
        let events = g.history_read_reverse(&key, &mut tok, page)?;
        let next_page_token = if tok.n < 1 { vec![] } else { tok.encode() };
        Ok(Response::new(wsv1::GetWorkflowExecutionHistoryReverseResponse {
            history: Some(history::History { events }),
            next_page_token,
        }))
    }

    async fn describe_workflow_execution(
        &self,
        r: Request<wsv1::DescribeWorkflowExecutionRequest>,
    ) -> Res<wsv1::DescribeWorkflowExecutionResponse> {
        let r = r.into_inner();
        self.check_ns(&r.namespace, ALLOW_DEFAULT)?;
        let (wid, rid) = exec_of(&r.execution);
        let mut g = self.engine.lock();
        let key = g.resolve(&r.namespace, &wid, &rid)?;
        Ok(Response::new(g.describe(&key)?))
    }

    async fn list_workflow_executions(
        &self,
        r: Request<wsv1::ListWorkflowExecutionsRequest>,
    ) -> Res<wsv1::ListWorkflowExecutionsResponse> {
        let r = r.into_inner();
        self.check_ns(&r.namespace, ALLOW_DEFAULT)?;
        let (executions, next_page_token) =
            self.engine
                .lock()
                .list(&r.namespace, &r.query, r.page_size, &r.next_page_token)?;
        Ok(Response::new(wsv1::ListWorkflowExecutionsResponse {
            executions,
            next_page_token,
        }))
    }

    async fn count_workflow_executions(
        &self,
        r: Request<wsv1::CountWorkflowExecutionsRequest>,
    ) -> Res<wsv1::CountWorkflowExecutionsResponse> {
        let r = r.into_inner();
        self.check_ns(&r.namespace, ALLOW_DEFAULT)?;
        let count = self.engine.lock().count(&r.namespace, &r.query)?;
        Ok(Response::new(wsv1::CountWorkflowExecutionsResponse {
            count,
            groups: vec![],
        }))
    }

    // ---------------- workflow tasks ----------------

    async fn poll_workflow_task_queue(
        &self,
        req: Request<wsv1::PollWorkflowTaskQueueRequest>,
    ) -> Res<wsv1::PollWorkflowTaskQueueResponse> {
        let timeout = poll_timeout_ms(grpc_timeout(&req), LONG_POLL_MS);
        let r = req.into_inner();
        self.check_ns(&r.namespace, ALLOW_DEFAULT)?;
        let name = r.task_queue.as_ref().map(|t| t.name.clone()).unwrap_or_default();
        if name.is_empty() {
            return Err(invalid("TaskQueue is not set on request."));
        }
        if self.engine.lock().is_bad_binary(&r.namespace, &r.binary_checksum) {
            return Err(invalid(format!(
                "Binary {} already marked as bad deployment.",
                r.binary_checksum
            )));
        }
        let got = self
            .engine
            .long_poll(
                timeout,
                |i| {
                    let q = i.queue(&r.namespace, &name, QKind::Workflow);
                    q.pollers.insert(r.identity.clone(), now_ms());
                    Ok(q.notify.clone())
                },
                |i| i.take_wft(&r.namespace, &name, &r.identity),
            )
            .await?;
        Ok(Response::new(got.unwrap_or_default()))
    }

    async fn respond_workflow_task_completed(
        &self,
        r: Request<wsv1::RespondWorkflowTaskCompletedRequest>,
    ) -> Res<wsv1::RespondWorkflowTaskCompletedResponse> {
        let r = r.into_inner();
        self.check_token_ns(&r.task_token)?;
        Ok(Response::new(self.engine.op(|i| i.respond_wft_completed(r))?))
    }

    async fn respond_workflow_task_failed(
        &self,
        r: Request<wsv1::RespondWorkflowTaskFailedRequest>,
    ) -> Res<wsv1::RespondWorkflowTaskFailedResponse> {
        let r = r.into_inner();
        self.check_token_ns(&r.task_token)?;
        self.engine.op(|i| i.respond_wft_failed(r))?;
        Ok(Response::new(wsv1::RespondWorkflowTaskFailedResponse::default()))
    }

    async fn reset_sticky_task_queue(
        &self,
        r: Request<wsv1::ResetStickyTaskQueueRequest>,
    ) -> Res<wsv1::ResetStickyTaskQueueResponse> {
        let r = r.into_inner();
        self.check_ns(&r.namespace, ALLOW_DEFAULT)?;
        let (wid, rid) = exec_of(&r.execution);
        self.engine.op(|i| {
            let key = i.resolve(&r.namespace, &wid, &rid)?;
            i.reset_sticky(&key)
        })?;
        Ok(Response::new(wsv1::ResetStickyTaskQueueResponse::default()))
    }

    // ---------------- queries ----------------

    async fn query_workflow(&self, req: Request<wsv1::QueryWorkflowRequest>) -> Res<wsv1::QueryWorkflowResponse> {
        let timeout = poll_timeout_ms(grpc_timeout(&req), 60_000);
        let r = req.into_inner();
        self.check_ns(&r.namespace, ALLOW_DEFAULT)?;
        let (wid, rid) = exec_of(&r.execution);
        let q = r.query.clone().ok_or_else(|| invalid("Query is not set on request."))?;
        let started = self.engine.op(|i| {
            let key = i.resolve(&r.namespace, &wid, &rid)?;
            i.ensure_loaded(&key)?;
            let status = i.execs[&key].s.status;
            let running = enums::WorkflowExecutionStatus::Running as i32;
            let reject = match enums::QueryRejectCondition::try_from(r.query_reject_condition)
                .unwrap_or(enums::QueryRejectCondition::None)
            {
                enums::QueryRejectCondition::NotOpen => status != running,
                enums::QueryRejectCondition::NotCompletedCleanly => {
                    status != running && status != enums::WorkflowExecutionStatus::Completed as i32
                }
                _ => false,
            };
            if reject {
                return Ok(Err(status));
            }
            i.new_query(&key, q).map(Ok)
        })?;
        let (qid, rx) = match started {
            Ok(x) => x,
            Err(status) => {
                return Ok(Response::new(wsv1::QueryWorkflowResponse {
                    query_rejected: Some(query_pb::QueryRejected { status }),
                    ..Default::default()
                }));
            }
        };
        match tokio::time::timeout(std_dur(timeout), rx).await {
            Ok(Ok(QueryOutcome::Answered(p))) => Ok(Response::new(wsv1::QueryWorkflowResponse {
                query_result: Some(p),
                ..Default::default()
            })),
            Ok(Ok(QueryOutcome::Failed(msg, f))) => Err(status_with_detail(
                tonic::Code::InvalidArgument,
                &msg,
                "temporal.api.errordetails.v1.QueryFailedFailure",
                &errordetails::QueryFailedFailure { failure: f },
            )),
            _ => {
                self.engine.lock().cancel_query(&qid);
                Err(Status::deadline_exceeded("query timed out: no worker answered"))
            }
        }
    }

    async fn respond_query_task_completed(
        &self,
        r: Request<wsv1::RespondQueryTaskCompletedRequest>,
    ) -> Res<wsv1::RespondQueryTaskCompletedResponse> {
        let r = r.into_inner();
        self.check_token_ns(&r.task_token)?;
        self.engine.op(|i| i.respond_query_task(r))?;
        Ok(Response::new(wsv1::RespondQueryTaskCompletedResponse::default()))
    }

    // ---------------- activity tasks ----------------

    async fn poll_activity_task_queue(
        &self,
        req: Request<wsv1::PollActivityTaskQueueRequest>,
    ) -> Res<wsv1::PollActivityTaskQueueResponse> {
        let timeout = poll_timeout_ms(grpc_timeout(&req), LONG_POLL_MS);
        let r = req.into_inner();
        self.check_ns(&r.namespace, ALLOW_DEFAULT)?;
        let name = r.task_queue.as_ref().map(|t| t.name.clone()).unwrap_or_default();
        if name.is_empty() {
            return Err(invalid("TaskQueue is not set on request."));
        }
        self.engine.lock().ns_id(&r.namespace)?;
        let got = self
            .engine
            .long_poll(
                timeout,
                |i| {
                    let q = i.queue(&r.namespace, &name, QKind::Activity);
                    q.pollers.insert(r.identity.clone(), now_ms());
                    Ok(q.notify.clone())
                },
                |i| i.take_activity(&r.namespace, &name, &r.identity),
            )
            .await?;
        Ok(Response::new(got.unwrap_or_default()))
    }

    async fn record_activity_task_heartbeat(
        &self,
        r: Request<wsv1::RecordActivityTaskHeartbeatRequest>,
    ) -> Res<wsv1::RecordActivityTaskHeartbeatResponse> {
        let r = r.into_inner();
        self.check_token_ns(&r.task_token)?;
        let tok = act_token(&r.task_token)?;
        let cancel_requested = self.engine.op(|i| i.heartbeat(&tok, r.details))?;
        Ok(Response::new(wsv1::RecordActivityTaskHeartbeatResponse {
            cancel_requested,
            ..Default::default()
        }))
    }

    async fn record_activity_task_heartbeat_by_id(
        &self,
        r: Request<wsv1::RecordActivityTaskHeartbeatByIdRequest>,
    ) -> Res<wsv1::RecordActivityTaskHeartbeatByIdResponse> {
        let r = r.into_inner();
        self.check_ns(&r.namespace, ALLOW_DEFAULT)?;
        let cancel_requested = self.engine.op(|i| {
            let tok = i.find_activity_by_id(&r.namespace, &r.workflow_id, &r.run_id, &r.activity_id)?;
            i.heartbeat(&tok, r.details)
        })?;
        Ok(Response::new(wsv1::RecordActivityTaskHeartbeatByIdResponse {
            cancel_requested,
            ..Default::default()
        }))
    }

    async fn respond_activity_task_completed(
        &self,
        r: Request<wsv1::RespondActivityTaskCompletedRequest>,
    ) -> Res<wsv1::RespondActivityTaskCompletedResponse> {
        let r = r.into_inner();
        self.check_token_ns(&r.task_token)?;
        let tok = act_token(&r.task_token)?;
        self.engine
            .op(|i| i.activity_ended(&tok, ActEnd::Completed(r.result), &r.identity))?;
        Ok(Response::new(wsv1::RespondActivityTaskCompletedResponse::default()))
    }

    async fn respond_activity_task_completed_by_id(
        &self,
        r: Request<wsv1::RespondActivityTaskCompletedByIdRequest>,
    ) -> Res<wsv1::RespondActivityTaskCompletedByIdResponse> {
        let r = r.into_inner();
        self.check_ns(&r.namespace, ALLOW_DEFAULT)?;
        self.engine.op(|i| {
            let tok = i.find_activity_by_id(&r.namespace, &r.workflow_id, &r.run_id, &r.activity_id)?;
            i.activity_ended(&tok, ActEnd::Completed(r.result), &r.identity)
        })?;
        Ok(Response::new(wsv1::RespondActivityTaskCompletedByIdResponse::default()))
    }

    async fn respond_activity_task_failed(
        &self,
        r: Request<wsv1::RespondActivityTaskFailedRequest>,
    ) -> Res<wsv1::RespondActivityTaskFailedResponse> {
        let r = r.into_inner();
        self.check_token_ns(&r.task_token)?;
        let tok = act_token(&r.task_token)?;
        let f = r.failure.unwrap_or_else(|| server_failure("activity failed", false));
        self.engine
            .op(|i| i.activity_ended(&tok, ActEnd::Failed(f, r.last_heartbeat_details), &r.identity))?;
        Ok(Response::new(wsv1::RespondActivityTaskFailedResponse {
            failures: vec![],
        }))
    }

    async fn respond_activity_task_failed_by_id(
        &self,
        r: Request<wsv1::RespondActivityTaskFailedByIdRequest>,
    ) -> Res<wsv1::RespondActivityTaskFailedByIdResponse> {
        let r = r.into_inner();
        self.check_ns(&r.namespace, ALLOW_DEFAULT)?;
        let f = r.failure.unwrap_or_else(|| server_failure("activity failed", false));
        self.engine.op(|i| {
            let tok = i.find_activity_by_id(&r.namespace, &r.workflow_id, &r.run_id, &r.activity_id)?;
            i.activity_ended(&tok, ActEnd::Failed(f, r.last_heartbeat_details), &r.identity)
        })?;
        Ok(Response::new(wsv1::RespondActivityTaskFailedByIdResponse {
            failures: vec![],
        }))
    }

    async fn respond_activity_task_canceled(
        &self,
        r: Request<wsv1::RespondActivityTaskCanceledRequest>,
    ) -> Res<wsv1::RespondActivityTaskCanceledResponse> {
        let r = r.into_inner();
        self.check_token_ns(&r.task_token)?;
        let tok = act_token(&r.task_token)?;
        self.engine
            .op(|i| i.activity_ended(&tok, ActEnd::Canceled(r.details), &r.identity))?;
        Ok(Response::new(wsv1::RespondActivityTaskCanceledResponse::default()))
    }

    async fn respond_activity_task_canceled_by_id(
        &self,
        r: Request<wsv1::RespondActivityTaskCanceledByIdRequest>,
    ) -> Res<wsv1::RespondActivityTaskCanceledByIdResponse> {
        let r = r.into_inner();
        self.check_ns(&r.namespace, ALLOW_DEFAULT)?;
        self.engine.op(|i| {
            let tok = i.find_activity_by_id(&r.namespace, &r.workflow_id, &r.run_id, &r.activity_id)?;
            i.activity_ended(&tok, ActEnd::Canceled(r.details), &r.identity)
        })?;
        Ok(Response::new(wsv1::RespondActivityTaskCanceledByIdResponse::default()))
    }

    // ---------------- workers ----------------

    async fn describe_task_queue(
        &self,
        r: Request<wsv1::DescribeTaskQueueRequest>,
    ) -> Res<wsv1::DescribeTaskQueueResponse> {
        let r = r.into_inner();
        self.check_ns(&r.namespace, ALLOW_DEFAULT)?;
        let name = r.task_queue.as_ref().map(|t| t.name.clone()).unwrap_or_default();
        let kind = if r.task_queue_type == enums::TaskQueueType::Activity as i32 {
            QKind::Activity
        } else {
            QKind::Workflow
        };
        let mut g = self.engine.lock();
        g.ns_id(&r.namespace)?;
        let cutoff = now_ms() - 5 * 60_000;
        let pollers = g
            .queue(&r.namespace, &name, kind)
            .pollers
            .iter()
            .filter(|(_, t)| **t >= cutoff)
            .map(|(id, t)| taskqueue::PollerInfo {
                identity: id.clone(),
                last_access_time: Some(ts_from_ms(*t)),
                ..Default::default()
            })
            .collect();
        Ok(Response::new(wsv1::DescribeTaskQueueResponse {
            pollers,
            ..Default::default()
        }))
    }

    async fn shutdown_worker(&self, r: Request<wsv1::ShutdownWorkerRequest>) -> Res<wsv1::ShutdownWorkerResponse> {
        self.check_ns(&r.get_ref().namespace, ALLOW_DEFAULT)?;
        Ok(Response::new(wsv1::ShutdownWorkerResponse::default()))
    }

    async fn record_worker_heartbeat(
        &self,
        r: Request<wsv1::RecordWorkerHeartbeatRequest>,
    ) -> Res<wsv1::RecordWorkerHeartbeatResponse> {
        self.check_ns(&r.get_ref().namespace, ALLOW_DEFAULT)?;
        Ok(Response::new(wsv1::RecordWorkerHeartbeatResponse::default()))
    }
}
