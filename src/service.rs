//! The gRPC WorkflowService the Temporal SDKs talk to. Unlisted methods answer "unimplemented".

use std::sync::Arc;
use std::time::Duration as StdDuration;

use tonic::{Request, Response, Status};

use crate::engine::activity::ActEnd;
use crate::engine::namespace::{ALLOW_DEFAULT, ALLOW_START};
use crate::engine::update::{Reached, UPDATE_LONG_POLL_MS, wait_stage};
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
        let (run_id, started) = self.engine.op(|i| i.start_workflow(r))?;
        Ok(Response::new(wsv1::StartWorkflowExecutionResponse {
            run_id,
            started,
            status: enums::WorkflowExecutionStatus::Running as i32,
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

    // ---------------- workflow updates ----------------

    async fn update_workflow_execution(
        &self,
        req: Request<wsv1::UpdateWorkflowExecutionRequest>,
    ) -> Res<wsv1::UpdateWorkflowExecutionResponse> {
        use enums::UpdateWorkflowExecutionLifecycleStage as S;
        let hard = hard_deadline(&req);
        let mut r = req.into_inner();
        self.check_ns(&r.namespace, ALLOW_DEFAULT)?;
        let exec = r
            .workflow_execution
            .clone()
            .ok_or_else(|| invalid("Execution is not set on request."))?;
        if exec.workflow_id.is_empty() {
            return Err(invalid("WorkflowId is not set on request."));
        }
        if !exec.run_id.is_empty() && uuid::Uuid::parse_str(&exec.run_id).is_err() {
            return Err(invalid("Invalid RunId."));
        }
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
        let upd = upd.clone();
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
                    let link = update_link(&r.namespace, &h.workflow_id, &h.run_id, &reached);
                    return Ok(Response::new(wsv1::UpdateWorkflowExecutionResponse {
                        update_ref: Some(update_pb::UpdateRef {
                            workflow_execution: Some(common::WorkflowExecution {
                                workflow_id: h.workflow_id,
                                run_id: h.run_id,
                            }),
                            update_id,
                        }),
                        outcome: reached.outcome,
                        stage: reached.stage as i32,
                        link,
                        ..Default::default()
                    }));
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
        let (wid, rid) = exec_of(&r.execution);
        let key = self.engine.lock().resolve(&r.namespace, &wid, &rid)?;
        let close_only = r.history_event_filter_type == enums::HistoryEventFilterType::CloseEvent as i32;
        let from: i64 = std::str::from_utf8(&r.next_page_token)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1);
        let resp = |events: Vec<history::HistoryEvent>, token: Vec<u8>| {
            Response::new(wsv1::GetWorkflowExecutionHistoryResponse {
                history: Some(history::History { events }),
                next_page_token: token,
                ..Default::default()
            })
        };
        if !r.wait_new_event {
            let page = self.engine.lock().history_from(&key, from, close_only)?;
            let mut events = page.events;
            events.extend(page.transient);
            return Ok(resp(events, vec![]));
        }
        let got = self
            .engine
            .long_poll(
                timeout,
                |i| i.notify_of_run(&key),
                |i| {
                    let page = i.history_from(&key, from, close_only)?;
                    let (mut events, closed, next) = (page.events, page.closed, page.next);
                    if close_only {
                        return Ok(if closed { Some((events, vec![])) } else { None });
                    }
                    if events.is_empty() {
                        return Ok(if closed { Some((events, vec![])) } else { None });
                    }
                    // Unwritten retry events go along with new written events (never alone, or a
                    // waiting reader would be woken over and over).
                    events.extend(page.transient);
                    let token = if closed { vec![] } else { next.to_string().into_bytes() };
                    Ok(Some((events, token)))
                },
            )
            .await?;
        Ok(match got {
            Some((events, token)) => resp(events, token),
            // Nothing new yet: an empty page that continues from the same place.
            None => resp(
                vec![],
                if r.next_page_token.is_empty() {
                    from.to_string().into_bytes()
                } else {
                    r.next_page_token
                },
            ),
        })
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
