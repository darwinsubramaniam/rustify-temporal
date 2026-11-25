//! Starting, signalling, cancelling and terminating runs; closing runs (with workflow retries
//! and continue-as-new); and the cross-run actions (children, external signals and cancels).

use super::events::{flush_buffered, push, push_external};
use super::*;
use crate::state::ParentInfo;
use history::history_event::Attributes as A;

#[derive(Default, Clone)]
pub struct StartParams {
    pub ns: String,
    pub workflow_id: String,
    pub workflow_type: String,
    pub task_queue: String,
    pub input: Option<common::Payloads>,
    pub exec_timeout_ms: i64,
    pub run_timeout_ms: i64,
    pub wft_timeout_ms: i64,
    pub identity: String,
    pub request_id: String,
    pub retry_policy: Option<common::RetryPolicy>,
    pub cron: String,
    pub memo: Option<common::Memo>,
    pub search_attributes: Option<common::SearchAttributes>,
    pub header: Option<common::Header>,
    pub start_delay_ms: i64,
    pub attempt: i32,
    pub first_run_id: String,
    pub continued_run_id: String,
    pub initiator: i32,
    pub continued_failure: Option<failure::Failure>,
    pub last_completion_result: Option<common::Payloads>,
    pub parent: Option<ParentInfo>,
    pub expiration_ms: i64,
    pub priority: Option<common::Priority>,
    pub user_metadata: Option<crate::proto::temporal::api::sdk::v1::UserMetadata>,
}

pub enum Retry {
    After(i64),
    No(enums::RetryState),
}

pub fn default_retry(p: Option<common::RetryPolicy>) -> common::RetryPolicy {
    let mut p = p.unwrap_or_default();
    if dur_ms(&p.initial_interval) <= 0 {
        p.initial_interval = Some(dur_from_ms(1000));
    }
    if p.backoff_coefficient < 1.0 {
        p.backoff_coefficient = 2.0;
    }
    if dur_ms(&p.maximum_interval) <= 0 {
        p.maximum_interval = Some(dur_from_ms(dur_ms(&p.initial_interval) * 100));
    }
    p
}

fn is_non_retryable(policy: &common::RetryPolicy, f: Option<&failure::Failure>) -> bool {
    use failure::failure::FailureInfo as FI;
    let Some(f) = f else { return false };
    match &f.failure_info {
        Some(FI::ApplicationFailureInfo(a)) => a.non_retryable || policy.non_retryable_error_types.contains(&a.r#type),
        Some(FI::TimeoutFailureInfo(t)) => {
            t.timeout_type == enums::TimeoutType::ScheduleToStart as i32
                || t.timeout_type == enums::TimeoutType::ScheduleToClose as i32
        }
        Some(FI::CanceledFailureInfo(_)) | Some(FI::TerminatedFailureInfo(_)) => true,
        Some(FI::ServerFailureInfo(s)) => s.non_retryable,
        _ => false,
    }
}

fn next_retry_delay(f: Option<&failure::Failure>) -> i64 {
    use failure::failure::FailureInfo as FI;
    match f.and_then(|f| f.failure_info.as_ref()) {
        Some(FI::ApplicationFailureInfo(a)) => dur_ms(&a.next_retry_delay),
        _ => 0,
    }
}

/// Should a failed attempt be retried, and after how long?
pub fn retry_decision(
    policy: Option<&common::RetryPolicy>,
    attempt: i32,
    f: Option<&failure::Failure>,
    expiration_ms: i64,
) -> Retry {
    let Some(p) = policy else {
        return Retry::No(enums::RetryState::RetryPolicyNotSet);
    };
    if is_non_retryable(p, f) {
        return Retry::No(enums::RetryState::NonRetryableFailure);
    }
    if p.maximum_attempts > 0 && attempt >= p.maximum_attempts {
        return Retry::No(enums::RetryState::MaximumAttemptsReached);
    }
    let mut delay = next_retry_delay(f);
    if delay <= 0 {
        let init = dur_ms(&p.initial_interval).max(1);
        let coef = if p.backoff_coefficient < 1.0 {
            2.0
        } else {
            p.backoff_coefficient
        };
        let d = (init as f64) * coef.powi((attempt - 1).clamp(0, 60));
        let max = dur_ms(&p.maximum_interval);
        delay = if max > 0 {
            d.min(max as f64) as i64
        } else {
            d.min(1e15) as i64
        };
    }
    if expiration_ms > 0 && now_ms() + delay >= expiration_ms {
        return Retry::No(enums::RetryState::Timeout);
    }
    Retry::After(delay)
}

fn started_attrs(ex: &Execution) -> history::WorkflowExecutionStartedEventAttributes {
    match ex.events.first().and_then(|e| e.attributes.clone()) {
        Some(A::WorkflowExecutionStartedEventAttributes(a)) => a,
        _ => Default::default(),
    }
}

impl Inner {
    /// Create a new run and its first events.
    pub fn create_run(&mut self, p: StartParams) -> R<Key> {
        let ns_id = self.ns_id(&p.ns)?;
        if !p.cron.is_empty() {
            return Err(invalid("Cron schedules are not supported by this server yet."));
        }
        let run_id = new_id();
        let now = now_ms();
        let exec_t = p.exec_timeout_ms.max(0);
        let mut run_t = p.run_timeout_ms.max(0);
        if run_t == 0 || (exec_t > 0 && run_t > exec_t) {
            run_t = exec_t;
        }
        let mut wft_t = p.wft_timeout_ms;
        if wft_t <= 0 {
            wft_t = DEFAULT_WFT_TIMEOUT_MS;
        }
        wft_t = wft_t.min(MAX_WFT_TIMEOUT_MS);
        if run_t > 0 {
            wft_t = wft_t.min(run_t);
        }
        let expiration = if p.expiration_ms > 0 {
            p.expiration_ms
        } else if exec_t > 0 {
            now + exec_t + p.start_delay_ms.max(0)
        } else {
            0
        };
        let first_run_id = if p.first_run_id.is_empty() {
            run_id.clone()
        } else {
            p.first_run_id.clone()
        };
        let retry = p.retry_policy.clone().map(|r| default_retry(Some(r)));
        let attempt = p.attempt.max(1);
        let s = ExecState {
            namespace: p.ns.clone(),
            namespace_id: ns_id,
            workflow_id: p.workflow_id.clone(),
            run_id: run_id.clone(),
            first_run_id: first_run_id.clone(),
            workflow_type: p.workflow_type.clone(),
            task_queue: p.task_queue.clone(),
            status: enums::WorkflowExecutionStatus::Running as i32,
            start_ms: now,
            execution_ms: now + p.start_delay_ms.max(0),
            start_request_id: p.request_id.clone(),
            run_timeout: opt_dur(run_t),
            execution_timeout: opt_dur(exec_t),
            wft_timeout: Some(dur_from_ms(wft_t)),
            expiration_ms: expiration,
            retry_policy: retry.clone(),
            attempt,
            memo: p.memo.clone(),
            search_attributes: p.search_attributes.clone(),
            parent: p.parent.clone(),
            next_event_id: 1,
            header: p.header.clone(),
            identity: p.identity.clone(),
            ..Default::default()
        };
        let mut ex = Execution::new(s, Vec::new());
        let parent = p.parent.clone();
        let attrs = history::WorkflowExecutionStartedEventAttributes {
            workflow_type: Some(common::WorkflowType {
                name: p.workflow_type.clone(),
            }),
            parent_workflow_namespace: parent.as_ref().map(|x| x.namespace.clone()).unwrap_or_default(),
            parent_workflow_namespace_id: parent.as_ref().map(|x| x.namespace_id.clone()).unwrap_or_default(),
            parent_workflow_execution: parent.as_ref().map(|x| common::WorkflowExecution {
                workflow_id: x.workflow_id.clone(),
                run_id: x.run_id.clone(),
            }),
            parent_initiated_event_id: parent.as_ref().map(|x| x.initiated_event_id).unwrap_or(0),
            task_queue: Some(taskqueue::TaskQueue {
                name: p.task_queue.clone(),
                kind: enums::TaskQueueKind::Normal as i32,
                normal_name: String::new(),
            }),
            input: p.input.clone(),
            workflow_execution_timeout: opt_dur(exec_t),
            workflow_run_timeout: opt_dur(run_t),
            workflow_task_timeout: Some(dur_from_ms(wft_t)),
            continued_execution_run_id: p.continued_run_id.clone(),
            initiator: p.initiator,
            continued_failure: p.continued_failure.clone(),
            last_completion_result: p.last_completion_result.clone(),
            original_execution_run_id: run_id.clone(),
            identity: p.identity.clone(),
            first_execution_run_id: first_run_id,
            retry_policy: retry,
            attempt,
            workflow_execution_expiration_time: if expiration > 0 {
                Some(ts_from_ms(expiration))
            } else {
                None
            },
            cron_schedule: p.cron.clone(),
            first_workflow_task_backoff: opt_dur(p.start_delay_ms),
            memo: p.memo.clone(),
            search_attributes: p.search_attributes.clone(),
            header: p.header.clone(),
            workflow_id: p.workflow_id.clone(),
            priority: p.priority.clone(),
            ..Default::default()
        };
        push(&mut ex, A::WorkflowExecutionStartedEventAttributes(attrs));
        if let Some(um) = p.user_metadata {
            ex.events[0].user_metadata = Some(um);
        }
        if p.start_delay_ms > 0 {
            ex.s.wft_due_ms = now + p.start_delay_ms;
            ex.s.wft_due_attempt = 1;
            self.add_timer(ex.s.wft_due_ms, TimerKind::WftDue { key: ex.key() });
        } else {
            self.schedule_wft(&mut ex, 1);
        }
        self.arm_run_timeout(&ex);
        self.current
            .insert((p.ns.clone(), p.workflow_id.clone()), run_id.clone());
        let key = ex.key();
        self.put(ex);
        Ok(key)
    }

    pub fn start_workflow(&mut self, r: wsv1::StartWorkflowExecutionRequest) -> R<(String, bool)> {
        use enums::WorkflowIdConflictPolicy as CP;
        use enums::WorkflowIdReusePolicy as RP;
        self.ns_id(&r.namespace)?;
        if r.workflow_id.is_empty() {
            return Err(invalid("WorkflowId is not set on request."));
        }
        let wtype = r.workflow_type.as_ref().map(|t| t.name.clone()).unwrap_or_default();
        if wtype.is_empty() {
            return Err(invalid("WorkflowType is not set on request."));
        }
        let tq = r.task_queue.as_ref().map(|t| t.name.clone()).unwrap_or_default();
        if tq.is_empty() {
            return Err(invalid("TaskQueue is not set on request."));
        }
        if let Some(rid) = self.current.get(&(r.namespace.clone(), r.workflow_id.clone())).cloned() {
            let key = (r.namespace.clone(), rid.clone());
            if self.ensure_loaded(&key)? {
                let (open, req_id, status) = {
                    let ex = &self.execs[&key];
                    (ex.s.is_open(), ex.s.start_request_id.clone(), ex.s.status)
                };
                if !r.request_id.is_empty() && req_id == r.request_id {
                    return Ok((rid, true));
                }
                if open {
                    let conflict = CP::try_from(r.workflow_id_conflict_policy).unwrap_or(CP::Unspecified);
                    let reuse = RP::try_from(r.workflow_id_reuse_policy).unwrap_or(RP::Unspecified);
                    match conflict {
                        CP::UseExisting => return Ok((rid, false)),
                        CP::TerminateExisting => {
                            self.terminate_key(&key, "TerminateExisting conflict policy", None, &r.identity)?
                        }
                        _ if reuse == RP::TerminateIfRunning => {
                            self.terminate_key(&key, "TerminateIfRunning reuse policy", None, &r.identity)?
                        }
                        _ => {
                            return Err(already_started(
                                &format!(
                                    "Workflow execution is already running. WorkflowId: {}, RunId: {}.",
                                    r.workflow_id, rid
                                ),
                                &req_id,
                                &rid,
                            ));
                        }
                    }
                } else {
                    match RP::try_from(r.workflow_id_reuse_policy).unwrap_or(RP::Unspecified) {
                        RP::RejectDuplicate => {
                            return Err(already_started(
                                "Workflow execution already finished and the reuse policy rejects duplicates.",
                                &req_id,
                                &rid,
                            ));
                        }
                        RP::AllowDuplicateFailedOnly if status == enums::WorkflowExecutionStatus::Completed as i32 => {
                            return Err(already_started(
                                "Workflow execution already completed successfully.",
                                &req_id,
                                &rid,
                            ));
                        }
                        _ => {}
                    }
                }
            }
        }
        let p = StartParams {
            ns: r.namespace,
            workflow_id: r.workflow_id,
            workflow_type: wtype,
            task_queue: tq,
            input: r.input,
            exec_timeout_ms: dur_ms(&r.workflow_execution_timeout),
            run_timeout_ms: dur_ms(&r.workflow_run_timeout),
            wft_timeout_ms: dur_ms(&r.workflow_task_timeout),
            identity: r.identity,
            request_id: r.request_id,
            retry_policy: r.retry_policy,
            cron: r.cron_schedule,
            memo: r.memo,
            search_attributes: r.search_attributes,
            header: r.header,
            start_delay_ms: dur_ms(&r.workflow_start_delay),
            attempt: 1,
            initiator: enums::ContinueAsNewInitiator::Unspecified as i32,
            continued_failure: r.continued_failure,
            last_completion_result: r.last_completion_result,
            priority: r.priority,
            user_metadata: r.user_metadata,
            ..Default::default()
        };
        let key = self.create_run(p)?;
        Ok((key.1, true))
    }

    // Mirrors the fields of a signal request one to one.
    #[allow(clippy::too_many_arguments)]
    pub fn signal_key(
        &mut self,
        key: &Key,
        name: &str,
        input: Option<common::Payloads>,
        header: Option<common::Header>,
        identity: &str,
        request_id: &str,
        external: Option<common::WorkflowExecution>,
    ) -> R<()> {
        let mut ex = self.take(key)?;
        if !ex.s.is_open() {
            self.put(ex);
            return Err(wf_completed_error());
        }
        if !request_id.is_empty() {
            if ex.s.signal_request_ids.iter().any(|r| r == request_id) {
                self.put(ex);
                return Ok(());
            }
            ex.s.signal_request_ids.push(request_id.to_string());
            if ex.s.signal_request_ids.len() > 100 {
                ex.s.signal_request_ids.remove(0);
            }
        }
        push_external(
            &mut ex,
            A::WorkflowExecutionSignaledEventAttributes(history::WorkflowExecutionSignaledEventAttributes {
                signal_name: name.to_string(),
                input,
                identity: identity.to_string(),
                header,
                external_workflow_execution: external,
                request_id: request_id.to_string(),
                ..Default::default()
            }),
        );
        self.ensure_wft(&mut ex);
        self.put(ex);
        Ok(())
    }

    pub fn signal_with_start(&mut self, r: wsv1::SignalWithStartWorkflowExecutionRequest) -> R<(String, bool)> {
        self.ns_id(&r.namespace)?;
        if let Some(rid) = self.current.get(&(r.namespace.clone(), r.workflow_id.clone())).cloned() {
            let key = (r.namespace.clone(), rid.clone());
            if self.ensure_loaded(&key)? && self.execs[&key].s.is_open() {
                self.signal_key(
                    &key,
                    &r.signal_name,
                    r.signal_input.clone(),
                    r.header.clone(),
                    &r.identity,
                    &r.request_id,
                    None,
                )?;
                return Ok((rid, false));
            }
        }
        let start = wsv1::StartWorkflowExecutionRequest {
            namespace: r.namespace.clone(),
            workflow_id: r.workflow_id.clone(),
            workflow_type: r.workflow_type,
            task_queue: r.task_queue,
            input: r.input,
            workflow_execution_timeout: r.workflow_execution_timeout,
            workflow_run_timeout: r.workflow_run_timeout,
            workflow_task_timeout: r.workflow_task_timeout,
            identity: r.identity.clone(),
            request_id: r.request_id.clone(),
            workflow_id_reuse_policy: r.workflow_id_reuse_policy,
            retry_policy: r.retry_policy,
            cron_schedule: r.cron_schedule,
            memo: r.memo,
            search_attributes: r.search_attributes,
            header: r.header.clone(),
            workflow_start_delay: r.workflow_start_delay,
            user_metadata: r.user_metadata,
            priority: r.priority,
            ..Default::default()
        };
        let (rid, _) = self.start_workflow(start)?;
        let key = (r.namespace.clone(), rid.clone());
        self.signal_key(&key, &r.signal_name, r.signal_input, r.header, &r.identity, "", None)?;
        Ok((rid, true))
    }

    pub fn terminate_key(
        &mut self,
        key: &Key,
        reason: &str,
        details: Option<common::Payloads>,
        identity: &str,
    ) -> R<()> {
        let mut ex = self.take(key)?;
        if !ex.s.is_open() {
            self.put(ex);
            return Err(wf_completed_error());
        }
        self.force_close_wft(&mut ex);
        push(
            &mut ex,
            A::WorkflowExecutionTerminatedEventAttributes(history::WorkflowExecutionTerminatedEventAttributes {
                reason: reason.to_string(),
                details,
                identity: identity.to_string(),
            }),
        );
        self.finish_close(&mut ex, enums::WorkflowExecutionStatus::Terminated, true);
        self.put(ex);
        Ok(())
    }

    pub fn request_cancel_key(
        &mut self,
        key: &Key,
        reason: &str,
        identity: &str,
        external: Option<(i64, common::WorkflowExecution)>,
    ) -> R<()> {
        let mut ex = self.take(key)?;
        if !ex.s.is_open() {
            self.put(ex);
            return Err(wf_completed_error());
        }
        if ex.s.cancel_requested {
            self.put(ex);
            return Ok(());
        }
        ex.s.cancel_requested = true;
        push_external(
            &mut ex,
            A::WorkflowExecutionCancelRequestedEventAttributes(
                history::WorkflowExecutionCancelRequestedEventAttributes {
                    cause: reason.to_string(),
                    external_initiated_event_id: external.as_ref().map(|e| e.0).unwrap_or(0),
                    external_workflow_execution: external.map(|e| e.1),
                    identity: identity.to_string(),
                },
            ),
        );
        self.ensure_wft(&mut ex);
        self.put(ex);
        Ok(())
    }

    /// Mark a run closed after its close event was written.
    pub fn finish_close(&mut self, ex: &mut Execution, status: enums::WorkflowExecutionStatus, notify_parent: bool) {
        ex.s.status = status as i32;
        ex.s.close_ms = now_ms();
        ex.s.wft = None;
        ex.s.wft_due_ms = 0;
        ex.s.timers.clear();
        ex.s.activities.clear();
        ex.s.sticky_queue.clear();
        let me = common::WorkflowExecution {
            workflow_id: ex.s.workflow_id.clone(),
            run_id: ex.s.run_id.clone(),
        };
        for c in std::mem::take(&mut ex.s.children).into_values() {
            if c.run_id.is_empty() {
                continue;
            }
            if c.parent_close_policy == enums::ParentClosePolicy::Terminate as i32
                || c.parent_close_policy == enums::ParentClosePolicy::RequestCancel as i32
                || c.parent_close_policy == enums::ParentClosePolicy::Unspecified as i32
            {
                self.actions.push_back(Action::ApplyParentClosePolicy {
                    ns: c.namespace,
                    workflow_id: c.workflow_id,
                    run_id: c.run_id,
                    policy: c.parent_close_policy,
                    parent: me.clone(),
                });
            }
        }
        if notify_parent
            && let Some(p) = ex.s.parent.clone()
            && let Some(close) = ex.events.last().and_then(|e| e.attributes.clone())
        {
            self.actions.push_back(Action::ChildClosed {
                parent: p,
                child_ns: ex.s.namespace.clone(),
                child: me,
                child_type: ex.s.workflow_type.clone(),
                close,
            });
        }
        ex.notify.notify_waiters();
        self.requeue_queries(ex);
        // In-flight updates end with the run; callers retry onto a new run if there is one.
        if !self.defer_update_abort {
            let reason = if ex.s.new_run_id.is_empty() {
                super::update::Abort::Closed
            } else {
                super::update::Abort::Continuing
            };
            super::update::abort_updates(ex, reason);
        }
    }

    /// Params for the next run in the chain (retry or continue-as-new), copied from this run.
    fn next_run_params(&self, ex: &Execution) -> StartParams {
        let a = started_attrs(ex);
        StartParams {
            ns: ex.s.namespace.clone(),
            workflow_id: ex.s.workflow_id.clone(),
            workflow_type: ex.s.workflow_type.clone(),
            task_queue: ex.s.task_queue.clone(),
            input: a.input,
            exec_timeout_ms: dur_ms(&ex.s.execution_timeout),
            run_timeout_ms: dur_ms(&ex.s.run_timeout),
            wft_timeout_ms: dur_ms(&ex.s.wft_timeout),
            identity: ex.s.identity.clone(),
            request_id: new_id(),
            retry_policy: ex.s.retry_policy.clone(),
            memo: ex.s.memo.clone(),
            search_attributes: ex.s.search_attributes.clone(),
            header: ex.s.header.clone(),
            attempt: 1,
            first_run_id: ex.s.first_run_id.clone(),
            continued_run_id: ex.s.run_id.clone(),
            parent: ex.s.parent.clone(),
            expiration_ms: ex.s.expiration_ms,
            priority: a.priority,
            ..Default::default()
        }
    }

    /// The workflow failed (or its run timed out): retry it as a new run if its retry policy allows.
    pub fn fail_workflow(
        &mut self,
        ex: &mut Execution,
        f: Option<failure::Failure>,
        timed_out: bool,
        wft_completed_id: i64,
    ) {
        let decision = retry_decision(ex.s.retry_policy.as_ref(), ex.s.attempt, f.as_ref(), ex.s.expiration_ms);
        let (new_run_id, retry_state) = match decision {
            Retry::After(delay) => {
                let mut p = self.next_run_params(ex);
                p.attempt = ex.s.attempt + 1;
                p.initiator = enums::ContinueAsNewInitiator::Retry as i32;
                p.continued_failure = f.clone();
                p.start_delay_ms = delay;
                match self.create_run(p) {
                    Ok(k) => (k.1, enums::RetryState::InProgress),
                    Err(e) => {
                        tracing::warn!("workflow retry failed to start: {e}");
                        (String::new(), enums::RetryState::InternalServerError)
                    }
                }
            }
            Retry::No(rs) => (String::new(), rs),
        };
        if timed_out {
            push(
                ex,
                A::WorkflowExecutionTimedOutEventAttributes(history::WorkflowExecutionTimedOutEventAttributes {
                    retry_state: retry_state as i32,
                    new_execution_run_id: new_run_id.clone(),
                }),
            );
        } else {
            push(
                ex,
                A::WorkflowExecutionFailedEventAttributes(history::WorkflowExecutionFailedEventAttributes {
                    failure: f,
                    retry_state: retry_state as i32,
                    workflow_task_completed_event_id: wft_completed_id,
                    new_execution_run_id: new_run_id.clone(),
                }),
            );
        }
        ex.s.new_run_id = new_run_id.clone();
        let status = if timed_out {
            enums::WorkflowExecutionStatus::TimedOut
        } else {
            enums::WorkflowExecutionStatus::Failed
        };
        self.finish_close(ex, status, new_run_id.is_empty());
    }

    pub fn continue_as_new(
        &mut self,
        ex: &mut Execution,
        c: command::ContinueAsNewWorkflowExecutionCommandAttributes,
        wft_completed_id: i64,
    ) -> R<()> {
        let mut p = self.next_run_params(ex);
        if let Some(t) = c.workflow_type.as_ref().filter(|t| !t.name.is_empty()) {
            p.workflow_type = t.name.clone();
        }
        if let Some(t) = c.task_queue.as_ref().filter(|t| !t.name.is_empty()) {
            p.task_queue = t.name.clone();
        }
        p.input = c.input.clone();
        if dur_ms(&c.workflow_run_timeout) > 0 {
            p.run_timeout_ms = dur_ms(&c.workflow_run_timeout);
        }
        if dur_ms(&c.workflow_task_timeout) > 0 {
            p.wft_timeout_ms = dur_ms(&c.workflow_task_timeout);
        }
        p.start_delay_ms = dur_ms(&c.backoff_start_interval);
        if c.retry_policy.is_some() {
            p.retry_policy = c.retry_policy.clone();
        }
        if c.header.is_some() {
            p.header = c.header.clone();
        }
        if c.memo.is_some() {
            p.memo = c.memo.clone();
        }
        if c.search_attributes.is_some() {
            p.search_attributes = c.search_attributes.clone();
        }
        p.initiator = if c.initiator == 0 {
            enums::ContinueAsNewInitiator::Workflow as i32
        } else {
            c.initiator
        };
        p.continued_failure = c.failure.clone();
        p.last_completion_result = c.last_completion_result.clone();
        let wtype = p.workflow_type.clone();
        let tq = p.task_queue.clone();
        let run_t = p.run_timeout_ms;
        let wft_t = p.wft_timeout_ms;
        let key = self.create_run(p)?;
        push(
            ex,
            A::WorkflowExecutionContinuedAsNewEventAttributes(
                history::WorkflowExecutionContinuedAsNewEventAttributes {
                    new_execution_run_id: key.1.clone(),
                    workflow_type: Some(common::WorkflowType { name: wtype }),
                    task_queue: Some(taskqueue::TaskQueue {
                        name: tq,
                        kind: enums::TaskQueueKind::Normal as i32,
                        normal_name: String::new(),
                    }),
                    input: c.input,
                    workflow_run_timeout: opt_dur(run_t),
                    workflow_task_timeout: opt_dur(wft_t),
                    workflow_task_completed_event_id: wft_completed_id,
                    backoff_start_interval: c.backoff_start_interval,
                    initiator: c.initiator,
                    failure: c.failure,
                    last_completion_result: c.last_completion_result,
                    header: c.header,
                    memo: c.memo,
                    search_attributes: c.search_attributes,
                    ..Default::default()
                },
            ),
        );
        ex.s.new_run_id = key.1;
        self.finish_close(ex, enums::WorkflowExecutionStatus::ContinuedAsNew, false);
        Ok(())
    }

    /// Run the cross-run actions queued by the last change.
    pub fn run_actions(&mut self) {
        let mut guard = 0;
        while let Some(a) = self.actions.pop_front() {
            guard += 1;
            if guard > 10_000 {
                tracing::error!("too many chained actions; stopping");
                break;
            }
            let r = match a {
                Action::StartChild {
                    parent,
                    initiated_id,
                    attrs,
                } => self.act_start_child(parent, initiated_id, attrs),
                Action::ChildClosed {
                    parent,
                    child_ns,
                    child,
                    child_type,
                    close,
                } => self.act_child_closed(parent, child_ns, child, child_type, close),
                Action::SignalExternal {
                    source,
                    initiated_id,
                    attrs,
                    request_id,
                } => self.act_signal_external(source, initiated_id, attrs, request_id),
                Action::CancelExternal {
                    source,
                    initiated_id,
                    attrs,
                } => self.act_cancel_external(source, initiated_id, attrs),
                Action::ApplyParentClosePolicy {
                    ns,
                    workflow_id,
                    run_id,
                    policy,
                    parent,
                } => {
                    let key = (ns, run_id);
                    let open = self.ensure_loaded(&key).unwrap_or(false)
                        && self.execs[&key].s.is_open()
                        && self.execs[&key].s.workflow_id == workflow_id;
                    if open {
                        if policy == enums::ParentClosePolicy::RequestCancel as i32 {
                            self.request_cancel_key(&key, "by parent close policy", "server", Some((0, parent)))
                        } else {
                            self.terminate_key(&key, "by parent close policy", None, "server")
                        }
                    } else {
                        Ok(())
                    }
                }
            };
            if let Err(e) = r {
                tracing::warn!("action failed: {e}");
            }
        }
    }

    /// Add an event to another run (if it's still open) and give it a workflow task.
    fn deliver(&mut self, key: &Key, a: A) -> R<i64> {
        let mut ex = self.take(key)?;
        let id = if ex.s.is_open() {
            let id = push_external(&mut ex, a);
            self.ensure_wft(&mut ex);
            id
        } else {
            0
        };
        self.put(ex);
        Ok(id)
    }

    fn act_start_child(
        &mut self,
        parent: Key,
        initiated_id: i64,
        a: history::StartChildWorkflowExecutionInitiatedEventAttributes,
    ) -> R<()> {
        let ns = if a.namespace.is_empty() {
            parent.0.clone()
        } else {
            a.namespace.clone()
        };
        let wtype = a.workflow_type.clone().unwrap_or_default();
        let fail = |this: &mut Inner, cause: enums::StartChildWorkflowExecutionFailedCause| -> R<()> {
            let ns_id = this.ns_id(&ns).unwrap_or_default();
            if let Some(ex) = this.execs.get_mut(&parent) {
                ex.s.children.remove(&initiated_id);
            }
            this.deliver(
                &parent,
                A::StartChildWorkflowExecutionFailedEventAttributes(
                    history::StartChildWorkflowExecutionFailedEventAttributes {
                        namespace: ns.clone(),
                        namespace_id: ns_id,
                        workflow_id: a.workflow_id.clone(),
                        workflow_type: Some(wtype.clone()),
                        cause: cause as i32,
                        control: a.control.clone(),
                        initiated_event_id: initiated_id,
                        workflow_task_completed_event_id: a.workflow_task_completed_event_id,
                    },
                ),
            )?;
            Ok(())
        };
        if self.ns_id(&ns).is_err() {
            return fail(self, enums::StartChildWorkflowExecutionFailedCause::NamespaceNotFound);
        }
        if let Some(rid) = self.current.get(&(ns.clone(), a.workflow_id.clone())).cloned() {
            let k = (ns.clone(), rid);
            if self.ensure_loaded(&k)? {
                let ex = &self.execs[&k];
                let reject = ex.s.is_open()
                    || a.workflow_id_reuse_policy == enums::WorkflowIdReusePolicy::RejectDuplicate as i32
                    || (a.workflow_id_reuse_policy == enums::WorkflowIdReusePolicy::AllowDuplicateFailedOnly as i32
                        && ex.s.status == enums::WorkflowExecutionStatus::Completed as i32);
                if reject {
                    return fail(
                        self,
                        enums::StartChildWorkflowExecutionFailedCause::WorkflowAlreadyExists,
                    );
                }
            }
        }
        let (pwid, pns_id) = match self.ensure_loaded(&parent)? {
            true => (
                self.execs[&parent].s.workflow_id.clone(),
                self.execs[&parent].s.namespace_id.clone(),
            ),
            false => return Ok(()),
        };
        let p = StartParams {
            ns: ns.clone(),
            workflow_id: a.workflow_id.clone(),
            workflow_type: wtype.name.clone(),
            task_queue: a.task_queue.as_ref().map(|t| t.name.clone()).unwrap_or_default(),
            input: a.input.clone(),
            exec_timeout_ms: dur_ms(&a.workflow_execution_timeout),
            run_timeout_ms: dur_ms(&a.workflow_run_timeout),
            wft_timeout_ms: dur_ms(&a.workflow_task_timeout),
            identity: "parent".into(),
            request_id: new_id(),
            retry_policy: a.retry_policy.clone(),
            cron: a.cron_schedule.clone(),
            memo: a.memo.clone(),
            search_attributes: a.search_attributes.clone(),
            header: a.header.clone(),
            attempt: 1,
            parent: Some(ParentInfo {
                namespace: parent.0.clone(),
                namespace_id: pns_id,
                workflow_id: pwid,
                run_id: parent.1.clone(),
                initiated_event_id: initiated_id,
            }),
            priority: a.priority.clone(),
            ..Default::default()
        };
        let child = match self.create_run(p) {
            Ok(k) => k,
            Err(e) => {
                tracing::warn!("child start failed: {e}");
                return fail(self, enums::StartChildWorkflowExecutionFailedCause::Unspecified);
            }
        };
        if let Some(ex) = self.execs.get_mut(&parent)
            && let Some(c) = ex.s.children.get_mut(&initiated_id)
        {
            c.run_id = child.1.clone();
        }
        let ns_id = self.ns_id(&ns)?;
        let id = self.deliver(
            &parent,
            A::ChildWorkflowExecutionStartedEventAttributes(history::ChildWorkflowExecutionStartedEventAttributes {
                namespace: ns,
                namespace_id: ns_id,
                initiated_event_id: initiated_id,
                workflow_execution: Some(common::WorkflowExecution {
                    workflow_id: a.workflow_id,
                    run_id: child.1,
                }),
                workflow_type: Some(wtype),
                header: a.header,
            }),
        )?;
        if id > 0
            && let Some(ex) = self.execs.get_mut(&parent)
            && let Some(c) = ex.s.children.get_mut(&initiated_id)
        {
            c.started_id = id;
        }
        Ok(())
    }

    fn act_child_closed(
        &mut self,
        p: ParentInfo,
        child_ns: String,
        child: common::WorkflowExecution,
        child_type: String,
        close: A,
    ) -> R<()> {
        let key = (p.namespace.clone(), p.run_id.clone());
        if !self.ensure_loaded(&key)? {
            return Ok(());
        }
        let started_id = {
            let ex = self.execs.get_mut(&key).expect("loaded");
            if !ex.s.is_open() {
                return Ok(());
            }
            match ex.s.children.remove(&p.initiated_event_id) {
                Some(c) => c.started_id,
                None => return Ok(()),
            }
        };
        let ns_id = self.ns_id(&child_ns).unwrap_or_default();
        let wt = Some(common::WorkflowType { name: child_type });
        let we = Some(child);
        let init = p.initiated_event_id;
        let ev = match close {
            A::WorkflowExecutionCompletedEventAttributes(c) => A::ChildWorkflowExecutionCompletedEventAttributes(
                history::ChildWorkflowExecutionCompletedEventAttributes {
                    result: c.result,
                    namespace: child_ns,
                    namespace_id: ns_id,
                    workflow_execution: we,
                    workflow_type: wt,
                    initiated_event_id: init,
                    started_event_id: started_id,
                },
            ),
            A::WorkflowExecutionFailedEventAttributes(c) => {
                A::ChildWorkflowExecutionFailedEventAttributes(history::ChildWorkflowExecutionFailedEventAttributes {
                    failure: c.failure,
                    namespace: child_ns,
                    namespace_id: ns_id,
                    workflow_execution: we,
                    workflow_type: wt,
                    initiated_event_id: init,
                    started_event_id: started_id,
                    retry_state: c.retry_state,
                })
            }
            A::WorkflowExecutionCanceledEventAttributes(c) => A::ChildWorkflowExecutionCanceledEventAttributes(
                history::ChildWorkflowExecutionCanceledEventAttributes {
                    details: c.details,
                    namespace: child_ns,
                    namespace_id: ns_id,
                    workflow_execution: we,
                    workflow_type: wt,
                    initiated_event_id: init,
                    started_event_id: started_id,
                },
            ),
            A::WorkflowExecutionTimedOutEventAttributes(c) => A::ChildWorkflowExecutionTimedOutEventAttributes(
                history::ChildWorkflowExecutionTimedOutEventAttributes {
                    namespace: child_ns,
                    namespace_id: ns_id,
                    workflow_execution: we,
                    workflow_type: wt,
                    initiated_event_id: init,
                    started_event_id: started_id,
                    retry_state: c.retry_state,
                },
            ),
            _ => A::ChildWorkflowExecutionTerminatedEventAttributes(
                history::ChildWorkflowExecutionTerminatedEventAttributes {
                    namespace: child_ns,
                    namespace_id: ns_id,
                    workflow_execution: we,
                    workflow_type: wt,
                    initiated_event_id: init,
                    started_event_id: started_id,
                },
            ),
        };
        self.deliver(&key, ev)?;
        Ok(())
    }

    fn act_signal_external(
        &mut self,
        source: Key,
        initiated_id: i64,
        a: history::SignalExternalWorkflowExecutionInitiatedEventAttributes,
        request_id: String,
    ) -> R<()> {
        let ns = if a.namespace.is_empty() {
            source.0.clone()
        } else {
            a.namespace.clone()
        };
        let target = a.workflow_execution.clone().unwrap_or_default();
        let src_exec = self.execs.get(&source).map(|e| common::WorkflowExecution {
            workflow_id: e.s.workflow_id.clone(),
            run_id: e.s.run_id.clone(),
        });
        let ok = match self.resolve(&ns, &target.workflow_id, &target.run_id) {
            Ok(k) => self
                .signal_key(
                    &k,
                    &a.signal_name,
                    a.input.clone(),
                    a.header.clone(),
                    "",
                    &request_id,
                    src_exec,
                )
                .is_ok(),
            Err(_) => false,
        };
        let ns_id = self.ns_id(&ns).unwrap_or_default();
        let ev = if ok {
            A::ExternalWorkflowExecutionSignaledEventAttributes(
                history::ExternalWorkflowExecutionSignaledEventAttributes {
                    initiated_event_id: initiated_id,
                    namespace: ns,
                    namespace_id: ns_id,
                    workflow_execution: Some(target),
                    control: a.control,
                },
            )
        } else {
            A::SignalExternalWorkflowExecutionFailedEventAttributes(
                history::SignalExternalWorkflowExecutionFailedEventAttributes {
                    cause: enums::SignalExternalWorkflowExecutionFailedCause::ExternalWorkflowExecutionNotFound as i32,
                    workflow_task_completed_event_id: a.workflow_task_completed_event_id,
                    namespace: ns,
                    namespace_id: ns_id,
                    workflow_execution: Some(target),
                    initiated_event_id: initiated_id,
                    control: a.control,
                },
            )
        };
        self.deliver(&source, ev)?;
        Ok(())
    }

    fn act_cancel_external(
        &mut self,
        source: Key,
        initiated_id: i64,
        a: history::RequestCancelExternalWorkflowExecutionInitiatedEventAttributes,
    ) -> R<()> {
        let ns = if a.namespace.is_empty() {
            source.0.clone()
        } else {
            a.namespace.clone()
        };
        let target = a.workflow_execution.clone().unwrap_or_default();
        let src_exec = self.execs.get(&source).map(|e| common::WorkflowExecution {
            workflow_id: e.s.workflow_id.clone(),
            run_id: e.s.run_id.clone(),
        });
        let ok = match self.resolve(&ns, &target.workflow_id, &target.run_id) {
            Ok(k) => self
                .request_cancel_key(&k, &a.reason, "", src_exec.map(|e| (initiated_id, e)))
                .is_ok(),
            Err(_) => false,
        };
        let ns_id = self.ns_id(&ns).unwrap_or_default();
        let ev = if ok {
            A::ExternalWorkflowExecutionCancelRequestedEventAttributes(
                history::ExternalWorkflowExecutionCancelRequestedEventAttributes {
                    initiated_event_id: initiated_id,
                    namespace: ns,
                    namespace_id: ns_id,
                    workflow_execution: Some(target),
                },
            )
        } else {
            A::RequestCancelExternalWorkflowExecutionFailedEventAttributes(
                history::RequestCancelExternalWorkflowExecutionFailedEventAttributes {
                    cause: enums::CancelExternalWorkflowExecutionFailedCause::ExternalWorkflowExecutionNotFound as i32,
                    workflow_task_completed_event_id: a.workflow_task_completed_event_id,
                    namespace: ns,
                    namespace_id: ns_id,
                    workflow_execution: Some(target),
                    initiated_event_id: initiated_id,
                    control: a.control,
                },
            )
        };
        self.deliver(&source, ev)?;
        Ok(())
    }

    /// The run's own timeout fired.
    pub fn run_timed_out(&mut self, key: &Key) -> R<()> {
        let mut ex = self.take(key)?;
        if ex.s.is_open() {
            self.force_close_wft(&mut ex);
            flush_buffered(&mut ex);
            self.fail_workflow(&mut ex, None, true, 0);
        }
        self.put(ex);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(max: i32) -> common::RetryPolicy {
        default_retry(Some(common::RetryPolicy {
            maximum_attempts: max,
            ..Default::default()
        }))
    }

    #[test]
    fn retries_with_exponential_backoff() {
        let p = policy(0);
        assert!(matches!(retry_decision(Some(&p), 1, None, 0), Retry::After(1000)));
        assert!(matches!(retry_decision(Some(&p), 3, None, 0), Retry::After(4000)));
        // capped at 100x the initial interval
        assert!(matches!(retry_decision(Some(&p), 30, None, 0), Retry::After(100_000)));
    }

    #[test]
    fn stops_retrying() {
        assert!(matches!(
            retry_decision(None, 1, None, 0),
            Retry::No(enums::RetryState::RetryPolicyNotSet)
        ));
        assert!(matches!(
            retry_decision(Some(&policy(2)), 2, None, 0),
            Retry::No(enums::RetryState::MaximumAttemptsReached)
        ));
        let f = failure::Failure {
            failure_info: Some(failure::failure::FailureInfo::ApplicationFailureInfo(
                failure::ApplicationFailureInfo {
                    non_retryable: true,
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        assert!(matches!(
            retry_decision(Some(&policy(0)), 1, Some(&f), 0),
            Retry::No(enums::RetryState::NonRetryableFailure)
        ));
        assert!(matches!(
            retry_decision(Some(&policy(0)), 1, None, now_ms() + 10),
            Retry::No(enums::RetryState::Timeout)
        ));
    }
}
