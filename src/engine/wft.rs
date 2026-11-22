//! Workflow tasks: handing them to workers, and applying the commands a worker sends back.

use super::events::{flush_buffered, push};
use super::*;
use crate::state::{ACT_SCHEDULED, ActivityInfo, ChildInfo, TimerInfo};
use command::command::Attributes as C;
use history::history_event::Attributes as A;

impl Inner {
    /// Try to start one task from a workflow task queue.
    pub fn take_wft(
        &mut self,
        ns: &str,
        queue: &str,
        identity: &str,
    ) -> R<Option<wsv1::PollWorkflowTaskQueueResponse>> {
        loop {
            let t = match self.queue(ns, queue, QKind::Workflow).tasks.pop_front() {
                Some(t) => t,
                None => return Ok(None),
            };
            let r = match t {
                TaskRef::Wft { run_id, scheduled_id } => {
                    self.start_wft(&(ns.to_string(), run_id), scheduled_id, queue, identity)?
                }
                TaskRef::Query { query_id } => self.start_query_task(&query_id, queue)?,
                TaskRef::Activity { .. } => None,
            };
            if r.is_some() {
                return Ok(r);
            }
        }
    }

    /// Mark the workflow task started and build what the worker receives.
    pub fn start_wft(
        &mut self,
        key: &Key,
        scheduled_id: i64,
        queue: &str,
        identity: &str,
    ) -> R<Option<wsv1::PollWorkflowTaskQueueResponse>> {
        let Some(ex) = self.execs.get(key) else { return Ok(None) };
        let valid = ex.s.is_open()
            && ex
                .s
                .wft
                .as_ref()
                .is_some_and(|w| w.scheduled_id == scheduled_id && w.started_id == 0 && w.queue == queue);
        if !valid {
            return Ok(None);
        }
        let mut ex = self.execs.remove(key).expect("present");
        let now = now_ms();
        let request_id = new_id();
        let hist_size = ex.s.history_size;
        let started_id = push(
            &mut ex,
            A::WorkflowTaskStartedEventAttributes(history::WorkflowTaskStartedEventAttributes {
                scheduled_event_id: scheduled_id,
                identity: identity.to_string(),
                request_id: request_id.clone(),
                history_size_bytes: hist_size,
                ..Default::default()
            }),
        );
        let w = ex.s.wft.as_mut().expect("wft");
        w.started_id = started_id;
        w.started_ms = now;
        w.request_id = request_id;
        let (attempt, timeout, sched_ms) = (w.attempt, w.timeout_ms, w.scheduled_ms);
        self.add_timer(
            now + timeout,
            TimerKind::WftTimeout {
                key: key.clone(),
                scheduled_id,
                started_id,
            },
        );
        let sticky = !ex.s.sticky_queue.is_empty() && ex.s.sticky_queue == queue;
        let first = if sticky { ex.s.last_completed_started_id + 1 } else { 1 };
        let events: Vec<_> = ex.events.iter().filter(|e| e.event_id >= first).cloned().collect();
        let queries = self.attach_queries(key, started_id);
        let token = TaskToken {
            k: "wft".into(),
            ns: key.0.clone(),
            wid: ex.s.workflow_id.clone(),
            rid: key.1.clone(),
            sid: scheduled_id,
            att: attempt,
            st: started_id,
            ..Default::default()
        };
        let resp = wsv1::PollWorkflowTaskQueueResponse {
            task_token: token.encode(),
            workflow_execution: Some(common::WorkflowExecution {
                workflow_id: ex.s.workflow_id.clone(),
                run_id: key.1.clone(),
            }),
            workflow_type: Some(common::WorkflowType {
                name: ex.s.workflow_type.clone(),
            }),
            previous_started_event_id: ex.s.last_completed_started_id,
            started_event_id: started_id,
            attempt,
            backlog_count_hint: 0,
            history: Some(history::History { events }),
            workflow_execution_task_queue: Some(taskqueue::TaskQueue {
                name: ex.s.task_queue.clone(),
                kind: enums::TaskQueueKind::Normal as i32,
                normal_name: String::new(),
            }),
            scheduled_time: Some(ts_from_ms(sched_ms)),
            started_time: Some(ts_from_ms(now)),
            queries,
            ..Default::default()
        };
        self.put(ex);
        Ok(Some(resp))
    }

    fn check_wft_token(&mut self, tok: &TaskToken) -> R<Execution> {
        let key = (tok.ns.clone(), tok.rid.clone());
        if !self.ensure_loaded(&key)? {
            return Err(task_not_found());
        }
        let ok = {
            let ex = &self.execs[&key];
            ex.s.is_open()
                && ex
                    .s
                    .wft
                    .as_ref()
                    .is_some_and(|w| w.scheduled_id == tok.sid && w.started_id == tok.st && w.started_id > 0)
        };
        if !ok {
            return Err(task_not_found());
        }
        self.take(&key)
    }

    pub fn respond_wft_completed(
        &mut self,
        r: wsv1::RespondWorkflowTaskCompletedRequest,
    ) -> R<wsv1::RespondWorkflowTaskCompletedResponse> {
        let tok = TaskToken::decode(&r.task_token)?;
        if tok.k != "wft" {
            return Err(invalid("Not a workflow task token."));
        }
        let mut ex = self.check_wft_token(&tok)?;
        let key = ex.key();
        let w = ex.s.wft.clone().expect("checked");

        self.answer_queries(&key, &r.query_results);

        // A build marked bad in the namespace can't complete workflow tasks (like the Go server).
        if self.is_bad_binary(&key.0, &r.binary_checksum) {
            let msg = format!("binary {} is marked as bad deployment", r.binary_checksum);
            self.fail_wft_inner(
                &mut ex,
                enums::WorkflowTaskFailedCause::BadBinary,
                Some(server_failure(&msg, true)),
                &r.identity,
                true,
            );
            self.put(ex);
            return Err(invalid(msg));
        }

        // Validate commands first so a bad one fails the task without partial writes.
        if let Err((cause, msg)) = validate_commands(&ex, &r.commands) {
            self.fail_wft_inner(&mut ex, cause, Some(server_failure(&msg, true)), &r.identity, true);
            self.put(ex);
            return Err(invalid(msg));
        }
        // A workflow can't close while new events it hasn't seen are waiting.
        let closes = r.commands.iter().any(|c| {
            matches!(
                c.attributes,
                Some(C::CompleteWorkflowExecutionCommandAttributes(_))
                    | Some(C::FailWorkflowExecutionCommandAttributes(_))
                    | Some(C::CancelWorkflowExecutionCommandAttributes(_))
                    | Some(C::ContinueAsNewWorkflowExecutionCommandAttributes(_))
            )
        });
        if closes && !ex.s.buffered.is_empty() {
            self.fail_wft_inner(
                &mut ex,
                enums::WorkflowTaskFailedCause::UnhandledCommand,
                None,
                &r.identity,
                false,
            );
            self.put(ex);
            return Ok(Default::default());
        }

        let completed_id = push(
            &mut ex,
            A::WorkflowTaskCompletedEventAttributes(history::WorkflowTaskCompletedEventAttributes {
                scheduled_event_id: w.scheduled_id,
                started_event_id: w.started_id,
                identity: r.identity.clone(),
                binary_checksum: r.binary_checksum.clone(),
                worker_version: r.worker_version_stamp.clone(),
                sdk_metadata: r.sdk_metadata.clone(),
                metering_metadata: r.metering_metadata,
                deployment: r.deployment.clone(),
                versioning_behavior: r.versioning_behavior,
                ..Default::default()
            }),
        );
        ex.s.last_completed_started_id = w.started_id;
        ex.s.wft = None;
        match r.sticky_attributes.as_ref().and_then(|s| {
            s.worker_task_queue
                .as_ref()
                .map(|q| (q.name.clone(), dur_ms(&s.schedule_to_start_timeout)))
        }) {
            Some((name, t)) if !name.is_empty() => {
                ex.s.sticky_queue = name;
                ex.s.sticky_timeout_ms = t;
            }
            _ => {
                ex.s.sticky_queue.clear();
            }
        }

        for c in r.commands {
            if !ex.s.is_open() {
                break;
            }
            let Some(a) = c.attributes else { continue };
            let user_metadata = c.user_metadata;
            self.apply_command(&mut ex, a, completed_id, &r.identity)?;
            if user_metadata.is_some()
                && let Some(last) = ex.events.last_mut()
                && last.event_id > completed_id
                && last.user_metadata.is_none()
            {
                last.user_metadata = user_metadata;
            }
        }

        let mut new_task = None;
        if ex.s.is_open() {
            let had_buffered = flush_buffered(&mut ex);
            if had_buffered || r.force_create_new_workflow_task {
                self.schedule_wft(&mut ex, 1);
                if r.return_new_workflow_task {
                    let w = ex.s.wft.clone().expect("scheduled");
                    let k = ex.key();
                    self.put(ex);
                    new_task = self.start_wft(&k, w.scheduled_id, &w.queue, &r.identity)?;
                    ex = self.take(&k)?;
                }
            }
        }
        // Queries that arrived while this task ran and have no task to ride on go out on their own.
        if ex.s.wft.is_none() {
            self.requeue_queries(&ex);
        }
        self.put(ex);
        Ok(wsv1::RespondWorkflowTaskCompletedResponse {
            workflow_task: new_task,
            activity_tasks: vec![],
            reset_history_event_id: 0,
        })
    }

    fn apply_command(&mut self, ex: &mut Execution, a: C, completed_id: i64, identity: &str) -> R<()> {
        let key = ex.key();
        match a {
            C::ScheduleActivityTaskCommandAttributes(c) => {
                let run_t = dur_ms(&ex.s.run_timeout);
                let mut s2c = dur_ms(&c.schedule_to_close_timeout);
                let mut st2c = dur_ms(&c.start_to_close_timeout);
                let mut s2s = dur_ms(&c.schedule_to_start_timeout);
                if s2c <= 0 && run_t > 0 {
                    s2c = run_t;
                }
                if st2c <= 0 {
                    st2c = s2c;
                }
                if s2c > 0 && st2c > s2c {
                    st2c = s2c;
                }
                if s2c > 0 && (s2s <= 0 || s2s > s2c) {
                    s2s = s2c;
                }
                let mut tq = c.task_queue.clone().unwrap_or_default();
                if tq.name.is_empty() {
                    tq.name = ex.s.task_queue.clone();
                }
                tq.kind = enums::TaskQueueKind::Normal as i32;
                let attrs = history::ActivityTaskScheduledEventAttributes {
                    activity_id: c.activity_id,
                    activity_type: c.activity_type,
                    task_queue: Some(tq.clone()),
                    header: c.header,
                    input: c.input,
                    schedule_to_close_timeout: opt_dur(s2c),
                    schedule_to_start_timeout: opt_dur(s2s),
                    start_to_close_timeout: opt_dur(st2c),
                    heartbeat_timeout: c.heartbeat_timeout,
                    workflow_task_completed_event_id: completed_id,
                    retry_policy: Some(super::workflow::default_retry(c.retry_policy)),
                    use_workflow_build_id: c.use_workflow_build_id,
                    priority: c.priority,
                };
                let sid = push(ex, A::ActivityTaskScheduledEventAttributes(attrs.clone()));
                let now = now_ms();
                let mut expiration = if s2c > 0 { now + s2c } else { 0 };
                if ex.s.expiration_ms > 0 && (expiration == 0 || ex.s.expiration_ms < expiration) {
                    expiration = ex.s.expiration_ms;
                }
                ex.s.activities.insert(
                    sid,
                    ActivityInfo {
                        scheduled_id: sid,
                        attrs: Some(attrs),
                        scheduled_ms: now,
                        attempt: 1,
                        state: ACT_SCHEDULED,
                        attempt_scheduled_ms: now,
                        expiration_ms: expiration,
                        ..Default::default()
                    },
                );
                self.enqueue(
                    &key.0,
                    &tq.name,
                    QKind::Activity,
                    TaskRef::Activity {
                        run_id: key.1.clone(),
                        scheduled_id: sid,
                        attempt: 1,
                    },
                );
                if s2s > 0 && s2s < s2c {
                    self.add_timer(
                        now + s2s,
                        TimerKind::ActScheduleToStart {
                            key: key.clone(),
                            scheduled_id: sid,
                            attempt: 1,
                        },
                    );
                }
                if expiration > 0 {
                    self.add_timer(
                        expiration,
                        TimerKind::ActScheduleToClose {
                            key: key.clone(),
                            scheduled_id: sid,
                        },
                    );
                }
            }
            C::RequestCancelActivityTaskCommandAttributes(c) => {
                let sid = c.scheduled_event_id;
                if let Some(act) = ex.s.activities.get(&sid).cloned() {
                    let cr = push(
                        ex,
                        A::ActivityTaskCancelRequestedEventAttributes(
                            history::ActivityTaskCancelRequestedEventAttributes {
                                scheduled_event_id: sid,
                                workflow_task_completed_event_id: completed_id,
                            },
                        ),
                    );
                    if act.state == crate::state::ACT_STARTED {
                        let a = ex.s.activities.get_mut(&sid).expect("present");
                        a.cancel_requested = true;
                        a.cancel_requested_event_id = cr;
                    } else {
                        ex.s.activities.remove(&sid);
                        push(
                            ex,
                            A::ActivityTaskCanceledEventAttributes(history::ActivityTaskCanceledEventAttributes {
                                details: None,
                                latest_cancel_requested_event_id: cr,
                                scheduled_event_id: sid,
                                started_event_id: 0,
                                identity: identity.to_string(),
                                worker_version: None,
                            }),
                        );
                    }
                }
            }
            C::StartTimerCommandAttributes(c) => {
                let ms = dur_ms(&c.start_to_fire_timeout).max(1);
                let sid = push(
                    ex,
                    A::TimerStartedEventAttributes(history::TimerStartedEventAttributes {
                        timer_id: c.timer_id.clone(),
                        start_to_fire_timeout: c.start_to_fire_timeout,
                        workflow_task_completed_event_id: completed_id,
                    }),
                );
                let fire = now_ms() + ms;
                ex.s.timers.insert(
                    c.timer_id.clone(),
                    TimerInfo {
                        timer_id: c.timer_id.clone(),
                        started_id: sid,
                        fire_ms: fire,
                    },
                );
                self.add_timer(
                    fire,
                    TimerKind::User {
                        key,
                        timer_id: c.timer_id,
                        started_id: sid,
                    },
                );
            }
            C::CancelTimerCommandAttributes(c) => {
                if let Some(t) = ex.s.timers.remove(&c.timer_id) {
                    push(
                        ex,
                        A::TimerCanceledEventAttributes(history::TimerCanceledEventAttributes {
                            timer_id: c.timer_id,
                            started_event_id: t.started_id,
                            workflow_task_completed_event_id: completed_id,
                            identity: identity.to_string(),
                        }),
                    );
                }
            }
            C::CompleteWorkflowExecutionCommandAttributes(c) => {
                push(
                    ex,
                    A::WorkflowExecutionCompletedEventAttributes(history::WorkflowExecutionCompletedEventAttributes {
                        result: c.result,
                        workflow_task_completed_event_id: completed_id,
                        new_execution_run_id: String::new(),
                    }),
                );
                self.finish_close(ex, enums::WorkflowExecutionStatus::Completed, true);
            }
            C::FailWorkflowExecutionCommandAttributes(c) => {
                self.fail_workflow(ex, c.failure, false, completed_id);
            }
            C::CancelWorkflowExecutionCommandAttributes(c) => {
                push(
                    ex,
                    A::WorkflowExecutionCanceledEventAttributes(history::WorkflowExecutionCanceledEventAttributes {
                        workflow_task_completed_event_id: completed_id,
                        details: c.details,
                    }),
                );
                self.finish_close(ex, enums::WorkflowExecutionStatus::Canceled, true);
            }
            C::ContinueAsNewWorkflowExecutionCommandAttributes(c) => {
                self.continue_as_new(ex, c, completed_id)?;
            }
            C::RecordMarkerCommandAttributes(c) => {
                push(
                    ex,
                    A::MarkerRecordedEventAttributes(history::MarkerRecordedEventAttributes {
                        marker_name: c.marker_name,
                        details: c.details,
                        workflow_task_completed_event_id: completed_id,
                        header: c.header,
                        failure: c.failure,
                    }),
                );
            }
            C::UpsertWorkflowSearchAttributesCommandAttributes(c) => {
                if let Some(sa) = &c.search_attributes {
                    let cur = ex.s.search_attributes.get_or_insert_with(Default::default);
                    for (k, v) in &sa.indexed_fields {
                        if v.data.is_empty() || v.data == b"null" {
                            cur.indexed_fields.remove(k);
                        } else {
                            cur.indexed_fields.insert(k.clone(), v.clone());
                        }
                    }
                }
                push(
                    ex,
                    A::UpsertWorkflowSearchAttributesEventAttributes(
                        history::UpsertWorkflowSearchAttributesEventAttributes {
                            workflow_task_completed_event_id: completed_id,
                            search_attributes: c.search_attributes,
                        },
                    ),
                );
            }
            C::ModifyWorkflowPropertiesCommandAttributes(c) => {
                if let Some(m) = &c.upserted_memo {
                    let cur = ex.s.memo.get_or_insert_with(Default::default);
                    for (k, v) in &m.fields {
                        if v.data.is_empty() || v.data == b"null" {
                            cur.fields.remove(k);
                        } else {
                            cur.fields.insert(k.clone(), v.clone());
                        }
                    }
                }
                push(
                    ex,
                    A::WorkflowPropertiesModifiedEventAttributes(history::WorkflowPropertiesModifiedEventAttributes {
                        workflow_task_completed_event_id: completed_id,
                        upserted_memo: c.upserted_memo,
                    }),
                );
            }
            C::StartChildWorkflowExecutionCommandAttributes(c) => {
                let ns = if c.namespace.is_empty() {
                    key.0.clone()
                } else {
                    c.namespace.clone()
                };
                let ns_id = self.ns_id(&ns).unwrap_or_default();
                let mut tq = c.task_queue.clone().unwrap_or_default();
                if tq.name.is_empty() {
                    tq.name = ex.s.task_queue.clone();
                }
                tq.kind = enums::TaskQueueKind::Normal as i32;
                let attrs = history::StartChildWorkflowExecutionInitiatedEventAttributes {
                    namespace: ns.clone(),
                    namespace_id: ns_id,
                    workflow_id: c.workflow_id.clone(),
                    workflow_type: c.workflow_type.clone(),
                    task_queue: Some(tq),
                    input: c.input,
                    workflow_execution_timeout: c.workflow_execution_timeout,
                    workflow_run_timeout: c.workflow_run_timeout,
                    workflow_task_timeout: c.workflow_task_timeout,
                    parent_close_policy: c.parent_close_policy,
                    control: c.control,
                    workflow_task_completed_event_id: completed_id,
                    workflow_id_reuse_policy: c.workflow_id_reuse_policy,
                    retry_policy: c.retry_policy,
                    cron_schedule: c.cron_schedule,
                    header: c.header,
                    memo: c.memo,
                    search_attributes: c.search_attributes,
                    inherit_build_id: c.inherit_build_id,
                    priority: c.priority,
                    ..Default::default()
                };
                let iid = push(
                    ex,
                    A::StartChildWorkflowExecutionInitiatedEventAttributes(attrs.clone()),
                );
                ex.s.children.insert(
                    iid,
                    ChildInfo {
                        initiated_id: iid,
                        started_id: 0,
                        namespace: ns,
                        workflow_id: c.workflow_id,
                        run_id: String::new(),
                        workflow_type: c.workflow_type.map(|t| t.name).unwrap_or_default(),
                        parent_close_policy: c.parent_close_policy,
                    },
                );
                self.actions.push_back(Action::StartChild {
                    parent: key,
                    initiated_id: iid,
                    attrs,
                });
            }
            C::SignalExternalWorkflowExecutionCommandAttributes(c) => {
                let ns = if c.namespace.is_empty() {
                    key.0.clone()
                } else {
                    c.namespace.clone()
                };
                let ns_id = self.ns_id(&ns).unwrap_or_default();
                let attrs = history::SignalExternalWorkflowExecutionInitiatedEventAttributes {
                    workflow_task_completed_event_id: completed_id,
                    namespace: ns,
                    namespace_id: ns_id,
                    workflow_execution: c.execution,
                    signal_name: c.signal_name,
                    input: c.input,
                    control: c.control,
                    child_workflow_only: c.child_workflow_only,
                    header: c.header,
                };
                let iid = push(
                    ex,
                    A::SignalExternalWorkflowExecutionInitiatedEventAttributes(attrs.clone()),
                );
                let request_id = format!("{}:{}", key.1, iid);
                self.actions.push_back(Action::SignalExternal {
                    source: key,
                    initiated_id: iid,
                    attrs,
                    request_id,
                });
            }
            C::RequestCancelExternalWorkflowExecutionCommandAttributes(c) => {
                let ns = if c.namespace.is_empty() {
                    key.0.clone()
                } else {
                    c.namespace.clone()
                };
                let ns_id = self.ns_id(&ns).unwrap_or_default();
                let attrs = history::RequestCancelExternalWorkflowExecutionInitiatedEventAttributes {
                    workflow_task_completed_event_id: completed_id,
                    namespace: ns,
                    namespace_id: ns_id,
                    workflow_execution: Some(common::WorkflowExecution {
                        workflow_id: c.workflow_id,
                        run_id: c.run_id,
                    }),
                    control: c.control,
                    child_workflow_only: c.child_workflow_only,
                    reason: c.reason,
                };
                let iid = push(
                    ex,
                    A::RequestCancelExternalWorkflowExecutionInitiatedEventAttributes(attrs.clone()),
                );
                self.actions.push_back(Action::CancelExternal {
                    source: key,
                    initiated_id: iid,
                    attrs,
                });
            }
            _ => {}
        }
        Ok(())
    }

    pub fn respond_wft_failed(&mut self, r: wsv1::RespondWorkflowTaskFailedRequest) -> R<()> {
        let tok = TaskToken::decode(&r.task_token)?;
        let mut ex = self.check_wft_token(&tok)?;
        let cause =
            enums::WorkflowTaskFailedCause::try_from(r.cause).unwrap_or(enums::WorkflowTaskFailedCause::Unspecified);
        self.fail_wft_inner(&mut ex, cause, r.failure, &r.identity, true);
        self.put(ex);
        Ok(())
    }

    /// Record a failed workflow task and schedule the next attempt.
    fn fail_wft_inner(
        &mut self,
        ex: &mut Execution,
        cause: enums::WorkflowTaskFailedCause,
        f: Option<failure::Failure>,
        identity: &str,
        count_attempt: bool,
    ) {
        let w = ex.s.wft.clone().expect("running task");
        push(
            ex,
            A::WorkflowTaskFailedEventAttributes(history::WorkflowTaskFailedEventAttributes {
                scheduled_event_id: w.scheduled_id,
                started_event_id: w.started_id,
                cause: cause as i32,
                failure: f,
                identity: identity.to_string(),
                ..Default::default()
            }),
        );
        let next_attempt = if count_attempt && cause != enums::WorkflowTaskFailedCause::ResetStickyTaskQueue {
            w.attempt + 1
        } else {
            1
        };
        self.retry_wft(ex, next_attempt);
    }

    /// After a failed or timed-out workflow task: drop stickiness, write held-back events and
    /// try again (with a growing pause after repeated failures).
    pub fn retry_wft(&mut self, ex: &mut Execution, attempt: i32) {
        ex.s.wft = None;
        ex.s.sticky_queue.clear();
        flush_buffered(ex);
        let key = ex.key();
        if attempt >= 3 {
            let delay = (1000i64 << (attempt - 3).min(6)).min(60_000);
            ex.s.wft_due_ms = now_ms() + delay;
            ex.s.wft_due_attempt = attempt;
            self.add_timer(ex.s.wft_due_ms, TimerKind::WftDue { key });
        } else {
            self.schedule_wft(ex, attempt);
        }
        self.requeue_queries(ex);
    }

    pub fn wft_timed_out(&mut self, key: &Key, scheduled_id: i64, started_id: i64) -> R<()> {
        let Some(ex) = self.execs.get(key) else { return Ok(()) };
        let valid = ex.s.is_open()
            && ex
                .s
                .wft
                .as_ref()
                .is_some_and(|w| w.scheduled_id == scheduled_id && w.started_id == started_id);
        if !valid {
            return Ok(());
        }
        let mut ex = self.take(key)?;
        let w = ex.s.wft.clone().expect("checked");
        push(
            &mut ex,
            A::WorkflowTaskTimedOutEventAttributes(history::WorkflowTaskTimedOutEventAttributes {
                scheduled_event_id: w.scheduled_id,
                started_event_id: w.started_id,
                timeout_type: enums::TimeoutType::StartToClose as i32,
            }),
        );
        self.retry_wft(&mut ex, w.attempt + 1);
        self.put(ex);
        Ok(())
    }

    /// The sticky worker didn't pick the task up in time: move it to the normal queue.
    pub fn sticky_timed_out(&mut self, key: &Key, scheduled_id: i64) {
        let Some(ex) = self.execs.get_mut(key) else { return };
        let Some(w) = ex.s.wft.as_mut() else { return };
        if w.scheduled_id != scheduled_id || w.started_id != 0 || w.queue == ex.s.task_queue {
            return;
        }
        w.queue = ex.s.task_queue.clone();
        ex.s.sticky_queue.clear();
        let tq = ex.s.task_queue.clone();
        self.dirty.insert(key.clone());
        self.enqueue(
            &key.0,
            &tq,
            QKind::Workflow,
            TaskRef::Wft {
                run_id: key.1.clone(),
                scheduled_id,
            },
        );
    }

    pub fn wft_due(&mut self, key: &Key) -> R<()> {
        let Some(ex) = self.execs.get(key) else { return Ok(()) };
        if !ex.s.is_open() || ex.s.wft_due_ms == 0 || ex.s.wft_due_ms > now_ms() || ex.s.wft.is_some() {
            return Ok(());
        }
        let mut ex = self.take(key)?;
        let attempt = ex.s.wft_due_attempt.max(1);
        ex.s.wft_due_ms = 0;
        ex.s.wft_due_attempt = 0;
        self.schedule_wft(&mut ex, attempt);
        self.put(ex);
        Ok(())
    }

    pub fn reset_sticky(&mut self, key: &Key) -> R<()> {
        let mut ex = self.take(key)?;
        ex.s.sticky_queue.clear();
        // A task already waiting on the sticky queue goes to the normal queue.
        if let Some(w) = ex.s.wft.as_mut()
            && w.started_id == 0
            && w.queue != ex.s.task_queue
        {
            w.queue = ex.s.task_queue.clone();
            let sid = w.scheduled_id;
            let tq = ex.s.task_queue.clone();
            self.enqueue(
                &key.0,
                &tq,
                QKind::Workflow,
                TaskRef::Wft {
                    run_id: key.1.clone(),
                    scheduled_id: sid,
                },
            );
        }
        self.put(ex);
        Ok(())
    }
}

fn validate_commands(
    ex: &Execution,
    cmds: &[command::Command],
) -> Result<(), (enums::WorkflowTaskFailedCause, String)> {
    use enums::WorkflowTaskFailedCause as F;
    let mut activity_ids: std::collections::HashSet<String> =
        ex.s.activities
            .values()
            .filter_map(|a| a.attrs.as_ref().map(|x| x.activity_id.clone()))
            .collect();
    let mut timer_ids: std::collections::HashSet<String> = ex.s.timers.keys().cloned().collect();
    for c in cmds {
        match &c.attributes {
            Some(C::ScheduleActivityTaskCommandAttributes(a)) => {
                if a.activity_id.is_empty() {
                    return Err((F::BadScheduleActivityAttributes, "ActivityId is not set.".into()));
                }
                if a.activity_type.as_ref().is_none_or(|t| t.name.is_empty()) {
                    return Err((F::BadScheduleActivityAttributes, "ActivityType is not set.".into()));
                }
                if dur_ms(&a.schedule_to_close_timeout) <= 0
                    && dur_ms(&a.start_to_close_timeout) <= 0
                    && dur_ms(&ex.s.run_timeout) <= 0
                {
                    return Err((
                        F::BadScheduleActivityAttributes,
                        "A valid StartToClose or ScheduleToCloseTimeout is not set on ScheduleActivityTaskCommand."
                            .into(),
                    ));
                }
                if !activity_ids.insert(a.activity_id.clone()) {
                    return Err((
                        F::BadScheduleActivityAttributes,
                        format!("Activity id {} is already in use.", a.activity_id),
                    ));
                }
            }
            Some(C::StartTimerCommandAttributes(a)) => {
                if a.timer_id.is_empty() {
                    return Err((F::BadStartTimerAttributes, "TimerId is not set.".into()));
                }
                if !timer_ids.insert(a.timer_id.clone()) {
                    return Err((
                        F::StartTimerDuplicateId,
                        format!("Timer id {} is already in use.", a.timer_id),
                    ));
                }
            }
            Some(C::StartChildWorkflowExecutionCommandAttributes(a)) => {
                if a.workflow_id.is_empty() || a.workflow_type.as_ref().is_none_or(|t| t.name.is_empty()) {
                    return Err((
                        F::BadStartChildExecutionAttributes,
                        "Child workflow id and type must be set.".into(),
                    ));
                }
            }
            Some(C::ProtocolMessageCommandAttributes(_)) => {
                return Err((
                    F::BadUpdateWorkflowExecutionMessage,
                    "Workflow updates are not supported by this server yet.".into(),
                ));
            }
            Some(C::ScheduleNexusOperationCommandAttributes(_))
            | Some(C::RequestCancelNexusOperationCommandAttributes(_)) => {
                return Err((
                    F::BadScheduleNexusOperationAttributes,
                    "Nexus operations are not supported by this server yet.".into(),
                ));
            }
            _ => {}
        }
    }
    Ok(())
}
