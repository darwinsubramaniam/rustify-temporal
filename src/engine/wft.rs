//! Workflow tasks: handing them to workers, and applying the commands a worker sends back.

use super::events::{
    base_wft_timeout, flush_buffered, push, schedule_to_start_ms, suggest_continue_as_new, transient_events,
    wft_scheduled_attrs, wft_started_attrs, write_unwritten_wft_events,
};
use super::update::{self, Abort, MsgError, Step};
use prost::Message;

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
                    self.start_wft(&(ns.to_string(), run_id), scheduled_id, queue, identity, false, false)?
                }
                TaskRef::Query { query_id } => self.start_query_task(&query_id, queue)?,
                TaskRef::Activity { .. } => None,
            };
            if r.is_some() {
                return Ok(r);
            }
        }
    }

    /// Mark the workflow task started and build what the worker receives. `partial` sends only
    /// the events since the last completed task (for a task returned with a completion);
    /// `heartbeat` (a task the worker forced) carries only updates it hasn't been sent yet.
    pub fn start_wft(
        &mut self,
        key: &Key,
        scheduled_id: i64,
        queue: &str,
        identity: &str,
        partial: bool,
        heartbeat: bool,
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
        // A speculative task with no updates left to deliver is dropped by matching (Go server);
        // its schedule-to-start timeout turns it into a normal task.
        if ex.s.wft.as_ref().is_some_and(|w| w.speculative) && !ex.updates.has_outgoing(!heartbeat) {
            return Ok(None);
        }
        let mut ex = self.execs.remove(key).expect("present");
        let now = now_ms();
        let sticky = !ex.s.sticky_queue.is_empty() && ex.s.sticky_queue == queue;
        // A transient retry stops being transient once new events were written since it was
        // scheduled: it becomes a normal attempt 1 with written events (as in the Go server).
        let mut w = ex.s.wft.clone().expect("wft");
        if w.transient && w.scheduled_id != ex.s.next_event_id {
            let base = base_wft_timeout(&ex);
            let attrs = wft_scheduled_attrs(&ex, queue, sticky, base, 1);
            w.scheduled_id = push(&mut ex, A::WorkflowTaskScheduledEventAttributes(attrs));
            w.attempt = 1;
            w.transient = false;
            w.timeout_ms = base;
        }
        w.identity = identity.to_string();
        w.request_id = new_id();
        w.started_ms = now;
        w.history_size_at_start = ex.s.history_size;
        let reasons = suggest_continue_as_new(&ex);
        w.started_id = if w.transient || w.speculative {
            w.scheduled_id + 1
        } else {
            push(
                &mut ex,
                A::WorkflowTaskStartedEventAttributes(wft_started_attrs(&w, reasons)),
            )
        };
        let (sid, started_id, attempt, timeout, sched_ms) =
            (w.scheduled_id, w.started_id, w.attempt, w.timeout_ms, w.scheduled_ms);
        ex.s.wft = Some(w);
        if !ex.s.wft.as_ref().is_some_and(|w| w.transient || w.speculative) {
            ex.notify.notify_waiters();
        }
        self.add_timer(
            now + timeout,
            TimerKind::WftTimeout {
                key: key.clone(),
                scheduled_id: sid,
                started_id,
            },
        );
        let first = if sticky || partial {
            ex.s.last_completed_started_id + 1
        } else {
            1
        };
        let mut events: Vec<_> = ex.events.iter().filter(|e| e.event_id >= first).cloned().collect();
        events.extend(transient_events(&ex));
        let queries = self.attach_queries(key, started_id);
        let messages = update::send_messages(&mut ex, started_id, !heartbeat);
        let token = TaskToken {
            k: "wft".into(),
            ns: key.0.clone(),
            wid: ex.s.workflow_id.clone(),
            rid: key.1.clone(),
            sid,
            att: attempt,
            st: started_id,
            stt: now,
            ..Default::default()
        };
        let backlog = self.queue(&key.0, queue, QKind::Workflow).tasks.len() as i64;
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
            backlog_count_hint: backlog,
            history: Some(history::History { events }),
            workflow_execution_task_queue: Some(taskqueue::TaskQueue {
                name: ex.s.task_queue.clone(),
                kind: enums::TaskQueueKind::Normal as i32,
                normal_name: String::new(),
            }),
            scheduled_time: Some(ts_from_ms(sched_ms)),
            started_time: Some(ts_from_ms(now)),
            queries,
            messages,
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
                && ex.s.wft.as_ref().is_some_and(|w| {
                    w.scheduled_id == tok.sid
                        && w.started_id == tok.st
                        && w.started_id > 0
                        && (tok.stt == 0 || tok.stt == w.started_ms)
                })
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
            let reject = Reject {
                cause: enums::WorkflowTaskFailedCause::BadBinary,
                msg,
                terminate: false,
            };
            let st = self.reject_wft(&mut ex, &w, reject, &r.identity);
            self.put(ex);
            return Err(st);
        }

        // Check every command and update message before writing anything; a bad one fails the
        // task (or, for an oversized payload, terminates the workflow), as the Go server does.
        let mut reject = validate_commands(&ex, &r.commands).err();
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
        if reject.is_none() && closes && !ex.s.buffered.is_empty() {
            reject = Some(Reject {
                cause: enums::WorkflowTaskFailedCause::UnhandledCommand,
                msg: String::new(),
                terminate: false,
            });
        }
        let mut steps = Vec::new();
        if reject.is_none() {
            match update::plan_messages(&ex, &r.commands, &r.messages) {
                Ok(s) => steps = s,
                Err(MsgError::Fail(rj)) => reject = Some(rj),
                Err(MsgError::Error(st)) => {
                    self.put(ex);
                    return Err(st);
                }
            }
        }
        if let Some(reject) = reject {
            let st = self.reject_wft(&mut ex, &w, reject, &r.identity);
            self.put(ex);
            return Err(st);
        }

        // A speculative task that only rejected updates (and changed nothing else) disappears:
        // none of its events are written and the worker rolls back to its previous task.
        let discard = w.speculative
            && r.commands.is_empty()
            && !r.force_create_new_workflow_task
            && {
                let may_resend = r
                    .capabilities
                    .as_ref()
                    .is_some_and(|c| c.discard_speculative_workflow_task_with_events);
                let shipped = ex.s.next_event_id - (ex.s.last_completed_started_id + 2);
                if may_resend {
                    shipped <= update::DISCARD_SPECULATIVE_MAX_EVENTS
                } else {
                    shipped <= 0
                }
            }
            && r.messages.iter().all(update::is_rejection);

        let mut completed_id = 0;
        if !discard {
            // A transient retry's or speculative task's scheduled and started events are
            // written now that it completed.
            if w.transient || w.speculative {
                write_unwritten_wft_events(&mut ex, &w);
            }
            completed_id = push(
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
        }
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

        let mut need_new_task = false;
        let mut eager = Vec::new();
        let mut commands: Vec<Option<command::Command>> = r.commands.into_iter().map(Some).collect();
        // A run closed by these commands aborts its updates only after every message is handled.
        self.defer_update_abort = true;
        for step in steps {
            match step {
                Step::Command(i) => {
                    if !ex.s.is_open() {
                        continue;
                    }
                    let Some(c) = commands[i].take() else { continue };
                    let Some(a) = c.attributes else { continue };
                    let user_metadata = c.user_metadata;
                    let effect = match self.apply_command(&mut ex, a, completed_id, &r.identity) {
                        Ok(e) => e,
                        Err(e) => {
                            self.defer_update_abort = false;
                            return Err(e);
                        }
                    };
                    need_new_task |= effect.new_task;
                    eager.extend(effect.eager_activity);
                    if user_metadata.is_some()
                        && let Some(last) = ex.events.last_mut()
                        && last.event_id > completed_id
                        && last.user_metadata.is_none()
                    {
                        last.user_metadata = user_metadata;
                    }
                }
                Step::Message(i) => update::apply_message(&mut ex, &r.messages[i]),
                Step::SkipMessage => {}
            }
        }
        self.defer_update_abort = false;
        // Updates sent on this task that the worker ignored are rejected on its behalf.
        if ex.s.is_open() && !r.force_create_new_workflow_task {
            update::reject_unprocessed(&mut ex);
        }

        // Activities the worker asked to run right away are handed back in the response.
        let mut activity_tasks = Vec::new();
        if ex.s.is_open() {
            for sid in eager {
                if let Some(t) = self.start_activity_in(&mut ex, sid, 1, &r.identity) {
                    activity_tasks.push(t);
                }
            }
        }
        let mut new_task = None;
        if ex.s.is_open() {
            let had_buffered = flush_buffered(&mut ex);
            let k = ex.key();
            if had_buffered || need_new_task || r.force_create_new_workflow_task {
                self.schedule_wft(&mut ex, 1);
            } else if ex.updates.has_outgoing(true) {
                // Updates that arrived while this task ran: a speculative task when it goes
                // straight back to the worker, otherwise a normal one.
                if r.return_new_workflow_task {
                    self.schedule_speculative_wft(&mut ex);
                } else {
                    self.schedule_wft(&mut ex, 1);
                }
            }
            if r.return_new_workflow_task
                && let Some(w) = ex.s.wft.clone().filter(|w| w.started_id == 0)
            {
                self.put(ex);
                new_task = self.start_wft(
                    &k,
                    w.scheduled_id,
                    &w.queue,
                    &r.identity,
                    true,
                    r.force_create_new_workflow_task,
                )?;
                ex = self.take(&k)?;
            }
        } else {
            let reason = if ex.s.new_run_id.is_empty() {
                Abort::Closed
            } else {
                Abort::Continuing
            };
            update::abort_updates(&mut ex, reason);
        }
        ex.updates.publish();
        // Queries that arrived while this task ran and have no task to ride on go out on their own.
        if ex.s.wft.is_none() {
            self.requeue_queries(&ex);
        }
        let reset_history_event_id = if discard { ex.s.last_completed_started_id } else { 0 };
        self.put(ex);
        Ok(wsv1::RespondWorkflowTaskCompletedResponse {
            workflow_task: new_task,
            activity_tasks,
            reset_history_event_id,
        })
    }

    fn apply_command(&mut self, ex: &mut Execution, a: C, completed_id: i64, identity: &str) -> R<CommandEffect> {
        let key = ex.key();
        let mut effect = CommandEffect::default();
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
                if c.request_eager_execution {
                    // Started in this response instead of going through the task queue.
                    effect.eager_activity = Some(sid);
                } else {
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
                }
                if s2s > 0 && s2s < s2c && !c.request_eager_execution {
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
                        // The workflow learns of the cancellation in a new task (Go server).
                        effect.new_task = true;
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
        Ok(effect)
    }

    pub fn respond_wft_failed(&mut self, r: wsv1::RespondWorkflowTaskFailedRequest) -> R<()> {
        let tok = TaskToken::decode(&r.task_token)?;
        let mut ex = self.check_wft_token(&tok)?;
        let cause =
            enums::WorkflowTaskFailedCause::try_from(r.cause).unwrap_or(enums::WorkflowTaskFailedCause::Unspecified);
        self.fail_wft_inner(&mut ex, cause, r.failure, &r.identity);
        self.put(ex);
        Ok(())
    }

    /// A completion the server refuses. On a retry (attempt > 1) nothing is recorded and the
    /// task keeps running until it times out; otherwise the task fails (and for oversized
    /// payloads the workflow is terminated). Returns the error for the worker (Go server rules).
    fn reject_wft(&mut self, ex: &mut Execution, w: &crate::state::WftInfo, reject: Reject, identity: &str) -> Status {
        let msg = if reject.msg.is_empty() {
            reject.cause.as_str_name().to_string()
        } else {
            format!("{}: {}", reject.cause.as_str_name(), reject.msg)
        };
        // Updates the failed task carried can't be answered by it (Go server's abort reasons).
        update::abort_updates(ex, Abort::TaskFailed);
        if w.attempt > 1 && reject.cause != enums::WorkflowTaskFailedCause::UnhandledCommand {
            return invalid(msg);
        }
        let failure = (!reject.msg.is_empty()).then(|| server_failure(&reject.msg, true));
        let next = self.record_wft_failure(ex, reject.cause, failure, identity);
        if reject.terminate {
            ex.s.wft = None;
            flush_buffered(ex);
            push(
                ex,
                A::WorkflowExecutionTerminatedEventAttributes(history::WorkflowExecutionTerminatedEventAttributes {
                    reason: msg.clone(),
                    details: None,
                    identity: HISTORY_SERVICE_IDENTITY.into(),
                }),
            );
            self.finish_close(ex, enums::WorkflowExecutionStatus::Terminated, true);
        } else {
            self.retry_wft(ex, next);
        }
        invalid(msg)
    }

    /// Record a failed workflow task and schedule the next attempt.
    fn fail_wft_inner(
        &mut self,
        ex: &mut Execution,
        cause: enums::WorkflowTaskFailedCause,
        f: Option<failure::Failure>,
        identity: &str,
    ) {
        let next = self.record_wft_failure(ex, cause, f, identity);
        self.retry_wft(ex, next);
    }

    /// Write the failed event (not for a transient retry) and work out the next attempt number.
    fn record_wft_failure(
        &mut self,
        ex: &mut Execution,
        cause: enums::WorkflowTaskFailedCause,
        f: Option<failure::Failure>,
        identity: &str,
    ) -> i32 {
        let w = ex.s.wft.clone().expect("running task");
        if w.speculative {
            write_unwritten_wft_events(ex, &w);
        }
        if !w.transient {
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
        }
        next_attempt(ex, &w, true)
    }

    /// After a failed or timed-out workflow task: write held-back events and schedule the next
    /// attempt right away (later attempts get longer timeouts instead of a delay). New events
    /// reset the attempt to 1, as in the Go server.
    pub fn retry_wft(&mut self, ex: &mut Execution, attempt: i32) {
        ex.s.wft = None;
        let attempt = if flush_buffered(ex) { 1 } else { attempt };
        self.schedule_wft(ex, attempt);
        self.requeue_queries(ex);
    }

    pub fn wft_timed_out(&mut self, key: &Key, scheduled_id: i64, started_id: i64) -> R<()> {
        let Some(ex) = self.execs.get(key) else { return Ok(()) };
        // A speculative task can be replaced by one with the same event ids: only time out the
        // task this timer was set for.
        let valid = ex.s.is_open()
            && ex.s.wft.as_ref().is_some_and(|w| {
                w.scheduled_id == scheduled_id && w.started_id == started_id && now_ms() >= w.started_ms + w.timeout_ms
            });
        if !valid {
            return Ok(());
        }
        let mut ex = self.take(key)?;
        let w = ex.s.wft.clone().expect("checked");
        if w.speculative {
            write_unwritten_wft_events(&mut ex, &w);
        }
        if !w.transient {
            push(
                &mut ex,
                A::WorkflowTaskTimedOutEventAttributes(history::WorkflowTaskTimedOutEventAttributes {
                    scheduled_event_id: w.scheduled_id,
                    started_event_id: w.started_id,
                    timeout_type: enums::TimeoutType::StartToClose as i32,
                }),
            );
        }
        let next = next_attempt(&mut ex, &w, true);
        self.retry_wft(&mut ex, next);
        self.put(ex);
        Ok(())
    }

    /// The sticky worker didn't pick the task up in time (or nobody took a speculative task):
    /// record the schedule-to-start timeout and schedule a new task on the normal queue (Go server).
    pub fn sticky_timed_out(&mut self, key: &Key, scheduled_id: i64) -> R<()> {
        let Some(ex) = self.execs.get(key) else { return Ok(()) };
        let due = schedule_to_start_ms(ex);
        let valid = ex.s.is_open()
            && ex.s.wft.as_ref().is_some_and(|w| {
                w.scheduled_id == scheduled_id
                    && w.started_id == 0
                    && (w.queue != ex.s.task_queue || w.speculative)
                    && now_ms() >= w.scheduled_ms + due
            });
        if !valid {
            return Ok(());
        }
        let mut ex = self.take(key)?;
        let w = ex.s.wft.clone().expect("checked");
        if w.speculative {
            write_unwritten_wft_events(&mut ex, &w);
        }
        if !w.transient {
            push(
                &mut ex,
                A::WorkflowTaskTimedOutEventAttributes(history::WorkflowTaskTimedOutEventAttributes {
                    scheduled_event_id: w.scheduled_id,
                    started_event_id: 0,
                    timeout_type: enums::TimeoutType::ScheduleToStart as i32,
                }),
            );
        }
        let next = next_attempt(&mut ex, &w, false);
        self.retry_wft(&mut ex, next);
        self.put(ex);
        Ok(())
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

/// Why a completion is refused.
pub struct Reject {
    pub cause: enums::WorkflowTaskFailedCause,
    pub msg: String,
    /// Oversized payloads terminate the workflow instead of retrying the task.
    pub terminate: bool,
}

#[derive(Default)]
struct CommandEffect {
    /// The workflow needs another task (e.g. it cancelled an activity that hadn't started).
    new_task: bool,
    /// An activity to start in the completion response.
    eager_activity: Option<i64>,
}

/// The attempt after a failure: back to 1 (and off the sticky queue) if the task was sticky,
/// otherwise one more when `increment` (Go server's failWorkflowTask).
fn next_attempt(ex: &mut Execution, w: &crate::state::WftInfo, increment: bool) -> i32 {
    if !ex.s.sticky_queue.is_empty() {
        ex.s.sticky_queue.clear();
        return 1;
    }
    if increment { w.attempt + 1 } else { 1 }
}

fn payloads_size(p: &Option<common::Payloads>) -> usize {
    p.as_ref().map(|p| p.encoded_len()).unwrap_or(0)
}

fn validate_commands(ex: &Execution, cmds: &[command::Command]) -> Result<(), Reject> {
    use enums::WorkflowTaskFailedCause as F;
    let fail = |cause: F, msg: String| Reject {
        cause,
        msg,
        terminate: false,
    };
    let too_big = |cause: F, msg: &str| Reject {
        cause,
        msg: msg.to_string(),
        terminate: true,
    };
    let mut activity_ids: std::collections::HashSet<String> =
        ex.s.activities
            .values()
            .filter_map(|a| a.attrs.as_ref().map(|x| x.activity_id.clone()))
            .collect();
    let mut timer_ids: std::collections::HashSet<String> = ex.s.timers.keys().cloned().collect();
    let mut pending_activities = ex.s.activities.len();
    let mut pending_children = ex.s.children.len();
    for c in cmds {
        match &c.attributes {
            Some(C::ScheduleActivityTaskCommandAttributes(a)) => {
                if a.activity_id.is_empty() {
                    return Err(fail(F::BadScheduleActivityAttributes, "ActivityId is not set.".into()));
                }
                if a.activity_type.as_ref().is_none_or(|t| t.name.is_empty()) {
                    return Err(fail(
                        F::BadScheduleActivityAttributes,
                        "ActivityType is not set.".into(),
                    ));
                }
                if dur_ms(&a.schedule_to_close_timeout) <= 0
                    && dur_ms(&a.start_to_close_timeout) <= 0
                    && dur_ms(&ex.s.run_timeout) <= 0
                {
                    return Err(fail(
                        F::BadScheduleActivityAttributes,
                        "A valid StartToClose or ScheduleToCloseTimeout is not set on ScheduleActivityTaskCommand."
                            .into(),
                    ));
                }
                if !activity_ids.insert(a.activity_id.clone()) {
                    return Err(fail(
                        F::ScheduleActivityDuplicateId,
                        format!("Activity id {} is already in use.", a.activity_id),
                    ));
                }
                if payloads_size(&a.input) > BLOB_SIZE_LIMIT {
                    return Err(too_big(
                        F::BadScheduleActivityAttributes,
                        "ScheduleActivityTaskCommandAttributes.Input exceeds size limit.",
                    ));
                }
                if pending_activities >= PENDING_ACTIVITIES_LIMIT {
                    return Err(fail(
                        F::PendingActivitiesLimitExceeded,
                        format!(
                            "the number of pending activities, {pending_activities}, has reached the per-workflow limit of {PENDING_ACTIVITIES_LIMIT}"
                        ),
                    ));
                }
                pending_activities += 1;
            }
            Some(C::StartTimerCommandAttributes(a)) => {
                if a.timer_id.is_empty() {
                    return Err(fail(F::BadStartTimerAttributes, "TimerId is not set.".into()));
                }
                if !timer_ids.insert(a.timer_id.clone()) {
                    return Err(fail(
                        F::StartTimerDuplicateId,
                        format!("Timer id {} is already in use.", a.timer_id),
                    ));
                }
            }
            Some(C::CompleteWorkflowExecutionCommandAttributes(a)) => {
                if payloads_size(&a.result) > BLOB_SIZE_LIMIT {
                    // The Go server reports this cause for an oversized result.
                    return Err(too_big(
                        F::BadScheduleActivityAttributes,
                        "CompleteWorkflowExecutionCommandAttributes.Result exceeds size limit.",
                    ));
                }
            }
            Some(C::FailWorkflowExecutionCommandAttributes(a)) => {
                if a.failure.as_ref().map(|f| f.encoded_len()).unwrap_or(0) > BLOB_SIZE_LIMIT {
                    return Err(too_big(
                        F::BadFailWorkflowExecutionAttributes,
                        "FailWorkflowExecutionCommandAttributes.Failure exceeds size limit.",
                    ));
                }
            }
            Some(C::RecordMarkerCommandAttributes(a)) => {
                let size: usize = a.details.values().map(|p| p.encoded_len()).sum();
                if size > BLOB_SIZE_LIMIT {
                    return Err(too_big(
                        F::BadRecordMarkerAttributes,
                        "RecordMarkerCommandAttributes.Details exceeds size limit.",
                    ));
                }
            }
            Some(C::ContinueAsNewWorkflowExecutionCommandAttributes(a)) => {
                if payloads_size(&a.input) > BLOB_SIZE_LIMIT {
                    return Err(too_big(
                        F::BadContinueAsNewAttributes,
                        "ContinueAsNewWorkflowExecutionCommandAttributes. Input exceeds size limit.",
                    ));
                }
                if a.memo.as_ref().map(|m| m.encoded_len()).unwrap_or(0) > MEMO_SIZE_LIMIT {
                    return Err(too_big(
                        F::BadContinueAsNewAttributes,
                        "ContinueAsNewWorkflowExecutionCommandAttributes. Memo exceeds size limit.",
                    ));
                }
            }
            Some(C::StartChildWorkflowExecutionCommandAttributes(a)) => {
                if a.workflow_id.is_empty() || a.workflow_type.as_ref().is_none_or(|t| t.name.is_empty()) {
                    return Err(fail(
                        F::BadStartChildExecutionAttributes,
                        "Child workflow id and type must be set.".into(),
                    ));
                }
                if payloads_size(&a.input) > BLOB_SIZE_LIMIT {
                    return Err(too_big(
                        F::BadStartChildExecutionAttributes,
                        "StartChildWorkflowExecutionCommandAttributes. Input exceeds size limit.",
                    ));
                }
                if pending_children >= PENDING_CHILDREN_LIMIT {
                    return Err(fail(
                        F::PendingChildWorkflowsLimitExceeded,
                        format!(
                            "the number of pending child workflows, {pending_children}, has reached the per-workflow limit of {PENDING_CHILDREN_LIMIT}"
                        ),
                    ));
                }
                pending_children += 1;
            }
            Some(C::SignalExternalWorkflowExecutionCommandAttributes(a)) => {
                if payloads_size(&a.input) > BLOB_SIZE_LIMIT {
                    return Err(too_big(
                        F::BadSignalWorkflowExecutionAttributes,
                        "SignalExternalWorkflowExecutionCommandAttributes.Input exceeds size limit.",
                    ));
                }
            }
            Some(C::UpsertWorkflowSearchAttributesCommandAttributes(a)) => {
                if a.search_attributes.as_ref().map(|x| x.encoded_len()).unwrap_or(0) > BLOB_SIZE_LIMIT {
                    return Err(too_big(
                        F::BadSearchAttributes,
                        "UpsertWorkflowSearchAttributesCommandAttributes exceeds size limit.",
                    ));
                }
            }
            Some(C::ModifyWorkflowPropertiesCommandAttributes(a)) => {
                if a.upserted_memo.as_ref().map(|x| x.encoded_len()).unwrap_or(0) > MEMO_SIZE_LIMIT {
                    return Err(too_big(
                        F::BadModifyWorkflowPropertiesAttributes,
                        "ModifyWorkflowPropertiesCommandAttributes exceeds size limit.",
                    ));
                }
            }
            Some(C::ScheduleNexusOperationCommandAttributes(_))
            | Some(C::RequestCancelNexusOperationCommandAttributes(_)) => {
                return Err(fail(
                    F::BadScheduleNexusOperationAttributes,
                    "Nexus operations are not supported by this server yet.".into(),
                ));
            }
            _ => {}
        }
    }
    Ok(())
}
