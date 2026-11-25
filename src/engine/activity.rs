//! Activity tasks: dispatch, heartbeats, completion, server-side retries and timeouts.

use super::events::push_external;
use super::workflow::{Retry, retry_decision};
use super::*;
use crate::state::{ACT_BACKOFF, ACT_SCHEDULED, ACT_STARTED, ActivityInfo};
use history::history_event::Attributes as A;

/// How an activity attempt ended.
// Short-lived values passed once; boxing the large variant would only add allocations.
#[allow(clippy::large_enum_variant)]
pub enum ActEnd {
    Completed(Option<common::Payloads>),
    Failed(failure::Failure, Option<common::Payloads>),
    Canceled(Option<common::Payloads>),
    TimedOut(enums::TimeoutType),
}

impl Inner {
    pub fn take_activity(
        &mut self,
        ns: &str,
        queue: &str,
        identity: &str,
    ) -> R<Option<wsv1::PollActivityTaskQueueResponse>> {
        loop {
            let t = match self.queue(ns, queue, QKind::Activity).tasks.pop_front() {
                Some(t) => t,
                None => return Ok(None),
            };
            if let TaskRef::Activity {
                run_id,
                scheduled_id,
                attempt,
            } = t
            {
                let key = (ns.to_string(), run_id);
                if let Some(r) = self.start_activity(&key, scheduled_id, attempt, identity) {
                    return Ok(Some(r));
                }
            }
        }
    }

    fn start_activity(
        &mut self,
        key: &Key,
        sid: i64,
        attempt: i32,
        identity: &str,
    ) -> Option<wsv1::PollActivityTaskQueueResponse> {
        let mut ex = self.execs.remove(key)?;
        let r = self.start_activity_in(&mut ex, sid, attempt, identity);
        self.put(ex);
        r
    }

    /// Start an activity attempt of a run that's out of the map (also used for activities a
    /// workflow task completion hands straight back to the worker).
    pub fn start_activity_in(
        &mut self,
        ex: &mut Execution,
        sid: i64,
        attempt: i32,
        identity: &str,
    ) -> Option<wsv1::PollActivityTaskQueueResponse> {
        let key = &ex.key();
        if !ex.s.is_open() {
            return None;
        }
        let a = ex.s.activities.get(&sid)?;
        if a.state != ACT_SCHEDULED || a.attempt != attempt {
            return None;
        }
        // A write to the run: a speculative workflow task becomes a normal one (Go server).
        super::events::convert_speculative(ex);
        let a = ex.s.activities.get_mut(&sid)?;
        let now = now_ms();
        a.state = ACT_STARTED;
        a.started_ms = now;
        a.identity = identity.to_string();
        a.last_identity = identity.to_string();
        a.request_id = new_id();
        let a = a.clone();
        let attrs = a.attrs.clone().unwrap_or_default();
        let wid = ex.s.workflow_id.clone();
        let wtype = ex.s.workflow_type.clone();
        let st2c = dur_ms(&attrs.start_to_close_timeout);
        if st2c > 0 {
            self.add_timer(
                now + st2c,
                TimerKind::ActStartToClose {
                    key: key.clone(),
                    scheduled_id: sid,
                    attempt,
                },
            );
        }
        let hb = dur_ms(&attrs.heartbeat_timeout);
        if hb > 0 {
            self.add_timer(
                now + hb,
                TimerKind::ActHeartbeat {
                    key: key.clone(),
                    scheduled_id: sid,
                    attempt,
                },
            );
        }
        let token = TaskToken {
            k: "act".into(),
            ns: key.0.clone(),
            wid: wid.clone(),
            rid: key.1.clone(),
            sid,
            att: attempt,
            aid: attrs.activity_id.clone(),
            ..Default::default()
        };
        Some(wsv1::PollActivityTaskQueueResponse {
            task_token: token.encode(),
            workflow_namespace: key.0.clone(),
            workflow_type: Some(common::WorkflowType { name: wtype }),
            workflow_execution: Some(common::WorkflowExecution {
                workflow_id: wid,
                run_id: key.1.clone(),
            }),
            activity_type: attrs.activity_type.clone(),
            activity_id: attrs.activity_id.clone(),
            header: attrs.header.clone(),
            input: attrs.input.clone(),
            heartbeat_details: a.heartbeat_details.clone(),
            scheduled_time: Some(ts_from_ms(a.scheduled_ms)),
            current_attempt_scheduled_time: Some(ts_from_ms(a.attempt_scheduled_ms)),
            started_time: Some(ts_from_ms(now)),
            attempt,
            schedule_to_close_timeout: attrs.schedule_to_close_timeout,
            start_to_close_timeout: attrs.start_to_close_timeout,
            heartbeat_timeout: attrs.heartbeat_timeout,
            retry_policy: attrs.retry_policy.clone(),
            priority: attrs.priority.clone(),
            ..Default::default()
        })
    }

    /// Find the running activity a token (or an activity id) points at.
    fn find_activity(&mut self, tok: &TaskToken) -> R<(Key, i64)> {
        let key = (tok.ns.clone(), tok.rid.clone());
        if !self.ensure_loaded(&key)? {
            return Err(activity_not_found());
        }
        let ex = &self.execs[&key];
        if !ex.s.is_open() {
            return Err(activity_not_found());
        }
        match ex.s.activities.get(&tok.sid) {
            Some(a) if a.state == ACT_STARTED && a.attempt == tok.att => Ok((key, tok.sid)),
            _ => Err(activity_not_found()),
        }
    }

    pub fn find_activity_by_id(&mut self, ns: &str, wid: &str, rid: &str, activity_id: &str) -> R<TaskToken> {
        let key = self.resolve(ns, wid, rid)?;
        let ex = &self.execs[&key];
        if !ex.s.is_open() {
            return Err(activity_not_found());
        }
        let a =
            ex.s.activities
                .values()
                .find(|a| a.attrs.as_ref().is_some_and(|x| x.activity_id == activity_id))
                .ok_or_else(activity_not_found)?;
        Ok(TaskToken {
            k: "act".into(),
            ns: ns.to_string(),
            wid: wid.to_string(),
            rid: key.1.clone(),
            sid: a.scheduled_id,
            att: a.attempt,
            aid: activity_id.to_string(),
            ..Default::default()
        })
    }

    /// Record a heartbeat; returns whether cancellation was requested.
    pub fn heartbeat(&mut self, tok: &TaskToken, details: Option<common::Payloads>) -> R<bool> {
        let key = (tok.ns.clone(), tok.rid.clone());
        if !self.ensure_loaded(&key)? {
            return Err(activity_not_found());
        }
        let ex = self.execs.get_mut(&key).expect("loaded");
        if !ex.s.is_open() {
            return Err(activity_not_found());
        }
        let a = ex.s.activities.get(&tok.sid).ok_or_else(activity_not_found)?;
        if a.attempt != tok.att || a.state != ACT_STARTED {
            return Err(activity_not_found());
        }
        super::events::convert_speculative(ex);
        let a = ex.s.activities.get_mut(&tok.sid).ok_or_else(activity_not_found)?;
        if details.is_some() {
            a.heartbeat_details = details;
        }
        a.last_heartbeat_ms = now_ms();
        let c = a.cancel_requested;
        self.dirty.insert(key);
        Ok(c)
    }

    /// A worker reported the end of an activity attempt.
    pub fn activity_ended(&mut self, tok: &TaskToken, end: ActEnd, identity: &str) -> R<()> {
        let (key, sid) = self.find_activity(tok)?;
        let mut ex = self.take(&key)?;
        self.end_activity_attempt(&mut ex, sid, end, identity);
        self.put(ex);
        Ok(())
    }

    /// Apply the end of an attempt: retry it, or write the final events.
    pub fn end_activity_attempt(&mut self, ex: &mut Execution, sid: i64, end: ActEnd, identity: &str) {
        let Some(a) = ex.s.activities.get(&sid).cloned() else {
            return;
        };
        let key = ex.key();
        let attrs = a.attrs.clone().unwrap_or_default();
        let identity = if identity.is_empty() {
            a.identity.clone()
        } else {
            identity.to_string()
        };
        // Failures and timeouts may be retried.
        let fail = match &end {
            ActEnd::Failed(f, _) => Some(f.clone()),
            ActEnd::TimedOut(t) => Some(timeout_failure(*t, a.heartbeat_details.clone(), "activity")),
            _ => None,
        };
        let mut retry_state = enums::RetryState::Unspecified;
        if let Some(f) = &fail {
            match retry_decision(attrs.retry_policy.as_ref(), a.attempt, Some(f), a.expiration_ms) {
                Retry::After(delay) => {
                    let now = now_ms();
                    let next = a.attempt + 1;
                    let ai = ex.s.activities.get_mut(&sid).expect("present");
                    ai.state = ACT_BACKOFF;
                    ai.attempt = next;
                    ai.last_failure = Some(f.clone());
                    ai.next_attempt_ms = now + delay;
                    ai.last_attempt_complete_ms = now;
                    ai.cancel_requested = false;
                    if let ActEnd::Failed(_, Some(d)) = &end {
                        ai.heartbeat_details = Some(d.clone());
                    }
                    self.add_timer(
                        now + delay,
                        TimerKind::ActRetry {
                            key,
                            scheduled_id: sid,
                            attempt: next,
                        },
                    );
                    return;
                }
                Retry::No(rs) => retry_state = rs,
            }
        }
        ex.s.activities.remove(&sid);
        let was_started = a.state == ACT_STARTED;
        let started_id = if was_started {
            push_external(
                ex,
                A::ActivityTaskStartedEventAttributes(history::ActivityTaskStartedEventAttributes {
                    scheduled_event_id: sid,
                    identity: identity.clone(),
                    request_id: a.request_id.clone(),
                    attempt: a.attempt,
                    last_failure: a.last_failure.clone(),
                    ..Default::default()
                }),
            )
        } else {
            0
        };
        let ev = match end {
            ActEnd::Completed(result) => {
                A::ActivityTaskCompletedEventAttributes(history::ActivityTaskCompletedEventAttributes {
                    result,
                    scheduled_event_id: sid,
                    started_event_id: started_id,
                    identity,
                    worker_version: None,
                })
            }
            ActEnd::Failed(f, _) => A::ActivityTaskFailedEventAttributes(history::ActivityTaskFailedEventAttributes {
                failure: Some(f),
                scheduled_event_id: sid,
                started_event_id: started_id,
                identity,
                retry_state: retry_state as i32,
                ..Default::default()
            }),
            ActEnd::Canceled(details) => {
                A::ActivityTaskCanceledEventAttributes(history::ActivityTaskCanceledEventAttributes {
                    details,
                    latest_cancel_requested_event_id: a.cancel_requested_event_id,
                    scheduled_event_id: sid,
                    started_event_id: started_id,
                    identity,
                    worker_version: None,
                })
            }
            ActEnd::TimedOut(_) => {
                let mut f = fail.expect("timeout failure");
                // Keep the last application failure as the cause when retries ran out.
                if let Some(last) = a.last_failure.clone() {
                    f.cause = Some(Box::new(last));
                }
                A::ActivityTaskTimedOutEventAttributes(history::ActivityTaskTimedOutEventAttributes {
                    failure: Some(f),
                    scheduled_event_id: sid,
                    started_event_id: started_id,
                    retry_state: retry_state as i32,
                })
            }
        };
        push_external(ex, ev);
        self.ensure_wft(ex);
    }

    // ---- timers ----

    pub fn activity_retry_due(&mut self, key: &Key, sid: i64, attempt: i32) {
        let Some(ex) = self.execs.get_mut(key) else { return };
        if !ex.s.is_open() {
            return;
        }
        let Some(a) = ex.s.activities.get_mut(&sid) else { return };
        if a.state != ACT_BACKOFF || a.attempt != attempt {
            return;
        }
        let now = now_ms();
        a.state = ACT_SCHEDULED;
        a.attempt_scheduled_ms = now;
        let attrs = a.attrs.clone().unwrap_or_default();
        let tq = attrs.task_queue.as_ref().map(|t| t.name.clone()).unwrap_or_default();
        self.dirty.insert(key.clone());
        self.enqueue(
            &key.0,
            &tq,
            QKind::Activity,
            TaskRef::Activity {
                run_id: key.1.clone(),
                scheduled_id: sid,
                attempt,
            },
        );
        let s2s = dur_ms(&attrs.schedule_to_start_timeout);
        let s2c = dur_ms(&attrs.schedule_to_close_timeout);
        if s2s > 0 && (s2c <= 0 || s2s < s2c) {
            self.add_timer(
                now + s2s,
                TimerKind::ActScheduleToStart {
                    key: key.clone(),
                    scheduled_id: sid,
                    attempt,
                },
            );
        }
    }

    pub fn activity_timer(&mut self, key: &Key, sid: i64, attempt: Option<i32>, kind: enums::TimeoutType) -> R<()> {
        let Some(ex) = self.execs.get(key) else { return Ok(()) };
        if !ex.s.is_open() {
            return Ok(());
        }
        let Some(a) = ex.s.activities.get(&sid).cloned() else {
            return Ok(());
        };
        if let Some(att) = attempt
            && a.attempt != att
        {
            return Ok(());
        }
        let attrs = a.attrs.clone().unwrap_or_default();
        let now = now_ms();
        let fire = match kind {
            enums::TimeoutType::StartToClose => a.state == ACT_STARTED,
            enums::TimeoutType::ScheduleToStart => a.state == ACT_SCHEDULED,
            enums::TimeoutType::Heartbeat => {
                if a.state != ACT_STARTED {
                    false
                } else {
                    let hb = dur_ms(&attrs.heartbeat_timeout);
                    let last = a.last_heartbeat_ms.max(a.started_ms);
                    if now < last + hb {
                        self.add_timer(
                            last + hb,
                            TimerKind::ActHeartbeat {
                                key: key.clone(),
                                scheduled_id: sid,
                                attempt: a.attempt,
                            },
                        );
                        false
                    } else {
                        true
                    }
                }
            }
            enums::TimeoutType::ScheduleToClose => a.expiration_ms > 0 && now >= a.expiration_ms,
            _ => false,
        };
        if !fire {
            return Ok(());
        }
        let mut ex = self.take(key)?;
        if kind == enums::TimeoutType::ScheduleToClose {
            // No retry once the overall deadline passed.
            if let Some(ai) = ex.s.activities.get_mut(&sid) {
                ai.expiration_ms = 1;
            }
        }
        self.end_activity_attempt(&mut ex, sid, ActEnd::TimedOut(kind), "");
        self.put(ex);
        Ok(())
    }

    pub fn pending_activity_info(a: &ActivityInfo) -> crate::proto::temporal::api::workflow::v1::PendingActivityInfo {
        let attrs = a.attrs.clone().unwrap_or_default();
        let state = if a.cancel_requested {
            enums::PendingActivityState::CancelRequested
        } else if a.state == ACT_STARTED {
            enums::PendingActivityState::Started
        } else {
            enums::PendingActivityState::Scheduled
        };
        crate::proto::temporal::api::workflow::v1::PendingActivityInfo {
            activity_id: attrs.activity_id.clone(),
            activity_type: attrs.activity_type.clone(),
            state: state as i32,
            heartbeat_details: a.heartbeat_details.clone(),
            last_heartbeat_time: if a.last_heartbeat_ms > 0 {
                Some(ts_from_ms(a.last_heartbeat_ms))
            } else {
                None
            },
            last_started_time: if a.started_ms > 0 {
                Some(ts_from_ms(a.started_ms))
            } else {
                None
            },
            attempt: a.attempt,
            maximum_attempts: attrs.retry_policy.as_ref().map(|r| r.maximum_attempts).unwrap_or(0),
            scheduled_time: Some(ts_from_ms(a.attempt_scheduled_ms)),
            expiration_time: if a.expiration_ms > 0 {
                Some(ts_from_ms(a.expiration_ms))
            } else {
                None
            },
            last_failure: a.last_failure.clone(),
            last_worker_identity: a.last_identity.clone(),
            last_attempt_complete_time: if a.last_attempt_complete_ms > 0 {
                Some(ts_from_ms(a.last_attempt_complete_ms))
            } else {
                None
            },
            next_attempt_schedule_time: if a.state == ACT_BACKOFF {
                Some(ts_from_ms(a.next_attempt_ms))
            } else {
                None
            },
            ..Default::default()
        }
    }
}
