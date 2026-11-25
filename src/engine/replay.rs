//! Rebuilding a run's state from its history (the Go server's state rebuilder), used by reset:
//! the reset run starts from the base run's events up to the reset point.

use super::*;
use crate::state::{ACT_SCHEDULED, ACT_STARTED, ActivityInfo, ChildInfo, TimerInfo, UpdateInfo, WftInfo};
use history::history_event::Attributes as A;

fn ms_of(e: &history::HistoryEvent) -> i64 {
    e.event_time
        .as_ref()
        .map(|t| t.seconds * 1000 + (t.nanos as i64) / 1_000_000)
        .unwrap_or(0)
}

/// The state of `base` as it was after event `upto`, as a new run `run_id`. Its history is the
/// base's events 1..=upto (in the same batches).
pub fn rebuild(base: &Execution, upto: i64, run_id: &str, request_id: &str) -> Execution {
    let events: Vec<history::HistoryEvent> = base.events.iter().filter(|e| e.event_id <= upto).cloned().collect();
    let mut s = base.s.clone();
    s.run_id = run_id.to_string();
    s.status = enums::WorkflowExecutionStatus::Running as i32;
    s.close_ms = 0;
    s.next_event_id = upto + 1;
    s.last_completed_started_id = 0;
    s.wft = None;
    s.wft_due_ms = 0;
    s.wft_due_attempt = 0;
    s.sticky_queue.clear();
    s.activities.clear();
    s.timers.clear();
    s.children.clear();
    s.buffered.clear();
    s.new_run_id.clear();
    s.history_size = 0;
    s.transitions = 0;
    s.cancel_requested = false;
    s.signal_request_ids.clear();
    s.updates.clear();
    s.start_request_id = request_id.to_string();
    s.batch_starts.retain(|&b| b <= upto);
    s.memo = None;
    s.search_attributes = None;
    s.priority = None;
    for e in &events {
        s.history_size += prost::Message::encoded_len(e) as i64;
        s.transitions += 1;
        let at = ms_of(e);
        let Some(a) = &e.attributes else { continue };
        match a {
            A::WorkflowExecutionStartedEventAttributes(x) => {
                s.memo = x.memo.clone();
                s.search_attributes = x.search_attributes.clone();
                s.priority = x.priority.clone();
            }
            A::WorkflowTaskScheduledEventAttributes(x) => {
                let queue = x.task_queue.as_ref().map(|q| q.name.clone()).unwrap_or_default();
                s.wft = Some(WftInfo {
                    scheduled_id: e.event_id,
                    attempt: x.attempt.max(1),
                    queue,
                    scheduled_ms: at,
                    timeout_ms: dur_ms(&x.start_to_close_timeout),
                    original_scheduled_ms: at,
                    ..Default::default()
                });
            }
            A::WorkflowTaskStartedEventAttributes(x) => {
                if let Some(w) = s.wft.as_mut() {
                    w.started_id = e.event_id;
                    w.started_ms = at;
                    w.request_id = x.request_id.clone();
                    w.identity = x.identity.clone();
                    w.history_size_at_start = x.history_size_bytes;
                }
            }
            A::WorkflowTaskCompletedEventAttributes(x) => {
                s.last_completed_started_id = x.started_event_id;
                s.wft = None;
            }
            A::WorkflowTaskFailedEventAttributes(_) | A::WorkflowTaskTimedOutEventAttributes(_) => {
                s.wft = None;
            }
            A::ActivityTaskScheduledEventAttributes(x) => {
                let s2c = dur_ms(&x.schedule_to_close_timeout);
                let mut expiration = if s2c > 0 { at + s2c } else { 0 };
                if s.expiration_ms > 0 && (expiration == 0 || s.expiration_ms < expiration) {
                    expiration = s.expiration_ms;
                }
                s.activities.insert(
                    e.event_id,
                    ActivityInfo {
                        scheduled_id: e.event_id,
                        attrs: Some(x.clone()),
                        scheduled_ms: at,
                        attempt: 1,
                        state: ACT_SCHEDULED,
                        attempt_scheduled_ms: at,
                        expiration_ms: expiration,
                        ..Default::default()
                    },
                );
            }
            A::ActivityTaskStartedEventAttributes(x) => {
                if let Some(act) = s.activities.get_mut(&x.scheduled_event_id) {
                    act.state = ACT_STARTED;
                    act.started_ms = at;
                    act.identity = x.identity.clone();
                    act.attempt = x.attempt.max(1);
                }
            }
            A::ActivityTaskCompletedEventAttributes(x) => {
                s.activities.remove(&x.scheduled_event_id);
            }
            A::ActivityTaskFailedEventAttributes(x) => {
                s.activities.remove(&x.scheduled_event_id);
            }
            A::ActivityTaskTimedOutEventAttributes(x) => {
                s.activities.remove(&x.scheduled_event_id);
            }
            A::ActivityTaskCanceledEventAttributes(x) => {
                s.activities.remove(&x.scheduled_event_id);
            }
            A::ActivityTaskCancelRequestedEventAttributes(x) => {
                if let Some(act) = s.activities.get_mut(&x.scheduled_event_id) {
                    act.cancel_requested = true;
                    act.cancel_requested_event_id = e.event_id;
                }
            }
            A::TimerStartedEventAttributes(x) => {
                s.timers.insert(
                    x.timer_id.clone(),
                    TimerInfo {
                        timer_id: x.timer_id.clone(),
                        started_id: e.event_id,
                        fire_ms: at + dur_ms(&x.start_to_fire_timeout).max(1),
                    },
                );
            }
            A::TimerFiredEventAttributes(x) => {
                s.timers.remove(&x.timer_id);
            }
            A::TimerCanceledEventAttributes(x) => {
                s.timers.remove(&x.timer_id);
            }
            A::StartChildWorkflowExecutionInitiatedEventAttributes(x) => {
                s.children.insert(
                    e.event_id,
                    ChildInfo {
                        initiated_id: e.event_id,
                        started_id: 0,
                        namespace: x.namespace.clone(),
                        workflow_id: x.workflow_id.clone(),
                        run_id: String::new(),
                        workflow_type: x.workflow_type.as_ref().map(|t| t.name.clone()).unwrap_or_default(),
                        parent_close_policy: x.parent_close_policy,
                    },
                );
            }
            A::ChildWorkflowExecutionStartedEventAttributes(x) => {
                if let Some(c) = s.children.get_mut(&x.initiated_event_id) {
                    c.started_id = e.event_id;
                    c.run_id = x
                        .workflow_execution
                        .as_ref()
                        .map(|w| w.run_id.clone())
                        .unwrap_or_default();
                }
            }
            A::StartChildWorkflowExecutionFailedEventAttributes(x) => {
                s.children.remove(&x.initiated_event_id);
            }
            A::ChildWorkflowExecutionCompletedEventAttributes(x) => {
                s.children.remove(&x.initiated_event_id);
            }
            A::ChildWorkflowExecutionFailedEventAttributes(x) => {
                s.children.remove(&x.initiated_event_id);
            }
            A::ChildWorkflowExecutionCanceledEventAttributes(x) => {
                s.children.remove(&x.initiated_event_id);
            }
            A::ChildWorkflowExecutionTimedOutEventAttributes(x) => {
                s.children.remove(&x.initiated_event_id);
            }
            A::ChildWorkflowExecutionTerminatedEventAttributes(x) => {
                s.children.remove(&x.initiated_event_id);
            }
            A::UpsertWorkflowSearchAttributesEventAttributes(x) => {
                if let Some(sa) = &x.search_attributes {
                    let cur = s.search_attributes.get_or_insert_with(Default::default);
                    for (k, v) in &sa.indexed_fields {
                        if v.data.is_empty() || v.data == b"null" {
                            cur.indexed_fields.remove(k);
                        } else {
                            cur.indexed_fields.insert(k.clone(), v.clone());
                        }
                    }
                }
            }
            A::WorkflowPropertiesModifiedEventAttributes(x) => {
                if let Some(m) = &x.upserted_memo {
                    let cur = s.memo.get_or_insert_with(Default::default);
                    for (k, v) in &m.fields {
                        if v.data.is_empty() || v.data == b"null" {
                            cur.fields.remove(k);
                        } else {
                            cur.fields.insert(k.clone(), v.clone());
                        }
                    }
                }
            }
            A::WorkflowExecutionSignaledEventAttributes(x) => {
                if !x.request_id.is_empty() {
                    s.signal_request_ids.push(x.request_id.clone());
                }
            }
            A::WorkflowExecutionCancelRequestedEventAttributes(_) => {
                s.cancel_requested = true;
            }
            A::WorkflowExecutionUpdateAdmittedEventAttributes(x) => {
                if let Some(id) = x
                    .request
                    .as_ref()
                    .and_then(|r| r.meta.as_ref())
                    .map(|m| m.update_id.clone())
                {
                    s.updates.entry(id).or_default().admitted_event_id = e.event_id;
                }
            }
            A::WorkflowExecutionUpdateAcceptedEventAttributes(x) => {
                s.updates.insert(
                    x.protocol_instance_id.clone(),
                    UpdateInfo {
                        accepted_event_id: e.event_id,
                        ..Default::default()
                    },
                );
            }
            A::WorkflowExecutionUpdateCompletedEventAttributes(x) => {
                let id = x.meta.as_ref().map(|m| m.update_id.clone()).unwrap_or_default();
                if let Some(i) = s.updates.get_mut(&id) {
                    i.completed_event_id = e.event_id;
                }
            }
            A::WorkflowExecutionOptionsUpdatedEventAttributes(x) if x.priority.is_some() => {
                s.priority = x.priority.clone();
            }
            _ => {}
        }
    }
    let mut ex = Execution::new(s, events);
    // Nothing of the new run is saved yet.
    ex.saved_events = 0;
    ex
}
