//! Appending history events, buffering events that arrive during a running workflow task,
//! and scheduling workflow tasks.

use prost::Message;

use super::*;
use crate::state::WftInfo;
use history::history_event::Attributes as A;

pub fn event_type(a: &A) -> enums::EventType {
    use enums::EventType as E;
    match a {
        A::WorkflowExecutionStartedEventAttributes(_) => E::WorkflowExecutionStarted,
        A::WorkflowExecutionCompletedEventAttributes(_) => E::WorkflowExecutionCompleted,
        A::WorkflowExecutionFailedEventAttributes(_) => E::WorkflowExecutionFailed,
        A::WorkflowExecutionTimedOutEventAttributes(_) => E::WorkflowExecutionTimedOut,
        A::WorkflowTaskScheduledEventAttributes(_) => E::WorkflowTaskScheduled,
        A::WorkflowTaskStartedEventAttributes(_) => E::WorkflowTaskStarted,
        A::WorkflowTaskCompletedEventAttributes(_) => E::WorkflowTaskCompleted,
        A::WorkflowTaskTimedOutEventAttributes(_) => E::WorkflowTaskTimedOut,
        A::WorkflowTaskFailedEventAttributes(_) => E::WorkflowTaskFailed,
        A::ActivityTaskScheduledEventAttributes(_) => E::ActivityTaskScheduled,
        A::ActivityTaskStartedEventAttributes(_) => E::ActivityTaskStarted,
        A::ActivityTaskCompletedEventAttributes(_) => E::ActivityTaskCompleted,
        A::ActivityTaskFailedEventAttributes(_) => E::ActivityTaskFailed,
        A::ActivityTaskTimedOutEventAttributes(_) => E::ActivityTaskTimedOut,
        A::TimerStartedEventAttributes(_) => E::TimerStarted,
        A::TimerFiredEventAttributes(_) => E::TimerFired,
        A::ActivityTaskCancelRequestedEventAttributes(_) => E::ActivityTaskCancelRequested,
        A::ActivityTaskCanceledEventAttributes(_) => E::ActivityTaskCanceled,
        A::TimerCanceledEventAttributes(_) => E::TimerCanceled,
        A::MarkerRecordedEventAttributes(_) => E::MarkerRecorded,
        A::WorkflowExecutionSignaledEventAttributes(_) => E::WorkflowExecutionSignaled,
        A::WorkflowExecutionTerminatedEventAttributes(_) => E::WorkflowExecutionTerminated,
        A::WorkflowExecutionCancelRequestedEventAttributes(_) => E::WorkflowExecutionCancelRequested,
        A::WorkflowExecutionCanceledEventAttributes(_) => E::WorkflowExecutionCanceled,
        A::RequestCancelExternalWorkflowExecutionInitiatedEventAttributes(_) => {
            E::RequestCancelExternalWorkflowExecutionInitiated
        }
        A::RequestCancelExternalWorkflowExecutionFailedEventAttributes(_) => {
            E::RequestCancelExternalWorkflowExecutionFailed
        }
        A::ExternalWorkflowExecutionCancelRequestedEventAttributes(_) => E::ExternalWorkflowExecutionCancelRequested,
        A::WorkflowExecutionContinuedAsNewEventAttributes(_) => E::WorkflowExecutionContinuedAsNew,
        A::StartChildWorkflowExecutionInitiatedEventAttributes(_) => E::StartChildWorkflowExecutionInitiated,
        A::StartChildWorkflowExecutionFailedEventAttributes(_) => E::StartChildWorkflowExecutionFailed,
        A::ChildWorkflowExecutionStartedEventAttributes(_) => E::ChildWorkflowExecutionStarted,
        A::ChildWorkflowExecutionCompletedEventAttributes(_) => E::ChildWorkflowExecutionCompleted,
        A::ChildWorkflowExecutionFailedEventAttributes(_) => E::ChildWorkflowExecutionFailed,
        A::ChildWorkflowExecutionCanceledEventAttributes(_) => E::ChildWorkflowExecutionCanceled,
        A::ChildWorkflowExecutionTimedOutEventAttributes(_) => E::ChildWorkflowExecutionTimedOut,
        A::ChildWorkflowExecutionTerminatedEventAttributes(_) => E::ChildWorkflowExecutionTerminated,
        A::SignalExternalWorkflowExecutionInitiatedEventAttributes(_) => E::SignalExternalWorkflowExecutionInitiated,
        A::SignalExternalWorkflowExecutionFailedEventAttributes(_) => E::SignalExternalWorkflowExecutionFailed,
        A::ExternalWorkflowExecutionSignaledEventAttributes(_) => E::ExternalWorkflowExecutionSignaled,
        A::UpsertWorkflowSearchAttributesEventAttributes(_) => E::UpsertWorkflowSearchAttributes,
        A::WorkflowPropertiesModifiedEventAttributes(_) => E::WorkflowPropertiesModified,
        _ => E::Unspecified,
    }
}

fn make_event(id: i64, a: A) -> history::HistoryEvent {
    history::HistoryEvent {
        event_id: id,
        event_time: Some(now_ts()),
        event_type: event_type(&a) as i32,
        task_id: id,
        attributes: Some(a),
        ..Default::default()
    }
}

/// Append an event to the history now.
pub fn push(ex: &mut Execution, a: A) -> i64 {
    let id = ex.s.next_event_id;
    ex.s.next_event_id += 1;
    let ev = make_event(id, a);
    ex.s.history_size += ev.encoded_len() as i64;
    ex.s.transitions += 1;
    ex.events.push(ev);
    ex.notify.notify_waiters();
    id
}

pub fn wft_running(ex: &Execution) -> bool {
    ex.s.wft.as_ref().is_some_and(|w| w.started_id > 0)
}

/// Append an event that came from outside the workflow's code. While a workflow task is
/// running it is held back and written right after that task completes. Returns the event id,
/// or 0 when buffered.
pub fn push_external(ex: &mut Execution, a: A) -> i64 {
    if wft_running(ex) {
        ex.s.buffered.push(make_event(0, a));
        0
    } else {
        push(ex, a)
    }
}

/// Write the held-back events, fixing references between them (an activity's completion
/// points at its started event, a child's completion at the child's started event).
pub fn flush_buffered(ex: &mut Execution) -> bool {
    if ex.s.buffered.is_empty() {
        return false;
    }
    let buf = std::mem::take(&mut ex.s.buffered);
    let mut act_started: std::collections::HashMap<i64, i64> = Default::default();
    let mut child_started: std::collections::HashMap<i64, i64> = Default::default();
    for ev in buf {
        let mut a = ev.attributes.expect("buffered event has attributes");
        match &mut a {
            A::ActivityTaskCompletedEventAttributes(x) if x.started_event_id <= 0 => {
                x.started_event_id = *act_started.get(&x.scheduled_event_id).unwrap_or(&0)
            }
            A::ActivityTaskFailedEventAttributes(x) if x.started_event_id <= 0 => {
                x.started_event_id = *act_started.get(&x.scheduled_event_id).unwrap_or(&0)
            }
            A::ActivityTaskTimedOutEventAttributes(x) if x.started_event_id <= 0 => {
                x.started_event_id = *act_started.get(&x.scheduled_event_id).unwrap_or(&0)
            }
            A::ActivityTaskCanceledEventAttributes(x) if x.started_event_id <= 0 => {
                x.started_event_id = *act_started.get(&x.scheduled_event_id).unwrap_or(&0)
            }
            A::ChildWorkflowExecutionCompletedEventAttributes(x) if x.started_event_id <= 0 => {
                x.started_event_id = *child_started.get(&x.initiated_event_id).unwrap_or(&0)
            }
            A::ChildWorkflowExecutionFailedEventAttributes(x) if x.started_event_id <= 0 => {
                x.started_event_id = *child_started.get(&x.initiated_event_id).unwrap_or(&0)
            }
            A::ChildWorkflowExecutionCanceledEventAttributes(x) if x.started_event_id <= 0 => {
                x.started_event_id = *child_started.get(&x.initiated_event_id).unwrap_or(&0)
            }
            A::ChildWorkflowExecutionTimedOutEventAttributes(x) if x.started_event_id <= 0 => {
                x.started_event_id = *child_started.get(&x.initiated_event_id).unwrap_or(&0)
            }
            A::ChildWorkflowExecutionTerminatedEventAttributes(x) if x.started_event_id <= 0 => {
                x.started_event_id = *child_started.get(&x.initiated_event_id).unwrap_or(&0)
            }
            _ => {}
        }
        let scheduled = match &a {
            A::ActivityTaskStartedEventAttributes(x) => Some((true, x.scheduled_event_id)),
            A::ChildWorkflowExecutionStartedEventAttributes(x) => Some((false, x.initiated_event_id)),
            _ => None,
        };
        let id = push(ex, a);
        match scheduled {
            Some((true, s)) => {
                act_started.insert(s, id);
            }
            Some((false, s)) => {
                if let Some(c) = ex.s.children.get_mut(&s) {
                    c.started_id = id;
                }
                child_started.insert(s, id);
            }
            None => {}
        }
    }
    true
}

impl Inner {
    /// Schedule a workflow task (on the sticky queue when the worker asked for one).
    pub fn schedule_wft(&mut self, ex: &mut Execution, attempt: i32) {
        let sticky = !ex.s.sticky_queue.is_empty();
        let queue = if sticky {
            ex.s.sticky_queue.clone()
        } else {
            ex.s.task_queue.clone()
        };
        let timeout = {
            let t = dur_ms(&ex.s.wft_timeout);
            if t <= 0 {
                DEFAULT_WFT_TIMEOUT_MS
            } else {
                t.min(MAX_WFT_TIMEOUT_MS)
            }
        };
        let tq = taskqueue::TaskQueue {
            name: queue.clone(),
            kind: if sticky {
                enums::TaskQueueKind::Sticky as i32
            } else {
                enums::TaskQueueKind::Normal as i32
            },
            normal_name: if sticky { ex.s.task_queue.clone() } else { String::new() },
        };
        let sid = push(
            ex,
            A::WorkflowTaskScheduledEventAttributes(history::WorkflowTaskScheduledEventAttributes {
                task_queue: Some(tq),
                start_to_close_timeout: Some(dur_from_ms(timeout)),
                attempt,
            }),
        );
        let now = now_ms();
        ex.s.wft = Some(WftInfo {
            scheduled_id: sid,
            started_id: 0,
            attempt,
            queue: queue.clone(),
            scheduled_ms: now,
            started_ms: 0,
            timeout_ms: timeout,
            request_id: String::new(),
            original_scheduled_ms: now,
        });
        let key = ex.key();
        self.enqueue(
            &key.0,
            &queue,
            QKind::Workflow,
            TaskRef::Wft {
                run_id: key.1.clone(),
                scheduled_id: sid,
            },
        );
        if sticky {
            let st = if ex.s.sticky_timeout_ms > 0 {
                ex.s.sticky_timeout_ms
            } else {
                DEFAULT_STICKY_TIMEOUT_MS
            };
            self.add_timer(now + st, TimerKind::StickyTimeout { key, scheduled_id: sid });
        }
    }

    /// Make sure the workflow gets a task to see new events.
    pub fn ensure_wft(&mut self, ex: &mut Execution) {
        if ex.s.is_open() && ex.s.wft.is_none() && ex.s.wft_due_ms == 0 {
            self.schedule_wft(ex, 1);
        }
    }

    /// Before force-closing a run (terminate, timeout), close out a running workflow task and
    /// write anything held back.
    pub fn force_close_wft(&mut self, ex: &mut Execution) {
        if let Some(w) = ex.s.wft.clone()
            && w.started_id > 0
        {
            push(
                ex,
                A::WorkflowTaskFailedEventAttributes(history::WorkflowTaskFailedEventAttributes {
                    scheduled_event_id: w.scheduled_id,
                    started_event_id: w.started_id,
                    cause: enums::WorkflowTaskFailedCause::ForceCloseCommand as i32,
                    identity: "server".into(),
                    ..Default::default()
                }),
            );
        }
        ex.s.wft = None;
        ex.s.wft_due_ms = 0;
        flush_buffered(ex);
    }
}
