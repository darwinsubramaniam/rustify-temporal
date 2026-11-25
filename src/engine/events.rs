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
        A::WorkflowExecutionUpdateAcceptedEventAttributes(_) => E::WorkflowExecutionUpdateAccepted,
        A::WorkflowExecutionUpdateCompletedEventAttributes(_) => E::WorkflowExecutionUpdateCompleted,
        A::WorkflowExecutionOptionsUpdatedEventAttributes(_) => E::WorkflowExecutionOptionsUpdated,
        A::WorkflowExecutionUpdateAdmittedEventAttributes(_) => E::WorkflowExecutionUpdateAdmitted,
        _ => E::Unspecified,
    }
}

pub fn make_event(id: i64, a: A) -> history::HistoryEvent {
    history::HistoryEvent {
        event_id: id,
        event_time: Some(now_ts()),
        event_type: event_type(&a) as i32,
        task_id: id,
        attributes: Some(a),
        ..Default::default()
    }
}

static WRITE_GEN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// A new write begins (the Go server's transaction): the next event any run gets starts a new
/// history batch.
pub fn new_write() {
    WRITE_GEN.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

/// Append an event to the history now.
pub fn push(ex: &mut Execution, a: A) -> i64 {
    let id = ex.s.next_event_id;
    ex.s.next_event_id += 1;
    // Batches as the Go server writes them: one per write, and a workflow task started event
    // always ends its batch.
    let gen_now = WRITE_GEN.load(std::sync::atomic::Ordering::Relaxed);
    if ex.batch_gen != gen_now || ex.new_batch || ex.s.batch_starts.is_empty() {
        ex.s.batch_starts.push(id);
        ex.batch_gen = gen_now;
        ex.new_batch = false;
    }
    if matches!(a, A::WorkflowTaskStartedEventAttributes(_)) {
        ex.new_batch = true;
    }
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
    convert_speculative(ex);
    if wft_running(ex) {
        ex.s.buffered.push(make_event(0, a));
        0
    } else {
        push(ex, a)
    }
}

/// A speculative workflow task becomes a normal one as soon as anything else changes the run:
/// its scheduled (and started) events are written now, as the Go server does on its next write.
pub fn convert_speculative(ex: &mut Execution) {
    let Some(w) = ex.s.wft.clone().filter(|w| w.speculative) else {
        return;
    };
    write_unwritten_wft_events(ex, &w);
    if let Some(w) = ex.s.wft.as_mut() {
        w.speculative = false;
    }
}

/// Write the scheduled (and, once started, started) events of a transient or speculative task.
pub fn write_unwritten_wft_events(ex: &mut Execution, w: &WftInfo) {
    let sticky = !ex.s.sticky_queue.is_empty() && w.queue == ex.s.sticky_queue;
    let attrs = wft_scheduled_attrs(ex, &w.queue, sticky, w.timeout_ms, w.attempt);
    push(ex, A::WorkflowTaskScheduledEventAttributes(attrs));
    if w.started_id > 0 {
        let reasons = suggest_continue_as_new(ex);
        push(ex, A::WorkflowTaskStartedEventAttributes(wft_started_attrs(w, reasons)));
    }
}

/// How long a scheduled task may wait for a worker: the sticky timeout on a sticky queue, 5 s
/// for a speculative task on the normal queue (Go server).
pub fn schedule_to_start_ms(ex: &Execution) -> i64 {
    let sticky =
        ex.s.wft
            .as_ref()
            .is_some_and(|w| !ex.s.sticky_queue.is_empty() && w.queue == ex.s.sticky_queue);
    if sticky {
        if ex.s.sticky_timeout_ms > 0 {
            ex.s.sticky_timeout_ms
        } else {
            DEFAULT_STICKY_TIMEOUT_MS
        }
    } else {
        super::update::SPECULATIVE_SCHEDULE_TO_START_MS
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
        let timeout = wft_timeout_for_attempt(base_wft_timeout(ex), attempt);
        // Like the Go server, a retry (attempt > 1) is "transient": its scheduled and started
        // events are only written if it completes, so a workflow failing in a loop doesn't grow
        // its history. It takes the next event id without writing it.
        let transient = attempt > 1;
        let sid = if transient {
            ex.s.next_event_id
        } else {
            push(
                ex,
                A::WorkflowTaskScheduledEventAttributes(wft_scheduled_attrs(ex, &queue, sticky, timeout, attempt)),
            )
        };
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
            transient,
            identity: String::new(),
            history_size_at_start: 0,
            speculative: false,
        });
        if transient {
            ex.notify.notify_waiters();
        }
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
        convert_speculative(ex);
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

/// The run's configured workflow task timeout (10 s by default, at most 120 s).
pub fn base_wft_timeout(ex: &Execution) -> i64 {
    let t = dur_ms(&ex.s.wft_timeout);
    if t <= 0 {
        DEFAULT_WFT_TIMEOUT_MS
    } else {
        t.min(MAX_WFT_TIMEOUT_MS)
    }
}

/// The Go server slows down repeated retries by lengthening their timeout instead of delaying
/// them: from attempt 4 on, 5 s doubling per attempt, at most 10 minutes, is added.
pub fn wft_timeout_for_attempt(base: i64, attempt: i32) -> i64 {
    const MIN_ATTEMPTS: i32 = 3;
    if attempt <= MIN_ATTEMPTS {
        return base;
    }
    let n = (attempt - MIN_ATTEMPTS - 1).min(30) as u32;
    base + (5_000i64.saturating_mul(1i64 << n)).min(600_000)
}

pub fn wft_scheduled_attrs(
    ex: &Execution,
    queue: &str,
    sticky: bool,
    timeout: i64,
    attempt: i32,
) -> history::WorkflowTaskScheduledEventAttributes {
    history::WorkflowTaskScheduledEventAttributes {
        task_queue: Some(taskqueue::TaskQueue {
            name: queue.to_string(),
            kind: if sticky {
                enums::TaskQueueKind::Sticky as i32
            } else {
                enums::TaskQueueKind::Normal as i32
            },
            normal_name: if sticky { ex.s.task_queue.clone() } else { String::new() },
        }),
        start_to_close_timeout: Some(dur_from_ms(timeout)),
        attempt,
    }
}

/// Continue-as-new hints for a starting workflow task (the Go server's 4096 events / 4 MB).
pub fn suggest_continue_as_new(ex: &Execution) -> Vec<i32> {
    let mut reasons = Vec::new();
    if ex.s.history_size >= SUGGEST_CAN_HISTORY_SIZE {
        reasons.push(enums::SuggestContinueAsNewReason::HistorySizeTooLarge as i32);
    }
    if ex.s.next_event_id >= SUGGEST_CAN_HISTORY_COUNT {
        reasons.push(enums::SuggestContinueAsNewReason::TooManyHistoryEvents as i32);
    }
    if super::update::suggest_can_for_updates(ex) {
        reasons.push(enums::SuggestContinueAsNewReason::TooManyUpdates as i32);
    }
    reasons
}

pub fn wft_started_attrs(w: &WftInfo, reasons: Vec<i32>) -> history::WorkflowTaskStartedEventAttributes {
    history::WorkflowTaskStartedEventAttributes {
        scheduled_event_id: w.scheduled_id,
        identity: w.identity.clone(),
        request_id: w.request_id.clone(),
        suggest_continue_as_new: !reasons.is_empty(),
        suggest_continue_as_new_reasons: reasons,
        history_size_bytes: w.history_size_at_start,
        ..Default::default()
    }
}

/// The unwritten events of a transient or speculative workflow task (scheduled, and started
/// once a worker has it), shown to workers and history readers after the written history, like
/// the Go server.
pub fn transient_events(ex: &Execution) -> Vec<history::HistoryEvent> {
    // Only while it still sits at the end of the history: once other events were written after
    // it was scheduled, it will start as a normal task instead.
    let Some(w) =
        ex.s.wft
            .as_ref()
            .filter(|w| (w.transient || w.speculative) && w.scheduled_id == ex.s.next_event_id)
    else {
        return Vec::new();
    };
    let sticky = !ex.s.sticky_queue.is_empty() && w.queue == ex.s.sticky_queue;
    let mut out = vec![make_event(
        w.scheduled_id,
        A::WorkflowTaskScheduledEventAttributes(wft_scheduled_attrs(ex, &w.queue, sticky, w.timeout_ms, w.attempt)),
    )];
    out[0].event_time = Some(ts_from_ms(w.scheduled_ms));
    if w.started_id > 0 {
        let mut e = make_event(
            w.started_id,
            A::WorkflowTaskStartedEventAttributes(wft_started_attrs(w, suggest_continue_as_new(ex))),
        );
        e.event_time = Some(ts_from_ms(w.started_ms));
        out.push(e);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::wft_timeout_for_attempt;

    #[test]
    fn retries_get_longer_timeouts_like_the_go_server() {
        assert_eq!(wft_timeout_for_attempt(10_000, 1), 10_000);
        assert_eq!(wft_timeout_for_attempt(10_000, 3), 10_000);
        assert_eq!(wft_timeout_for_attempt(10_000, 4), 15_000);
        assert_eq!(wft_timeout_for_attempt(10_000, 5), 20_000);
        assert_eq!(wft_timeout_for_attempt(10_000, 40), 610_000);
    }
}
