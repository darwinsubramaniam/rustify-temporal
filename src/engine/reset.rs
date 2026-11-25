//! ResetWorkflowExecution, following the Go server's workflow resetter: a new run is rebuilt
//! from the base run's history up to the reset point, the workflow task there fails with cause
//! RESET_WORKFLOW, events that came later (signals, updates, options, child results) are
//! applied again, the current run is terminated, and the reset run becomes current.

use super::events::{flush_buffered, push};
use super::*;
use crate::state::ACT_SCHEDULED;
use history::history_event::Attributes as A;

const IDENTITY_RESETTER: &str = "history-resetter";

fn reset_failure(reason: &str, details: Option<common::Payloads>) -> failure::Failure {
    failure::Failure {
        message: reason.to_string(),
        failure_info: Some(failure::failure::FailureInfo::ResetWorkflowFailureInfo(
            failure::ResetWorkflowFailureInfo {
                last_heartbeat_details: details,
            },
        )),
        ..Default::default()
    }
}

impl Inner {
    pub fn reset_workflow(&mut self, r: wsv1::ResetWorkflowExecutionRequest) -> R<String> {
        use enums::ResetReapplyExcludeType as X;
        use enums::ResetReapplyType as T;
        let ns = r.namespace.clone();
        let (wid, rid) = r
            .workflow_execution
            .as_ref()
            .map(|e| (e.workflow_id.clone(), e.run_id.clone()))
            .unwrap_or_default();
        let base_key = self.resolve(&ns, &wid, &rid)?;
        if !self.ensure_loaded(&base_key)? {
            return Err(wf_not_found(&wid, &rid));
        }
        let base_next = self.execs[&base_key].s.next_event_id;
        let finish = r.workflow_task_finish_event_id;
        if finish <= 1 || finish >= base_next {
            return Err(invalid(
                "Workflow task finish ID must be > 1 && <= workflow last event ID.",
            ));
        }
        let current = self.current.get(&(ns.clone(), wid.clone())).cloned();
        // A retried reset request gets the run it already created.
        if let Some(cur) = &current {
            let ck = (ns.clone(), cur.clone());
            if self.ensure_loaded(&ck)? && self.execs[&ck].s.start_request_id == r.request_id {
                return Ok(cur.clone());
            }
        }
        let mut exclude_signal = false;
        let mut exclude_update = false;
        match T::try_from(r.reset_reapply_type).unwrap_or(T::Unspecified) {
            T::Signal => exclude_update = true,
            T::None => {
                exclude_signal = true;
                exclude_update = true;
            }
            _ => {}
        }
        for x in &r.reset_reapply_exclude_types {
            match X::try_from(*x).unwrap_or(X::Unspecified) {
                X::Signal => exclude_signal = true,
                X::Update => exclude_update = true,
                _ => {}
            }
        }

        let upto = finish - 1;
        let new_rid = new_id();
        let mut reset = super::replay::rebuild(&self.execs[&base_key], upto, &new_rid, &r.request_id);
        let Some(mut w) = reset.s.wft.clone() else {
            return Err(invalid(format!(
                "Can only reset workflow to event ID in range [WorkflowTaskScheduled +1, WorkflowTaskStarted + 1]: {}",
                upto + 1
            )));
        };

        // The current run ends first (its last events may still have to be applied again).
        if let Some(cur) = &current {
            let ck = (ns.clone(), cur.clone());
            if self.execs.get(&ck).is_some_and(|e| e.s.is_open()) {
                self.terminate_key(&ck, &r.reason, None, IDENTITY_RESETTER)?;
            }
        }

        super::events::new_write();
        if w.started_id == 0 {
            w.request_id = new_id();
            w.started_id = push(
                &mut reset,
                A::WorkflowTaskStartedEventAttributes(history::WorkflowTaskStartedEventAttributes {
                    scheduled_event_id: w.scheduled_id,
                    identity: HISTORY_SERVICE_IDENTITY.into(),
                    request_id: w.request_id.clone(),
                    ..Default::default()
                }),
            );
        }
        push(
            &mut reset,
            A::WorkflowTaskFailedEventAttributes(history::WorkflowTaskFailedEventAttributes {
                scheduled_event_id: w.scheduled_id,
                started_event_id: w.started_id,
                cause: enums::WorkflowTaskFailedCause::ResetWorkflow as i32,
                failure: Some(reset_failure(&r.reason, None)),
                identity: HISTORY_SERVICE_IDENTITY.into(),
                base_run_id: base_key.1.clone(),
                new_run_id: new_rid.clone(),
                ..Default::default()
            }),
        );
        reset.s.wft = None;

        // In-flight activities: started ones fail, the rest are scheduled again from now.
        let now = now_ms();
        let started: Vec<i64> = reset
            .s
            .activities
            .values()
            .filter(|a| a.state != ACT_SCHEDULED)
            .map(|a| a.scheduled_id)
            .collect();
        for sid in started {
            let a = reset.s.activities.remove(&sid).expect("present");
            push(
                &mut reset,
                A::ActivityTaskFailedEventAttributes(history::ActivityTaskFailedEventAttributes {
                    failure: Some(reset_failure(&r.reason, a.heartbeat_details.clone())),
                    scheduled_event_id: sid,
                    started_event_id: 0,
                    identity: a.identity.clone(),
                    retry_state: enums::RetryState::NonRetryableFailure as i32,
                    ..Default::default()
                }),
            );
        }
        for a in reset.s.activities.values_mut() {
            a.scheduled_ms = now;
            a.attempt_scheduled_ms = now;
        }

        // Events after the reset point, from the base run and the runs it continued into.
        let mut chain: Vec<history::HistoryEvent> = self.execs[&base_key]
            .events
            .iter()
            .filter(|e| e.event_id > upto)
            .cloned()
            .collect();
        let mut next_run = continued_run(&chain);
        let mut guard = 0;
        while let Some(run) = next_run.take() {
            guard += 1;
            let k = (ns.clone(), run);
            if guard > 1000 || !self.ensure_loaded(&k).unwrap_or(false) {
                break;
            }
            let evs = self.execs[&k].events.clone();
            next_run = continued_run(&evs);
            chain.extend(evs);
        }
        for e in chain {
            let Some(a) = e.attributes else { continue };
            match a {
                A::WorkflowExecutionSignaledEventAttributes(x) if !exclude_signal => {
                    push(
                        &mut reset,
                        A::WorkflowExecutionSignaledEventAttributes(
                            history::WorkflowExecutionSignaledEventAttributes {
                                signal_name: x.signal_name,
                                input: x.input,
                                identity: x.identity,
                                header: x.header,
                                request_id: x.request_id,
                                ..Default::default()
                            },
                        ),
                    );
                }
                A::WorkflowExecutionUpdateAdmittedEventAttributes(x) if !exclude_update => {
                    let id = x
                        .request
                        .as_ref()
                        .and_then(|q| q.meta.as_ref())
                        .map(|m| m.update_id.clone());
                    readmit_update(&mut reset, id, x.request, x.origin);
                }
                A::WorkflowExecutionUpdateAcceptedEventAttributes(x) if !exclude_update => {
                    if x.accepted_request.is_none() {
                        continue;
                    }
                    readmit_update(
                        &mut reset,
                        Some(x.protocol_instance_id),
                        x.accepted_request,
                        enums::UpdateAdmittedEventOrigin::Reapply as i32,
                    );
                }
                A::WorkflowExecutionOptionsUpdatedEventAttributes(x) => {
                    if x.priority.is_some() {
                        reset.s.priority = x.priority.clone();
                    }
                    push(&mut reset, A::WorkflowExecutionOptionsUpdatedEventAttributes(x));
                }
                a @ (A::ChildWorkflowExecutionStartedEventAttributes(_)
                | A::StartChildWorkflowExecutionFailedEventAttributes(_)
                | A::ChildWorkflowExecutionCompletedEventAttributes(_)
                | A::ChildWorkflowExecutionFailedEventAttributes(_)
                | A::ChildWorkflowExecutionCanceledEventAttributes(_)
                | A::ChildWorkflowExecutionTimedOutEventAttributes(_)
                | A::ChildWorkflowExecutionTerminatedEventAttributes(_)) => {
                    reapply_child_event(&mut reset, a);
                }
                _ => {}
            }
        }

        for op in &r.post_reset_operations {
            use workflow_pb::post_reset_operation::Variant;
            if let Some(Variant::UpdateWorkflowOptions(o)) = &op.variant {
                let fields: Vec<String> = o.update_mask.as_ref().map(|m| m.paths.clone()).unwrap_or_default();
                let fields: Vec<String> = fields.iter().map(|p| camel_path(p)).collect();
                let cur = workflow_pb::WorkflowExecutionOptions {
                    priority: reset.s.priority.clone(),
                    ..Default::default()
                };
                let req = o.workflow_execution_options.clone().unwrap_or_default();
                let merged = super::workflow::merge_options(&cur, &req, &fields)?;
                if merged != cur {
                    if merged.priority.is_some() {
                        reset.s.priority = merged.priority.clone();
                    }
                    push(
                        &mut reset,
                        A::WorkflowExecutionOptionsUpdatedEventAttributes(
                            history::WorkflowExecutionOptionsUpdatedEventAttributes {
                                unset_versioning_override: true,
                                priority: merged.priority,
                                ..Default::default()
                            },
                        ),
                    );
                }
            }
        }

        // The next task is a fresh attempt 1 (the reset failure doesn't count as a retry).
        flush_buffered(&mut reset);
        let attempt = 1;
        self.current.insert((ns.clone(), wid.clone()), new_rid.clone());
        self.rearm(&mut reset);
        self.schedule_wft(&mut reset, attempt);
        self.put(reset);
        Ok(new_rid)
    }
}

/// The run a history continued as new into, if it did.
fn continued_run(events: &[history::HistoryEvent]) -> Option<String> {
    match events.last().and_then(|e| e.attributes.as_ref()) {
        Some(A::WorkflowExecutionContinuedAsNewEventAttributes(x)) if !x.new_execution_run_id.is_empty() => {
            Some(x.new_execution_run_id.clone())
        }
        _ => None,
    }
}

fn camel_path(p: &str) -> String {
    p.split('.')
        .map(|seg| {
            let mut out = String::new();
            let mut up = false;
            for ch in seg.chars() {
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
        })
        .collect::<Vec<_>>()
        .join(".")
}

/// Admit an update again as a WorkflowExecutionUpdateAdmitted event carrying its request.
fn readmit_update(ex: &mut Execution, id: Option<String>, request: Option<update_pb::Request>, origin: i32) {
    let Some(id) = id else { return };
    let eid = push(
        ex,
        A::WorkflowExecutionUpdateAdmittedEventAttributes(history::WorkflowExecutionUpdateAdmittedEventAttributes {
            request,
            origin,
        }),
    );
    ex.s.updates.entry(id.clone()).or_default().admitted_event_id = eid;
    super::update::readmit(ex, &id);
}

/// A child's result after the reset point, for a child the reset run still waits on.
fn reapply_child_event(ex: &mut Execution, a: A) {
    let initiated = match &a {
        A::ChildWorkflowExecutionStartedEventAttributes(x) => x.initiated_event_id,
        A::StartChildWorkflowExecutionFailedEventAttributes(x) => x.initiated_event_id,
        A::ChildWorkflowExecutionCompletedEventAttributes(x) => x.initiated_event_id,
        A::ChildWorkflowExecutionFailedEventAttributes(x) => x.initiated_event_id,
        A::ChildWorkflowExecutionCanceledEventAttributes(x) => x.initiated_event_id,
        A::ChildWorkflowExecutionTimedOutEventAttributes(x) => x.initiated_event_id,
        A::ChildWorkflowExecutionTerminatedEventAttributes(x) => x.initiated_event_id,
        _ => return,
    };
    let Some(child) = ex.s.children.get(&initiated).cloned() else {
        return;
    };
    let started = matches!(a, A::ChildWorkflowExecutionStartedEventAttributes(_));
    if started && child.started_id != 0 {
        return;
    }
    let mut a = a;
    // Results point at the reset run's own started event.
    match &mut a {
        A::ChildWorkflowExecutionCompletedEventAttributes(x) => x.started_event_id = child.started_id,
        A::ChildWorkflowExecutionFailedEventAttributes(x) => x.started_event_id = child.started_id,
        A::ChildWorkflowExecutionCanceledEventAttributes(x) => x.started_event_id = child.started_id,
        A::ChildWorkflowExecutionTimedOutEventAttributes(x) => x.started_event_id = child.started_id,
        A::ChildWorkflowExecutionTerminatedEventAttributes(x) => x.started_event_id = child.started_id,
        _ => {}
    }
    let run_id = match &a {
        A::ChildWorkflowExecutionStartedEventAttributes(x) => x
            .workflow_execution
            .as_ref()
            .map(|w| w.run_id.clone())
            .unwrap_or_default(),
        _ => String::new(),
    };
    let eid = push(ex, a);
    if started {
        if let Some(c) = ex.s.children.get_mut(&initiated) {
            c.started_id = eid;
            c.run_id = run_id;
        }
    } else {
        ex.s.children.remove(&initiated);
    }
}
