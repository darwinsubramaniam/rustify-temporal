//! Workflow updates, following the Go server's update registry.
//!
//! An update request is held in memory (never written) until the workflow accepts it: it goes
//! to the worker as a protocol message on a workflow task. A rejected update leaves no trace in
//! the history; an accepted one writes `WorkflowExecutionUpdateAccepted`, and its result writes
//! `WorkflowExecutionUpdateCompleted`. Callers wait on a watch channel for the stage they asked
//! for. If no workflow task is pending, a speculative one delivers the update: its events are
//! only written if the workflow does something with it.

use std::collections::HashMap;

use prost::Message;
use tokio::sync::watch;

use super::events::push;
use super::*;
use crate::state::{UpdateInfo, WftInfo};
use history::history_event::Attributes as A;

// Go server limits (dynamic config defaults).
pub const MAX_IN_FLIGHT_UPDATES: usize = 10;
pub const MAX_IN_FLIGHT_UPDATE_PAYLOADS: usize = 20 * 1024 * 1024;
pub const MAX_TOTAL_UPDATES: usize = 2000;
/// 90% of the total limit: workflow tasks then suggest continue-as-new.
pub const SUGGEST_CAN_UPDATES: usize = 1800;
/// From this workflow task attempt on, new updates are refused.
pub const FAIL_UPDATE_WFT_ATTEMPT: i32 = 3;
/// A speculative task on the normal queue that no worker picks up becomes a normal task.
pub const SPECULATIVE_SCHEDULE_TO_START_MS: i64 = 5_000;
/// A speculative task that shipped more events than this is written even if the worker can
/// take them again.
pub const DISCARD_SPECULATIVE_MAX_EVENTS: i64 = 10;
/// How long an update call waits before answering with the stage reached so far.
pub const UPDATE_LONG_POLL_MS: i64 = 20_000;

const UPDATE_PROTOCOL: &str = "temporal.api.update.v1";
const REQUEST_TYPE: &str = "temporal.api.update.v1.Request";
const ACCEPTANCE_TYPE: &str = "temporal.api.update.v1.Acceptance";
const REJECTION_TYPE: &str = "temporal.api.update.v1.Rejection";
const RESPONSE_TYPE: &str = "temporal.api.update.v1.Response";

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum UState {
    Admitted,
    Sent,
    Accepted,
    Completed,
    Aborted,
}

impl UState {
    fn name(self) -> &'static str {
        match self {
            UState::Admitted => "Admitted",
            UState::Sent => "Sent",
            UState::Accepted => "Accepted",
            UState::Completed => "Completed",
            UState::Aborted => "Aborted",
        }
    }
}

type AcceptedRes = Result<Option<failure::Failure>, Status>;
type OutcomeRes = Result<update_pb::Outcome, Status>;

/// What a waiting caller can see. Each part is set once.
#[derive(Clone, Default, Debug)]
pub struct UpdStatus {
    /// `Ok(None)` accepted, `Ok(Some(f))` rejected.
    pub accepted: Option<AcceptedRes>,
    pub outcome: Option<OutcomeRes>,
    pub accepted_event_id: i64,
}

pub struct Update {
    pub id: String,
    pub state: UState,
    /// The original request, until it is written in the accepted event.
    pub request: Option<update_pb::Request>,
    /// Admission order: updates go to the worker in this order.
    seq: u64,
    pub accepted_event_id: i64,
    status: UpdStatus,
    dirty: bool,
    tx: watch::Sender<UpdStatus>,
}

impl Update {
    fn new(id: &str, state: UState, request: Option<update_pb::Request>, seq: u64) -> Update {
        let (tx, _) = watch::channel(UpdStatus::default());
        Update {
            id: id.to_string(),
            state,
            request,
            seq,
            accepted_event_id: 0,
            status: UpdStatus::default(),
            dirty: false,
            tx,
        }
    }

    pub fn subscribe(&self) -> watch::Receiver<UpdStatus> {
        self.tx.subscribe()
    }

    fn publish(&mut self) {
        self.dirty = false;
        self.tx.send_replace(self.status.clone());
    }

    fn set_accepted(&mut self, v: AcceptedRes) {
        self.status.accepted = Some(v);
        self.dirty = true;
    }

    fn set_outcome(&mut self, v: OutcomeRes) {
        self.status.outcome = Some(v);
        self.dirty = true;
    }

    fn size(&self) -> usize {
        self.id.len() + self.request.as_ref().map(|r| r.encoded_len()).unwrap_or(0)
    }
}

/// The in-flight updates of one run (in memory only, like the Go server's registry).
#[derive(Default)]
pub struct Registry {
    pub map: HashMap<String, Update>,
    seq: u64,
    /// Finished updates (removed from `map`) whose callers haven't been told yet.
    done: Vec<Update>,
}

impl Registry {
    /// Rebuild from a run's saved state: accepted updates that haven't completed are in flight
    /// (or aborted, if the run is closed).
    pub fn load(s: &crate::state::ExecState) -> Registry {
        let mut r = Registry::default();
        for (id, info) in &s.updates {
            if info.completed_event_id > 0 {
                continue;
            }
            if info.accepted_event_id == 0 {
                // Admitted from a history event: the request is in that event, not sent again.
                if info.admitted_event_id > 0 && s.is_open() {
                    let seq = r.next_seq();
                    let mut u = Update::new(id, UState::Admitted, None, seq);
                    u.publish();
                    r.map.insert(id.clone(), u);
                }
                continue;
            }
            let mut u = Update::new(id, UState::Accepted, None, 0);
            u.accepted_event_id = info.accepted_event_id;
            u.status.accepted_event_id = info.accepted_event_id;
            u.set_accepted(Ok(None));
            if !s.is_open() {
                abort_one(&mut u, Abort::Closed);
            }
            u.publish();
            r.map.insert(id.clone(), u);
        }
        r
    }

    fn next_seq(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }

    /// Admitted or (with `include_sent`) sent updates waiting for a worker.
    pub fn has_outgoing(&self, include_sent: bool) -> bool {
        self.map
            .values()
            .any(|u| u.state == UState::Admitted || (include_sent && u.state == UState::Sent))
    }

    pub fn in_flight(&self) -> usize {
        self.map.len()
    }

    fn finish(&mut self, id: &str) {
        if let Some(u) = self.map.remove(id) {
            self.done.push(u);
        }
    }

    /// Tell waiting callers about everything that changed.
    pub fn publish(&mut self) {
        for u in self.map.values_mut().filter(|u| u.dirty) {
            u.publish();
        }
        for mut u in std::mem::take(&mut self.done) {
            u.publish();
        }
    }
}

/// Why in-flight updates are cut short (the Go server's abort reasons).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Abort {
    /// The run closed without a new run.
    Closed,
    /// The run closed into a new run (continue-as-new, retry): callers retry onto it.
    Continuing,
    /// The workflow task that would have delivered them failed.
    TaskFailed,
}

pub fn aborted_by_closing() -> Status {
    not_found("workflow update was aborted by closing workflow")
}

pub fn aborted_by_server() -> Status {
    Status::unavailable("workflow update was aborted")
}

pub fn workflow_closing() -> Status {
    status_with_detail(
        tonic::Code::ResourceExhausted,
        "workflow operation can not be applied because workflow is closing",
        "temporal.api.errordetails.v1.ResourceExhaustedFailure",
        &errordetails::ResourceExhaustedFailure {
            cause: enums::ResourceExhaustedCause::BusyWorkflow as i32,
            scope: enums::ResourceExhaustedScope::Namespace as i32,
        },
    )
}

pub fn workflow_not_ready(msg: &str) -> Status {
    status_with_detail(
        tonic::Code::FailedPrecondition,
        msg,
        "temporal.api.errordetails.v1.WorkflowNotReadyFailure",
        &errordetails::WorkflowNotReadyFailure {},
    )
}

fn resource_exhausted(msg: String) -> Status {
    status_with_detail(
        tonic::Code::ResourceExhausted,
        &msg,
        "temporal.api.errordetails.v1.ResourceExhaustedFailure",
        &errordetails::ResourceExhaustedFailure {
            cause: enums::ResourceExhaustedCause::ConcurrentLimit as i32,
            scope: enums::ResourceExhaustedScope::Namespace as i32,
        },
    )
}

fn app_failure(msg: &str, kind: &str) -> failure::Failure {
    failure::Failure {
        message: msg.to_string(),
        source: "Server".into(),
        failure_info: Some(failure::failure::FailureInfo::ApplicationFailureInfo(
            failure::ApplicationFailureInfo {
                r#type: kind.to_string(),
                non_retryable: true,
                ..Default::default()
            },
        )),
        ..Default::default()
    }
}

fn unprocessed_failure() -> failure::Failure {
    app_failure(
        "Workflow Update is rejected because it wasn't processed by worker. Probably, Workflow Update is not supported by the worker.",
        "UnprocessedUpdate",
    )
}

fn accepted_then_closed_failure() -> failure::Failure {
    app_failure(
        "Workflow Update failed because the Workflow completed before the Update completed.",
        "AcceptedUpdateCompletedWorkflow",
    )
}

fn failure_outcome(f: failure::Failure) -> update_pb::Outcome {
    update_pb::Outcome {
        value: Some(update_pb::outcome::Value::Failure(f)),
    }
}

/// The Go server's abort matrix for one update.
fn abort_one(u: &mut Update, reason: Abort) {
    let (outcome, accepted): (Option<OutcomeRes>, Option<AcceptedRes>) = match (u.state, reason) {
        (UState::Completed | UState::Aborted, _) => return,
        (UState::Accepted, Abort::TaskFailed) => return,
        (UState::Accepted, _) => (Some(Ok(failure_outcome(accepted_then_closed_failure()))), None),
        (_, Abort::Closed) => (Some(Err(aborted_by_closing())), Some(Err(aborted_by_closing()))),
        (_, Abort::Continuing) => (Some(Err(workflow_closing())), Some(Err(workflow_closing()))),
        (UState::Sent, Abort::TaskFailed) => {
            let e = workflow_not_ready(
                "Unable to perform workflow execution update due to unexpected workflow task failure.",
            );
            (Some(Err(e.clone())), Some(Err(e)))
        }
        // Not seen by the workflow yet: the caller retries and it is admitted again.
        (_, Abort::TaskFailed) => (Some(Err(aborted_by_server())), Some(Err(aborted_by_server()))),
    };
    u.state = UState::Aborted;
    u.request = None;
    if let Some(o) = outcome {
        u.set_outcome(o);
    }
    if let Some(a) = accepted {
        u.set_accepted(a);
    }
}

/// Abort every in-flight update of a run and tell the callers.
pub fn abort_updates(ex: &mut Execution, reason: Abort) {
    let ids: Vec<String> = ex.updates.map.keys().cloned().collect();
    for id in ids {
        // An update admitted by a history event is reloaded as admitted when the Go server
        // rebuilds the run after a failed task; it stays in flight.
        let durable =
            ex.s.updates
                .get(&id)
                .is_some_and(|i| i.admitted_event_id > 0 && i.accepted_event_id == 0);
        let u = ex.updates.map.get_mut(&id).expect("present");
        if reason == Abort::TaskFailed && durable && u.state == UState::Sent {
            u.state = UState::Admitted;
            continue;
        }
        let before = u.state;
        abort_one(u, reason);
        // Updates the workflow never saw are dropped when its task failed, so a retried call
        // admits them afresh.
        if reason == Abort::TaskFailed && before == UState::Admitted {
            ex.updates.finish(&id);
        }
    }
    ex.updates.publish();
}

/// Number of updates that completed after being accepted (they count toward the total limit).
pub fn completed_count(ex: &Execution) -> usize {
    ex.s.updates.values().filter(|i| i.completed_event_id > 0).count()
}

/// Whether workflow tasks should suggest continue-as-new because of the update count.
pub fn suggest_can_for_updates(ex: &Execution) -> bool {
    ex.updates.in_flight() + completed_count(ex) >= SUGGEST_CAN_UPDATES
}

/// A finished update looked up from the history (an already-resolved channel).
fn completed_from_history(ex: &Execution, id: &str) -> Option<watch::Receiver<UpdStatus>> {
    let info = ex.s.updates.get(id).filter(|i| i.completed_event_id > 0)?;
    let ev = ex.events.iter().find(|e| e.event_id == info.completed_event_id)?;
    let Some(A::WorkflowExecutionUpdateCompletedEventAttributes(a)) = &ev.attributes else {
        return None;
    };
    let status = UpdStatus {
        accepted: Some(Ok(None)),
        outcome: Some(Ok(a.outcome.clone().unwrap_or_default())),
        accepted_event_id: info.accepted_event_id,
    };
    let (_tx, rx) = watch::channel(status);
    Some(rx)
}

/// What an update call waits on.
pub struct UpdHandle {
    pub rx: watch::Receiver<UpdStatus>,
    pub workflow_id: String,
    pub run_id: String,
}

fn any_of<M: Message>(type_name: &str, m: &M) -> prost_types::Any {
    prost_types::Any {
        type_url: format!("type.googleapis.com/{type_name}"),
        value: m.encode_to_vec(),
    }
}

/// The message type name of a protocol message body ("" if missing).
fn body_type(m: &protocol_pb::Message) -> Option<&str> {
    let b = m.body.as_ref()?;
    Some(b.type_url.rsplit('/').next().unwrap_or(""))
}

pub fn is_rejection(m: &protocol_pb::Message) -> bool {
    body_type(m) == Some(REJECTION_TYPE)
}

/// The update messages a starting workflow task carries, oldest first; they become "sent".
pub fn send_messages(ex: &mut Execution, started_id: i64, include_sent: bool) -> Vec<protocol_pb::Message> {
    let mut ups: Vec<&mut Update> = ex
        .updates
        .map
        .values_mut()
        .filter(|u| u.state == UState::Admitted || (include_sent && u.state == UState::Sent))
        .collect();
    ups.sort_by_key(|u| u.seq);
    let mut out = Vec::new();
    for u in ups {
        u.state = UState::Sent;
        let Some(req) = &u.request else { continue };
        out.push(protocol_pb::Message {
            id: format!("{}/request", u.id),
            protocol_instance_id: u.id.clone(),
            body: Some(any_of(REQUEST_TYPE, req)),
            sequencing_id: Some(protocol_pb::message::SequencingId::EventId(started_id - 1)),
        });
    }
    out
}

/// One step of a workflow task completion: a command, or a message (by index).
#[derive(Clone, Copy, Debug)]
pub enum Step {
    Command(usize),
    Message(usize),
    /// A message the closing workflow can't act on any more (its update is aborted).
    SkipMessage,
}

// Decoded once per message and dropped right away.
#[allow(clippy::large_enum_variant)]
enum Body {
    Acceptance(update_pb::Acceptance),
    Rejection(update_pb::Rejection),
    Response(update_pb::Response),
}

/// Checking a completion's messages can end in a failed workflow task, or in an error returned
/// to the worker with nothing recorded.
pub enum MsgError {
    Fail(super::wft::Reject),
    Error(Status),
}

fn fail(msg: String) -> MsgError {
    MsgError::Fail(super::wft::Reject {
        cause: enums::WorkflowTaskFailedCause::BadUpdateWorkflowExecutionMessage,
        msg,
        terminate: false,
    })
}

fn decode_body(m: &protocol_pb::Message) -> Result<Body, MsgError> {
    let b = m.body.as_ref().expect("identified");
    let name = body_type(m).unwrap_or("");
    let bad = |e: prost::DecodeError| fail(format!("unable to unmarshal request: {e}"));
    Ok(match name {
        ACCEPTANCE_TYPE => Body::Acceptance(update_pb::Acceptance::decode(b.value.as_slice()).map_err(bad)?),
        REJECTION_TYPE => Body::Rejection(update_pb::Rejection::decode(b.value.as_slice()).map_err(bad)?),
        RESPONSE_TYPE => Body::Response(update_pb::Response::decode(b.value.as_slice()).map_err(bad)?),
        REQUEST_TYPE => return Err(fail("Message type *update.Request not supported".into())),
        other => return Err(fail(format!("unable to unmarshal request: proto: not found: {other}"))),
    })
}

/// Work out the order commands and messages are handled in (messages named by a
/// protocol-message command go there; the rest after all commands), checking every message
/// against the updates' states first so nothing is written for a completion that fails.
pub fn plan_messages(
    ex: &Execution,
    cmds: &[command::Command],
    msgs: &[protocol_pb::Message],
) -> Result<Vec<Step>, MsgError> {
    use command::command::Attributes as C;
    let mut by_id: HashMap<&str, usize> = HashMap::new();
    for (i, m) in msgs.iter().enumerate() {
        by_id.entry(m.id.as_str()).or_insert(i);
    }
    let mut taken = vec![false; msgs.len()];
    let mut steps = Vec::new();
    // Simulated update states: in-flight ones, then any we learn of along the way.
    let mut sim: HashMap<String, UState> = ex.updates.map.iter().map(|(k, u)| (k.clone(), u.state)).collect();
    let mut closed = false;
    let mut total = ex.updates.in_flight() + completed_count(ex);

    let check = |i: usize,
                 closed: bool,
                 sim: &mut HashMap<String, UState>,
                 total: &mut usize|
     -> Result<Step, MsgError> {
        let m = &msgs[i];
        let Some(name) = body_type(m) else {
            return Err(MsgError::Error(invalid("nil message body")));
        };
        if name.is_empty() {
            return Err(MsgError::Error(invalid("message body name is empty")));
        }
        let Some(dot) = name.rfind('.') else {
            return Err(MsgError::Error(invalid(format!(
                "failed to extract protocol type: no . found in {name:?}"
            ))));
        };
        let size = m.body.as_ref().map(|b| b.encoded_len()).unwrap_or(0);
        if size > BLOB_SIZE_LIMIT {
            return Err(MsgError::Fail(super::wft::Reject {
                cause: enums::WorkflowTaskFailedCause::BadUpdateWorkflowExecutionMessage,
                msg: format!("Message type {name} exceeds size limit."),
                terminate: true,
            }));
        }
        if &name[..dot] != UPDATE_PROTOCOL {
            return Err(fail(format!("unsupported protocol type {}", &name[..dot])));
        }
        let id = m.protocol_instance_id.clone();
        let known = sim.contains_key(&id) || ex.s.updates.get(&id).is_some_and(|i| i.completed_event_id > 0);
        let body = decode_body(m);
        if !known {
            // An update the server lost can come back with the request inside the message.
            let request = match &body {
                Ok(Body::Acceptance(a)) => a.accepted_request.clone(),
                Ok(Body::Rejection(r)) => r.rejected_request.clone(),
                _ => None,
            };
            if request.is_none() {
                return Err(fail(format!(
                    "update {id} wasn't found on the server. This is most likely a transient error which will be resolved automatically by retries"
                )));
            }
            if *total >= MAX_TOTAL_UPDATES {
                return Err(MsgError::Error(total_limit_error()));
            }
            *total += 1;
            sim.insert(id.clone(), UState::Admitted);
        }
        let body = body?;
        let state = sim.get(&id).copied().unwrap_or(UState::Completed);
        let is_rej = matches!(body, Body::Rejection(_));
        if closed && !is_rej {
            return Ok(Step::SkipMessage);
        }
        let bad_state = |t: &str| {
            fail(format!(
                "invalid state transition attempted for Update {id}: received *update.{t} message while in state {}",
                state.name()
            ))
        };
        match body {
            Body::Acceptance(a) => {
                if !matches!(state, UState::Sent | UState::Admitted) {
                    return Err(bad_state("Acceptance"));
                }
                if a.accepted_request_sequencing_event_id == 0 {
                    return Err(fail(
                        "invalid *update.Acceptance: accepted_request_sequencing_event_id is not set".into(),
                    ));
                }
                if a.accepted_request_message_id.is_empty() {
                    return Err(fail(
                        "invalid *update.Acceptance: accepted_request_message_id is not set".into(),
                    ));
                }
                sim.insert(id, UState::Accepted);
            }
            Body::Rejection(_) => {
                if !matches!(state, UState::Sent | UState::Admitted) {
                    return Err(bad_state("Rejection"));
                }
                sim.insert(id, UState::Completed);
            }
            Body::Response(r) => {
                if state != UState::Accepted {
                    return Err(bad_state("Response"));
                }
                let meta = r.meta.as_ref();
                if meta.is_none() {
                    return Err(fail("invalid *update.Response: meta is not set".into()));
                }
                let uid = meta.map(|m| m.update_id.as_str()).unwrap_or("");
                if uid.is_empty() {
                    return Err(fail("invalid *update.Response: meta.update_id is not set".into()));
                }
                if uid != id {
                    return Err(fail(format!("invalid *update.Response: meta.update_id != {id}")));
                }
                if r.outcome.is_none() {
                    return Err(fail("invalid *update.Response: outcome is not set".into()));
                }
                sim.insert(id, UState::Completed);
            }
        }
        Ok(Step::Message(i))
    };

    for (ci, c) in cmds.iter().enumerate() {
        match &c.attributes {
            Some(C::ProtocolMessageCommandAttributes(p)) => {
                if p.message_id.is_empty() {
                    return Err(MsgError::Fail(super::wft::Reject {
                        cause: enums::WorkflowTaskFailedCause::BadUpdateWorkflowExecutionMessage,
                        msg: "MessageID is not set on command.".into(),
                        terminate: false,
                    }));
                }
                let Some(&i) = by_id.get(p.message_id.as_str()).filter(|&&i| !taken[i]) else {
                    return Err(fail(format!(
                        "ProtocolMessageCommand referenced absent message ID {}",
                        p.message_id
                    )));
                };
                taken[i] = true;
                steps.push(check(i, closed, &mut sim, &mut total)?);
            }
            Some(a) => {
                steps.push(Step::Command(ci));
                if matches!(
                    a,
                    C::CompleteWorkflowExecutionCommandAttributes(_)
                        | C::FailWorkflowExecutionCommandAttributes(_)
                        | C::CancelWorkflowExecutionCommandAttributes(_)
                        | C::ContinueAsNewWorkflowExecutionCommandAttributes(_)
                ) {
                    closed = true;
                }
            }
            None => {}
        }
    }
    for (i, t) in taken.iter().enumerate() {
        if !t {
            steps.push(check(i, closed, &mut sim, &mut total)?);
        }
    }
    Ok(steps)
}

fn total_limit_error() -> Status {
    Status::failed_precondition(format!(
        "The limit on the total number of distinct updates in this workflow has been reached ({MAX_TOTAL_UPDATES}). Make sure any duplicate updates share an Update ID so the server can deduplicate them, and consider rejecting updates that you aren't going to process. You can also Continue-as-New to avoid this; we recommend you check Continue-as-New Suggested in your Workflow."
    ))
}

/// Apply one checked message from a workflow task completion.
pub fn apply_message(ex: &mut Execution, m: &protocol_pb::Message) {
    let id = m.protocol_instance_id.clone();
    let Ok(body) = decode_body(m) else { return };
    if !ex.updates.map.contains_key(&id) {
        let request = match &body {
            Body::Acceptance(a) => a.accepted_request.clone(),
            Body::Rejection(r) => r.rejected_request.clone(),
            Body::Response(_) => None,
        };
        let Some(request) = request else { return };
        let seq = ex.updates.next_seq();
        ex.updates
            .map
            .insert(id.clone(), Update::new(&id, UState::Admitted, Some(request), seq));
    }
    match body {
        Body::Acceptance(a) => {
            let req = ex.updates.map.get_mut(&id).and_then(|u| u.request.take());
            let eid = push(
                ex,
                A::WorkflowExecutionUpdateAcceptedEventAttributes(
                    history::WorkflowExecutionUpdateAcceptedEventAttributes {
                        protocol_instance_id: id.clone(),
                        accepted_request_message_id: format!("{id}/request"),
                        accepted_request_sequencing_event_id: a.accepted_request_sequencing_event_id,
                        accepted_request: req,
                    },
                ),
            );
            ex.s.updates.insert(
                id.clone(),
                UpdateInfo {
                    accepted_event_id: eid,
                    ..Default::default()
                },
            );
            let u = ex.updates.map.get_mut(&id).expect("present");
            u.state = UState::Accepted;
            u.accepted_event_id = eid;
            u.status.accepted_event_id = eid;
            u.set_accepted(Ok(None));
        }
        Body::Rejection(r) => {
            let f = r.failure.unwrap_or_default();
            let u = ex.updates.map.get_mut(&id).expect("present");
            u.state = UState::Completed;
            u.request = None;
            u.set_accepted(Ok(Some(f.clone())));
            u.set_outcome(Ok(failure_outcome(f)));
            ex.updates.finish(&id);
        }
        Body::Response(r) => {
            let accepted_id = ex.updates.map.get(&id).map(|u| u.accepted_event_id).unwrap_or(0);
            let outcome = r.outcome.clone().unwrap_or_default();
            let eid = push(
                ex,
                A::WorkflowExecutionUpdateCompletedEventAttributes(
                    history::WorkflowExecutionUpdateCompletedEventAttributes {
                        meta: r.meta,
                        accepted_event_id: accepted_id,
                        outcome: r.outcome,
                    },
                ),
            );
            if let Some(i) = ex.s.updates.get_mut(&id) {
                i.completed_event_id = eid;
            }
            let u = ex.updates.map.get_mut(&id).expect("present");
            u.state = UState::Completed;
            u.set_outcome(Ok(outcome));
            ex.updates.finish(&id);
        }
    }
}

/// Updates sent on a task the worker completed without acting on them are rejected for it
/// (an SDK without update support), so they aren't sent over and over.
pub fn reject_unprocessed(ex: &mut Execution) {
    let ids: Vec<String> = ex
        .updates
        .map
        .values()
        .filter(|u| u.state == UState::Sent)
        .map(|u| u.id.clone())
        .collect();
    for id in ids {
        let u = ex.updates.map.get_mut(&id).expect("present");
        let f = unprocessed_failure();
        u.state = UState::Completed;
        u.request = None;
        u.set_accepted(Ok(Some(f.clone())));
        u.set_outcome(Ok(failure_outcome(f)));
        ex.updates.finish(&id);
    }
}

impl Inner {
    /// UpdateWorkflowExecution, under the lock: find the update or admit it, and make sure a
    /// workflow task will deliver it (a speculative one when none is pending).
    pub fn update_workflow(&mut self, key: &Key, first_run_id: &str, req: update_pb::Request) -> R<UpdHandle> {
        let id = req.meta.as_ref().map(|m| m.update_id.clone()).unwrap_or_default();
        let mut ex = self.take(key)?;
        let r = self.update_in(&mut ex, &id, first_run_id, req);
        self.put(ex);
        r
    }

    fn update_in(&mut self, ex: &mut Execution, id: &str, first_run_id: &str, req: update_pb::Request) -> R<UpdHandle> {
        let handle = |ex: &Execution, rx| UpdHandle {
            rx,
            workflow_id: ex.s.workflow_id.clone(),
            run_id: ex.s.run_id.clone(),
        };
        if !first_run_id.is_empty() && ex.s.first_run_id != first_run_id {
            return Err(not_found("workflow execution not found"));
        }
        if let Some(u) = ex.updates.map.get(id) {
            return Ok(handle(ex, u.subscribe()));
        }
        if let Some(rx) = completed_from_history(ex, id) {
            return Ok(handle(ex, rx));
        }
        if !ex.s.is_open() {
            return Err(wf_completed_error());
        }
        if ex.s.wft.as_ref().is_some_and(|w| w.attempt >= FAIL_UPDATE_WFT_ATTEMPT) {
            return Err(workflow_not_ready(
                "Unable to perform workflow execution update due to Workflow Task in failed state.",
            ));
        }
        if ex.updates.in_flight() >= MAX_IN_FLIGHT_UPDATES {
            return Err(resource_exhausted(format!(
                "limit on number of concurrent in-flight updates has been reached ({MAX_IN_FLIGHT_UPDATES})"
            )));
        }
        if ex.updates.in_flight() + completed_count(ex) >= MAX_TOTAL_UPDATES {
            return Err(total_limit_error());
        }
        let in_flight_size: usize = ex.updates.map.iter().map(|(k, u)| k.len() + u.size()).sum();
        if in_flight_size + req.encoded_len() >= MAX_IN_FLIGHT_UPDATE_PAYLOADS {
            return Err(resource_exhausted(format!(
                "limit on total payload size of in-flight updates has been reached ({MAX_IN_FLIGHT_UPDATE_PAYLOADS} bytes)"
            )));
        }
        let seq = ex.updates.next_seq();
        let u = Update::new(id, UState::Admitted, Some(req), seq);
        let rx = u.subscribe();
        ex.updates.map.insert(id.to_string(), u);
        if ex.s.wft.is_none() {
            self.schedule_speculative_wft(ex);
        }
        Ok(handle(ex, rx))
    }

    /// A workflow task only to deliver updates: nothing is written for it now.
    pub fn schedule_speculative_wft(&mut self, ex: &mut Execution) {
        let sticky = !ex.s.sticky_queue.is_empty();
        let queue = if sticky {
            ex.s.sticky_queue.clone()
        } else {
            ex.s.task_queue.clone()
        };
        let now = now_ms();
        let sid = ex.s.next_event_id;
        ex.s.wft = Some(WftInfo {
            scheduled_id: sid,
            attempt: 1,
            queue: queue.clone(),
            scheduled_ms: now,
            timeout_ms: super::events::base_wft_timeout(ex),
            original_scheduled_ms: now,
            speculative: true,
            ..Default::default()
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
        let s2s = super::events::schedule_to_start_ms(ex);
        self.add_timer(now + s2s, TimerKind::StickyTimeout { key, scheduled_id: sid });
    }

    /// PollWorkflowExecutionUpdate, under the lock.
    pub fn poll_update(&mut self, key: &Key, id: &str) -> R<UpdHandle> {
        if !self.ensure_loaded(key)? {
            return Err(wf_not_found("", &key.1));
        }
        let ex = &self.execs[key];
        let rx = match ex.updates.map.get(id) {
            Some(u) => u.subscribe(),
            None => completed_from_history(ex, id).ok_or_else(|| not_found(format!("update {id:?} not found")))?,
        };
        Ok(UpdHandle {
            rx,
            workflow_id: ex.s.workflow_id.clone(),
            run_id: ex.s.run_id.clone(),
        })
    }
}

/// Track an update re-admitted by a reset (its request is in the admitted event).
pub fn readmit(ex: &mut Execution, id: &str) {
    if ex.updates.map.contains_key(id) {
        return;
    }
    let seq = ex.updates.next_seq();
    ex.updates
        .map
        .insert(id.to_string(), Update::new(id, UState::Admitted, None, seq));
}

/// The run an update-with-start landed on.
pub struct UwsRun {
    pub run_id: String,
    pub first_run_id: String,
    pub started: bool,
    pub status: i32,
    /// The start response links to the started event (not when answered from history).
    pub link: bool,
}

impl Inner {
    /// ExecuteMultiOperation's update-with-start, under the lock: update the running workflow
    /// when it is the one asked for (same start request, update already known, or the
    /// use-existing conflict policy), otherwise start a new run with the update admitted.
    /// Errors are per operation: [start, update].
    pub fn update_with_start(
        &mut self,
        start: wsv1::StartWorkflowExecutionRequest,
        req: update_pb::Request,
    ) -> Result<(UwsRun, UpdHandle), [Option<Status>; 2]> {
        use enums::WorkflowIdConflictPolicy as CP;
        let aborted = multi_op_aborted;
        let id = req.meta.as_ref().map(|m| m.update_id.clone()).unwrap_or_default();
        let ns = start.namespace.clone();
        let current = self.current.get(&(ns.clone(), start.workflow_id.clone())).cloned();
        if let Some(rid) = &current {
            let key = (ns.clone(), rid.clone());
            if self.ensure_loaded(&key).unwrap_or(false) {
                let ex = &self.execs[&key];
                let run = UwsRun {
                    run_id: rid.clone(),
                    first_run_id: ex.s.first_run_id.clone(),
                    started: false,
                    status: ex.s.status,
                    link: true,
                };
                if let Some(rx) = completed_from_history(ex, &id) {
                    let run = UwsRun { link: false, ..run };
                    let h = UpdHandle {
                        rx,
                        workflow_id: ex.s.workflow_id.clone(),
                        run_id: rid.clone(),
                    };
                    return Ok((run, h));
                }
                let reuse = ex.s.is_open()
                    && (start.request_id == ex.s.start_request_id
                        || ex.updates.map.contains_key(&id)
                        || start.workflow_id_conflict_policy == CP::UseExisting as i32);
                if reuse {
                    return match self.update_workflow(&key, "", req) {
                        Ok(h) => Ok((run, h)),
                        Err(e) => Err([Some(aborted()), Some(e)]),
                    };
                }
            }
        }
        let (rid, _) = self.start_workflow(start).map_err(|e| [Some(e), Some(aborted())])?;
        if current.as_ref() == Some(&rid) {
            return Err([
                Some(Status::unavailable("Workflow was not started: StartDeduped")),
                Some(aborted()),
            ]);
        }
        let key = (ns, rid.clone());
        let first = self.first_run_id(&key);
        let h = self
            .update_workflow(&key, "", req)
            .map_err(|e| [Some(aborted()), Some(e)])?;
        Ok((
            UwsRun {
                run_id: rid,
                first_run_id: first,
                started: true,
                status: enums::WorkflowExecutionStatus::Running as i32,
                link: true,
            },
            h,
        ))
    }

    /// The first run id of a run's chain ("" if the run is unknown).
    pub fn first_run_id(&mut self, key: &Key) -> String {
        if self.ensure_loaded(key).unwrap_or(false) {
            self.execs[key].s.first_run_id.clone()
        } else {
            String::new()
        }
    }
}

/// The stage an update call answers with.
pub struct Reached {
    pub stage: enums::UpdateWorkflowExecutionLifecycleStage,
    pub outcome: Option<update_pb::Outcome>,
    pub accepted_event_id: i64,
}

/// Wait for `stage` (Go's WaitLifecycleStage): the outcome when it is known, otherwise the
/// stage reached when `soft_ms` runs out. `hard` is the caller's own deadline.
pub async fn wait_stage(
    mut rx: watch::Receiver<UpdStatus>,
    stage: enums::UpdateWorkflowExecutionLifecycleStage,
    soft_ms: i64,
    hard: Option<tokio::time::Instant>,
) -> R<Reached> {
    use enums::UpdateWorkflowExecutionLifecycleStage as S;
    let soft = tokio::time::Instant::now() + std_dur(soft_ms);
    let deadline = match hard {
        Some(h) if h < soft => h,
        _ => soft,
    };
    let completed = |o: update_pb::Outcome, aid: i64| Reached {
        stage: S::Completed,
        outcome: Some(o),
        accepted_event_id: aid,
    };
    loop {
        {
            let s = rx.borrow_and_update();
            // A known outcome always wins.
            if let Some(o) = &s.outcome {
                return match o {
                    Ok(o) => Ok(completed(o.clone(), s.accepted_event_id)),
                    Err(e) => Err(e.clone()),
                };
            }
            // Unless the caller waits for the result, acceptance (or rejection) answers.
            if stage != S::Completed
                && let Some(a) = &s.accepted
            {
                return match a {
                    Ok(Some(f)) => Ok(completed(failure_outcome(f.clone()), 0)),
                    Ok(None) => Ok(Reached {
                        stage: S::Accepted,
                        outcome: None,
                        accepted_event_id: s.accepted_event_id,
                    }),
                    Err(e) => Err(e.clone()),
                };
            }
            if stage != S::Completed && stage != S::Accepted {
                break;
            }
        }
        match tokio::time::timeout_at(deadline, rx.changed()).await {
            Ok(Ok(())) => continue,
            Ok(Err(_)) => return Err(aborted_by_server()),
            Err(_) if deadline < soft => {
                return Err(Status::deadline_exceeded("context deadline exceeded"));
            }
            Err(_) => break,
        }
    }
    // Out of time: report the stage reached so far.
    let s = rx.borrow();
    if let Some(Ok(o)) = &s.outcome {
        return Ok(completed(o.clone(), s.accepted_event_id));
    }
    match &s.accepted {
        Some(Ok(Some(f))) => Ok(completed(failure_outcome(f.clone()), 0)),
        Some(Ok(None)) => Ok(Reached {
            stage: S::Accepted,
            outcome: None,
            accepted_event_id: s.accepted_event_id,
        }),
        _ => Ok(Reached {
            stage: S::Admitted,
            outcome: None,
            accepted_event_id: 0,
        }),
    }
}
