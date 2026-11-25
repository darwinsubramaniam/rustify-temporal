//! Small shared helpers: proto module aliases, time conversions, task tokens and errors.

use std::time::{Duration as StdDuration, SystemTime, UNIX_EPOCH};

use prost::Message;
use prost_types::{Duration, Timestamp};
use serde::{Deserialize, Serialize};
use tonic::Status;

pub use crate::proto::temporal::api::command::v1 as command;
pub use crate::proto::temporal::api::common::v1 as common;
pub use crate::proto::temporal::api::enums::v1 as enums;
pub use crate::proto::temporal::api::errordetails::v1 as errordetails;
pub use crate::proto::temporal::api::failure::v1 as failure;
pub use crate::proto::temporal::api::history::v1 as history;
pub use crate::proto::temporal::api::namespace::v1 as namespace_pb;
pub use crate::proto::temporal::api::protocol::v1 as protocol_pb;
pub use crate::proto::temporal::api::query::v1 as query_pb;
pub use crate::proto::temporal::api::taskqueue::v1 as taskqueue;
pub use crate::proto::temporal::api::update::v1 as update_pb;
pub use crate::proto::temporal::api::workflow::v1 as workflow_pb;
pub use crate::proto::temporal::api::workflowservice::v1 as wsv1;

pub type R<T> = Result<T, Status>;

/// Milliseconds since the Unix epoch.
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub fn ts_from_ms(ms: i64) -> Timestamp {
    Timestamp {
        seconds: ms.div_euclid(1000),
        nanos: (ms.rem_euclid(1000) * 1_000_000) as i32,
    }
}

pub fn now_ts() -> Timestamp {
    ts_from_ms(now_ms())
}

/// Duration in milliseconds; `None` or non-positive becomes 0 (meaning "not set").
pub fn dur_ms(d: &Option<Duration>) -> i64 {
    match d {
        Some(d) => {
            let ms = d.seconds * 1000 + (d.nanos as i64) / 1_000_000;
            ms.max(0)
        }
        None => 0,
    }
}

pub fn dur_from_ms(ms: i64) -> Duration {
    Duration {
        seconds: ms / 1000,
        nanos: ((ms % 1000) * 1_000_000) as i32,
    }
}

pub fn opt_dur(ms: i64) -> Option<Duration> {
    if ms > 0 { Some(dur_from_ms(ms)) } else { None }
}

pub fn std_dur(ms: i64) -> StdDuration {
    StdDuration::from_millis(ms.max(0) as u64)
}

pub fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// What a task token points at. Tokens are opaque bytes to the SDKs.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct TaskToken {
    /// "wft", "act" or "query"
    pub k: String,
    pub ns: String,
    #[serde(default)]
    pub wid: String,
    #[serde(default)]
    pub rid: String,
    /// Scheduled event id (workflow task or activity).
    #[serde(default)]
    pub sid: i64,
    #[serde(default)]
    pub att: i32,
    /// Workflow-task started event id.
    #[serde(default)]
    pub st: i64,
    /// Query id (query tasks).
    #[serde(default)]
    pub q: String,
    #[serde(default)]
    pub aid: String,
    /// Workflow-task start time (ms): a lost speculative task can't be completed in place of
    /// a newer one with the same event ids (the Go server's StartedTime in the token).
    #[serde(default)]
    pub stt: i64,
}

impl TaskToken {
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }
    pub fn decode(b: &[u8]) -> R<TaskToken> {
        serde_json::from_slice(b).map_err(|_| Status::invalid_argument("Invalid task token."))
    }
}

pub fn not_found(msg: impl Into<String>) -> Status {
    Status::not_found(msg.into())
}

pub fn invalid(msg: impl Into<String>) -> Status {
    Status::invalid_argument(msg.into())
}

/// google.rpc.Status, used to attach Temporal's typed error details to a gRPC status.
#[derive(Clone, PartialEq, Message)]
pub struct RpcStatus {
    #[prost(int32, tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub message: String,
    #[prost(message, repeated, tag = "3")]
    pub details: Vec<prost_types::Any>,
}

/// Build a status carrying a Temporal error-details message (what the SDKs decode into typed errors).
pub fn status_with_detail<M: Message>(code: tonic::Code, msg: &str, type_name: &str, detail: &M) -> Status {
    let any = prost_types::Any {
        type_url: format!("type.googleapis.com/{type_name}"),
        value: detail.encode_to_vec(),
    };
    let st = RpcStatus {
        code: code as i32,
        message: msg.to_string(),
        details: vec![any],
    };
    Status::with_details(code, msg.to_string(), st.encode_to_vec().into())
}

pub fn already_started(msg: &str, start_request_id: &str, run_id: &str) -> Status {
    status_with_detail(
        tonic::Code::AlreadyExists,
        msg,
        "temporal.api.errordetails.v1.WorkflowExecutionAlreadyStartedFailure",
        &errordetails::WorkflowExecutionAlreadyStartedFailure {
            start_request_id: start_request_id.to_string(),
            run_id: run_id.to_string(),
            ..Default::default()
        },
    )
}

pub fn namespace_not_found(ns: &str) -> Status {
    status_with_detail(
        tonic::Code::NotFound,
        &format!("Namespace {ns} is not found."),
        "temporal.api.errordetails.v1.NamespaceNotFoundFailure",
        &errordetails::NamespaceNotFoundFailure {
            namespace: ns.to_string(),
        },
    )
}

pub fn wf_not_found(wid: &str, rid: &str) -> Status {
    let msg = if rid.is_empty() {
        format!("workflow execution not found for workflow ID \"{wid}\"")
    } else {
        format!("workflow execution not found for workflow ID \"{wid}\" and run ID \"{rid}\"")
    };
    status_with_detail(
        tonic::Code::NotFound,
        &msg,
        "temporal.api.errordetails.v1.NotFoundFailure",
        &errordetails::NotFoundFailure {
            current_cluster: "active".into(),
            active_cluster: "active".into(),
        },
    )
}

pub fn task_not_found() -> Status {
    not_found("Workflow task not found.")
}

pub fn activity_not_found() -> Status {
    not_found("invalid activityID or activity already timed out or invoking workflow is completed")
}

pub fn wf_completed_error() -> Status {
    not_found("workflow execution already completed")
}

/// Failure helpers.
pub fn timeout_failure(kind: enums::TimeoutType, last_hb: Option<common::Payloads>, what: &str) -> failure::Failure {
    let name = match kind {
        enums::TimeoutType::StartToClose => "StartToClose",
        enums::TimeoutType::ScheduleToStart => "ScheduleToStart",
        enums::TimeoutType::ScheduleToClose => "ScheduleToClose",
        enums::TimeoutType::Heartbeat => "Heartbeat",
        _ => "Unspecified",
    };
    failure::Failure {
        message: format!("{what} {name} timeout"),
        source: "Server".into(),
        failure_info: Some(failure::failure::FailureInfo::TimeoutFailureInfo(
            failure::TimeoutFailureInfo {
                timeout_type: kind as i32,
                last_heartbeat_details: last_hb,
            },
        )),
        ..Default::default()
    }
}

pub fn server_failure(msg: &str, non_retryable: bool) -> failure::Failure {
    failure::Failure {
        message: msg.to_string(),
        source: "Server".into(),
        failure_info: Some(failure::failure::FailureInfo::ServerFailureInfo(
            failure::ServerFailureInfo { non_retryable },
        )),
        ..Default::default()
    }
}
