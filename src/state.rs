//! Mutable state of a workflow run. It is a protobuf message itself so it can be saved as one
//! blob next to the run's history events.

use std::collections::HashMap;

use prost::Message;
use prost_types::{Duration, Timestamp};

use crate::util::{common, failure, history};

#[derive(Clone, PartialEq, Message)]
pub struct ParentInfo {
    #[prost(string, tag = "1")]
    pub namespace: String,
    #[prost(string, tag = "2")]
    pub namespace_id: String,
    #[prost(string, tag = "3")]
    pub workflow_id: String,
    #[prost(string, tag = "4")]
    pub run_id: String,
    #[prost(int64, tag = "5")]
    pub initiated_event_id: i64,
}

/// The workflow task currently scheduled or running.
#[derive(Clone, PartialEq, Message)]
pub struct WftInfo {
    #[prost(int64, tag = "1")]
    pub scheduled_id: i64,
    /// 0 while only scheduled.
    #[prost(int64, tag = "2")]
    pub started_id: i64,
    #[prost(int32, tag = "3")]
    pub attempt: i32,
    /// The queue the task was put on (the sticky queue or the normal one).
    #[prost(string, tag = "4")]
    pub queue: String,
    #[prost(int64, tag = "5")]
    pub scheduled_ms: i64,
    #[prost(int64, tag = "6")]
    pub started_ms: i64,
    #[prost(int64, tag = "7")]
    pub timeout_ms: i64,
    #[prost(string, tag = "8")]
    pub request_id: String,
    #[prost(int64, tag = "9")]
    pub original_scheduled_ms: i64,
}

pub const ACT_SCHEDULED: i32 = 0;
pub const ACT_STARTED: i32 = 1;
pub const ACT_BACKOFF: i32 = 2;

#[derive(Clone, PartialEq, Message)]
pub struct ActivityInfo {
    #[prost(int64, tag = "1")]
    pub scheduled_id: i64,
    #[prost(message, optional, tag = "2")]
    pub attrs: Option<history::ActivityTaskScheduledEventAttributes>,
    #[prost(int64, tag = "3")]
    pub scheduled_ms: i64,
    #[prost(int32, tag = "4")]
    pub attempt: i32,
    #[prost(int32, tag = "5")]
    pub state: i32,
    #[prost(int64, tag = "6")]
    pub started_ms: i64,
    #[prost(string, tag = "7")]
    pub identity: String,
    #[prost(string, tag = "8")]
    pub request_id: String,
    #[prost(int64, tag = "9")]
    pub attempt_scheduled_ms: i64,
    #[prost(message, optional, tag = "10")]
    pub heartbeat_details: Option<common::Payloads>,
    #[prost(int64, tag = "11")]
    pub last_heartbeat_ms: i64,
    #[prost(message, optional, tag = "12")]
    pub last_failure: Option<failure::Failure>,
    #[prost(bool, tag = "13")]
    pub cancel_requested: bool,
    #[prost(int64, tag = "14")]
    pub cancel_requested_event_id: i64,
    /// Absolute deadline from schedule_to_close (0 = none).
    #[prost(int64, tag = "15")]
    pub expiration_ms: i64,
    #[prost(int64, tag = "16")]
    pub next_attempt_ms: i64,
    #[prost(int64, tag = "17")]
    pub last_attempt_complete_ms: i64,
    #[prost(string, tag = "18")]
    pub last_identity: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct TimerInfo {
    #[prost(string, tag = "1")]
    pub timer_id: String,
    #[prost(int64, tag = "2")]
    pub started_id: i64,
    #[prost(int64, tag = "3")]
    pub fire_ms: i64,
}

#[derive(Clone, PartialEq, Message)]
pub struct ChildInfo {
    #[prost(int64, tag = "1")]
    pub initiated_id: i64,
    #[prost(int64, tag = "2")]
    pub started_id: i64,
    #[prost(string, tag = "3")]
    pub namespace: String,
    #[prost(string, tag = "4")]
    pub workflow_id: String,
    #[prost(string, tag = "5")]
    pub run_id: String,
    #[prost(string, tag = "6")]
    pub workflow_type: String,
    #[prost(int32, tag = "7")]
    pub parent_close_policy: i32,
}

#[derive(Clone, PartialEq, Message)]
pub struct ExecState {
    #[prost(string, tag = "1")]
    pub namespace: String,
    #[prost(string, tag = "2")]
    pub namespace_id: String,
    #[prost(string, tag = "3")]
    pub workflow_id: String,
    #[prost(string, tag = "4")]
    pub run_id: String,
    #[prost(string, tag = "5")]
    pub first_run_id: String,
    #[prost(string, tag = "6")]
    pub workflow_type: String,
    #[prost(string, tag = "7")]
    pub task_queue: String,
    #[prost(int32, tag = "8")]
    pub status: i32,
    #[prost(int64, tag = "9")]
    pub start_ms: i64,
    #[prost(int64, tag = "10")]
    pub close_ms: i64,
    #[prost(int64, tag = "11")]
    pub execution_ms: i64,
    #[prost(string, tag = "12")]
    pub start_request_id: String,
    #[prost(message, optional, tag = "13")]
    pub run_timeout: Option<Duration>,
    #[prost(message, optional, tag = "14")]
    pub execution_timeout: Option<Duration>,
    #[prost(message, optional, tag = "15")]
    pub wft_timeout: Option<Duration>,
    /// Absolute end of the whole chain of runs (execution timeout), 0 = none.
    #[prost(int64, tag = "16")]
    pub expiration_ms: i64,
    #[prost(message, optional, tag = "17")]
    pub retry_policy: Option<common::RetryPolicy>,
    #[prost(int32, tag = "18")]
    pub attempt: i32,
    #[prost(message, optional, tag = "19")]
    pub memo: Option<common::Memo>,
    #[prost(message, optional, tag = "20")]
    pub search_attributes: Option<common::SearchAttributes>,
    #[prost(message, optional, tag = "21")]
    pub parent: Option<ParentInfo>,
    #[prost(int64, tag = "22")]
    pub next_event_id: i64,
    /// Started event id of the last completed workflow task (previous_started_event_id).
    #[prost(int64, tag = "23")]
    pub last_completed_started_id: i64,
    #[prost(message, optional, tag = "24")]
    pub wft: Option<WftInfo>,
    /// When set, a workflow task is due at this time (start delay or retry backoff).
    #[prost(int64, tag = "25")]
    pub wft_due_ms: i64,
    #[prost(int32, tag = "26")]
    pub wft_due_attempt: i32,
    #[prost(string, tag = "27")]
    pub sticky_queue: String,
    #[prost(int64, tag = "28")]
    pub sticky_timeout_ms: i64,
    #[prost(map = "int64, message", tag = "29")]
    pub activities: HashMap<i64, ActivityInfo>,
    #[prost(map = "string, message", tag = "30")]
    pub timers: HashMap<String, TimerInfo>,
    #[prost(map = "int64, message", tag = "31")]
    pub children: HashMap<i64, ChildInfo>,
    #[prost(message, repeated, tag = "32")]
    pub buffered: Vec<history::HistoryEvent>,
    #[prost(string, tag = "33")]
    pub new_run_id: String,
    #[prost(int64, tag = "34")]
    pub history_size: i64,
    #[prost(int64, tag = "35")]
    pub transitions: i64,
    #[prost(bool, tag = "36")]
    pub cancel_requested: bool,
    #[prost(string, tag = "37")]
    pub cron_schedule: String,
    #[prost(message, optional, tag = "38")]
    pub header: Option<common::Header>,
    #[prost(string, tag = "39")]
    pub identity: String,
    #[prost(message, optional, tag = "40")]
    pub start_time: Option<Timestamp>,
    #[prost(string, repeated, tag = "41")]
    pub signal_request_ids: Vec<String>,
}

impl ExecState {
    pub fn is_open(&self) -> bool {
        self.status == crate::util::enums::WorkflowExecutionStatus::Running as i32
    }
}
