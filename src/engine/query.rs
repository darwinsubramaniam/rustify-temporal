//! Workflow queries. A query rides on the next workflow task when one is pending or running;
//! otherwise it goes to a worker as a query-only task.

use std::collections::HashMap;

use super::*;

// Short-lived values passed once; boxing the large variant would only add allocations.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum QueryOutcome {
    Answered(common::Payloads),
    Failed(String, Option<failure::Failure>),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum QState {
    /// Waiting for the next workflow task.
    Buffered,
    /// Sent along with a running workflow task (its started event id).
    Attached(i64),
    /// Put on a queue as a query-only task.
    Queued,
    /// A worker took the query-only task.
    Dispatched,
}

pub struct PendingQuery {
    pub key: Key,
    pub query: query_pb::WorkflowQuery,
    pub state: QState,
    pub tx: Option<QueryTx>,
}

impl Inner {
    /// Register a query. Returns its id and the channel the answer arrives on.
    pub fn new_query(&mut self, key: &Key, q: query_pb::WorkflowQuery) -> R<(String, oneshot::Receiver<QueryOutcome>)> {
        if !self.ensure_loaded(key)? {
            return Err(wf_not_found("", &key.1));
        }
        let (tx, rx) = oneshot::channel();
        let id = new_id();
        let ex = &self.execs[key];
        let open = ex.s.is_open();
        let has_task = ex.s.wft.is_some() || ex.s.wft_due_ms > 0;
        let never_ran = ex.s.last_completed_started_id == 0;
        let state = if open && (has_task || never_ran) {
            QState::Buffered
        } else {
            QState::Queued
        };
        self.queries.insert(
            id.clone(),
            PendingQuery {
                key: key.clone(),
                query: q,
                state,
                tx: Some(tx),
            },
        );
        if state == QState::Queued {
            self.enqueue_query(&id);
        }
        Ok((id, rx))
    }

    fn enqueue_query(&mut self, id: &str) {
        let Some(q) = self.queries.get(id) else { return };
        let key = q.key.clone();
        let Some(ex) = self.execs.get(&key) else { return };
        let sticky = ex.s.sticky_queue.clone();
        let normal = ex.s.task_queue.clone();
        let sticky_t = ex.s.sticky_timeout_ms;
        if sticky.is_empty() {
            self.enqueue(
                &key.0,
                &normal,
                QKind::Workflow,
                TaskRef::Query {
                    query_id: id.to_string(),
                },
            );
        } else {
            self.enqueue(
                &key.0,
                &sticky,
                QKind::Workflow,
                TaskRef::Query {
                    query_id: id.to_string(),
                },
            );
            let t = if sticky_t > 0 {
                sticky_t
            } else {
                DEFAULT_STICKY_TIMEOUT_MS
            };
            self.add_timer(
                now_ms() + t,
                TimerKind::QueryStickyFallback {
                    query_id: id.to_string(),
                },
            );
        }
    }

    pub fn query_sticky_fallback(&mut self, id: &str) {
        let Some(q) = self.queries.get(id) else { return };
        if q.state != QState::Queued {
            return;
        }
        let key = q.key.clone();
        if let Some(ex) = self.execs.get_mut(&key) {
            ex.s.sticky_queue.clear();
            let tq = ex.s.task_queue.clone();
            self.enqueue(
                &key.0,
                &tq,
                QKind::Workflow,
                TaskRef::Query {
                    query_id: id.to_string(),
                },
            );
        }
    }

    pub fn cancel_query(&mut self, id: &str) {
        self.queries.remove(id);
    }

    /// Hand a query-only task to a worker.
    pub fn start_query_task(&mut self, id: &str, queue: &str) -> R<Option<wsv1::PollWorkflowTaskQueueResponse>> {
        let Some(q) = self.queries.get_mut(id) else {
            return Ok(None);
        };
        if q.state != QState::Queued {
            return Ok(None);
        }
        let key = q.key.clone();
        if !self.ensure_loaded(&key)? {
            return Ok(None);
        }
        let q = self.queries.get_mut(id).expect("present");
        q.state = QState::Dispatched;
        let wq = q.query.clone();
        let ex = &self.execs[&key];
        let sticky = !ex.s.sticky_queue.is_empty() && ex.s.sticky_queue == queue;
        // Like the Go server: a sticky worker already holds the workflow, so it gets no history;
        // any other worker gets the whole history to replay.
        let events: Vec<_> = if sticky { Vec::new() } else { ex.events.clone() };
        let token = TaskToken {
            k: "query".into(),
            ns: key.0.clone(),
            wid: ex.s.workflow_id.clone(),
            rid: key.1.clone(),
            q: id.to_string(),
            ..Default::default()
        };
        Ok(Some(wsv1::PollWorkflowTaskQueueResponse {
            task_token: token.encode(),
            workflow_execution: Some(common::WorkflowExecution {
                workflow_id: ex.s.workflow_id.clone(),
                run_id: key.1.clone(),
            }),
            workflow_type: Some(common::WorkflowType {
                name: ex.s.workflow_type.clone(),
            }),
            previous_started_event_id: ex.s.last_completed_started_id,
            started_event_id: 0,
            attempt: 1,
            history: Some(history::History { events }),
            query: Some(wq),
            workflow_execution_task_queue: Some(taskqueue::TaskQueue {
                name: ex.s.task_queue.clone(),
                kind: enums::TaskQueueKind::Normal as i32,
                normal_name: String::new(),
            }),
            scheduled_time: Some(now_ts()),
            started_time: Some(now_ts()),
            ..Default::default()
        }))
    }

    pub fn respond_query_task(&mut self, r: wsv1::RespondQueryTaskCompletedRequest) -> R<()> {
        let tok = TaskToken::decode(&r.task_token)?;
        let Some(mut q) = self.queries.remove(&tok.q) else {
            return Err(not_found("Query does not exist or has timed out."));
        };
        let outcome = if r.completed_type == enums::QueryResultType::Answered as i32 {
            QueryOutcome::Answered(r.query_result.unwrap_or_default())
        } else {
            QueryOutcome::Failed(r.error_message, r.failure)
        };
        if let Some(tx) = q.tx.take() {
            let _ = tx.send(outcome);
        }
        Ok(())
    }

    /// Queries that should travel with the workflow task that just started.
    pub fn attach_queries(&mut self, key: &Key, started_id: i64) -> HashMap<String, query_pb::WorkflowQuery> {
        let mut out = HashMap::new();
        for (id, q) in self.queries.iter_mut() {
            if &q.key == key && q.state == QState::Buffered {
                q.state = QState::Attached(started_id);
                out.insert(id.clone(), q.query.clone());
            }
        }
        out
    }

    /// Answers that came back with a workflow task completion.
    pub fn answer_queries(&mut self, key: &Key, results: &HashMap<String, query_pb::WorkflowQueryResult>) {
        for (id, res) in results {
            if let Some(mut q) = self.queries.remove(id) {
                if &q.key != key {
                    self.queries.insert(id.clone(), q);
                    continue;
                }
                let outcome = if res.result_type == enums::QueryResultType::Answered as i32 {
                    QueryOutcome::Answered(res.answer.clone().unwrap_or_default())
                } else {
                    QueryOutcome::Failed(res.error_message.clone(), res.failure.clone())
                };
                if let Some(tx) = q.tx.take() {
                    let _ = tx.send(outcome);
                }
            }
        }
    }

    /// The workflow task a query rode on is gone (or the run changed): wait for the run's next
    /// workflow task, or go to a worker as a query-only task when there won't be one.
    pub fn requeue_queries(&mut self, ex: &Execution) {
        let key = ex.key();
        let open = ex.s.is_open();
        let has_task = ex.s.wft.is_some() || ex.s.wft_due_ms > 0;
        let mut alone = Vec::new();
        for (id, q) in self.queries.iter_mut() {
            if q.key == key && matches!(q.state, QState::Attached(_) | QState::Buffered) {
                if open && has_task {
                    q.state = QState::Buffered;
                } else {
                    q.state = QState::Queued;
                    alone.push(id.clone());
                }
            }
        }
        for id in alone {
            if ex.s.sticky_queue.is_empty() {
                self.enqueue(
                    &key.0,
                    &ex.s.task_queue,
                    QKind::Workflow,
                    TaskRef::Query { query_id: id },
                );
            } else {
                self.enqueue(
                    &key.0,
                    &ex.s.sticky_queue,
                    QKind::Workflow,
                    TaskRef::Query { query_id: id.clone() },
                );
                let t = if ex.s.sticky_timeout_ms > 0 {
                    ex.s.sticky_timeout_ms
                } else {
                    DEFAULT_STICKY_TIMEOUT_MS
                };
                self.add_timer(now_ms() + t, TimerKind::QueryStickyFallback { query_id: id });
            }
        }
    }
}
