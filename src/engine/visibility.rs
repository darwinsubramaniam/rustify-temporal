//! Reading runs: history (with long-poll), describe, and list/count with a small query language.

use super::*;
use crate::store::ListFilter;

pub fn exec_info(ex: &Execution) -> workflow_pb::WorkflowExecutionInfo {
    let s = &ex.s;
    workflow_pb::WorkflowExecutionInfo {
        execution: Some(common::WorkflowExecution { workflow_id: s.workflow_id.clone(), run_id: s.run_id.clone() }),
        r#type: Some(common::WorkflowType { name: s.workflow_type.clone() }),
        start_time: Some(ts_from_ms(s.start_ms)),
        close_time: if s.close_ms > 0 { Some(ts_from_ms(s.close_ms)) } else { None },
        status: s.status,
        history_length: s.next_event_id - 1,
        parent_namespace_id: s.parent.as_ref().map(|p| p.namespace_id.clone()).unwrap_or_default(),
        parent_execution: s.parent.as_ref().map(|p| common::WorkflowExecution { workflow_id: p.workflow_id.clone(), run_id: p.run_id.clone() }),
        execution_time: Some(ts_from_ms(s.execution_ms.max(s.start_ms))),
        memo: s.memo.clone(),
        search_attributes: s.search_attributes.clone(),
        task_queue: s.task_queue.clone(),
        state_transition_count: s.transitions,
        history_size_bytes: s.history_size,
        execution_duration: if s.close_ms > 0 { Some(dur_from_ms(s.close_ms - s.execution_ms.max(s.start_ms))) } else { None },
        first_run_id: s.first_run_id.clone(),
        ..Default::default()
    }
}

/// Parse a list query like `WorkflowType = 'x' AND ExecutionStatus = "Running"`.
pub fn parse_query(q: &str) -> R<ListFilter> {
    let mut f = ListFilter::default();
    let mut q = q.trim().to_string();
    if let Some(i) = q.to_lowercase().find("order by") {
        q.truncate(i);
    }
    let q = q.trim();
    if q.is_empty() {
        return Ok(f);
    }
    let lower = q.to_lowercase();
    let mut parts = Vec::new();
    let mut start = 0;
    let bytes = lower.as_bytes();
    let mut i = 0;
    let mut quote: Option<u8> = None;
    while i < bytes.len() {
        let c = bytes[i];
        match quote {
            Some(qc) if c == qc => quote = None,
            Some(_) => {}
            None if c == b'\'' || c == b'"' => quote = Some(c),
            None if lower[i..].starts_with(" and ") => {
                parts.push(&q[start..i]);
                start = i + 5;
                i += 4;
            }
            None => {}
        }
        i += 1;
    }
    parts.push(&q[start..]);
    for p in parts {
        let p = p.trim().trim_start_matches('(').trim_end_matches(')').trim();
        let (field, value, op) = if let Some(idx) = p.find("!=") {
            (p[..idx].trim(), p[idx + 2..].trim(), "!=")
        } else if let Some(idx) = p.find('=') {
            (p[..idx].trim(), p[idx + 1..].trim(), "=")
        } else {
            return Err(invalid(format!("Unsupported list filter: {p}. Use Field = 'value' joined with AND.")));
        };
        let value = value.trim_matches(|c| c == '\'' || c == '"').to_string();
        let col: &'static str = match (field.trim_matches('`'), op) {
            ("WorkflowId", "=") => "workflow_id =",
            ("WorkflowId", _) => "workflow_id !=",
            ("RunId", "=") => "run_id =",
            ("RunId", _) => "run_id !=",
            ("WorkflowType", "=") => "workflow_type =",
            ("WorkflowType", _) => "workflow_type !=",
            ("TaskQueue", "=") => "task_queue =",
            ("TaskQueue", _) => "task_queue !=",
            ("ExecutionStatus", op) => {
                let st = match value.as_str() {
                    "Running" => 1,
                    "Completed" => 2,
                    "Failed" => 3,
                    "Canceled" => 4,
                    "Terminated" => 5,
                    "ContinuedAsNew" => 6,
                    "TimedOut" => 7,
                    other => other.parse::<i64>().map_err(|_| invalid(format!("Unknown ExecutionStatus {other}")))?,
                };
                f.conds.push((if op == "=" { "status =" } else { "status !=" }, st.into()));
                continue;
            }
            (other, _) => return Err(invalid(format!("Filtering on {other} is not supported by this server yet."))),
        };
        f.conds.push((col, value.into()));
    }
    Ok(f)
}

impl Inner {
    /// History of a run starting at `from` (an event id). Returns the events and whether the run is closed.
    pub fn history_from(&mut self, key: &Key, from: i64, close_only: bool) -> R<(Vec<history::HistoryEvent>, bool, i64)> {
        if !self.ensure_loaded(key)? {
            return Err(wf_not_found("", &key.1));
        }
        let ex = &self.execs[key];
        let closed = !ex.s.is_open();
        if close_only {
            let ev = if closed { ex.events.last().cloned().into_iter().collect() } else { vec![] };
            return Ok((ev, closed, ex.s.next_event_id));
        }
        let ev = ex.events.iter().filter(|e| e.event_id >= from).cloned().collect();
        Ok((ev, closed, ex.s.next_event_id))
    }

    pub fn notify_of_run(&mut self, key: &Key) -> R<Arc<Notify>> {
        if !self.ensure_loaded(key)? {
            return Err(wf_not_found("", &key.1));
        }
        Ok(self.execs[key].notify.clone())
    }

    pub fn describe(&mut self, key: &Key) -> R<wsv1::DescribeWorkflowExecutionResponse> {
        if !self.ensure_loaded(key)? {
            return Err(wf_not_found("", &key.1));
        }
        let ex = &self.execs[key];
        let s = &ex.s;
        let mut acts: Vec<_> = s.activities.values().collect();
        acts.sort_by_key(|a| a.scheduled_id);
        let mut kids: Vec<_> = s.children.values().collect();
        kids.sort_by_key(|c| c.initiated_id);
        Ok(wsv1::DescribeWorkflowExecutionResponse {
            execution_config: Some(workflow_pb::WorkflowExecutionConfig {
                task_queue: Some(taskqueue::TaskQueue { name: s.task_queue.clone(), kind: enums::TaskQueueKind::Normal as i32, normal_name: String::new() }),
                workflow_execution_timeout: s.execution_timeout,
                workflow_run_timeout: s.run_timeout,
                default_workflow_task_timeout: s.wft_timeout,
                user_metadata: ex.events.first().and_then(|e| e.user_metadata.clone()),
            }),
            workflow_execution_info: Some(exec_info(ex)),
            pending_activities: acts.into_iter().map(Inner::pending_activity_info).collect(),
            pending_children: kids
                .into_iter()
                .map(|c| workflow_pb::PendingChildExecutionInfo {
                    workflow_id: c.workflow_id.clone(),
                    run_id: c.run_id.clone(),
                    workflow_type_name: c.workflow_type.clone(),
                    initiated_id: c.initiated_id,
                    parent_close_policy: c.parent_close_policy,
                })
                .collect(),
            pending_workflow_task: s.wft.as_ref().map(|w| workflow_pb::PendingWorkflowTaskInfo {
                state: if w.started_id > 0 { enums::PendingWorkflowTaskState::Started as i32 } else { enums::PendingWorkflowTaskState::Scheduled as i32 },
                scheduled_time: Some(ts_from_ms(w.scheduled_ms)),
                original_scheduled_time: Some(ts_from_ms(w.original_scheduled_ms)),
                started_time: if w.started_ms > 0 { Some(ts_from_ms(w.started_ms)) } else { None },
                attempt: w.attempt,
            }),
            ..Default::default()
        })
    }

    pub fn list(&mut self, ns: &str, query: &str, page_size: i32, token: &[u8]) -> R<(Vec<workflow_pb::WorkflowExecutionInfo>, Vec<u8>)> {
        self.ns_id(ns)?;
        let f = parse_query(query)?;
        let size = if page_size <= 0 { 1000 } else { page_size.min(1000) } as i64;
        let offset = std::str::from_utf8(token).ok().and_then(|s| s.parse::<i64>().ok()).unwrap_or(0);
        // Make sure in-memory changes are visible.
        self.commit()?;
        let rows = self.store.list(ns, &f, size, offset).map_err(internal)?;
        let next = if rows.len() as i64 == size { (offset + size).to_string().into_bytes() } else { vec![] };
        Ok((rows, next))
    }

    pub fn count(&mut self, ns: &str, query: &str) -> R<i64> {
        self.ns_id(ns)?;
        let f = parse_query(query)?;
        self.commit()?;
        self.store.count(ns, &f).map_err(internal)
    }
}

#[cfg(test)]
mod tests {
    use super::parse_query;

    #[test]
    fn parses_simple_filters() {
        let f = parse_query("WorkflowType = 'Hello' AND ExecutionStatus = \"Running\" ORDER BY StartTime DESC").unwrap();
        assert_eq!(f.conds.len(), 2);
        assert_eq!(f.conds[0].0, "workflow_type =");
        assert_eq!(f.conds[1].0, "status =");
        assert!(parse_query("").unwrap().conds.is_empty());
        // "and" inside a quoted value is not a separator
        let f = parse_query("WorkflowId = 'a and b'").unwrap();
        assert_eq!(f.conds.len(), 1);
    }

    #[test]
    fn rejects_unsupported_filters() {
        assert!(parse_query("CustomKeywordField = 'x'").is_err());
        assert!(parse_query("StartTime > '2020-01-01'").is_err());
    }
}
