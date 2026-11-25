//! Reading runs: history (with long-poll), describe, and list/count with a small query language.

use super::*;
use crate::store::ListFilter;

/// A history continuation token (opaque to clients).
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct HistTok {
    /// Run id.
    pub r: String,
    /// First event id of the snapshot being read.
    pub f: i64,
    /// Next event id when the snapshot was taken (reverse: the newest event left to read).
    pub n: i64,
    /// The run was open at the snapshot.
    pub run: bool,
    /// First event id of the next batch to read (0: the snapshot is read).
    pub c: i64,
}

impl HistTok {
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }
    pub fn decode(b: &[u8]) -> R<HistTok> {
        serde_json::from_slice(b).map_err(|_| invalid("Invalid NextPageToken."))
    }
}

/// The history's batches as (first, last) event ids, oldest first. Events written before
/// batches were recorded count as one batch each.
fn batches(ex: &Execution) -> Vec<(i64, i64)> {
    let last = ex.s.next_event_id - 1;
    let recorded = ex.s.batch_starts.first().copied().unwrap_or(last + 1);
    let mut starts: Vec<i64> = (1..recorded.min(last + 1)).collect();
    starts.extend(ex.s.batch_starts.iter().copied().filter(|&s| s <= last));
    starts
        .iter()
        .enumerate()
        .map(|(i, &a)| (a, starts.get(i + 1).map(|b| b - 1).unwrap_or(last)))
        .collect()
}

/// The events of `ranges` below `before`.
fn events_in(ex: &Execution, ranges: &[(i64, i64)], before: i64) -> Vec<history::HistoryEvent> {
    let mut out = Vec::new();
    for &(a, b) in ranges {
        for id in a..=b.min(before - 1) {
            if let Some(e) = ex.events.get((id - 1) as usize).filter(|e| e.event_id == id) {
                out.push(e.clone());
            } else if let Some(e) = ex.events.iter().find(|e| e.event_id == id) {
                out.push(e.clone());
            }
        }
    }
    out
}

pub fn exec_info(ex: &Execution) -> workflow_pb::WorkflowExecutionInfo {
    let s = &ex.s;
    workflow_pb::WorkflowExecutionInfo {
        execution: Some(common::WorkflowExecution {
            workflow_id: s.workflow_id.clone(),
            run_id: s.run_id.clone(),
        }),
        r#type: Some(common::WorkflowType {
            name: s.workflow_type.clone(),
        }),
        start_time: Some(ts_from_ms(s.start_ms)),
        close_time: if s.close_ms > 0 {
            Some(ts_from_ms(s.close_ms))
        } else {
            None
        },
        status: s.status,
        history_length: s.next_event_id - 1,
        parent_namespace_id: s.parent.as_ref().map(|p| p.namespace_id.clone()).unwrap_or_default(),
        parent_execution: s.parent.as_ref().map(|p| common::WorkflowExecution {
            workflow_id: p.workflow_id.clone(),
            run_id: p.run_id.clone(),
        }),
        execution_time: Some(ts_from_ms(s.execution_ms.max(s.start_ms))),
        memo: s.memo.clone(),
        search_attributes: s.search_attributes.clone(),
        task_queue: s.task_queue.clone(),
        state_transition_count: s.transitions,
        history_size_bytes: s.history_size,
        execution_duration: if s.close_ms > 0 {
            Some(dur_from_ms(s.close_ms - s.execution_ms.max(s.start_ms)))
        } else {
            None
        },
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
            return Err(invalid(format!(
                "Unsupported list filter: {p}. Use Field = 'value' joined with AND."
            )));
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
                    other => other
                        .parse::<i64>()
                        .map_err(|_| invalid(format!("Unknown ExecutionStatus {other}")))?,
                };
                f.conds
                    .push((if op == "=" { "status =" } else { "status !=" }, st.into()));
                continue;
            }
            (other, _) => {
                return Err(invalid(format!(
                    "Filtering on {other} is not supported by this server yet."
                )));
            }
        };
        f.conds.push((col, value.into()));
    }
    Ok(f)
}

impl Inner {
    /// Where a history reader stands: the run, the snapshot of events being read
    /// (`first`..`next`), whether the run was open then, and the batch to read next (0 = done
    /// with the snapshot). The Go server's history continuation token.
    pub fn history_start(&mut self, key: &Key) -> R<HistTok> {
        if !self.ensure_loaded(key)? {
            return Err(wf_not_found("", &key.1));
        }
        let ex = &self.execs[key];
        Ok(HistTok {
            r: key.1.clone(),
            f: 1,
            n: ex.s.next_event_id,
            run: ex.s.is_open(),
            c: 0,
        })
    }

    /// The run's next event id and whether it is open.
    pub fn run_progress(&mut self, key: &Key) -> R<(i64, bool)> {
        if !self.ensure_loaded(key)? {
            return Err(wf_not_found("", &key.1));
        }
        let ex = &self.execs[key];
        Ok((ex.s.next_event_id, ex.s.is_open()))
    }

    /// The close event of a closed run.
    pub fn close_event(&mut self, key: &Key) -> R<Option<history::HistoryEvent>> {
        if !self.ensure_loaded(key)? {
            return Err(wf_not_found("", &key.1));
        }
        let ex = &self.execs[key];
        Ok(if ex.s.is_open() {
            None
        } else {
            ex.events.last().cloned()
        })
    }

    /// One page of history: up to `page` batches from where `tok` stands, then (once the
    /// snapshot is read) events written since and the unwritten events of a transient or
    /// speculative workflow task, as the Go server pages it.
    pub fn history_read(&mut self, key: &Key, tok: &mut HistTok, page: usize) -> R<Vec<history::HistoryEvent>> {
        if !self.ensure_loaded(key)? {
            return Err(wf_not_found("", &key.1));
        }
        let ex = &self.execs[key];
        if tok.f >= tok.n {
            return Ok(Vec::new());
        }
        let from = if tok.c > 0 { tok.c } else { tok.f };
        let all = batches(ex);
        let picked: Vec<(i64, i64)> = all
            .iter()
            .copied()
            .filter(|(a, _)| *a >= from && *a < tok.n)
            .take(page)
            .collect();
        let mut events = events_in(ex, &picked, tok.n);
        if picked.len() == page {
            tok.c = picked.last().map(|(_, b)| b + 1).unwrap_or(0);
            return Ok(events);
        }
        tok.c = 0;
        // Events written after the snapshot (one more page of them).
        if ex.s.next_event_id > tok.n {
            let gap: Vec<(i64, i64)> = all.iter().copied().filter(|(a, _)| *a >= tok.n).take(page).collect();
            events.extend(events_in(ex, &gap, ex.s.next_event_id));
            tok.n = ex.s.next_event_id;
            tok.run = ex.s.is_open();
        }
        events.extend(super::events::transient_events(ex));
        Ok(events)
    }

    /// One page of history newest first: up to `page` batches ending at `tok.n` (0 = the end).
    pub fn history_read_reverse(&mut self, key: &Key, tok: &mut HistTok, page: usize) -> R<Vec<history::HistoryEvent>> {
        if !self.ensure_loaded(key)? {
            return Err(wf_not_found("", &key.1));
        }
        let ex = &self.execs[key];
        let max = if tok.n == 0 { ex.s.next_event_id - 1 } else { tok.n };
        let mut picked: Vec<(i64, i64)> = batches(ex).into_iter().filter(|(a, _)| *a <= max).collect();
        let keep = picked.len().saturating_sub(page);
        let picked = picked.split_off(keep);
        let mut events = events_in(ex, &picked, max + 1);
        events.reverse();
        if let Some(last) = events.last() {
            tok.n = last.event_id - 1;
        }
        Ok(events)
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
                task_queue: Some(taskqueue::TaskQueue {
                    name: s.task_queue.clone(),
                    kind: enums::TaskQueueKind::Normal as i32,
                    normal_name: String::new(),
                }),
                workflow_execution_timeout: s.execution_timeout,
                workflow_run_timeout: s.run_timeout,
                default_workflow_task_timeout: s.wft_timeout,
                user_metadata: ex.events.first().and_then(|e| e.user_metadata.clone()),
            }),
            workflow_execution_info: Some({
                let mut info = exec_info(ex);
                info.priority = s.priority.clone().or_else(|| {
                    ex.events.first().and_then(|e| match &e.attributes {
                        Some(history::history_event::Attributes::WorkflowExecutionStartedEventAttributes(a)) => {
                            a.priority.clone()
                        }
                        _ => None,
                    })
                });
                info
            }),
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
                state: if w.started_id > 0 {
                    enums::PendingWorkflowTaskState::Started as i32
                } else {
                    enums::PendingWorkflowTaskState::Scheduled as i32
                },
                scheduled_time: Some(ts_from_ms(w.scheduled_ms)),
                original_scheduled_time: Some(ts_from_ms(w.original_scheduled_ms)),
                started_time: if w.started_ms > 0 {
                    Some(ts_from_ms(w.started_ms))
                } else {
                    None
                },
                attempt: w.attempt,
            }),
            ..Default::default()
        })
    }

    pub fn list(
        &mut self,
        ns: &str,
        query: &str,
        page_size: i32,
        token: &[u8],
    ) -> R<(Vec<workflow_pb::WorkflowExecutionInfo>, Vec<u8>)> {
        self.ns_id(ns)?;
        let f = parse_query(query)?;
        let size = if page_size <= 0 { 1000 } else { page_size.min(1000) } as i64;
        let offset = std::str::from_utf8(token)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);
        // Make sure in-memory changes are visible.
        self.commit()?;
        let rows = self.store.list(ns, &f, size, offset).map_err(internal)?;
        let next = if rows.len() as i64 == size {
            (offset + size).to_string().into_bytes()
        } else {
            vec![]
        };
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
        let f =
            parse_query("WorkflowType = 'Hello' AND ExecutionStatus = \"Running\" ORDER BY StartTime DESC").unwrap();
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
