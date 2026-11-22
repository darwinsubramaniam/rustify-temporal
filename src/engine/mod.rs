//! The workflow engine: one process holds every open run in memory, persists each change to
//! SQLite, hands out workflow and activity tasks through long-polled task queues, and runs the
//! server-side timers (user timers, task timeouts, retries, run timeouts).

pub mod activity;
mod events;
pub mod namespace;
mod query;
mod timers;
mod visibility;
mod wft;
mod workflow;

use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration as StdDuration;

use tokio::sync::{Notify, oneshot};
use tonic::Status;

use crate::state::ExecState;
use crate::store::{SaveItem, Store};
use crate::util::*;

pub use query::QueryOutcome;

pub type Key = (String, String); // (namespace, run_id)

pub const DEFAULT_WFT_TIMEOUT_MS: i64 = 10_000;
pub const MAX_WFT_TIMEOUT_MS: i64 = 120_000;
pub const DEFAULT_STICKY_TIMEOUT_MS: i64 = 5_000;
pub const LONG_POLL_MS: i64 = 55_000;
pub const HISTORY_LONG_POLL_MS: i64 = 20_000;

pub struct Execution {
    pub s: ExecState,
    pub events: Vec<history::HistoryEvent>,
    pub saved_events: usize,
    pub notify: Arc<Notify>,
}

impl Execution {
    pub fn new(s: ExecState, events: Vec<history::HistoryEvent>) -> Self {
        let saved = events.len();
        Execution {
            s,
            events,
            saved_events: saved,
            notify: Arc::new(Notify::new()),
        }
    }
    pub fn key(&self) -> Key {
        (self.s.namespace.clone(), self.s.run_id.clone())
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum QKind {
    Workflow,
    Activity,
}

pub type QKey = (String, String, QKind); // (namespace, name, kind)

#[derive(Clone, Debug)]
pub enum TaskRef {
    Wft {
        run_id: String,
        scheduled_id: i64,
    },
    Query {
        query_id: String,
    },
    Activity {
        run_id: String,
        scheduled_id: i64,
        attempt: i32,
    },
}

#[derive(Default)]
pub struct Queue {
    pub tasks: VecDeque<TaskRef>,
    pub notify: Arc<Notify>,
    pub pollers: HashMap<String, i64>,
}

#[derive(Clone, Debug)]
pub enum TimerKind {
    User {
        key: Key,
        timer_id: String,
        started_id: i64,
    },
    WftTimeout {
        key: Key,
        scheduled_id: i64,
        started_id: i64,
    },
    StickyTimeout {
        key: Key,
        scheduled_id: i64,
    },
    WftDue {
        key: Key,
    },
    ActRetry {
        key: Key,
        scheduled_id: i64,
        attempt: i32,
    },
    ActScheduleToStart {
        key: Key,
        scheduled_id: i64,
        attempt: i32,
    },
    ActStartToClose {
        key: Key,
        scheduled_id: i64,
        attempt: i32,
    },
    ActHeartbeat {
        key: Key,
        scheduled_id: i64,
        attempt: i32,
    },
    ActScheduleToClose {
        key: Key,
        scheduled_id: i64,
    },
    RunTimeout {
        key: Key,
    },
    QueryStickyFallback {
        query_id: String,
    },
}

pub struct TimerEntry {
    pub at_ms: i64,
    pub seq: u64,
    pub kind: TimerKind,
}
impl PartialEq for TimerEntry {
    fn eq(&self, o: &Self) -> bool {
        self.at_ms == o.at_ms && self.seq == o.seq
    }
}
impl Eq for TimerEntry {}
impl PartialOrd for TimerEntry {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for TimerEntry {
    fn cmp(&self, o: &Self) -> Ordering {
        (self.at_ms, self.seq).cmp(&(o.at_ms, o.seq))
    }
}

/// Work that touches another run, done after the current change (children, signals to other
/// workflows, telling a parent its child closed).
// Short-lived values passed once; boxing the large variant would only add allocations.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
pub enum Action {
    StartChild {
        parent: Key,
        initiated_id: i64,
        attrs: history::StartChildWorkflowExecutionInitiatedEventAttributes,
    },
    ChildClosed {
        parent: crate::state::ParentInfo,
        child_ns: String,
        child: common::WorkflowExecution,
        child_type: String,
        close: history::history_event::Attributes,
    },
    SignalExternal {
        source: Key,
        initiated_id: i64,
        attrs: history::SignalExternalWorkflowExecutionInitiatedEventAttributes,
        request_id: String,
    },
    CancelExternal {
        source: Key,
        initiated_id: i64,
        attrs: history::RequestCancelExternalWorkflowExecutionInitiatedEventAttributes,
    },
    ApplyParentClosePolicy {
        ns: String,
        workflow_id: String,
        run_id: String,
        policy: i32,
        parent: common::WorkflowExecution,
    },
}

pub struct NsEntry {
    pub id: String,
    pub desc: wsv1::DescribeNamespaceResponse,
}

pub struct Inner {
    pub store: Store,
    pub namespaces: HashMap<String, NsEntry>,
    pub execs: HashMap<Key, Execution>,
    pub current: HashMap<(String, String), String>,
    pub queues: HashMap<QKey, Queue>,
    pub timers: BinaryHeap<Reverse<TimerEntry>>,
    pub timer_seq: u64,
    pub timer_notify: Arc<Notify>,
    pub queries: HashMap<String, query::PendingQuery>,
    pub dirty: HashSet<Key>,
    pub actions: VecDeque<Action>,
}

pub struct Engine {
    inner: Mutex<Inner>,
    pub timer_notify: Arc<Notify>,
}

impl Engine {
    pub fn open(store: Store) -> anyhow::Result<Arc<Engine>> {
        let timer_notify = Arc::new(Notify::new());
        let mut inner = Inner {
            store,
            namespaces: HashMap::new(),
            execs: HashMap::new(),
            current: HashMap::new(),
            queues: HashMap::new(),
            timers: BinaryHeap::new(),
            timer_seq: 0,
            timer_notify: timer_notify.clone(),
            queries: HashMap::new(),
            dirty: HashSet::new(),
            actions: VecDeque::new(),
        };
        inner.recover()?;
        Ok(Arc::new(Engine {
            inner: Mutex::new(inner),
            timer_notify,
        }))
    }

    pub fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Run a change under the lock and save everything it touched.
    pub fn op<T>(&self, f: impl FnOnce(&mut Inner) -> R<T>) -> R<T> {
        let mut g = self.lock();
        let r = f(&mut g);
        let c = g.commit();
        let v = r?;
        c?;
        Ok(v)
    }

    /// Long-poll helper: `take` is tried, and if it yields nothing we wait for `notify` (or the deadline).
    pub async fn long_poll<T>(
        &self,
        timeout_ms: i64,
        notify_of: impl Fn(&mut Inner) -> R<Arc<Notify>>,
        take: impl Fn(&mut Inner) -> R<Option<T>>,
    ) -> R<Option<T>> {
        let deadline = tokio::time::Instant::now() + std_dur(timeout_ms);
        loop {
            let notify = notify_of(&mut self.lock())?;
            let fut = notify.notified();
            tokio::pin!(fut);
            fut.as_mut().enable();
            if let Some(v) = self.op(|i| take(i))? {
                return Ok(Some(v));
            }
            if tokio::time::timeout_at(deadline, fut).await.is_err() {
                return Ok(None);
            }
        }
    }
}

/// Poll deadline: the caller's grpc-timeout minus a margin, capped at the long-poll interval.
pub fn poll_timeout_ms(req_timeout: Option<StdDuration>, cap: i64) -> i64 {
    match req_timeout {
        Some(d) => ((d.as_millis() as i64) - 1500).clamp(1000, cap),
        None => cap,
    }
}

impl Inner {
    // ---------- recovery ----------

    fn recover(&mut self) -> anyhow::Result<()> {
        self.load_namespaces()?;
        for (ns, wid, rid) in self.store.load_current()? {
            self.current.insert((ns, wid), rid);
        }
        let running = enums::WorkflowExecutionStatus::Running as i32;
        let open = self.store.load_open(running)?;
        let n = open.len();
        for (s, events) in open {
            let mut ex = Execution::new(s, events);
            self.rearm(&mut ex);
            self.execs.insert(ex.key(), ex);
        }
        if n > 0 {
            tracing::info!("recovered {n} open workflow runs");
        }
        let _ = self.commit();
        Ok(())
    }

    /// Re-create the in-memory queue entries and timers of an open run after a restart.
    fn rearm(&mut self, ex: &mut Execution) {
        let key = ex.key();
        let now = now_ms();
        // Sticky workers don't survive a restart.
        ex.s.sticky_queue.clear();
        if let Some(w) = ex.s.wft.as_mut() {
            if w.started_id > 0 {
                let at = w.started_ms + w.timeout_ms;
                let (sid, st) = (w.scheduled_id, w.started_id);
                self.add_timer(
                    at,
                    TimerKind::WftTimeout {
                        key: key.clone(),
                        scheduled_id: sid,
                        started_id: st,
                    },
                );
            } else {
                w.queue = ex.s.task_queue.clone();
                let sid = w.scheduled_id;
                self.enqueue(
                    &key.0,
                    &ex.s.task_queue,
                    QKind::Workflow,
                    TaskRef::Wft {
                        run_id: key.1.clone(),
                        scheduled_id: sid,
                    },
                );
            }
        }
        if ex.s.wft_due_ms > 0 {
            self.add_timer(ex.s.wft_due_ms, TimerKind::WftDue { key: key.clone() });
        }
        let acts: Vec<_> = ex.s.activities.values().cloned().collect();
        for a in acts {
            let attrs = a.attrs.clone().unwrap_or_default();
            let tq = attrs.task_queue.as_ref().map(|t| t.name.clone()).unwrap_or_default();
            match a.state {
                crate::state::ACT_SCHEDULED => {
                    self.enqueue(
                        &key.0,
                        &tq,
                        QKind::Activity,
                        TaskRef::Activity {
                            run_id: key.1.clone(),
                            scheduled_id: a.scheduled_id,
                            attempt: a.attempt,
                        },
                    );
                    let s2s = dur_ms(&attrs.schedule_to_start_timeout);
                    if s2s > 0 {
                        self.add_timer(
                            a.attempt_scheduled_ms + s2s,
                            TimerKind::ActScheduleToStart {
                                key: key.clone(),
                                scheduled_id: a.scheduled_id,
                                attempt: a.attempt,
                            },
                        );
                    }
                }
                crate::state::ACT_STARTED => {
                    let st2c = dur_ms(&attrs.start_to_close_timeout);
                    if st2c > 0 {
                        self.add_timer(
                            a.started_ms + st2c,
                            TimerKind::ActStartToClose {
                                key: key.clone(),
                                scheduled_id: a.scheduled_id,
                                attempt: a.attempt,
                            },
                        );
                    }
                    let hb = dur_ms(&attrs.heartbeat_timeout);
                    if hb > 0 {
                        let last = a.last_heartbeat_ms.max(a.started_ms);
                        self.add_timer(
                            last + hb,
                            TimerKind::ActHeartbeat {
                                key: key.clone(),
                                scheduled_id: a.scheduled_id,
                                attempt: a.attempt,
                            },
                        );
                    }
                }
                _ => {
                    self.add_timer(
                        a.next_attempt_ms,
                        TimerKind::ActRetry {
                            key: key.clone(),
                            scheduled_id: a.scheduled_id,
                            attempt: a.attempt,
                        },
                    );
                }
            }
            if a.expiration_ms > 0 {
                self.add_timer(
                    a.expiration_ms,
                    TimerKind::ActScheduleToClose {
                        key: key.clone(),
                        scheduled_id: a.scheduled_id,
                    },
                );
            }
        }
        let timers: Vec<_> = ex.s.timers.values().cloned().collect();
        for t in timers {
            self.add_timer(
                t.fire_ms,
                TimerKind::User {
                    key: key.clone(),
                    timer_id: t.timer_id,
                    started_id: t.started_id,
                },
            );
        }
        self.arm_run_timeout(ex);
        let _ = now;
        self.dirty.insert(key);
    }

    pub fn arm_run_timeout(&mut self, ex: &Execution) {
        let run = dur_ms(&ex.s.run_timeout);
        let mut deadline = if run > 0 {
            ex.s.execution_ms.max(ex.s.start_ms) + run
        } else {
            0
        };
        if ex.s.expiration_ms > 0 && (deadline == 0 || ex.s.expiration_ms < deadline) {
            deadline = ex.s.expiration_ms;
        }
        if deadline > 0 {
            self.add_timer(deadline, TimerKind::RunTimeout { key: ex.key() });
        }
    }

    // ---------- runs ----------

    /// Make sure a run is in memory (closed runs are loaded from disk on demand).
    pub fn ensure_loaded(&mut self, key: &Key) -> R<bool> {
        if self.execs.contains_key(key) {
            return Ok(true);
        }
        match self.store.load_execution(&key.0, &key.1).map_err(internal)? {
            Some((s, ev)) => {
                self.execs.insert(key.clone(), Execution::new(s, ev));
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Take a run out of the map to change it; put it back with [`Inner::put`].
    pub fn take(&mut self, key: &Key) -> R<Execution> {
        if !self.ensure_loaded(key)? {
            return Err(wf_not_found("", &key.1));
        }
        Ok(self.execs.remove(key).expect("loaded"))
    }

    pub fn put(&mut self, ex: Execution) {
        let k = ex.key();
        self.dirty.insert(k.clone());
        self.execs.insert(k, ex);
    }

    /// Find the run for a workflow id (and optional run id).
    pub fn resolve(&mut self, ns: &str, wid: &str, rid: &str) -> R<Key> {
        self.ns_id(ns)?;
        if wid.is_empty() {
            return Err(invalid("WorkflowId is not set on request."));
        }
        if !rid.is_empty() {
            let key = (ns.to_string(), rid.to_string());
            if !self.ensure_loaded(&key)? || self.execs[&key].s.workflow_id != wid {
                return Err(wf_not_found(wid, rid));
            }
            return Ok(key);
        }
        match self.current.get(&(ns.to_string(), wid.to_string())) {
            Some(r) => Ok((ns.to_string(), r.clone())),
            None => Err(wf_not_found(wid, "")),
        }
    }

    // ---------- queues ----------

    pub fn queue(&mut self, ns: &str, name: &str, kind: QKind) -> &mut Queue {
        self.queues.entry((ns.to_string(), name.to_string(), kind)).or_default()
    }

    pub fn enqueue(&mut self, ns: &str, name: &str, kind: QKind, t: TaskRef) {
        let q = self.queue(ns, name, kind);
        q.tasks.push_back(t);
        q.notify.notify_waiters();
    }

    // ---------- timers ----------

    pub fn add_timer(&mut self, at_ms: i64, kind: TimerKind) {
        self.timer_seq += 1;
        let first = self.timers.peek().map(|Reverse(t)| t.at_ms);
        self.timers.push(Reverse(TimerEntry {
            at_ms,
            seq: self.timer_seq,
            kind,
        }));
        if first.is_none_or(|f| at_ms < f) {
            self.timer_notify.notify_one();
        }
    }

    // ---------- saving ----------

    pub fn commit(&mut self) -> R<()> {
        self.run_actions();
        if self.dirty.is_empty() {
            return Ok(());
        }
        let keys: Vec<Key> = self.dirty.drain().collect();
        let mut snapshots = Vec::new();
        for k in &keys {
            if let Some(ex) = self.execs.get(k) {
                let is_current = self.current.get(&(k.0.clone(), ex.s.workflow_id.clone())) == Some(&k.1);
                snapshots.push((k.clone(), ex.saved_events, is_current));
            }
        }
        let items: Vec<SaveItem<'_>> = snapshots
            .iter()
            .map(|(k, saved, is_current)| {
                let ex = &self.execs[k];
                SaveItem {
                    state: &ex.s,
                    new_events: &ex.events[*saved..],
                    info: visibility::exec_info(ex),
                    is_current: *is_current,
                }
            })
            .collect();
        let res = self.store.save(&items);
        drop(items);
        match res {
            Ok(()) => {
                for (k, _, _) in snapshots {
                    if let Some(ex) = self.execs.get_mut(&k) {
                        ex.saved_events = ex.events.len();
                        // Closed runs are reloaded from disk when needed.
                        if !ex.s.is_open() && ex.s.buffered.is_empty() {
                            let has_queries = self.queries.values().any(|q| q.key == k);
                            if !has_queries {
                                self.execs.remove(&k);
                            }
                        }
                    }
                }
                Ok(())
            }
            Err(e) => {
                tracing::error!("saving state failed: {e:#}");
                Err(internal(e))
            }
        }
    }
}

pub fn internal(e: impl std::fmt::Display) -> Status {
    Status::internal(e.to_string())
}

pub type QueryTx = oneshot::Sender<QueryOutcome>;
