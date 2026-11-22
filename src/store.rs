//! Durable storage in SQLite: namespaces, each run's mutable state, its history events,
//! the current run per workflow id, and a visibility row per run for listing.

use anyhow::Result;
use prost::Message;
use rusqlite::{Connection, OptionalExtension, params, params_from_iter};

use crate::state::ExecState;
use crate::util::{history, workflow_pb};

pub struct Store {
    conn: Connection,
}

/// One run to save: its state, the events not yet saved, and its visibility record.
pub struct SaveItem<'a> {
    pub state: &'a ExecState,
    pub new_events: &'a [history::HistoryEvent],
    pub info: workflow_pb::WorkflowExecutionInfo,
    pub is_current: bool,
}

/// Visibility filter: column = value pairs joined with AND.
#[derive(Default, Debug, Clone)]
pub struct ListFilter {
    pub conds: Vec<(&'static str, rusqlite::types::Value)>,
}

impl Store {
    pub fn open(path: &str) -> Result<Store> {
        let conn = if path == ":memory:" { Connection::open_in_memory()? } else { Connection::open(path)? };
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             PRAGMA foreign_keys=OFF;
             CREATE TABLE IF NOT EXISTS namespaces (
                name TEXT PRIMARY KEY,
                id   TEXT NOT NULL,
                data BLOB NOT NULL
             );
             CREATE TABLE IF NOT EXISTS executions (
                namespace     TEXT NOT NULL,
                run_id        TEXT NOT NULL,
                workflow_id   TEXT NOT NULL,
                workflow_type TEXT NOT NULL,
                task_queue    TEXT NOT NULL,
                status        INTEGER NOT NULL,
                start_ms      INTEGER NOT NULL,
                close_ms      INTEGER NOT NULL,
                state         BLOB NOT NULL,
                info          BLOB NOT NULL,
                PRIMARY KEY (namespace, run_id)
             );
             CREATE INDEX IF NOT EXISTS executions_by_start ON executions(namespace, start_ms DESC);
             CREATE INDEX IF NOT EXISTS executions_by_status ON executions(status);
             CREATE TABLE IF NOT EXISTS events (
                namespace TEXT NOT NULL,
                run_id    TEXT NOT NULL,
                event_id  INTEGER NOT NULL,
                data      BLOB NOT NULL,
                PRIMARY KEY (namespace, run_id, event_id)
             );
             CREATE TABLE IF NOT EXISTS current_executions (
                namespace   TEXT NOT NULL,
                workflow_id TEXT NOT NULL,
                run_id      TEXT NOT NULL,
                PRIMARY KEY (namespace, workflow_id)
             );",
        )?;
        Ok(Store { conn })
    }

    pub fn load_namespaces(&self) -> Result<Vec<Vec<u8>>> {
        let mut st = self.conn.prepare("SELECT data FROM namespaces ORDER BY name")?;
        let rows = st.query_map([], |r| r.get::<_, Vec<u8>>(0))?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn save_namespace(&self, name: &str, id: &str, data: &[u8]) -> Result<()> {
        self.conn.execute(
            "INSERT INTO namespaces(name, id, data) VALUES (?1, ?2, ?3)
             ON CONFLICT(name) DO UPDATE SET id = excluded.id, data = excluded.data",
            params![name, id, data],
        )?;
        Ok(())
    }

    pub fn load_current(&self) -> Result<Vec<(String, String, String)>> {
        let mut st = self.conn.prepare("SELECT namespace, workflow_id, run_id FROM current_executions")?;
        let rows = st.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    fn load_events(&self, ns: &str, run_id: &str) -> Result<Vec<history::HistoryEvent>> {
        let mut st = self
            .conn
            .prepare_cached("SELECT data FROM events WHERE namespace = ?1 AND run_id = ?2 ORDER BY event_id")?;
        let rows = st.query_map(params![ns, run_id], |r| r.get::<_, Vec<u8>>(0))?;
        let mut out = Vec::new();
        for b in rows {
            out.push(history::HistoryEvent::decode(b?.as_slice())?);
        }
        Ok(out)
    }

    /// Every run still open (status Running), with its history.
    pub fn load_open(&self, running_status: i32) -> Result<Vec<(ExecState, Vec<history::HistoryEvent>)>> {
        let mut st = self.conn.prepare("SELECT state FROM executions WHERE status = ?1")?;
        let rows = st.query_map(params![running_status], |r| r.get::<_, Vec<u8>>(0))?;
        let mut out = Vec::new();
        for b in rows {
            let s = ExecState::decode(b?.as_slice())?;
            let ev = self.load_events(&s.namespace, &s.run_id)?;
            out.push((s, ev));
        }
        Ok(out)
    }

    pub fn load_execution(&self, ns: &str, run_id: &str) -> Result<Option<(ExecState, Vec<history::HistoryEvent>)>> {
        let b: Option<Vec<u8>> = self
            .conn
            .query_row(
                "SELECT state FROM executions WHERE namespace = ?1 AND run_id = ?2",
                params![ns, run_id],
                |r| r.get(0),
            )
            .optional()?;
        match b {
            None => Ok(None),
            Some(b) => {
                let s = ExecState::decode(b.as_slice())?;
                let ev = self.load_events(ns, run_id)?;
                Ok(Some((s, ev)))
            }
        }
    }

    /// Save several runs in one transaction.
    pub fn save(&mut self, items: &[SaveItem<'_>]) -> Result<()> {
        let tx = self.conn.transaction()?;
        {
            let mut up = tx.prepare_cached(
                "INSERT INTO executions(namespace, run_id, workflow_id, workflow_type, task_queue, status, start_ms, close_ms, state, info)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                 ON CONFLICT(namespace, run_id) DO UPDATE SET status = excluded.status, close_ms = excluded.close_ms,
                    state = excluded.state, info = excluded.info, workflow_type = excluded.workflow_type,
                    task_queue = excluded.task_queue",
            )?;
            let mut ev = tx.prepare_cached(
                "INSERT OR REPLACE INTO events(namespace, run_id, event_id, data) VALUES (?1, ?2, ?3, ?4)",
            )?;
            let mut cur = tx.prepare_cached(
                "INSERT INTO current_executions(namespace, workflow_id, run_id) VALUES (?1, ?2, ?3)
                 ON CONFLICT(namespace, workflow_id) DO UPDATE SET run_id = excluded.run_id",
            )?;
            for it in items {
                let s = it.state;
                up.execute(params![
                    s.namespace,
                    s.run_id,
                    s.workflow_id,
                    s.workflow_type,
                    s.task_queue,
                    s.status,
                    s.start_ms,
                    s.close_ms,
                    s.encode_to_vec(),
                    it.info.encode_to_vec()
                ])?;
                for e in it.new_events {
                    ev.execute(params![s.namespace, s.run_id, e.event_id, e.encode_to_vec()])?;
                }
                if it.is_current {
                    cur.execute(params![s.namespace, s.workflow_id, s.run_id])?;
                }
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn where_clause(ns: &str, f: &ListFilter) -> (String, Vec<rusqlite::types::Value>) {
        let mut sql = String::from("namespace = ?");
        let mut args: Vec<rusqlite::types::Value> = vec![ns.to_string().into()];
        for (col, v) in &f.conds {
            sql.push_str(&format!(" AND {col} ?"));
            args.push(v.clone());
        }
        (sql, args)
    }

    pub fn list(&self, ns: &str, f: &ListFilter, limit: i64, offset: i64) -> Result<Vec<workflow_pb::WorkflowExecutionInfo>> {
        let (w, mut args) = Self::where_clause(ns, f);
        let sql = format!("SELECT info FROM executions WHERE {w} ORDER BY start_ms DESC, run_id LIMIT ? OFFSET ?");
        args.push(limit.into());
        args.push(offset.into());
        let mut st = self.conn.prepare(&sql)?;
        let rows = st.query_map(params_from_iter(args.iter()), |r| r.get::<_, Vec<u8>>(0))?;
        let mut out = Vec::new();
        for b in rows {
            out.push(workflow_pb::WorkflowExecutionInfo::decode(b?.as_slice())?);
        }
        Ok(out)
    }

    pub fn count(&self, ns: &str, f: &ListFilter) -> Result<i64> {
        let (w, args) = Self::where_clause(ns, f);
        let sql = format!("SELECT COUNT(*) FROM executions WHERE {w}");
        Ok(self.conn.query_row(&sql, params_from_iter(args.iter()), |r| r.get(0))?)
    }
}
