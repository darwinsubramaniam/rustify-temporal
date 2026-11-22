//! The timer loop: fires due timers (user timers, task timeouts, retries, run timeouts).

use std::cmp::Reverse;
use std::sync::Arc;

use super::events::push_external;
use super::*;
use history::history_event::Attributes as A;

impl Engine {
    /// Runs forever; spawn it once.
    pub async fn run_timers(self: Arc<Engine>) {
        loop {
            let notify = self.timer_notify.clone();
            let fut = notify.notified();
            tokio::pin!(fut);
            fut.as_mut().enable();
            let next = {
                let mut g = self.lock();
                g.fire_due_timers();
                if let Err(e) = g.commit() {
                    tracing::error!("saving after timers failed: {e}");
                }
                g.timers.peek().map(|Reverse(t)| t.at_ms)
            };
            match next {
                Some(at) => {
                    let wait = (at - now_ms()).max(0);
                    let _ = tokio::time::timeout(std_dur(wait), fut).await;
                }
                None => fut.await,
            }
        }
    }
}

impl Inner {
    pub fn fire_due_timers(&mut self) {
        let now = now_ms();
        let mut n = 0;
        while let Some(Reverse(t)) = self.timers.peek() {
            if t.at_ms > now || n > 5000 {
                break;
            }
            n += 1;
            let Reverse(t) = self.timers.pop().expect("peeked");
            if let Err(e) = self.fire(t.kind) {
                tracing::warn!("timer failed: {e}");
            }
        }
    }

    fn fire(&mut self, k: TimerKind) -> R<()> {
        match k {
            TimerKind::User { key, timer_id, started_id } => {
                let valid = self.execs.get(&key).is_some_and(|ex| ex.s.is_open() && ex.s.timers.get(&timer_id).is_some_and(|t| t.started_id == started_id));
                if valid {
                    let mut ex = self.take(&key)?;
                    ex.s.timers.remove(&timer_id);
                    push_external(&mut ex, A::TimerFiredEventAttributes(history::TimerFiredEventAttributes { timer_id, started_event_id: started_id }));
                    self.ensure_wft(&mut ex);
                    self.put(ex);
                }
            }
            TimerKind::WftTimeout { key, scheduled_id, started_id } => self.wft_timed_out(&key, scheduled_id, started_id)?,
            TimerKind::StickyTimeout { key, scheduled_id } => self.sticky_timed_out(&key, scheduled_id),
            TimerKind::WftDue { key } => self.wft_due(&key)?,
            TimerKind::ActRetry { key, scheduled_id, attempt } => self.activity_retry_due(&key, scheduled_id, attempt),
            TimerKind::ActScheduleToStart { key, scheduled_id, attempt } => {
                self.activity_timer(&key, scheduled_id, Some(attempt), enums::TimeoutType::ScheduleToStart)?
            }
            TimerKind::ActStartToClose { key, scheduled_id, attempt } => {
                self.activity_timer(&key, scheduled_id, Some(attempt), enums::TimeoutType::StartToClose)?
            }
            TimerKind::ActHeartbeat { key, scheduled_id, attempt } => {
                self.activity_timer(&key, scheduled_id, Some(attempt), enums::TimeoutType::Heartbeat)?
            }
            TimerKind::ActScheduleToClose { key, scheduled_id } => {
                self.activity_timer(&key, scheduled_id, None, enums::TimeoutType::ScheduleToClose)?
            }
            TimerKind::RunTimeout { key } => {
                let due = self.execs.get(&key).is_some_and(|ex| {
                    if !ex.s.is_open() {
                        return false;
                    }
                    let run = dur_ms(&ex.s.run_timeout);
                    let mut deadline = if run > 0 { ex.s.execution_ms.max(ex.s.start_ms) + run } else { i64::MAX };
                    if ex.s.expiration_ms > 0 {
                        deadline = deadline.min(ex.s.expiration_ms);
                    }
                    now_ms() >= deadline
                });
                if due {
                    self.run_timed_out(&key)?;
                }
            }
            TimerKind::QueryStickyFallback { query_id } => self.query_sticky_fallback(&query_id),
        }
        Ok(())
    }
}
