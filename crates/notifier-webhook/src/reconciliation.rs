use std::{collections::HashSet, future::Future, sync::Mutex, time::Duration};

use anyhow::Result;
use rand::RngExt;
use tracing::{debug, info, warn};

/// Tracks deferred startup reconciliation independently for each source.
#[derive(Default)]
pub struct ReconciliationRetry {
    pending: Mutex<HashSet<String>>,
}

impl ReconciliationRetry {
    pub fn record_result(
        &self,
        source_id: &str,
        plugin: &str,
        result: Result<()>,
        is_transient: impl Fn(&anyhow::Error) -> bool,
    ) -> Result<()> {
        match result {
            Err(error) if is_transient(&error) => {
                self.pending.lock().unwrap().insert(source_id.to_owned());
                warn!(source_id, plugin, error = %format!("{error:#}"),
                    "source reconciliation deferred; registration will retry in the background");
                Ok(())
            }
            result => {
                self.pending.lock().unwrap().remove(source_id);
                result
            }
        }
    }

    /// Retry a deferred source until success or a permanent failure. Cancellation
    /// drops the timer/request; the next server startup reconciles from scratch.
    pub async fn recover<F, Fut>(
        &self,
        source_id: &str,
        plugin: &str,
        mut reconcile: F,
        is_transient: impl Fn(&anyhow::Error) -> bool,
    ) -> Result<()>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        if !self.pending.lock().unwrap().remove(source_id) {
            return Ok(());
        }
        let mut attempt: u32 = 1;
        loop {
            let delay = retry_delay(attempt);
            debug!(
                source_id,
                plugin,
                attempt,
                retry_seconds = delay.as_secs(),
                "waiting to retry source reconciliation"
            );
            tokio::time::sleep(delay).await;
            match reconcile().await {
                Ok(()) => {
                    info!(
                        source_id,
                        plugin, attempt, "source reconciliation recovered"
                    );
                    return Ok(());
                }
                Err(error) if is_transient(&error) => {
                    warn!(source_id, plugin, attempt, error = %format!("{error:#}"),
                        "source reconciliation still unavailable");
                    attempt = attempt.saturating_add(1);
                }
                Err(error) => {
                    // Runtime background-task logs display only the outer error.
                    let message = format!(
                        "{plugin} reconciliation stopped after a non-transient error: {error:#}"
                    );
                    return Err(error.context(message));
                }
            }
        }
    }
}

fn retry_delay(attempt: u32) -> Duration {
    let ceiling = 30_u64
        .saturating_mul(2_u64.saturating_pow(attempt.saturating_sub(1)))
        .min(900);
    Duration::from_secs(rand::rng().random_range(ceiling * 4 / 5..=ceiling))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Error, ErrorKind};

    fn temporary() -> anyhow::Error {
        Error::from(ErrorKind::TimedOut).into()
    }

    fn is_transient(error: &anyhow::Error) -> bool {
        error
            .downcast_ref::<Error>()
            .is_some_and(|e| e.kind() == ErrorKind::TimedOut)
    }

    #[test]
    fn defers_only_transient_failures_and_keeps_sources_independent() {
        let retry = ReconciliationRetry::default();
        retry
            .record_result(
                "outage",
                "test",
                Err(temporary().context("listing hooks")),
                is_transient,
            )
            .unwrap();
        assert!(
            retry
                .record_result(
                    "invalid",
                    "test",
                    Err(anyhow::anyhow!("invalid credentials")),
                    is_transient
                )
                .is_err()
        );
        assert!(retry.pending.lock().unwrap().contains("outage"));
        assert!(!retry.pending.lock().unwrap().contains("invalid"));
        retry
            .record_result("outage", "test", Ok(()), is_transient)
            .unwrap();
        assert!(retry.pending.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn retries_back_off_and_stop_after_recovery() {
        let retry = ReconciliationRetry::default();
        retry
            .record_result("source", "test", Err(temporary()), is_transient)
            .unwrap();
        let start = tokio::time::Instant::now();
        let mut calls = Vec::new();
        retry
            .recover(
                "source",
                "test",
                || {
                    calls.push(tokio::time::Instant::now());
                    std::future::ready(if calls.len() < 3 {
                        Err(temporary())
                    } else {
                        Ok(())
                    })
                },
                is_transient,
            )
            .await
            .unwrap();
        assert_eq!(calls.len(), 3);
        assert!((24..=30).contains(&(calls[0] - start).as_secs()));
        assert!((48..=60).contains(&(calls[1] - calls[0]).as_secs()));
        assert!((96..=120).contains(&(calls[2] - calls[1]).as_secs()));
        retry
            .recover(
                "source",
                "test",
                || async { panic!("already recovered") },
                is_transient,
            )
            .await
            .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn stops_after_permanent_failure() {
        let retry = ReconciliationRetry::default();
        retry
            .record_result("source", "test", Err(temporary()), is_transient)
            .unwrap();
        let mut calls = 0;
        let error = retry
            .recover(
                "source",
                "test",
                || {
                    calls += 1;
                    std::future::ready(Err(anyhow::anyhow!("invalid credentials")))
                },
                is_transient,
            )
            .await
            .unwrap_err();
        assert_eq!(calls, 1);
        assert!(error.to_string().contains("invalid credentials"));
    }

    #[test]
    fn delays_remain_bounded() {
        for attempt in [1, 2, 6, 32, 64, u32::MAX] {
            assert!((24..=900).contains(&retry_delay(attempt).as_secs()));
        }
    }
}
