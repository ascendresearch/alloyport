//! Clock policy used by durable control-plane decisions.

use std::fmt::Debug;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Source of wall-clock timestamps used by durable lease decisions.
pub trait Clock: Debug + Send + Sync {
    fn now_unix_ms(&self) -> u64;
}

/// Production wall clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_unix_ms(&self) -> u64 {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        u64::try_from(millis).unwrap_or(u64::MAX)
    }
}

/// Deterministic clock for state-machine and restart tests.
#[derive(Clone, Debug)]
pub struct ManualClock {
    now_ms: Arc<std::sync::atomic::AtomicU64>,
}

impl ManualClock {
    #[must_use]
    pub fn new(now_ms: u64) -> Self {
        Self {
            now_ms: Arc::new(std::sync::atomic::AtomicU64::new(now_ms)),
        }
    }

    pub fn advance(&self, duration_ms: u64) {
        self.now_ms
            .fetch_add(duration_ms, std::sync::atomic::Ordering::Relaxed);
    }
}

impl Clock for ManualClock {
    fn now_unix_ms(&self) -> u64 {
        self.now_ms.load(std::sync::atomic::Ordering::Relaxed)
    }
}
