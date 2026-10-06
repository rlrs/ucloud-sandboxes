//! `pause_tier.PauseStats`: the node's monotonic pause-tier counters, under the
//! names the heartbeat reports today.

use std::sync::Mutex;

use serde_json::{Map, Value};

/// One counter. `name()` is Python's key; the order is Python's dict order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Counter {
    Pauses,
    Thaws,
    ThawMsTotal,
    ThawMsMax,
    PauseReclaims,
    PauseReclaimedBytes,
    PauseReclaimMsTotal,
    PauseReclaimCancellations,
    PauseReclaimStalls,
    PauseEscalations,
    ThawPrefetches,
    ThawPrefetchedBytes,
    ThawPrefetchMsTotal,
    PauseReclaimTargetReached,
    PauseReclaimNotShrinking,
    PauseReclaimPartial,
    PauseReclaimErrors,
}

impl Counter {
    pub const ALL: [Counter; 17] = [
        Counter::Pauses,
        Counter::Thaws,
        Counter::ThawMsTotal,
        Counter::ThawMsMax,
        Counter::PauseReclaims,
        Counter::PauseReclaimedBytes,
        Counter::PauseReclaimMsTotal,
        Counter::PauseReclaimCancellations,
        Counter::PauseReclaimStalls,
        Counter::PauseEscalations,
        Counter::ThawPrefetches,
        Counter::ThawPrefetchedBytes,
        Counter::ThawPrefetchMsTotal,
        Counter::PauseReclaimTargetReached,
        Counter::PauseReclaimNotShrinking,
        Counter::PauseReclaimPartial,
        Counter::PauseReclaimErrors,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Counter::Pauses => "pauses",
            Counter::Thaws => "thaws",
            Counter::ThawMsTotal => "thaw_ms_total",
            Counter::ThawMsMax => "thaw_ms_max",
            Counter::PauseReclaims => "pause_reclaims",
            Counter::PauseReclaimedBytes => "pause_reclaimed_bytes",
            Counter::PauseReclaimMsTotal => "pause_reclaim_ms_total",
            Counter::PauseReclaimCancellations => "pause_reclaim_cancellations",
            Counter::PauseReclaimStalls => "pause_reclaim_stalls",
            Counter::PauseEscalations => "pause_escalations",
            Counter::ThawPrefetches => "thaw_prefetches",
            Counter::ThawPrefetchedBytes => "thaw_prefetched_bytes",
            Counter::ThawPrefetchMsTotal => "thaw_prefetch_ms_total",
            Counter::PauseReclaimTargetReached => "pause_reclaim_target_reached",
            Counter::PauseReclaimNotShrinking => "pause_reclaim_not_shrinking",
            Counter::PauseReclaimPartial => "pause_reclaim_partial",
            Counter::PauseReclaimErrors => "pause_reclaim_errors",
        }
    }

    /// A `_max` counter keeps the largest amount instead of the sum.
    fn is_max(self) -> bool {
        self.name().ends_with("_max")
    }

    /// `STOP_COUNTERS`: why a paused reclaim ended; unknown reasons are errors.
    pub fn for_stop_reason(reason: &str) -> Counter {
        match reason {
            "target_reached" => Counter::PauseReclaimTargetReached,
            "not_shrinking" => Counter::PauseReclaimNotShrinking,
            "partial_reclaim" => Counter::PauseReclaimPartial,
            _ => Counter::PauseReclaimErrors, // "raised" and anything else
        }
    }
}

/// Exact sums (milliseconds are fractional); only the snapshot rounds.
#[derive(Debug, Default)]
pub struct PauseStats {
    values: Mutex<[f64; Counter::ALL.len()]>,
}

impl PauseStats {
    pub fn new() -> Self {
        Self::default()
    }

    /// Python `add(**amounts)`: one atomic update of several counters.
    pub fn add(&self, amounts: &[(Counter, f64)]) {
        let mut values = self.values.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        for &(counter, amount) in amounts {
            let value = &mut values[counter as usize];
            *value = if counter.is_max() { value.max(amount) } else { *value + amount };
        }
    }

    /// Count why a pause reclaim ended (a thaw's supersession is not counted).
    pub fn stopped(&self, reason: &str) {
        if reason != "superseded" {
            self.add(&[(Counter::for_stop_reason(reason), 1.0)]);
        }
    }

    /// One counter, rounded as the snapshot rounds it.
    pub fn get(&self, counter: Counter) -> i64 {
        let values = self.values.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        values[counter as usize].round_ties_even() as i64
    }

    /// Python `snapshot()`: every counter in Python's order, rounded half to
    /// even like Python's `round`.
    pub fn snapshot(&self) -> Map<String, Value> {
        let values = *self.values.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        Counter::ALL
            .iter()
            .map(|counter| (counter.name().to_string(), Value::from(values[*counter as usize].round_ties_even() as i64)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stats_are_monotonic_and_track_maxima() {
        let stats = PauseStats::new();
        stats.add(&[(Counter::Thaws, 1.0), (Counter::ThawMsTotal, 3.6), (Counter::ThawMsMax, 3.6)]);
        stats.add(&[(Counter::Thaws, 1.0), (Counter::ThawMsTotal, 1.0), (Counter::ThawMsMax, 1.0)]);
        let snapshot = stats.snapshot();
        // Exact sums; only the snapshot rounds (4.6 and 3.6 ms).
        assert_eq!((&snapshot["thaws"], &snapshot["thaw_ms_total"], &snapshot["thaw_ms_max"]), (&Value::from(2), &Value::from(5), &Value::from(4)));
    }

    #[test]
    fn snapshot_has_pythons_names_order_and_rounding() {
        let stats = PauseStats::new();
        let names: Vec<String> = stats.snapshot().keys().cloned().collect();
        assert_eq!(names, [
            "pauses", "thaws", "thaw_ms_total", "thaw_ms_max", "pause_reclaims", "pause_reclaimed_bytes",
            "pause_reclaim_ms_total", "pause_reclaim_cancellations", "pause_reclaim_stalls", "pause_escalations",
            "thaw_prefetches", "thaw_prefetched_bytes", "thaw_prefetch_ms_total", "pause_reclaim_target_reached",
            "pause_reclaim_not_shrinking", "pause_reclaim_partial", "pause_reclaim_errors",
        ]);
        assert!(stats.snapshot().values().all(|value| value == &Value::from(0)));
        stats.add(&[(Counter::ThawPrefetchMsTotal, 2.5), (Counter::ThawMsTotal, 3.5)]);
        assert_eq!((stats.get(Counter::ThawPrefetchMsTotal), stats.get(Counter::ThawMsTotal)), (2, 4)); // round(2.5) == 2
        stats.stopped("superseded");
        stats.stopped("not_shrinking");
        stats.stopped("raised");
        stats.stopped("kernel_error");
        assert_eq!((stats.get(Counter::PauseReclaimNotShrinking), stats.get(Counter::PauseReclaimErrors)), (1, 2));
    }
}
