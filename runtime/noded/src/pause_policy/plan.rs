//! `pause_tier.relief_plan` and the paused-wait bookkeeping around it
//! (`PausedWait`, `reclaim_stalled`, `note_reclaim`). Pure.

use super::decision::Decision;
use super::resident::ReclaimResult;

pub const MIB: u64 = 1 << 20;

/// Reclaims in flight at once, at one node-wide rate (qualification: one
/// reclaim moves 225-380 MiB/s with zswap off; eight share about 0.85 GiB/s).
pub const RECLAIM_CONCURRENCY: usize = 2;
pub const RECLAIM_BYTES_PER_SECOND: u64 = 512 * MIB;
/// One `memory.reclaim` write; cancellable between windows.
pub const RECLAIM_WINDOW_BYTES: u64 = 128 * MIB;
/// A reclaim freeing less than this (or its whole smaller target) stalled.
pub const STALL_BYTES: u64 = 16 * MIB;
pub const STALL_BACKOFF_SECONDS: f64 = 10.0;
/// Stalls in a row before a wait escalates to hibernate.
pub const MAX_STALLS: u32 = 2;
/// Hibernates in flight at once (`ESCALATION_CONCURRENCY`).
pub const ESCALATION_CONCURRENCY: usize = 2;

/// An incarnation: (sandbox id, generation).
pub type Key = (String, u64);

/// `pause_tier.PausedWait`: disposable scheduling metadata of one marker.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PausedWait {
    /// Monotonic seconds.
    pub paused_at: f64,
    /// A relay's predicted end of the wait, monotonic seconds.
    pub expected_until: Option<f64>,
    /// `None`: no live sample (unmeasured waits carry no credit).
    pub resident_bytes: Option<u64>,
    pub swapped_bytes: u64,
    /// The target of its in-flight reclaim (0: none).
    pub reclaiming: u64,
    pub escalating: bool,
    /// Reclaims in a row that made no progress, and when the next may start.
    pub stalls: u32,
    pub retry_at: f64,
    /// A node-local model wait's growth-forecast identity.
    pub local_request_id: String,
}

impl PausedWait {
    pub fn new(paused_at: f64) -> Self {
        PausedWait { paused_at, ..PausedWait::default() }
    }
}

/// `relief_plan(decision, paused, now=, swap_room=)`: (reclaims as (key,
/// target), escalations) that cover the decision's deficit. `waits` is in the
/// table's insertion order, which breaks ties as Python's stable sort does.
/// `swap_room`: `None` is unbounded.
///
/// Hinted waits go first by expected remaining idle x resident bytes, then the
/// rest by resident bytes, most recently paused first (a long-paused wait is
/// the likeliest to wake next). A wait swaps out while swap has room above the
/// reserve; one in its stall backoff waits. One that stalled MAX_STALLS times,
/// or that no longer fits in swap, escalates if it holds at least STALL_BYTES
/// resident and more resident than swapped. In-flight work counts against the
/// deficit (a reclaim its target, a hibernate all it holds) and an in-flight
/// reclaim's target against the room.
pub fn relief_plan(decision: &Decision, waits: &[(&Key, &PausedWait)], now: f64, swap_room: Option<u64>) -> (Vec<(Key, u64)>, Vec<Key>) {
    if !decision.reclaim() {
        return (Vec::new(), Vec::new());
    }
    let reclaiming: i128 = waits.iter().map(|(_, wait)| i128::from(wait.reclaiming)).sum();
    let escalating: i128 = waits.iter().filter(|(_, wait)| wait.escalating).map(|(_, wait)| i128::from(wait.resident_bytes.unwrap_or(0))).sum();
    let mut room: Option<i128> = swap_room.map(|room| i128::from(room) - reclaiming);
    let mut deficit = i128::from(decision.target_bytes) - reclaiming - escalating;

    let score = |wait: &PausedWait| -> (u8, f64) {
        let resident = wait.resident_bytes.unwrap_or(0) as f64;
        match wait.expected_until {
            Some(until) => (1, (until - now).max(0.0) * resident),
            None => (0, resident / (1.0 + (now - wait.paused_at).max(0.0))),
        }
    };
    let mut ranked: Vec<(usize, (u8, f64))> = waits.iter().enumerate().map(|(index, (_, wait))| (index, score(wait))).collect();
    // Descending, stable: equal scores keep the table's order.
    ranked.sort_by(|(_, left), (_, right)| right.0.cmp(&left.0).then(right.1.partial_cmp(&left.1).unwrap_or(std::cmp::Ordering::Equal)));

    let (mut reclaims, mut escalations) = (Vec::new(), Vec::new());
    for (index, _) in ranked {
        if deficit <= 0 {
            break;
        }
        let (key, wait) = waits[index];
        let resident = match wait.resident_bytes {
            Some(resident) if resident > 0 && wait.reclaiming == 0 && !wait.escalating => i128::from(resident),
            _ => continue,
        };
        let stalled = wait.stalls >= MAX_STALLS;
        let freed;
        if !stalled && room.is_none_or(|room| room >= i128::from(STALL_BYTES)) {
            if now < wait.retry_at {
                continue; // Backing off after a stall; others may relieve meanwhile.
            }
            freed = room.map_or(resident.min(deficit), |room| resident.min(deficit).min(room));
            reclaims.push((key.clone(), freed as u64));
            if let Some(room) = room.as_mut() {
                *room -= freed;
            }
        } else if resident >= i128::from(STALL_BYTES).max(i128::from(wait.swapped_bytes)) {
            freed = resident;
            escalations.push(key.clone());
        } else {
            continue;
        }
        deficit -= freed;
    }
    (reclaims, escalations)
}

/// `reclaim_stalled`: a reclaim that failed (`None`), or that freed less
/// than STALL_BYTES (or its whole smaller target) without being superseded.
pub fn reclaim_stalled(result: Option<&ReclaimResult>, target_bytes: u64) -> bool {
    result.is_none_or(|result| result.reason != "superseded" && result.reclaimed_bytes < target_bytes.min(STALL_BYTES))
}

/// `note_reclaim`: count a stall into its doubling backoff; progress clears both.
pub fn note_reclaim(wait: &mut PausedWait, stalled: bool, now: f64) {
    if stalled {
        wait.stalls += 1;
        wait.retry_at = now + STALL_BACKOFF_SECONDS * 2f64.powi(wait.stalls as i32 - 1);
    } else {
        (wait.stalls, wait.retry_at) = (0, 0.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pause_policy::decision::{MemoryDemand, Pressure, decide_resident_wait};

    const GIB: u64 = 1 << 30;

    fn key(name: &str) -> Key {
        (name.to_string(), 1)
    }

    fn wait(paused_at: f64, resident: Option<u64>) -> PausedWait {
        PausedWait { resident_bytes: resident, ..PausedWait::new(paused_at) }
    }

    fn hinted(paused_at: f64, until: f64, resident: u64) -> PausedWait {
        PausedWait { expected_until: Some(until), ..wait(paused_at, Some(resident)) }
    }

    fn plan(decision: &Decision, waits: &[(Key, PausedWait)], now: f64, room: Option<u64>) -> (Vec<(Key, u64)>, Vec<Key>) {
        let view: Vec<(&Key, &PausedWait)> = waits.iter().map(|(key, wait)| (key, wait)).collect();
        relief_plan(decision, &view, now, room)
    }

    fn pressed() -> Decision {
        decide_resident_wait(&Pressure::new(0.01, 0.0, 0.0, GIB), MemoryDemand::default(), false, false)
    }

    fn get<'a>(waits: &'a mut [(Key, PausedWait)], name: &str) -> &'a mut PausedWait {
        &mut waits.iter_mut().find(|(key, _)| key.0 == name).unwrap().1
    }

    #[test]
    fn reclaim_runs_only_under_pressure_and_evicts_the_longest_expected_idle() {
        let relaxed = decide_resident_wait(&Pressure::new(0.9, 0.0, 0.0, 90 * GIB), MemoryDemand::default(), false, false);
        let pressed = pressed();
        let mut waits = vec![
            (key("short"), wait(90.0, Some(4 * GIB))), // idle 10 s
            (key("long"), wait(0.0, Some(GIB))),       // idle 100 s
            (key("hinted"), hinted(99.0, 400.0, GIB)),
            (key("unmeasured"), wait(0.0, None)),
        ];
        assert_eq!(plan(&relaxed, &waits, 100.0, None), (vec![], vec![]));
        let (reclaims, escalations) = plan(&pressed, &waits, 100.0, None);
        let order: Vec<&str> = reclaims.iter().map(|(key, _)| key.0.as_str()).collect();
        assert_eq!(&order[..3], ["hinted", "short", "long"]);
        assert!(!order.contains(&"unmeasured"));
        assert!(reclaims.iter().map(|(_, target)| target).sum::<u64>() <= pressed.target_bytes);
        assert!(escalations.is_empty()); // Swap has room and nothing stalled.
        // In-flight reclaim counts its target against the same deficit.
        get(&mut waits, "hinted").reclaiming = pressed.target_bytes;
        assert_eq!(plan(&pressed, &waits, 100.0, None), (vec![], vec![]));
    }

    fn six() -> Vec<(Key, PausedWait)> {
        vec![
            (key("hinted"), hinted(99.0, 400.0, GIB)),
            (key("long"), wait(0.0, Some(GIB))),
            (key("short"), wait(90.0, Some(4 * GIB))),
            (key("small"), wait(0.0, Some(STALL_BYTES - 1))),
            (key("swapped"), PausedWait { swapped_bytes: GIB, ..wait(0.0, Some(GIB / 8)) }),
            (key("unmeasured"), wait(0.0, None)),
        ]
    }

    #[test]
    fn what_swap_cannot_hold_or_a_stalled_reclaim_escalates_to_hibernate() {
        let pressed = pressed(); // A 6.5 GiB deficit.
        // 1.5 GiB of swap room: the best two swap out, the rest hibernates.
        let (reclaims, escalations) = plan(&pressed, &six(), 100.0, Some(3 * GIB / 2));
        assert_eq!(reclaims, vec![(key("hinted"), GIB), (key("short"), GIB / 2)]);
        assert_eq!(escalations, vec![key("long")]);
        // Swap nearly full: every wait worth a hibernate escalates, best first.
        assert_eq!(plan(&pressed, &six(), 100.0, Some(0)), (vec![], vec![key("hinted"), key("short"), key("long")]));
        // One stall only backs off: no reclaim and no hibernate until it ends.
        let mut backing_off = six();
        note_reclaim(get(&mut backing_off, "hinted"), true, 100.0);
        let (reclaims, escalations) = plan(&pressed, &backing_off, 101.0, None);
        assert!(!reclaims.iter().any(|(key, _)| key.0 == "hinted") && !escalations.contains(&key("hinted")));
        let (reclaims, _) = plan(&pressed, &backing_off, 100.0 + STALL_BACKOFF_SECONDS, None);
        assert!(reclaims.iter().any(|(key, _)| key.0 == "hinted")); // Retried after the backoff.
        // MAX_STALLS in a row escalate even with room; others still swap.
        let mut stalled = six();
        get(&mut stalled, "hinted").stalls = MAX_STALLS;
        let (reclaims, escalations) = plan(&pressed, &stalled, 100.0, None);
        let mut swapped: Vec<&str> = reclaims.iter().map(|(key, _)| key.0.as_str()).collect();
        swapped.sort();
        assert_eq!((swapped, escalations), (vec!["long", "short", "small", "swapped"], vec![key("hinted")]));
        get(&mut stalled, "swapped").stalls = MAX_STALLS; // Already reclaimed: left alone.
        assert!(!plan(&pressed, &stalled, 100.0, Some(0)).1.contains(&key("swapped")));
        // An in-flight escalation counts against the deficit.
        let mut busy = six();
        get(&mut busy, "short").escalating = true;
        get(&mut busy, "short").resident_bytes = Some(pressed.target_bytes);
        assert_eq!(plan(&pressed, &busy, 100.0, Some(0)), (vec![], vec![]));
        // An in-flight reclaim fills swap with its target, not with all it holds:
        // counting 4 GiB would leave 1 GiB of room and escalate needlessly.
        let inflight = vec![
            (key("a"), PausedWait { reclaiming: GIB, ..wait(0.0, Some(4 * GIB)) }),
            (key("b"), wait(0.0, Some(4 * GIB))),
        ];
        assert_eq!(plan(&pressed, &inflight, 100.0, Some(5 * GIB)), (vec![(key("b"), 4 * GIB)], vec![]));
    }

    #[test]
    fn equal_scores_keep_the_table_order() {
        let waits = vec![(key("b"), wait(0.0, Some(GIB))), (key("a"), wait(0.0, Some(GIB))), (key("c"), wait(0.0, Some(GIB)))];
        let order: Vec<String> = plan(&pressed(), &waits, 10.0, None).0.into_iter().map(|(key, _)| key.0).collect();
        assert_eq!(order, ["b", "a", "c"]);
    }

    #[test]
    fn a_reclaim_that_frees_less_than_stall_bytes_has_stalled() {
        let result = |reason: &'static str, reclaimed: u64| ReclaimResult { requested_bytes: GIB, reclaimed_bytes: reclaimed, elapsed_seconds: 1.0, refault_file_pages: 0, reason };
        assert!(reclaim_stalled(None, GIB)); // It failed.
        assert!(reclaim_stalled(Some(&result("not_shrinking", MIB)), GIB));
        assert!(reclaim_stalled(Some(&result("kernel_error", 0)), GIB));
        assert!(!reclaim_stalled(Some(&result("superseded", 0)), GIB)); // A thaw, not a stall.
        assert!(!reclaim_stalled(Some(&result("partial_reclaim", RECLAIM_WINDOW_BYTES)), GIB));
        assert!(!reclaim_stalled(Some(&result("target_reached", MIB)), MIB)); // Small, but whole.
        assert!(!reclaim_stalled(Some(&result("not_shrinking", STALL_BYTES)), GIB)); // Some progress.
        let mut wait = PausedWait::new(0.0);
        note_reclaim(&mut wait, true, 10.0);
        note_reclaim(&mut wait, true, 20.0); // The backoff doubles.
        assert_eq!((wait.stalls, wait.retry_at), (2, 20.0 + 2.0 * STALL_BACKOFF_SECONDS));
        note_reclaim(&mut wait, false, 30.0);
        assert_eq!((wait.stalls, wait.retry_at), (0, 0.0));
    }
}
