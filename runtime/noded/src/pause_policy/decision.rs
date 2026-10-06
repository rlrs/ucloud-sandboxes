//! `warm_park.decide_resident_wait` and its inputs (`background_io.Pressure`,
//! `transition_admission.MemoryDemand`, `resource_evidence.MemoryBackingCapacity`),
//! and `pause_tier.swap_room_bytes`. Pure; the float arithmetic is Python's.

const GIB: f64 = (1u64 << 30) as f64;

/// Free swap below this share is the kernel's: reclaim stops short of it and
/// the rest of the deficit escalates to hibernate.
pub const SWAP_RESERVE_FRACTION: f64 = 0.10;

/// The configured RAM backing (tmpfs) capacity. Both counters `None` is
/// "configured but unknown", never "disabled" (that is `Pressure::memory_backing == None`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MemoryBackingCapacity {
    pub total_bytes: Option<u64>,
    pub available_bytes: Option<u64>,
    pub identity: Option<String>,
}

impl MemoryBackingCapacity {
    /// Python `MemoryBackingCapacity.from_dict` on a measurement: `None`
    /// unless `0 <= available <= total <= 2**63 - 1`, `total > 0` and the
    /// identity has 1-256 characters.
    pub fn measured(total: u64, available: u64, identity: String) -> Option<Self> {
        let valid = available <= total && total <= i64::MAX as u64 && total != 0 && (1..=256).contains(&identity.chars().count());
        valid.then_some(MemoryBackingCapacity { total_bytes: Some(total), available_bytes: Some(available), identity: Some(identity) })
    }

    fn unknown(&self) -> bool {
        self.total_bytes.is_none() || self.available_bytes.is_none()
    }
}

/// `background_io.Pressure`. The default is Python's: no memory evidence,
/// memory PSI "100" (unreadable), swap unknown.
#[derive(Clone, Debug, PartialEq)]
pub struct Pressure {
    /// MemAvailable / MemTotal; 0.0 when either is unknown.
    pub memory_fraction: f64,
    /// Memory PSI "some" avg10 (100.0 when unreadable).
    pub memory_stall: f64,
    /// IO PSI "some" avg10 (0.0 when unreadable).
    pub io_stall: f64,
    pub memory_available_bytes: u64,
    /// `None`: RAM backing is not configured.
    pub memory_backing: Option<MemoryBackingCapacity>,
    pub swap_total_bytes: Option<u64>,
    pub swap_free_bytes: Option<u64>,
}

impl Default for Pressure {
    fn default() -> Self {
        Pressure {
            memory_fraction: 0.0,
            memory_stall: 100.0,
            io_stall: 0.0,
            memory_available_bytes: 0,
            memory_backing: None,
            swap_total_bytes: None,
            swap_free_bytes: None,
        }
    }
}

impl Pressure {
    /// Python's positional `Pressure(fraction, memory_stall, io_stall, available)`.
    pub fn new(memory_fraction: f64, memory_stall: f64, io_stall: f64, memory_available_bytes: u64) -> Self {
        Pressure { memory_fraction, memory_stall, io_stall, memory_available_bytes, ..Pressure::default() }
    }
}

/// `transition_admission.MemoryDemand`: the next wave of foreground demand.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MemoryDemand {
    pub physical_bytes: u64,
    pub ram_backing_bytes: u64,
}

/// The heartbeat's `resident_wait.reason` enum; any other value voids a
/// heartbeat at the gateway (`models.ResidentWaitMetrics.from_dict`).
pub const REASONS: [&str; 7] = [
    "resident_headroom",
    "storage_backpressure",
    "queued_demand",
    "memory_headroom",
    "memory_reclaim",
    "memory_backing_headroom",
    "memory_backing_unavailable",
];

/// `warm_park.ResidentWaitDecision`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Decision {
    pub memory_reclaim: bool,
    pub psi_reclaim: bool,
    pub target_bytes: u64,
    /// One of `REASONS`.
    pub reason: &'static str,
    pub backing_reclaim: bool,
}

impl Decision {
    pub fn reclaim(&self) -> bool {
        self.memory_reclaim || self.psi_reclaim
    }
}

/// `decide_resident_wait(pressure, incoming, memory_reclaim=, psi_reclaim=)`:
/// the previous flags are hysteresis state (the reclaim tick passes none).
pub fn decide_resident_wait(pressure: &Pressure, incoming: MemoryDemand, previous_memory: bool, previous_psi: bool) -> Decision {
    let fraction = pressure.memory_fraction;
    let available = pressure.memory_available_bytes as f64;
    let total = if fraction > 0.0 { available / fraction } else { 0.0 };
    let reserve = (total * 0.05).max((2.0 * GIB).min(total * 0.10));
    let backing = pressure.memory_backing.as_ref();
    let backing_unknown = backing.is_some_and(MemoryBackingCapacity::unknown);
    // Python ints: exact, and possibly negative.
    let spare = (i128::from(pressure.memory_available_bytes) - i128::from(incoming.physical_bytes)) as f64;
    let threshold = reserve * if previous_memory { 1.5 } else { 1.0 };
    let backing_spare = backing.map(|backing| {
        let available = if backing_unknown { 0 } else { backing.available_bytes.unwrap_or(0) };
        (i128::from(available) - i128::from(incoming.ram_backing_bytes)) as f64
    });
    let backing_reclaim = backing.is_some() && (backing_unknown || backing_spare.is_some_and(|spare| spare <= threshold));
    let mut memory_reclaim = if total != 0.0 {
        spare <= threshold
    } else {
        fraction <= if previous_memory { 0.075 } else { 0.05 } || incoming.physical_bytes != 0
    };
    memory_reclaim = memory_reclaim || backing_reclaim;
    // PSI alone with abundant unreserved RAM is not a reason to reclaim.
    let psi_reclaim = spare <= reserve * 1.5 && pressure.io_stall < 20.0 && pressure.memory_stall >= if previous_psi { 2.0 } else { 10.0 };
    let reason = if backing_unknown {
        "memory_backing_unavailable"
    } else if (incoming.physical_bytes != 0 || incoming.ram_backing_bytes != 0) && memory_reclaim {
        "queued_demand"
    } else if backing_reclaim {
        "memory_backing_headroom"
    } else if memory_reclaim {
        "memory_headroom"
    } else if psi_reclaim {
        "memory_reclaim"
    } else if pressure.io_stall >= 20.0 {
        "storage_backpressure"
    } else {
        "resident_headroom"
    };
    let mut target = if memory_reclaim { (reserve * 1.5 - spare).max(1.0) } else { (total * 0.01).max(1.0) };
    if backing_reclaim && let Some(backing_spare) = backing_spare {
        target = target.max(reserve * 1.5 - backing_spare);
    }
    if pressure.io_stall >= 20.0 && total != 0.0 {
        target = target.min(total * 0.05);
    }
    Decision {
        memory_reclaim,
        psi_reclaim,
        // Python int(): truncation toward zero.
        target_bytes: if memory_reclaim || psi_reclaim { target as u64 } else { 0 },
        reason,
        backing_reclaim,
    }
}

/// Swap that reclaim may still fill above the kernel's reserve; `None` is
/// unbounded (unknown swap: reclaim tries, and a stalled one escalates).
pub fn swap_room_bytes(pressure: &Pressure) -> Option<u64> {
    let (Some(total), Some(free)) = (pressure.swap_total_bytes, pressure.swap_free_bytes) else { return None };
    Some(free.saturating_sub((total as f64 * SWAP_RESERVE_FRACTION) as u64))
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1 << 30;

    fn none() -> MemoryDemand {
        MemoryDemand::default()
    }

    fn demand(physical: u64, ram: u64) -> MemoryDemand {
        MemoryDemand { physical_bytes: physical, ram_backing_bytes: ram }
    }

    /// tests/test_resident_memory_backing.py `pressure(free)`.
    fn backed(free: u64) -> Pressure {
        Pressure {
            memory_backing: MemoryBackingCapacity::measured(95 * GIB, free, "mount".into()),
            ..Pressure::new(0.2, 0.0, 0.0, 20 * GIB)
        }
    }

    #[test]
    fn reclaim_only_under_pressure() {
        let relaxed = decide_resident_wait(&Pressure::new(0.9, 0.0, 0.0, 90 * GIB), none(), false, false);
        let pressed = decide_resident_wait(&Pressure::new(0.01, 0.0, 0.0, GIB), none(), false, false);
        assert!(!relaxed.reclaim() && relaxed.target_bytes == 0 && relaxed.reason == "resident_headroom");
        assert!(pressed.reclaim() && pressed.target_bytes > 2 * GIB);
        // 100 GiB total: reserve 5 GiB, a 6.5 GiB deficit.
        assert_eq!((pressed.target_bytes, pressed.reason), (13 * GIB / 2, "memory_headroom"));
    }

    #[test]
    fn backing_exhaustion_reclaims_even_with_physical_memory_available() {
        let decision = decide_resident_wait(&backed(2 * GIB), none(), false, false);
        assert!(decision.memory_reclaim && decision.backing_reclaim);
        assert_eq!((decision.reason, decision.target_bytes), ("memory_backing_headroom", 11 * GIB / 2));
        let unbacked = Pressure { memory_backing: None, ..backed(2 * GIB) };
        assert!(!decide_resident_wait(&unbacked, none(), false, false).reclaim());
    }

    #[test]
    fn backing_hysteresis_and_pending_demand_use_real_free_bytes() {
        let pressure = backed(6 * GIB);
        assert!(!decide_resident_wait(&pressure, none(), false, false).reclaim());
        assert!(decide_resident_wait(&pressure, none(), true, false).reclaim());
        let decision = decide_resident_wait(&backed(7 * GIB), demand(4 * GIB, 4 * GIB), false, false);
        assert!(decision.backing_reclaim);
        assert_eq!((decision.reason, decision.target_bytes), ("queued_demand", 9 * GIB / 2));
    }

    #[test]
    fn file_restore_demand_does_not_create_a_tmpfs_deficit() {
        let pressure = backed(7 * GIB);
        assert!(!decide_resident_wait(&pressure, demand(4 * GIB, 0), false, false).reclaim());
        assert!(decide_resident_wait(&pressure, demand(4 * GIB, 4 * GIB), false, false).backing_reclaim);
    }

    #[test]
    fn unknown_configured_backing_is_not_disabled_or_healthy() {
        let unknown = Pressure { memory_backing: Some(MemoryBackingCapacity::default()), ..backed(2 * GIB) };
        let decision = decide_resident_wait(&unknown, none(), false, false);
        assert!(decision.memory_reclaim && decision.backing_reclaim);
        assert_eq!(decision.reason, "memory_backing_unavailable");
        assert!(decision.target_bytes > 0);
    }

    #[test]
    fn host_memory_still_limits_when_backing_has_space() {
        let pressure = Pressure { memory_fraction: 0.02, memory_available_bytes: 2 * GIB, ..backed(80 * GIB) };
        let decision = decide_resident_wait(&pressure, none(), false, false);
        assert!(decision.memory_reclaim && !decision.backing_reclaim);
        assert_eq!(decision.reason, "memory_headroom");
    }

    #[test]
    fn io_pressure_limits_wave_but_cannot_stall_backing_reclaim() {
        let pressure = Pressure { io_stall: 90.0, ..backed(2 * GIB) };
        let decision = decide_resident_wait(&pressure, none(), false, false);
        assert!(decision.reclaim());
        assert_eq!(decision.target_bytes, 5 * GIB);
    }

    #[test]
    fn psi_alone_reclaims_a_small_probe_and_storage_backpressure_is_named() {
        // 10 GiB total, 1 GiB reserve, 1.2 GiB spare: within 1.5 reserves.
        let psi = decide_resident_wait(&Pressure::new(0.12, 15.0, 0.0, 1288490188), none(), false, false);
        assert!(!psi.memory_reclaim && psi.psi_reclaim);
        assert_eq!((psi.reason, psi.target_bytes), ("memory_reclaim", 107374182));
        // Hysteresis: a previous PSI reclaim continues from 2 % stall.
        assert!(!decide_resident_wait(&Pressure::new(0.12, 5.0, 0.0, 1288490188), none(), false, false).reclaim());
        assert!(decide_resident_wait(&Pressure::new(0.12, 5.0, 0.0, 1288490188), none(), false, true).psi_reclaim);
        let storage = decide_resident_wait(&Pressure::new(0.9, 0.0, 30.0, 90 * GIB), none(), false, false);
        assert_eq!((storage.reclaim(), storage.reason), (false, "storage_backpressure"));
        // Unknown totals fall back to the fraction rule.
        let unknown = decide_resident_wait(&Pressure::default(), none(), false, false);
        assert!(unknown.memory_reclaim && REASONS.contains(&unknown.reason));
        assert!(decide_resident_wait(&Pressure::default(), demand(1, 0), false, false).reason == "queued_demand");
    }

    #[test]
    fn swap_room_keeps_the_kernel_reserve_and_unknown_swap_is_unbounded() {
        let room = |total, free| swap_room_bytes(&Pressure { swap_total_bytes: total, swap_free_bytes: free, ..Pressure::default() });
        assert_eq!(room(Some(10 * GIB), Some(3 * GIB)), Some(2 * GIB));
        assert_eq!(room(Some(10 * GIB), Some(GIB / 2)), Some(0));
        assert_eq!(room(None, None), None);
        assert_eq!(room(Some(GIB), None), None);
    }
}
