//! `TransitionLedger` (ucloud_sandboxes/transition_admission.py), ported
//! exactly: typed in-flight transition evidence, owned by the admission lock.
//!
//! This is accounting, not another scheduler. Unknown byte quantities stay
//! unknown. Duplicate owners get independent tokens, so a cancellation cannot
//! release another operation's claim. Per owner the charge is the largest of
//! its growth forecast and its claims, never their sum.

use std::collections::{BTreeMap, HashMap};

use serde::Serialize;

use super::Owner;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Startup,
    Restore,
    Capture,
    Publication,
}

impl Kind {
    /// Python's `TransitionKind` iteration order.
    pub const ALL: [Kind; 4] = [Kind::Startup, Kind::Restore, Kind::Capture, Kind::Publication];

    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Startup => "startup",
            Kind::Restore => "restore",
            Kind::Capture => "capture",
            Kind::Publication => "publication",
        }
    }

    pub fn parse(text: &str) -> Option<Kind> {
        Kind::ALL.into_iter().find(|kind| kind.as_str() == text)
    }
}

/// The two byte quantities a projection can be asked about.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Resource {
    Memory,
    RamBacking,
}

impl Resource {
    pub const ALL: [Resource; 2] = [Resource::Memory, Resource::RamBacking];

    fn index(self) -> usize {
        self as usize
    }
}

/// `TransitionCost`: `None` is an unknown quantity, never zero.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cost {
    pub kind: Kind,
    pub memory_bytes: Option<u64>,
    pub read_bytes: Option<u64>,
    pub write_bytes: Option<u64>,
    pub provenance: String,
    pub ram_backing_bytes: Option<u64>,
}

impl Cost {
    /// `TransitionCost(kind, memory_bytes)` with the dataclass defaults.
    pub fn new(kind: Kind, memory_bytes: Option<u64>) -> Cost {
        Cost { kind, memory_bytes, read_bytes: None, write_bytes: None, provenance: "unknown".into(), ram_backing_bytes: None }
    }

    pub fn get(&self, resource: Resource) -> Option<u64> {
        match resource {
            Resource::Memory => self.memory_bytes,
            Resource::RamBacking => self.ram_backing_bytes,
        }
    }
}

/// `ResourceQuantity`: a configured shape, not a reservation of CPU or RAM.
#[derive(Clone, Debug, PartialEq, Default, Serialize)]
pub struct Shape {
    pub vcpu: f64,
    pub memory_mb: u64,
    pub disk_mb: u64,
}

/// `TransitionClaim`. Release compares every field, as dataclass equality does.
#[derive(Clone, Debug, PartialEq)]
pub struct Claim {
    pub token: u64,
    pub owner: Owner,
    pub cost: Cost,
    pub requested: Option<Shape>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NotOwned;

impl std::fmt::Display for NotOwned {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("transition claim was not owned")
    }
}

impl std::error::Error for NotOwned {}

/// One kind's row of `snapshot()`.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct KindSnapshot {
    pub active: usize,
    pub waiting: usize,
    pub memory_bytes: Option<u64>,
    pub read_bytes: Option<u64>,
    pub write_bytes: Option<u64>,
}

/// `demand_snapshot()`, the resident-wait metrics.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DemandSnapshot {
    pub admitted_demand_bytes: u64,
    pub pending_demand_bytes: u64,
    pub unknown_transition_memory_costs: u64,
    pub admitted_ram_backing_bytes: u64,
    pub pending_ram_backing_bytes: u64,
}

/// Concurrency limits per kind (`{TransitionKind: int}`); a missing kind is 0.
pub type Limits = [(Kind, usize)];

fn limit(limits: &Limits, kind: Kind) -> usize {
    limits.iter().find(|(k, _)| *k == kind).map_or(0, |(_, value)| *value)
}

#[derive(Debug, Default)]
pub struct Ledger {
    next: u64,
    // Tokens only grow, so token order is Python's dict insertion order.
    claims: BTreeMap<u64, Claim>,
    waiting: BTreeMap<u64, Claim>,
    growth: HashMap<Owner, Cost>,
    // Running sums over the forecasts: a projection costs O(in-flight
    // claims), not O(sandboxes on the node), under the admission guard.
    growth_totals: [u64; 2],
    growth_unknown: u64,
}

impl Ledger {
    pub fn new() -> Ledger {
        Ledger::default()
    }

    /// Publish queued demand; the token unwaits it.
    pub fn wait(&mut self, owner: Owner, cost: Cost) -> u64 {
        self.next += 1;
        self.waiting.insert(self.next, Claim { token: self.next, owner, cost, requested: None });
        self.next
    }

    pub fn unwait(&mut self, token: u64) {
        self.waiting.remove(&token);
    }

    /// Requote this owner's queued demand of the same kind.
    pub fn refresh_wait_cost(&mut self, owner: &Owner, cost: &Cost) {
        for item in self.waiting.values_mut() {
            if &item.owner == owner && item.cost.kind == cost.kind {
                item.cost = cost.clone();
            }
        }
    }

    pub fn claim(&mut self, owner: Owner, cost: Cost, requested: Option<Shape>) -> Claim {
        self.next += 1;
        let claim = Claim { token: self.next, owner, cost, requested };
        self.claims.insert(claim.token, claim.clone());
        claim
    }

    pub fn active_count(&self, kind: Kind) -> usize {
        self.claims.values().filter(|item| item.cost.kind == kind).count()
    }

    pub fn release(&mut self, claim: &Claim) -> Result<(), NotOwned> {
        if self.claims.get(&claim.token) != Some(claim) {
            return Err(NotOwned);
        }
        self.claims.remove(&claim.token);
        Ok(())
    }

    /// Configured shapes for the heartbeat/drain contract: per owner, in
    /// first-claim order, the latest claim's shape (a Python dict).
    pub fn resource_reservations(&self) -> Vec<(Owner, Shape)> {
        let mut result: Vec<(Owner, Shape)> = Vec::new();
        for item in self.claims.values() {
            if let Some(requested) = &item.requested {
                match result.iter_mut().find(|(owner, _)| owner == &item.owner) {
                    Some(entry) => entry.1 = requested.clone(),
                    None => result.push((item.owner.clone(), requested.clone())),
                }
            }
        }
        result
    }

    /// One owner's residual primary-process growth; `None` forgets it.
    pub fn set_growth_forecast(&mut self, owner: &Owner, cost: Option<Cost>) {
        if let Some(previous) = self.growth.remove(owner) {
            for resource in Resource::ALL {
                self.growth_totals[resource.index()] -= previous.get(resource).unwrap_or(0);
            }
            self.growth_unknown -= previous.memory_bytes.is_none() as u64;
        }
        if let Some(cost) = cost {
            for resource in Resource::ALL {
                self.growth_totals[resource.index()] += cost.get(resource).unwrap_or(0);
            }
            self.growth_unknown += cost.memory_bytes.is_none() as u64;
            self.growth.insert(owner.clone(), cost);
        }
    }

    pub fn set_growth_forecasts(&mut self, forecasts: impl IntoIterator<Item = (Owner, Cost)>) {
        let owners: Vec<Owner> = self.growth.keys().cloned().collect();
        for owner in owners {
            self.set_growth_forecast(&owner, None);
        }
        for (owner, cost) in forecasts {
            self.set_growth_forecast(&owner, Some(cost));
        }
    }

    pub fn growth_forecast(&self, owner: &Owner) -> Option<&Cost> {
        self.growth.get(owner)
    }

    pub fn growth_forecasts(&self) -> impl Iterator<Item = (&Owner, &Cost)> {
        self.growth.iter()
    }

    pub fn growth_total(&self, resource: Resource) -> u64 {
        self.growth_totals[resource.index()]
    }

    /// Forecasts whose memory cost is unknown.
    pub fn growth_unknown(&self) -> u64 {
        self.growth_unknown
    }

    /// Sum over owners of the largest of its forecast, claims and `extra`.
    pub fn total(&self, resource: Resource, extra: &[(Owner, Option<u64>)]) -> u64 {
        let mut peaks: Vec<(&Owner, u64)> = Vec::new();
        let claims = self.claims.values().map(|item| (&item.owner, item.cost.get(resource)));
        for (owner, value) in claims.chain(extra.iter().map(|(owner, value)| (owner, *value))) {
            let Some(value) = value else { continue };
            match peaks.iter_mut().find(|(peak, _)| *peak == owner) {
                Some(entry) => entry.1 = entry.1.max(value),
                None if value > 0 => peaks.push((owner, value)),
                None => {}
            }
        }
        let mut total = self.growth_totals[resource.index()];
        for (owner, value) in peaks {
            let forecast = self.growth.get(owner).and_then(|cost| cost.get(resource)).unwrap_or(0);
            total += value.saturating_sub(forecast);
        }
        total
    }

    pub fn known_memory_bytes(&self) -> u64 {
        self.total(Resource::Memory, &[])
    }

    pub fn projected_memory_bytes(&self, owner: &Owner, cost: &Cost, resource: Resource) -> u64 {
        let mut extra = vec![(owner.clone(), cost.get(resource))];
        if cost.kind == Kind::Startup {
            // Rollouts already running come first (DSec): a new sandbox may
            // spend only the headroom that every queued continuation and
            // restore leaves, not just the head one's. This reserves no
            // execution slot: all may proceed at once when their combined
            // cost fits.
            extra.extend(self.pending(Kind::Restore).into_iter().map(|item| (item.owner.clone(), item.cost.get(resource))));
        }
        self.total(resource, &extra)
    }

    /// The first queued demand per owner of this kind, FIFO, skipping owners
    /// that already hold a claim of the kind.
    fn pending(&self, kind: Kind) -> Vec<&Claim> {
        let active: Vec<&Owner> = self.claims.values().filter(|item| item.cost.kind == kind).map(|item| &item.owner).collect();
        let mut owners: Vec<&Claim> = Vec::new();
        for item in self.waiting.values() {
            if item.cost.kind == kind && !active.contains(&&item.owner) && !owners.iter().any(|seen| seen.owner == item.owner) {
                owners.push(item);
            }
        }
        owners
    }

    pub fn foreground_waiting(&self) -> bool {
        [Kind::Startup, Kind::Restore].into_iter().any(|kind| !self.pending(kind).is_empty())
    }

    fn next_pending(&self, kind: Kind, capacity: usize) -> Option<&Claim> {
        if self.active_count(kind) >= capacity {
            return None;
        }
        self.pending(kind).into_iter().next()
    }

    /// Reclaim for progress, not for filling every concurrency slot: admitted
    /// bytes stay fully charged, plus only the next eligible FIFO owner.
    /// Existing continuations precede new launches.
    pub fn next_memory_demands(&self, limits: &Limits, resource: Resource) -> u64 {
        for kind in [Kind::Restore, Kind::Startup] {
            if let Some(item) = self.next_pending(kind, limit(limits, kind)) {
                return self.total(resource, &[(item.owner.clone(), item.cost.get(resource))]);
            }
        }
        self.total(resource, &[])
    }

    pub fn demand_snapshot(&self, limits: &Limits) -> DemandSnapshot {
        let admitted = self.known_memory_bytes();
        let mut unknown = self.claims.values().filter(|item| item.cost.memory_bytes.is_none()).count() as u64;
        unknown += self.growth_unknown;
        for (kind, _) in limits {
            unknown += self.pending(*kind).iter().filter(|item| item.cost.memory_bytes.is_none()).count() as u64;
        }
        let admitted_ram = self.total(Resource::RamBacking, &[]);
        DemandSnapshot {
            admitted_demand_bytes: admitted,
            pending_demand_bytes: self.next_memory_demands(limits, Resource::Memory).saturating_sub(admitted),
            unknown_transition_memory_costs: unknown,
            admitted_ram_backing_bytes: admitted_ram,
            pending_ram_backing_bytes: self.next_memory_demands(limits, Resource::RamBacking).saturating_sub(admitted_ram),
        }
    }

    /// Per kind, in `TransitionKind` order: active claims, queued owners and
    /// summed costs (`None` when any is unknown).
    pub fn snapshot(&self) -> Vec<(Kind, KindSnapshot)> {
        fn sum(costs: &[&Cost], field: fn(&Cost) -> Option<u64>) -> Option<u64> {
            costs.iter().map(|cost| field(cost)).sum()
        }
        Kind::ALL
            .into_iter()
            .map(|kind| {
                let costs: Vec<&Cost> = self.claims.values().filter(|item| item.cost.kind == kind).map(|item| &item.cost).collect();
                (kind, KindSnapshot {
                    active: costs.len(),
                    waiting: self.pending(kind).len(),
                    memory_bytes: sum(&costs, |cost| cost.memory_bytes),
                    read_bytes: sum(&costs, |cost| cost.read_bytes),
                    write_bytes: sum(&costs, |cost| cost.write_bytes),
                })
            })
            .collect()
    }
}
