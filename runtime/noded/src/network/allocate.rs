//! Lease allocation with a group commit. Python allocates one lease per
//! durable write under the global state flock; a burst of creates then
//! queues behind one fsync'd write (file and directory) each, which on a
//! node disk is most of `network_ensure` (32 at once: p50 about 105 ms
//! against 1.3 ms of netlink work). Here the creates queued in this process
//! share one flock turn and one durable write. Each allocation is still
//! durable before its kernel work starts, and the state file, flocks and
//! per-lease semantics are Python's: another process sees the group as
//! allocations made one after another under the flock.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::Ordering;
use std::sync::{Condvar, Mutex};
use std::time::Instant;

use serde_json::{Map, Value, json};

use super::{Lease, MAX_SLOTS, NetworkError, NetworkManager, fail, pool_slots};

/// One queued allocation.
struct Request {
    ticket: u64,
    key: String,
    sandbox_id: String,
    generation: u64,
    requested_at: Instant,
}

/// The allocated lease and whether its slot came from the pool.
type Outcome = Result<(Lease, bool), NetworkError>;

#[derive(Default)]
struct Queue {
    next_ticket: u64,
    pending: Vec<Request>,
    done: BTreeMap<u64, Outcome>,
    /// Whether a thread is allocating a group now.
    leader: bool,
}

#[derive(Default)]
pub(super) struct Allocator {
    queue: Mutex<Queue>,
    changed: Condvar,
}

impl Allocator {
    #[cfg(test)]
    pub(super) fn pending(&self) -> usize {
        self.queue.lock().expect("not poisoned").pending.len()
    }
}

fn duplicate(error: &NetworkError) -> NetworkError {
    match error {
        NetworkError::Network(message) => NetworkError::Network(message.clone()),
        NetworkError::Io(error) => NetworkError::Io(std::io::Error::new(error.kind(), error.to_string())),
    }
}

impl NetworkManager {
    /// Allocate (or find) the lease for `key`, in a group with the creates
    /// queued meanwhile. Call holding the per-lease flock.
    pub(super) fn allocate(&self, key: String, sandbox_id: &str, generation: u64, requested_at: Instant) -> Outcome {
        let allocator = &self.allocator;
        let mut queue = allocator.queue.lock().expect("not poisoned");
        let ticket = queue.next_ticket;
        queue.next_ticket += 1;
        queue.pending.push(Request { ticket, key, sandbox_id: sandbox_id.to_string(), generation, requested_at });
        loop {
            if let Some(outcome) = queue.done.remove(&ticket) {
                return outcome;
            }
            if queue.leader {
                queue = allocator.changed.wait(queue).expect("not poisoned");
                continue;
            }
            queue.leader = true;
            drop(queue);
            let outcomes = self.allocate_group();
            queue = allocator.queue.lock().expect("not poisoned");
            queue.done.extend(outcomes);
            queue.leader = false;
            allocator.changed.notify_all();
        }
    }

    /// Take the global flock, then every queued request, and allocate them
    /// with one durable write.
    fn allocate_group(&self) -> Vec<(u64, Outcome)> {
        let state_lock = Self::locked(&self.lock_path);
        // Taken after the flock: requests that queued while it was held join.
        let group = std::mem::take(&mut self.allocator.queue.lock().expect("not poisoned").pending);
        let everyone = |error: NetworkError| group.iter().map(|request| (request.ticket, Err(duplicate(&error)))).collect();
        let _state_lock = match state_lock {
            Ok(lock) => lock,
            Err(error) => return everyone(error),
        };
        let mut state = match self.load() {
            Ok(state) => state,
            Err(error) => return everyone(error),
        };
        let mut outcomes: Vec<(u64, Outcome)> = Vec::with_capacity(group.len());
        let mut written = false;
        for request in &group {
            let outcome = self.allocate_one(&mut state, request).map(|(lease, pooled, new)| {
                written |= new;
                (lease, pooled)
            });
            outcomes.push((request.ticket, outcome));
        }
        // One durable write moves every pooled slot of the group to its lease.
        if written {
            if let Err(error) = self.store(&state, false) {
                // Nothing was allocated; a claimed pool slot is still durably
                // pooled, and the refill rechecks it.
                return everyone(error);
            }
            self.lease_writes.fetch_add(1, Ordering::Relaxed);
        }
        // One reconciliation, started after every member arrived, serves all.
        let latest = group.iter().map(|request| request.requested_at).max();
        if let Some(latest) = latest.filter(|_| outcomes.iter().any(|(_, outcome)| outcome.is_ok())) {
            if let Err(error) = self.ensure_host_rules_since(latest) {
                for (_, outcome) in outcomes.iter_mut().filter(|(_, outcome)| outcome.is_ok()) {
                    *outcome = Err(duplicate(&error));
                }
            }
        }
        outcomes
    }

    /// Python's allocation step for one request: its existing lease, else the
    /// lowest ready pooled slot, else the lowest free one. Whether the state
    /// changed is the third value.
    fn allocate_one(&self, state: &mut Map<String, Value>, request: &Request) -> Result<(Lease, bool, bool), NetworkError> {
        if state.get("policies").and_then(|policies| policies.get(&request.key)).is_some() {
            return Err(fail("network policy is immutable for a sandbox generation"));
        }
        let mut leases = state["leases"].as_object().cloned().unwrap_or_default();
        if let Some(slot) = leases.get(&request.key).and_then(Value::as_u64) {
            return Ok((self.lease(&request.sandbox_id, request.generation, slot as u32)?, false, false));
        }
        let pooled = self.pool.claim(state);
        let slot = match pooled {
            Some(slot) => slot,
            None => {
                let mut used: BTreeSet<u64> = leases.values().filter_map(Value::as_u64).collect();
                used.extend(pool_slots(state));
                (1..=MAX_SLOTS)
                    .find(|slot| !used.contains(&u64::from(*slot)))
                    .ok_or_else(|| fail("direct network slot capacity is exhausted"))?
            }
        };
        let lease = self.lease(&request.sandbox_id, request.generation, slot)?;
        leases.insert(request.key.clone(), json!(slot));
        state.insert("leases".into(), Value::Object(leases));
        Ok((lease, pooled.is_some(), true))
    }
}
