//! `FairCapacity` (ucloud_sandboxes/admission.py) on tokio: weighted FIFO
//! admission that waits before allocating, without retry races.
//!
//! A release grants eligible heads before any new caller can take the
//! capacity (no barging), and wakes only the waiters it granted. A waiter's
//! life has three steps, as in Python:
//! - `enter`: an immediate grant, a refusal (non-blocking) or a queued
//!   [`Waiter`];
//! - the wait itself, which ends with a wakeup or a timeout;
//! - [`Waiter::finish`]: a grant that raced the timeout is kept (true);
//!   otherwise the waiter leaves the queue and the next fitting heads are
//!   granted (false).
//!
//! Dropping a `Waiter` without finishing it (the `acquire` future dropped by a
//! client disconnect, or an interrupt) returns a grant it already holds, or
//! leaves the queue; Python's `except BaseException` path.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FairError {
    /// `capacity must be positive`.
    Capacity,
    /// `request weight exceeds capacity`.
    Weight,
    /// `capacity released without a reservation`.
    Release,
}

impl std::fmt::Display for FairError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            FairError::Capacity => "capacity must be positive",
            FairError::Weight => "request weight exceeds capacity",
            FairError::Release => "capacity released without a reservation",
        })
    }
}

impl std::error::Error for FairError {}

/// `_Waiter`. Its flags change only under the queue lock.
#[derive(Debug)]
pub struct Ticket<O> {
    pub weight: usize,
    pub owner: Option<O>,
    granted: AtomicBool,
    cancelled: AtomicBool,
    ready: AtomicBool,
    notify: Notify,
}

impl<O> Ticket<O> {
    pub fn granted(&self) -> bool {
        self.granted.load(Ordering::Relaxed)
    }

    pub fn cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }

    /// Python's `ready.is_set()`: granted or cancelled.
    pub fn ready(&self) -> bool {
        self.ready.load(Ordering::Relaxed)
    }

    fn set_ready(&self) {
        self.ready.store(true, Ordering::Relaxed);
        // A stored permit: a waiter not yet polling still sees it.
        self.notify.notify_one();
    }
}

#[derive(Debug)]
struct State<O> {
    available: usize,
    waiters: VecDeque<Arc<Ticket<O>>>,
}

#[derive(Debug)]
struct Inner<O> {
    capacity: usize,
    state: Mutex<State<O>>,
}

impl<O> Inner<O> {
    fn lock(&self) -> std::sync::MutexGuard<'_, State<O>> {
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Reserve before notifying, so new callers cannot steal a released slot.
/// Only eligible FIFO heads are woken.
fn grant_waiters<O>(state: &mut State<O>) {
    while state.waiters.front().is_some_and(|head| head.weight <= state.available) {
        let ticket = state.waiters.pop_front().expect("checked above");
        state.available -= ticket.weight;
        ticket.granted.store(true, Ordering::Relaxed);
        ticket.set_ready();
    }
}

fn remove<O>(state: &mut State<O>, ticket: &Arc<Ticket<O>>) -> bool {
    match state.waiters.iter().position(|item| Arc::ptr_eq(item, ticket)) {
        Some(index) => {
            state.waiters.remove(index);
            true
        }
        None => false,
    }
}

#[derive(Debug)]
pub struct FairCapacity<O> {
    inner: Arc<Inner<O>>,
}

impl<O> Clone for FairCapacity<O> {
    fn clone(&self) -> Self {
        FairCapacity { inner: self.inner.clone() }
    }
}

/// `enter`'s outcome.
#[derive(Debug)]
pub enum Entry<O> {
    Granted,
    Refused,
    Queued(Waiter<O>),
}

/// A queued acquire. Finish it after the wait, or drop it to give up.
#[derive(Debug)]
pub struct Waiter<O> {
    inner: Arc<Inner<O>>,
    ticket: Arc<Ticket<O>>,
    done: bool,
}

impl<O> Waiter<O> {
    pub fn ticket(&self) -> &Arc<Ticket<O>> {
        &self.ticket
    }

    /// Resolves once the ticket is granted or cancelled.
    pub async fn ready(&self) {
        while !self.ticket.ready() {
            self.ticket.notify.notified().await;
        }
    }

    /// After a wakeup or a timeout: a grant that raced the timeout owns the
    /// capacity until this caller accepts it, so it is never leaked or
    /// granted twice.
    pub fn finish(mut self) -> bool {
        self.done = true;
        let mut state = self.inner.lock();
        if self.ticket.granted() {
            return true;
        }
        if !self.ticket.cancelled() {
            remove(&mut state, &self.ticket);
        }
        grant_waiters(&mut state);
        false
    }
}

impl<O> Drop for Waiter<O> {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        let mut state = self.inner.lock();
        if self.ticket.granted() {
            state.available += self.ticket.weight;
        } else {
            remove(&mut state, &self.ticket);
        }
        grant_waiters(&mut state);
    }
}

impl<O: PartialEq> FairCapacity<O> {
    pub fn new(capacity: usize) -> Result<Self, FairError> {
        if capacity < 1 {
            return Err(FairError::Capacity);
        }
        let state = State { available: capacity, waiters: VecDeque::new() };
        Ok(FairCapacity { inner: Arc::new(Inner { capacity, state: Mutex::new(state) }) })
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    pub fn available(&self) -> usize {
        self.inner.lock().available
    }

    /// Queued tickets, head first.
    pub fn queue(&self) -> Vec<Arc<Ticket<O>>> {
        self.inner.lock().waiters.iter().cloned().collect()
    }

    pub fn waiting(&self) -> usize {
        self.inner.lock().waiters.len()
    }

    /// Take `weight` now if nobody queues ahead, else refuse or queue.
    pub fn enter(&self, weight: usize, owner: Option<O>, blocking: bool) -> Result<Entry<O>, FairError> {
        if !(0 < weight && weight <= self.inner.capacity) {
            return Err(FairError::Weight);
        }
        let mut state = self.inner.lock();
        if state.waiters.is_empty() && state.available >= weight {
            state.available -= weight;
            return Ok(Entry::Granted);
        }
        if !blocking {
            return Ok(Entry::Refused);
        }
        let ticket = Arc::new(Ticket {
            weight,
            owner,
            granted: AtomicBool::new(false),
            cancelled: AtomicBool::new(false),
            ready: AtomicBool::new(false),
            notify: Notify::new(),
        });
        state.waiters.push_back(ticket.clone());
        Ok(Entry::Queued(Waiter { inner: self.inner.clone(), ticket, done: false }))
    }

    /// `acquire(blocking=False)`.
    pub fn try_acquire(&self, weight: usize, owner: Option<O>) -> Result<bool, FairError> {
        Ok(matches!(self.enter(weight, owner, false)?, Entry::Granted))
    }

    /// `acquire(timeout=...)`: wait in FIFO order up to `timeout` (forever
    /// when `None`). Dropping the future gives the place, or the grant, back.
    pub async fn acquire(&self, weight: usize, owner: Option<O>, timeout: Option<Duration>) -> Result<bool, FairError> {
        let waiter = match self.enter(weight, owner, true)? {
            Entry::Granted => return Ok(true),
            Entry::Refused => unreachable!("blocking entries queue"),
            Entry::Queued(waiter) => waiter,
        };
        match timeout {
            Some(timeout) => {
                let _ = tokio::time::timeout(timeout, waiter.ready()).await;
            }
            None => waiter.ready().await,
        }
        Ok(waiter.finish())
    }

    pub fn release(&self, weight: usize) -> Result<(), FairError> {
        let mut state = self.inner.lock();
        if weight == 0 || state.available + weight > self.inner.capacity {
            return Err(FairError::Release);
        }
        state.available += weight;
        grant_waiters(&mut state);
        Ok(())
    }

    /// Wake queued owners (all when `owner` is `None`) without revoking
    /// capacity already granted; their acquires return false.
    pub fn cancel_waiters(&self, owner: Option<&O>) -> usize {
        let mut state = self.inner.lock();
        let mut cancelled = 0;
        state.waiters.retain(|ticket| {
            let selected = owner.is_none() || ticket.owner.as_ref() == owner;
            if selected {
                ticket.cancelled.store(true, Ordering::Relaxed);
                ticket.set_ready();
                cancelled += 1;
            }
            !selected
        });
        grant_waiters(&mut state);
        cancelled
    }
}
