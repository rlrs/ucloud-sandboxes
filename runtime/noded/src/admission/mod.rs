//! Node admission, ported from the Python agent (docs/admission-port-3c.md):
//! - [`ledger`]: `TransitionLedger`, the in-flight transition accounting;
//! - [`fair`]: `FairCapacity`, the weighted FIFO slot pools on tokio;
//! - [`growth`]: managed growth costs, intents, provisional claims and turns;
//! - [`decide`]: the pure admission decision (`admission_decision.py`);
//! - [`service`]: the async service that drives them (step 3c.2);
//! - [`routes`]: the agent's routes on the internal socket;
//! - [`port`]: noded's own callers (job start, terminal observation);
//! - [`sources`]: the daemon's registry, metrics and samples, the pause
//!   policy's demand and local waits' growth;
//! - [`standalone`]: `--admission-only`, for the agent's tests.
//!
//! Step 3c.1 ported the pure parts against Python goldens
//! (`testdata/goldens.py`). Step 3c.2 serves them behind
//! `--rust-admission`; the agent does not call them yet (3c.3).

pub mod decide;
pub mod fair;
pub mod growth;
pub mod ledger;
pub mod port;
pub mod routes;
pub mod service;
pub mod sources;
pub mod standalone;

#[cfg(test)]
mod tests;

/// `(sandbox_id, sandbox_generation)`.
pub type Owner = (String, u64);
