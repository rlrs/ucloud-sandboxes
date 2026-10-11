//! Node admission, ported from the Python agent (docs/admission-port-3c.md):
//! - [`ledger`]: `TransitionLedger`, the in-flight transition accounting;
//! - [`fair`]: `FairCapacity`, the weighted FIFO slot pools on tokio;
//! - [`growth`]: managed growth costs, intents, provisional claims and turns;
//! - [`decide`]: the pure admission decision (`admission_decision.py`).
//!
//! Step 3c.1: pure ports checked against Python goldens
//! (`testdata/goldens.py`); nothing calls them yet.

pub mod decide;
pub mod fair;
pub mod growth;
pub mod ledger;

#[cfg(test)]
mod tests;

/// `(sandbox_id, sandbox_generation)`.
pub type Owner = (String, u64);
