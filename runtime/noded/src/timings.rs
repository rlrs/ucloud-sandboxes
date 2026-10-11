//! Create phase timings, reported as `<name>_ms` like phase_timings.py: repeated
//! phases accumulate and nested phases overlap, so values are not additive.

use std::collections::BTreeMap;
use std::time::Instant;

#[derive(Debug, Default, Clone)]
pub struct Timings {
    phases: BTreeMap<String, u64>,
}

impl Timings {
    pub fn start(&self) -> Instant {
        Instant::now()
    }

    pub fn add(&mut self, name: &str, started: Instant) {
        self.add_ms(name, started.elapsed().as_millis() as u64);
    }

    /// A phase timed elsewhere (in-process admission).
    pub fn add_ms(&mut self, name: &str, milliseconds: u64) {
        *self.phases.entry(format!("{name}_ms")).or_insert(0) += milliseconds;
    }

    pub fn phases(&self) -> &BTreeMap<String, u64> {
        &self.phases
    }
}
