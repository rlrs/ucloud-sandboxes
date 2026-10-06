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
        let elapsed = started.elapsed().as_millis() as u64;
        *self.phases.entry(format!("{name}_ms")).or_insert(0) += elapsed;
    }

    pub fn phases(&self) -> &BTreeMap<String, u64> {
        &self.phases
    }
}
