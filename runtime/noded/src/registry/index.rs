//! The owner's in-memory index (`_RegistryIndex`) and the snapshot readers see.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};

use super::error::{RegistryError, Result};
use super::record::Registration;

/// One stored row: its image ID, its exact `record_json`, and the record.
#[derive(Debug)]
pub struct Row {
    pub image_id: String,
    pub encoded: String,
    pub record: Arc<Registration>,
}

/// The registrations exactly as committed at `revision`; never mutated once
/// published. `data_version` is the owner connection's: equal values prove
/// no other connection has committed since this state was read or written.
#[derive(Debug)]
pub struct Index {
    pub revision: i64,
    pub data_version: i64,
    pub rows: HashMap<String, Arc<Row>>,
    snapshot: OnceLock<Arc<Snapshot>>,
}

/// One coherent view of the durable registry (`DirectRegistrySnapshot`).
#[derive(Debug)]
pub struct Snapshot {
    /// Sorted by sandbox ID in code point order.
    pub records: Vec<Arc<Registration>>,
    pub by_sandbox_id: HashMap<String, Arc<Registration>>,
    /// Non-empty image IDs of every record.
    pub image_ids: HashSet<String>,
    pub activity_revision: i64,
}

impl Snapshot {
    pub fn get(&self, sandbox_id: &str) -> Option<&Arc<Registration>> {
        self.by_sandbox_id.get(sandbox_id)
    }
}

impl Index {
    pub fn new(revision: i64, data_version: i64, rows: HashMap<String, Arc<Row>>) -> Index {
        Index { revision, data_version, rows, snapshot: OnceLock::new() }
    }

    /// The snapshot, built once. The clock may never be behind a record.
    pub fn snapshot(&self) -> Result<Arc<Snapshot>> {
        if let Some(snapshot) = self.snapshot.get() {
            return Ok(snapshot.clone());
        }
        let mut keys: Vec<&String> = self.rows.keys().collect();
        keys.sort();
        let records: Vec<Arc<Registration>> = keys.into_iter().map(|key| self.rows[key].record.clone()).collect();
        if records.iter().any(|record| record.revision > self.revision) {
            return Err(RegistryError::registry("direct registry activity revision is invalid"));
        }
        let snapshot = Snapshot {
            by_sandbox_id: records.iter().map(|record| (record.sandbox_id().to_owned(), record.clone())).collect(),
            image_ids: records.iter().filter(|r| !r.image_id.is_empty()).map(|r| r.image_id.clone()).collect(),
            records,
            activity_revision: self.revision,
        };
        Ok(self.snapshot.get_or_init(|| Arc::new(snapshot)).clone())
    }

    /// A new index with `staged` applied (`None` deletes); `data_version` is kept.
    pub fn applied(&self, staged: HashMap<String, Option<Arc<Row>>>, revision: i64) -> Index {
        let mut rows = self.rows.clone();
        for (key, row) in staged {
            match row {
                Some(row) => rows.insert(key, row),
                None => rows.remove(&key),
            };
        }
        Index::new(revision, self.data_version, rows)
    }
}
