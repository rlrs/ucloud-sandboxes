//! The node registry, `direct-registry.sqlite` (ucloud_sandboxes/direct_registry.py,
//! `DirectSandboxRegistry`): the SQLite ownership bridge from admission through
//! create and delete. This port writes the same file Python reads: the same
//! DDL text, metadata and byte-identical `record_json`, and it keeps every
//! invariant Python relies on, so either implementation can own the file and
//! the other can read it (or take ownership back) without a migration.
//!
//! The activity revision is the node clock: every write listed as bumping in
//! the porting contract moves it by exactly one, and it is never behind any
//! record's revision. Operations block on SQLite; async callers run them on
//! a blocking thread.

mod error;
mod index;
mod record;
mod schema;
mod spec;
mod store;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{OptionalExtension, params};

pub use error::{RegistryError, Result};
pub use index::Snapshot;
pub use record::{
    DIRECT_REGISTRATION_VERSION, DiskClaim, DrainState, GrowthIntent, MIB, Phase, ReflinkOverlapClaim, Registration,
    SPLIT_REGISTRATION_VERSION, is_operation_id,
};
pub use schema::{APPLICATION_ID, SCHEMA, SCHEMA_VERSION};
pub use spec::SandboxSpec;
pub use store::ClaimsMb;

use spec::is_digest;
use store::{CLAIM_JOIN, ROW_CLAIM_MB, Store, Tx};

/// The registry file under the agent's state root.
pub const REGISTRY_FILE_NAME: &str = "direct-registry.sqlite";
const TRANSITION_FENCE: &str = "direct registration transition lost its ownership fence";

/// The rootfs identity `commit_rootfs` records (the `DirectSandbox` fields it reads).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rootfs {
    pub rootfs_sha256: String,
    pub container_id: String,
    /// Absolute and already normalized (Python stores `str(Path)`).
    pub bundle: String,
    pub memory_directory: String,
}

/// A prepared project quota: (project ID, MiB, absolute path).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Quota {
    pub project_id: i64,
    pub total_mb: i64,
    pub path: String,
}

/// A migration's ownership fence: its ID and manifest digest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Migration {
    pub id: String,
    pub sha256: String,
}

/// `plan`'s arguments.
#[derive(Clone, Debug)]
pub struct PlanRequest {
    pub spec: SandboxSpec,
    pub sandbox_generation: i64,
    pub operation_id: String,
    pub runtime_compatibility_sha256: String,
    pub split_memory_backing: bool,
    /// A dynamic claim charged instead of the spec's maximum (split only).
    pub initial_claim: Option<DiskClaim>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GrowthAction {
    Launch,
    Bind,
    Activate,
    Wait,
    Park,
    Terminal,
}

impl GrowthAction {
    pub fn parse(action: &str) -> Result<GrowthAction> {
        Ok(match action {
            "launch" => GrowthAction::Launch,
            "bind" => GrowthAction::Bind,
            "activate" => GrowthAction::Activate,
            "wait" => GrowthAction::Wait,
            "park" => GrowthAction::Park,
            "terminal" => GrowthAction::Terminal,
            _ => return Err(RegistryError::invalid("invalid growth action")),
        })
    }
}

/// How a transition is fenced, beyond its expected revision and phases.
struct Fences<'a> {
    from: &'a [Phase],
    migration: Option<&'a Migration>,
    expected_generation: Option<i64>,
    error: &'static str,
    /// Tombstone the record's current migration ID.
    retire: bool,
    durable: bool,
}

impl<'a> Fences<'a> {
    fn from(from: &'a [Phase]) -> Fences<'a> {
        Fences {
            from,
            migration: None,
            expected_generation: None,
            error: TRANSITION_FENCE,
            retire: false,
            durable: true,
        }
    }

    fn fenced(from: &'a [Phase], migration: &'a Migration, error: &'static str) -> Fences<'a> {
        Fences { migration: Some(migration), error, ..Fences::from(from) }
    }
}

fn now_ns() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(1, |elapsed| elapsed.as_nanos() as i64)
}

fn mib(value: i64) -> i128 {
    i128::from(value) * i128::from(MIB)
}

/// The node registry; `Send + Sync`, shared by reference across threads.
pub struct Registry {
    store: Store,
}

impl Registry {
    /// A non-owner instance: it reads and writes SQLite directly.
    pub fn new(path: impl Into<PathBuf>, hard_disk_capacity_mb: i64) -> Result<Registry> {
        let path = path.into();
        if !path.is_absolute() {
            return Err(RegistryError::invalid("direct registry path must be absolute"));
        }
        if hard_disk_capacity_mb < 0 {
            return Err(RegistryError::invalid("disk capacity cannot be negative"));
        }
        Ok(Registry { store: Store::new(path, hard_disk_capacity_mb) })
    }

    /// The owner instance: it holds the file's exclusive owner lock for its
    /// life (a second live owner in any process is refused) and serves
    /// registration reads from its index.
    pub fn owner(path: impl Into<PathBuf>, hard_disk_capacity_mb: i64) -> Result<Registry> {
        let registry = Registry::new(path, hard_disk_capacity_mb)?;
        registry.store.own()?;
        Ok(registry)
    }

    pub fn path(&self) -> &std::path::Path {
        self.store.path()
    }

    pub fn is_owner(&self) -> bool {
        self.store.is_owner()
    }

    pub fn hard_disk_capacity_mb(&self) -> i64 {
        self.store.hard_disk_capacity_mb.load(Ordering::SeqCst)
    }

    pub fn set_hard_disk_capacity_mb(&self, capacity_mb: i64) -> Result<()> {
        if capacity_mb < 0 {
            return Err(RegistryError::invalid("disk capacity cannot be negative"));
        }
        self.store.hard_disk_capacity_mb.store(capacity_mb, Ordering::SeqCst);
        Ok(())
    }

    /// Release ownership and idle connections; later calls use SQLite directly.
    pub fn close(&self) {
        self.store.close();
    }

    // Reads.

    pub fn get(&self, sandbox_id: &str) -> Result<Option<Arc<Registration>>> {
        Ok(self.store.view_index()?.rows.get(sandbox_id).map(|row| row.record.clone()))
    }

    /// Records, indexes and revision from one durable read.
    pub fn snapshot(&self) -> Result<Arc<Snapshot>> {
        self.store.view_index()?.snapshot()
    }

    /// Every registration, sorted by sandbox ID.
    pub fn list(&self) -> Result<Vec<Arc<Registration>>> {
        Ok(self.snapshot()?.records.clone())
    }

    /// Whether any registration uses this (non-empty) image ID.
    pub fn references_image(&self, image_id: &str) -> Result<bool> {
        Ok(self.snapshot()?.image_ids.contains(image_id))
    }

    /// The durable node clock, without decoding the inventory.
    pub fn activity_revision(&self) -> Result<i64> {
        if let Some(index) = self.store.owned_index()? {
            return Ok(index.revision);
        }
        self.store.read(|tx| Ok(tx.metadata()?.activity_revision))
    }

    /// Every registration's current charge in MiB, clamped at zero, keyed by
    /// (sandbox ID, generation). The owner keeps the charges read at its
    /// index until the index moves: every charge change bumps the clock.
    pub fn disk_claims_mb(&self) -> Result<ClaimsMb> {
        let index = self.store.owned_index()?;
        if let Some(index) = &index
            && let Some((cached, claims)) = &*self.store.claims.lock().unwrap_or_else(|p| p.into_inner())
            && Arc::ptr_eq(cached, index)
        {
            return Ok(claims.clone());
        }
        self.store.read(|tx| {
            let mut statement = tx.connection.prepare_cached(&format!(
                "SELECT d.sandbox_id, d.sandbox_generation, {ROW_CLAIM_MB} FROM {CLAIM_JOIN}"
            ))?;
            let claims = statement
                .query_map([], |row| {
                    Ok(((row.get::<_, String>(0)?, row.get::<_, i64>(1)?), row.get::<_, i64>(2)?.max(0)))
                })?
                .collect::<rusqlite::Result<HashMap<_, _>>>()?;
            if let Some(index) = &index
                && tx.metadata()?.activity_revision == index.revision
            {
                *self.store.claims.lock().unwrap_or_else(|p| p.into_inner()) = Some((index.clone(), claims.clone()));
            }
            Ok(claims)
        })
    }

    /// The current dynamic claim, or `None` for a fixed (legacy) claim.
    pub fn disk_claim(&self, sandbox_id: &str, sandbox_generation: i64) -> Result<Option<DiskClaim>> {
        self.store.read(|tx| {
            let row = tx
                .connection
                .prepare_cached(
                    "SELECT workspace_mb, memory_mb, dynamic FROM registration_disk WHERE sandbox_id=? AND sandbox_generation=?",
                )?
                .query_row(params![sandbox_id, sandbox_generation], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?))
                })
                .optional()?;
            match row {
                Some((workspace_mb, memory_mb, dynamic)) if dynamic != 0 => DiskClaim::new(workspace_mb, memory_mb).map(Some),
                _ => Ok(None),
            }
        })
    }

    /// Fence for a publication: capture before publishing, release with it.
    pub fn workspace_mount_epoch(&self, sandbox_id: &str, sandbox_generation: i64) -> Result<i64> {
        self.store.read(|tx| Ok(mount_epoch(tx, sandbox_id, sandbox_generation)?.0))
    }

    /// Reflink overlaps, of one incarnation or all, by (ID, generation, hibernation generation).
    pub fn list_reflink_overlaps(&self, incarnation: Option<(&str, i64)>) -> Result<Vec<ReflinkOverlapClaim>> {
        self.store.read(|tx| {
            let mut sql = "SELECT sandbox_id,sandbox_generation,hibernation_generation,allocated_bytes,manifest_sha256 FROM reflink_overlaps".to_string();
            if incarnation.is_some() {
                sql.push_str(" WHERE sandbox_id=? AND sandbox_generation=?");
            }
            sql.push_str(" ORDER BY sandbox_id,sandbox_generation,hibernation_generation");
            let mut statement = tx.connection.prepare_cached(&sql)?;
            let claim = |row: &rusqlite::Row<'_>| {
                Ok(ReflinkOverlapClaim {
                    sandbox_id: row.get(0)?,
                    sandbox_generation: row.get(1)?,
                    hibernation_generation: row.get(2)?,
                    allocated_bytes: row.get(3)?,
                    manifest_sha256: row.get(4)?,
                })
            };
            let claims = match incarnation {
                Some((sandbox_id, generation)) => statement.query_map(params![sandbox_id, generation], claim)?.collect(),
                None => statement.query_map([], claim)?.collect::<rusqlite::Result<Vec<_>>>(),
            };
            Ok(claims?)
        })
    }

    pub fn reflink_overlap_bytes(&self) -> Result<i64> {
        self.store.read(|tx| {
            Ok(tx
                .connection
                .query_row("SELECT COALESCE(SUM(allocated_bytes),0) FROM reflink_overlaps", [], |row| row.get(0))?)
        })
    }

    /// Every growth forecast, by sandbox ID.
    pub fn growth_intents(&self) -> Result<Vec<GrowthIntent>> {
        self.store.read(|tx| {
            let mut statement = tx.connection.prepare_cached("SELECT * FROM managed_growth ORDER BY sandbox_id")?;
            Ok(statement.query_map([], growth_row)?.collect::<rusqlite::Result<Vec<_>>>()?)
        })
    }

    pub fn load_drain(&self) -> Result<DrainState> {
        self.store.read(|tx| Ok(tx.metadata()?.drain))
    }

    // Metadata writes (none bumps the clock).

    /// Bind the node to one runtime compatibility; idempotent, and exactly
    /// one of two concurrent different binds wins.
    pub fn bind_runtime_compatibility(&self, expected_sha256: &str) -> Result<String> {
        if !is_digest(expected_sha256) {
            return Err(RegistryError::invalid("runtime compatibility digest is invalid"));
        }
        self.store.write(true, |tx| {
            let actual = tx.metadata()?.runtime_compatibility_sha256;
            if actual.as_ref().is_some_and(|actual| actual != expected_sha256) {
                return Err(RegistryError::registry("node state belongs to another runtime compatibility"));
            }
            let mut statement = tx.connection.prepare_cached("SELECT sandbox_id, image_id, record_json FROM registrations")?;
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                let record = schema::decode_row(row.get_ref(0)?, row.get_ref(1)?, row.get_ref(2)?)?;
                if record.runtime_compatibility_sha256 != expected_sha256 {
                    return Err(RegistryError::registry("direct registry contains another runtime compatibility"));
                }
            }
            if actual.is_none()
                && tx.connection.execute(
                    "UPDATE registry_metadata SET runtime_compatibility_sha256 = ? WHERE singleton = 1 AND runtime_compatibility_sha256 IS NULL",
                    [expected_sha256],
                )? != 1
            {
                return Err(RegistryError::registry("direct registry metadata changed"));
            }
            Ok(actual.unwrap_or_else(|| expected_sha256.to_owned()))
        })
    }

    pub fn save_drain(&self, drain: &DrainState) -> Result<()> {
        let encoded = drain.encode();
        DrainState::decode(&encoded)?;
        self.store.write(true, |tx| {
            if tx.connection.execute("UPDATE registry_metadata SET drain_json = ? WHERE singleton = 1", [&encoded])?
                != 1
            {
                return Err(RegistryError::registry("direct registry metadata changed"));
            }
            Ok(())
        })
    }

    // The registration state machine.

    /// Register a new incarnation as `planned`. An exact replay (same
    /// generation, operation, spec and compatibility) returns the existing
    /// record in any phase without writing.
    pub fn plan(&self, request: PlanRequest) -> Result<Arc<Registration>> {
        if request.sandbox_generation <= 0 {
            return Err(RegistryError::invalid("sandbox generation must be positive"));
        }
        if request.initial_claim.is_some() && !request.split_memory_backing {
            return Err(RegistryError::invalid("dynamic disk claims require split memory backing"));
        }
        let candidate = candidate(
            request.spec,
            request.sandbox_generation,
            request.operation_id,
            request.runtime_compatibility_sha256,
            request.split_memory_backing,
            Phase::Planned,
            None,
        )?;
        self.plan_candidate(candidate, request.initial_claim)
    }

    /// Register an incoming migration as `import_planned`. Generation
    /// tombstones do not apply; this migration ID's tombstone does.
    pub fn plan_import(
        &self,
        spec: SandboxSpec,
        sandbox_generation: i64,
        operation_id: String,
        runtime_compatibility_sha256: String,
        migration: Migration,
        split_memory_backing: bool,
    ) -> Result<Arc<Registration>> {
        if sandbox_generation <= 0 {
            return Err(RegistryError::invalid("sandbox generation must be positive"));
        }
        let candidate = candidate(
            spec,
            sandbox_generation,
            operation_id,
            runtime_compatibility_sha256,
            split_memory_backing,
            Phase::ImportPlanned,
            Some(migration),
        )?;
        self.plan_candidate(candidate, None)
    }

    fn plan_candidate(&self, candidate: Registration, initial_claim: Option<DiskClaim>) -> Result<Arc<Registration>> {
        let imported = candidate.phase == Phase::ImportPlanned;
        self.store.write(true, |tx| {
            let compatibility = tx.metadata()?.runtime_compatibility_sha256;
            if compatibility.is_some_and(|bound| bound != candidate.runtime_compatibility_sha256) {
                return Err(RegistryError::registry("direct registration belongs to another runtime compatibility"));
            }
            if let Some(existing) = tx.get(candidate.sandbox_id())? {
                let replay = existing.sandbox_generation == candidate.sandbox_generation
                    && existing.operation_id == candidate.operation_id
                    && existing.spec == candidate.spec
                    && existing.runtime_compatibility_sha256 == candidate.runtime_compatibility_sha256
                    && (!imported
                        || (existing.migration_id == candidate.migration_id
                            && existing.migration_sha256 == candidate.migration_sha256));
                if replay {
                    return Ok(existing);
                }
                return Err(RegistryError::RegistrationOwned("sandbox already has another direct registration".into()));
            }
            let (fenced, error) = if imported {
                let row = tx
                    .connection
                    .prepare_cached("SELECT 1 FROM migration_tombstones WHERE sandbox_id = ? AND migration_id = ?")?
                    .query_row([candidate.sandbox_id(), &candidate.migration_id], |_| Ok(()))
                    .optional()?;
                (row.is_some(), "migration import is fenced by a tombstone")
            } else {
                let tombstone: Option<i64> = tx
                    .connection
                    .prepare_cached("SELECT generation FROM generation_tombstones WHERE sandbox_id = ?")?
                    .query_row([candidate.sandbox_id()], |row| row.get(0))
                    .optional()?;
                (
                    tombstone.is_some_and(|generation| generation >= candidate.sandbox_generation),
                    "direct registration is fenced by a tombstone",
                )
            };
            if fenced {
                return Err(RegistryError::conflict(error));
            }
            let claim_mb = match initial_claim {
                Some(claim) => claim.total_mb(),
                None => candidate.spec.requested_disk_mb().map_err(RegistryError::Invalid)?,
            };
            let capacity_mb = self.hard_disk_capacity_mb();
            if capacity_mb != 0 && tx.reserved_disk_bytes()? + mib(claim_mb) > mib(capacity_mb) {
                return Err(RegistryError::CapacityUnavailable(
                    "combined workspace and memory backing capacity exhausted".into(),
                ));
            }
            let sandbox_id = candidate.sandbox_id().to_owned();
            let written = tx.write(candidate, true)?;
            if let Some(claim) = initial_claim {
                set_dynamic_claim(tx, &sandbox_id, claim)?;
            }
            tx.bump_activity()?;
            Ok(written)
        })
    }

    /// `_transition`: compare-and-swap on (revision, phase, fences), then
    /// write the next revision; `change` sets the fields the phase adds.
    fn transition(
        &self,
        sandbox_id: &str,
        revision: i64,
        fences: Fences<'_>,
        phase: Phase,
        change: impl FnOnce(&mut Registration),
    ) -> Result<Arc<Registration>> {
        self.store.write(fences.durable, |tx| {
            let record = tx.require(sandbox_id)?;
            if record.revision != revision
                || fences.expected_generation.is_some_and(|generation| generation != record.sandbox_generation)
                || !fences.from.contains(&record.phase)
                || fences.migration.is_some_and(|fence| {
                    (record.migration_id.as_str(), record.migration_sha256.as_str())
                        != (fence.id.as_str(), fence.sha256.as_str())
                })
            {
                return Err(RegistryError::conflict(fences.error));
            }
            if fences.retire && !record.migration_id.is_empty() {
                tx.retire(sandbox_id, &record.migration_id)?;
            }
            let mut updated = Registration::clone(&record);
            updated.phase = phase;
            updated.revision = record.revision + 1;
            updated.updated_ns = now_ns();
            change(&mut updated);
            updated.validate().map_err(RegistryError::Invalid)?;
            let written = tx.write(updated, false)?;
            tx.bump_activity()?;
            Ok(written)
        })
    }

    pub fn commit_quota(&self, sandbox_id: &str, expected_revision: i64, quota: &Quota) -> Result<Arc<Registration>> {
        self.transition(sandbox_id, expected_revision, Fences::from(&[Phase::Planned]), Phase::QuotaReady, |record| {
            set_quota(record, quota)
        })
    }

    pub fn commit_import_quota(
        &self,
        sandbox_id: &str,
        expected_revision: i64,
        quota: &Quota,
    ) -> Result<Arc<Registration>> {
        self.transition(
            sandbox_id,
            expected_revision,
            Fences::from(&[Phase::ImportPlanned]),
            Phase::Importing,
            |record| set_quota(record, quota),
        )
    }

    /// `quota_ready` to `rootfs_ready`, or with `quota` straight from
    /// `planned` (its prepare is owner-keyed, so a crash before here replays it).
    pub fn commit_rootfs(
        &self,
        sandbox_id: &str,
        expected_revision: i64,
        image_id: &str,
        rootfs: &Rootfs,
        quota: Option<&Quota>,
    ) -> Result<Arc<Registration>> {
        let from = if quota.is_some() { [Phase::Planned] } else { [Phase::QuotaReady] };
        self.transition(sandbox_id, expected_revision, Fences::from(&from), Phase::RootfsReady, |record| {
            set_rootfs(record, image_id, rootfs);
            if let Some(quota) = quota {
                set_quota(record, quota);
            }
        })
    }

    pub fn commit_import_rootfs(
        &self,
        sandbox_id: &str,
        expected_revision: i64,
        image_id: &str,
        rootfs: &Rootfs,
    ) -> Result<Arc<Registration>> {
        self.transition(
            sandbox_id,
            expected_revision,
            Fences::from(&[Phase::Importing]),
            Phase::RootfsReady,
            |record| set_rootfs(record, image_id, rootfs),
        )
    }

    pub fn commit_import_ready(
        &self,
        sandbox_id: &str,
        expected_revision: i64,
        migration: &Migration,
    ) -> Result<Arc<Registration>> {
        let fences = Fences::fenced(&[Phase::RootfsReady], migration, "import readiness lost its ownership fence");
        self.transition(sandbox_id, expected_revision, fences, Phase::ImportReady, |_| {})
    }

    /// `import_ready` to `owned`; the migration fields stay on the record.
    pub fn activate_import(
        &self,
        sandbox_id: &str,
        expected_revision: i64,
        migration: &Migration,
    ) -> Result<Arc<Registration>> {
        let fences = Fences::fenced(&[Phase::ImportReady], migration, "import activation lost its ownership fence");
        self.transition(sandbox_id, expected_revision, fences, Phase::Owned, |_| {})
    }

    /// `owned` to `moving_out` under a new migration; the record's previous
    /// migration ID is tombstoned.
    pub fn begin_move_out(
        &self,
        sandbox_id: &str,
        expected_revision: i64,
        migration: &Migration,
    ) -> Result<Arc<Registration>> {
        let fences = Fences {
            error: "move preparation lost its ownership fence",
            retire: true,
            ..Fences::from(&[Phase::Owned])
        };
        self.transition(sandbox_id, expected_revision, fences, Phase::MovingOut, |record| {
            record.migration_id = migration.id.clone();
            record.migration_sha256 = migration.sha256.clone();
        })
    }

    pub fn abort_move_out(
        &self,
        sandbox_id: &str,
        expected_revision: i64,
        migration: &Migration,
    ) -> Result<Arc<Registration>> {
        let fences = Fences::fenced(&[Phase::MovingOut], migration, "move abort lost its ownership fence");
        self.transition(sandbox_id, expected_revision, fences, Phase::Owned, |record| {
            record.migration_id.clear();
            record.migration_sha256.clear();
        })
    }

    /// `rootfs_ready` to `owned`. A non-owner commits it without fsync:
    /// recovery re-derives `owned` from `rootfs_ready` and the Warden journal.
    pub fn commit_owned(&self, sandbox_id: &str, expected_revision: i64) -> Result<Arc<Registration>> {
        let fences = Fences { durable: false, ..Fences::from(&[Phase::RootfsReady]) };
        self.transition(sandbox_id, expected_revision, fences, Phase::Owned, |_| {})
    }

    pub fn begin_delete(
        &self,
        sandbox_id: &str,
        expected_revision: i64,
        expected_generation: Option<i64>,
    ) -> Result<Arc<Registration>> {
        let from = [Phase::Planned, Phase::QuotaReady, Phase::RootfsReady, Phase::Owned];
        let fences = Fences { expected_generation, ..Fences::from(&from) };
        self.transition(sandbox_id, expected_revision, fences, Phase::Deleting, |_| {})
    }

    pub fn begin_delete_moved(
        &self,
        sandbox_id: &str,
        expected_revision: i64,
        migration: &Migration,
    ) -> Result<Arc<Registration>> {
        let fences = Fences::fenced(&[Phase::MovingOut], migration, "move finalization lost its ownership fence");
        self.transition(sandbox_id, expected_revision, fences, Phase::Deleting, |_| {})
    }

    pub fn begin_delete_import(
        &self,
        sandbox_id: &str,
        expected_revision: i64,
        migration: &Migration,
    ) -> Result<Arc<Registration>> {
        let from = [Phase::Importing, Phase::RootfsReady, Phase::ImportReady];
        let fences = Fences::fenced(&from, migration, "import abort lost its ownership fence");
        self.transition(sandbox_id, expected_revision, fences, Phase::Deleting, |_| {})
    }

    /// Delete an `import_planned` record (`retire` tombstones its migration
    /// ID). No generation tombstone is written.
    pub fn abort_import_planned(
        &self,
        sandbox_id: &str,
        expected_revision: i64,
        migration: &Migration,
        retire: bool,
    ) -> Result<()> {
        self.store.write(true, |tx| {
            let record = tx.require(sandbox_id)?;
            if record.phase != Phase::ImportPlanned
                || record.revision != expected_revision
                || (record.migration_id.as_str(), record.migration_sha256.as_str())
                    != (migration.id.as_str(), migration.sha256.as_str())
            {
                return Err(RegistryError::conflict("import plan abort lost its ownership fence"));
            }
            if retire {
                tx.retire(sandbox_id, &migration.id)?;
            }
            tx.delete(sandbox_id)?;
            tx.bump_activity()
        })
    }

    /// Delete a `deleting` record, tombstone its generation (and migration),
    /// and forget its workspace releases, growth forecast and wake fences.
    pub fn commit_deleted(&self, sandbox_id: &str, sandbox_generation: i64, expected_revision: i64) -> Result<()> {
        if sandbox_generation <= 0 {
            return Err(RegistryError::invalid("sandbox generation must be positive"));
        }
        self.store.write(true, |tx| {
            let record = tx.require(sandbox_id)?;
            if record.phase != Phase::Deleting
                || record.revision != expected_revision
                || record.sandbox_generation != sandbox_generation
            {
                return Err(RegistryError::conflict("direct deletion completion lost its ownership fence"));
            }
            let overlap = tx
                .connection
                .prepare_cached("SELECT 1 FROM reflink_overlaps WHERE sandbox_id=? AND sandbox_generation=? LIMIT 1")?
                .query_row(params![sandbox_id, sandbox_generation], |_| Ok(()))
                .optional()?;
            if overlap.is_some() {
                return Err(RegistryError::conflict("deletion retains unreconciled reflink overlap"));
            }
            tx.connection
                .prepare_cached(
                    "INSERT INTO generation_tombstones VALUES (?, ?) ON CONFLICT (sandbox_id) DO UPDATE SET generation = MAX(generation, excluded.generation)",
                )?
                .execute(params![sandbox_id, sandbox_generation])?;
            if !record.migration_id.is_empty() {
                tx.retire(sandbox_id, &record.migration_id)?;
            }
            tx.connection.prepare_cached("DELETE FROM workspace_capacity WHERE sandbox_id=?")?.execute([sandbox_id])?;
            tx.connection
                .prepare_cached("DELETE FROM managed_growth WHERE sandbox_id=? AND generation=?")?
                .execute(params![sandbox_id, sandbox_generation])?;
            tx.connection
                .prepare_cached("DELETE FROM relay_wake_fences WHERE sandbox_id=? AND generation=?")?
                .execute(params![sandbox_id, sandbox_generation])?;
            tx.delete(sandbox_id)?;
            tx.bump_activity()
        })
    }

    // Disk claims and published workspaces.

    /// Move a dynamic claim; a fixed claim is left unchanged (`None`) unless
    /// `adopt` first converts a split registration's fixed claim into the
    /// equal dynamic claim. `require_capacity` admits an increase that will
    /// create physical bytes (refused with a cap of 0); without it the update
    /// records bytes that already exist and always succeeds.
    pub fn update_disk_claim(
        &self,
        sandbox_id: &str,
        sandbox_generation: i64,
        workspace_mb: Option<i64>,
        memory_mb: Option<i64>,
        require_capacity: bool,
        adopt: bool,
    ) -> Result<Option<DiskClaim>> {
        if workspace_mb.is_some_and(|value| value < 0) || memory_mb.is_some_and(|value| value < 0) {
            return Err(RegistryError::invalid("disk claim components must be non-negative integers"));
        }
        self.store.write(true, |tx| {
            let row = tx
                .connection
                .prepare_cached(
                    "SELECT d.workspace_mb, d.memory_mb, d.dynamic, COALESCE(w.released_mb, 0) FROM registration_disk AS d \
                     LEFT JOIN workspace_capacity AS w ON w.sandbox_id = d.sandbox_id AND w.sandbox_generation = d.sandbox_generation \
                     WHERE d.sandbox_id=? AND d.sandbox_generation=?",
                )?
                .query_row(params![sandbox_id, sandbox_generation], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?, row.get::<_, i64>(3)?))
                })
                .optional()?;
            let Some((mut workspace, mut memory, dynamic, released)) = row else {
                return Err(RegistryError::conflict("disk claim lost incarnation ownership"));
            };
            if dynamic == 0 {
                let adopted = if adopt { adopt_dynamic_claim(tx, sandbox_id)? } else { None };
                let Some(adopted) = adopted else {
                    return Ok(None);
                };
                (workspace, memory) = (adopted.workspace_mb, adopted.memory_mb);
            }
            let old = DiskClaim::new(workspace, memory)?;
            let new = DiskClaim::new(workspace_mb.unwrap_or(old.workspace_mb), memory_mb.unwrap_or(old.memory_mb))?;
            if new == old {
                return Ok(Some(new));
            }
            let published = released > 0;
            let delta_mb = (new.memory_mb - old.memory_mb) + if published { 0 } else { new.workspace_mb - old.workspace_mb };
            let capacity_mb = self.hard_disk_capacity_mb();
            if require_capacity
                && delta_mb > 0
                && (capacity_mb == 0 || tx.reserved_disk_bytes()? + mib(delta_mb) > mib(capacity_mb))
            {
                return Err(RegistryError::CapacityUnavailable("physical disk capacity exhausted".into()));
            }
            tx.connection
                .prepare_cached("UPDATE registration_disk SET workspace_mb=?, memory_mb=? WHERE sandbox_id=? AND sandbox_generation=?")?
                .execute(params![new.workspace_mb, new.memory_mb, sandbox_id, sandbox_generation])?;
            if published {
                tx.connection
                    .prepare_cached(
                        "UPDATE workspace_capacity SET released_mb=? WHERE sandbox_id=? AND sandbox_generation=? AND released_mb > 0",
                    )?
                    .execute(params![new.workspace_mb.max(1), sandbox_id, sandbox_generation])?;
            }
            tx.bump_activity()?;
            Ok(Some(new))
        })
    }

    /// Stop charging a workspace the storage daemon has published. A mount
    /// since `expected_mount_epoch` was captured refuses it (`false`).
    pub fn release_published_workspace(
        &self,
        sandbox_id: &str,
        sandbox_generation: i64,
        workspace_mb: i64,
        expected_mount_epoch: i64,
    ) -> Result<bool> {
        if workspace_mb <= 0 {
            return Err(RegistryError::invalid("invalid released workspace size"));
        }
        self.store.write(true, |tx| {
            let owner = tx.require(sandbox_id)?;
            if owner.sandbox_generation != sandbox_generation || owner.phase != Phase::Owned {
                return Err(RegistryError::conflict("workspace release lost incarnation ownership"));
            }
            let (epoch, _) = mount_epoch(tx, sandbox_id, sandbox_generation)?;
            if epoch != expected_mount_epoch {
                return Ok(false);
            }
            let claim: Option<(i64, i64)> = tx
                .connection
                .prepare_cached("SELECT dynamic, workspace_mb FROM registration_disk WHERE sandbox_id=?")?
                .query_row([sandbox_id], |row| Ok((row.get(0)?, row.get(1)?)))
                .optional()?;
            // The remount re-reserves exactly the grant it will mount.
            let released_mb = match claim {
                Some((dynamic, granted)) if dynamic != 0 => granted.max(1),
                _ => workspace_mb,
            };
            tx.connection
                .prepare_cached("INSERT OR REPLACE INTO workspace_capacity VALUES (?,?,?,?)")?
                .execute(params![sandbox_id, sandbox_generation, epoch, released_mb])?;
            tx.bump_activity()?;
            Ok(true)
        })
    }

    /// Re-charge a released workspace before any mount, and always advance
    /// the mount epoch to fence late publishers.
    pub fn reserve_workspace_for_mount(&self, sandbox_id: &str, sandbox_generation: i64) -> Result<()> {
        self.store.write(true, |tx| {
            let owner = tx.require(sandbox_id)?;
            if owner.sandbox_generation != sandbox_generation {
                return Err(RegistryError::conflict("workspace mount lost incarnation ownership"));
            }
            let (epoch, released) = mount_epoch(tx, sandbox_id, sandbox_generation)?;
            let capacity_mb = self.hard_disk_capacity_mb();
            if released != 0 && capacity_mb != 0 && tx.reserved_disk_bytes()? + mib(released) > mib(capacity_mb) {
                return Err(RegistryError::CapacityUnavailable(
                    "workspace remount physical disk capacity exhausted".into(),
                ));
            }
            tx.connection
                .prepare_cached("INSERT OR REPLACE INTO workspace_capacity VALUES (?,?,?,0)")?
                .execute(params![sandbox_id, sandbox_generation, epoch + 1])?;
            tx.bump_activity()
        })
    }

    // Reflink overlaps.

    /// Persist exact source overlap before the caller raises a project quota;
    /// an identical retry is a no-op.
    pub fn reserve_reflink_overlap(
        &self,
        sandbox_id: &str,
        sandbox_generation: i64,
        hibernation_generation: i64,
        allocated_bytes: i64,
        manifest_sha256: &str,
    ) -> Result<()> {
        validate_overlap_identity(sandbox_id, sandbox_generation, hibernation_generation, manifest_sha256)?;
        if allocated_bytes < 0 {
            return Err(RegistryError::invalid("invalid reflink overlap bytes"));
        }
        self.store.write(true, |tx| {
            let record = tx.require(sandbox_id)?;
            if record.sandbox_generation != sandbox_generation || record.phase != Phase::Owned {
                return Err(RegistryError::conflict("reflink overlap lost incarnation ownership"));
            }
            let identity = params![sandbox_id, sandbox_generation, hibernation_generation];
            let existing: Option<(i64, String)> = tx
                .connection
                .prepare_cached(
                    "SELECT allocated_bytes,manifest_sha256 FROM reflink_overlaps WHERE sandbox_id=? AND sandbox_generation=? AND hibernation_generation=?",
                )?
                .query_row(identity, |row| Ok((row.get(0)?, row.get(1)?)))
                .optional()?;
            if let Some((bytes, digest)) = existing {
                if bytes != allocated_bytes || digest != manifest_sha256 {
                    return Err(RegistryError::conflict("reflink overlap source changed"));
                }
                return Ok(());
            }
            let capacity_mb = self.hard_disk_capacity_mb();
            if capacity_mb == 0 || tx.reserved_disk_bytes()? + i128::from(allocated_bytes) > mib(capacity_mb) {
                return Err(RegistryError::CapacityUnavailable("reflink overlap physical disk capacity exhausted".into()));
            }
            tx.connection
                .prepare_cached("INSERT INTO reflink_overlaps VALUES (?,?,?,?,?)")?
                .execute(params![sandbox_id, sandbox_generation, hibernation_generation, allocated_bytes, manifest_sha256])?;
            tx.bump_activity()
        })
    }

    /// Release an overlap after reconciliation; the digest fences stale retries.
    pub fn release_reflink_overlap(
        &self,
        sandbox_id: &str,
        sandbox_generation: i64,
        hibernation_generation: i64,
        manifest_sha256: &str,
    ) -> Result<()> {
        validate_overlap_identity(sandbox_id, sandbox_generation, hibernation_generation, manifest_sha256)?;
        self.store.write(true, |tx| {
            let identity = params![sandbox_id, sandbox_generation, hibernation_generation];
            let digest: Option<String> = tx
                .connection
                .prepare_cached(
                    "SELECT manifest_sha256 FROM reflink_overlaps WHERE sandbox_id=? AND sandbox_generation=? AND hibernation_generation=?",
                )?
                .query_row(identity, |row| row.get(0))
                .optional()?;
            match digest {
                None => Ok(()),
                Some(digest) if digest != manifest_sha256 => {
                    Err(RegistryError::conflict("reflink overlap release lost source ownership"))
                }
                Some(_) => {
                    tx.connection
                        .prepare_cached(
                            "DELETE FROM reflink_overlaps WHERE sandbox_id=? AND sandbox_generation=? AND hibernation_generation=?",
                        )?
                        .execute(identity)?;
                    tx.bump_activity()
                }
            }
        })
    }

    // Growth forecasts and wake fences (none bumps the clock).

    /// Whether a wake intent supersedes this request's park; `record`
    /// persists one. An existing fence never takes the writer turn.
    pub fn relay_wake_fence(&self, sandbox_id: &str, generation: i64, request_id: &str, record: bool) -> Result<bool> {
        if generation < 1 || !is_operation_id(request_id) {
            return Err(RegistryError::invalid("invalid relay lifecycle identity"));
        }
        let require = |tx: &Tx<'_>| {
            if tx.require(sandbox_id)?.sandbox_generation != generation {
                return Err(RegistryError::conflict("relay lifecycle generation changed"));
            }
            Ok(())
        };
        let present = self.store.read(|tx| {
            require(tx)?;
            wake_fenced(tx, sandbox_id, generation, request_id)
        })?;
        if present || !record {
            return Ok(present);
        }
        self.store.write(true, |tx| {
            require(tx)?;
            insert_wake_fence(tx, sandbox_id, generation, request_id)?;
            Ok(true)
        })
    }

    /// Move this incarnation's growth forecast under the same durable
    /// incarnation and wake fences (see `growth_intent_step`).
    pub fn growth_intent(
        &self,
        sandbox_id: &str,
        generation: i64,
        action: GrowthAction,
        job_id: &str,
        launch_sha256: &str,
        request_id: &str,
    ) -> Result<Option<GrowthIntent>> {
        self.store
            .write(true, |tx| growth_intent_step(tx, sandbox_id, generation, action, job_id, launch_sha256, request_id))
    }

    /// `((sandbox ID, generation), action, request ID)` steps in order, as one
    /// commit; each item's result or error stands alone (a SAVEPOINT undoes
    /// just that item). A SQLite failure aborts the whole batch.
    pub fn growth_intent_batch(
        &self,
        operations: &[((String, i64), GrowthAction, String)],
    ) -> Result<Vec<Result<Option<GrowthIntent>>>> {
        self.store.write(true, |tx| {
            let mut results = Vec::with_capacity(operations.len());
            for ((sandbox_id, generation), action, request_id) in operations {
                tx.connection.execute_batch("SAVEPOINT growth")?;
                match growth_intent_step(tx, sandbox_id, *generation, *action, "", "", request_id) {
                    Ok(intent) => results.push(Ok(intent)),
                    Err(error @ RegistryError::Unreadable { .. }) => return Err(error),
                    Err(error) => {
                        tx.connection.execute_batch("ROLLBACK TO growth")?;
                        results.push(Err(error));
                    }
                }
                tx.connection.execute_batch("RELEASE growth")?;
            }
            Ok(results)
        })
    }
}

/// A new record in its first phase, validated as Python's constructor does.
fn candidate(
    spec: SandboxSpec,
    sandbox_generation: i64,
    operation_id: String,
    runtime_compatibility_sha256: String,
    split_memory_backing: bool,
    phase: Phase,
    migration: Option<Migration>,
) -> Result<Registration> {
    let now = now_ns();
    let incarnation = format!("{}.sandbox-{sandbox_generation}", spec.id());
    let (workspace_directory, memory_allocation_id, version) = if split_memory_backing {
        (format!("workspace-{incarnation}"), incarnation, SPLIT_REGISTRATION_VERSION)
    } else {
        (String::new(), String::new(), DIRECT_REGISTRATION_VERSION)
    };
    let (migration_id, migration_sha256) = migration.map_or_else(Default::default, |m| (m.id, m.sha256));
    let record = Registration {
        spec,
        sandbox_generation,
        operation_id,
        runtime_compatibility_sha256,
        phase,
        revision: 1,
        created_ns: now,
        updated_ns: now,
        quota_project_id: None,
        quota_total_mb: None,
        quota_path: String::new(),
        image_id: String::new(),
        rootfs_sha256: String::new(),
        container_id: String::new(),
        bundle: String::new(),
        memory_directory: String::new(),
        workspace_directory,
        memory_allocation_id,
        migration_id,
        migration_sha256,
        version,
    };
    record.validate().map_err(RegistryError::Invalid)?;
    Ok(record)
}

fn set_quota(record: &mut Registration, quota: &Quota) {
    record.quota_project_id = Some(quota.project_id);
    record.quota_total_mb = Some(quota.total_mb);
    record.quota_path = quota.path.clone();
}

fn set_rootfs(record: &mut Registration, image_id: &str, rootfs: &Rootfs) {
    record.image_id = image_id.to_owned();
    record.rootfs_sha256 = rootfs.rootfs_sha256.clone();
    record.container_id = rootfs.container_id.clone();
    record.bundle = rootfs.bundle.clone();
    record.memory_directory = rootfs.memory_directory.clone();
}

fn set_dynamic_claim(tx: &Tx<'_>, sandbox_id: &str, claim: DiskClaim) -> Result<()> {
    tx.connection
        .prepare_cached(
            "UPDATE registration_disk SET reserved_mb=0, workspace_mb=?, memory_mb=?, dynamic=1 WHERE sandbox_id=?",
        )?
        .execute(params![claim.workspace_mb, claim.memory_mb, sandbox_id])?;
    Ok(())
}

/// `_adopt_dynamic_claim`: a split registration's fixed claim becomes the
/// workspace ceiling plus the rest as memory; the total is kept whenever the
/// reservation covers the workspace. A published release stays released.
fn adopt_dynamic_claim(tx: &Tx<'_>, sandbox_id: &str) -> Result<Option<DiskClaim>> {
    let Some(record) = tx.get(sandbox_id)? else {
        return Ok(None);
    };
    let (Some(_), Some(disk_mb)) = (record.memory_reference(), record.spec.disk_mb()) else {
        return Ok(None);
    };
    let reserved: i64 = tx
        .connection
        .prepare_cached("SELECT reserved_mb FROM registration_disk WHERE sandbox_id=?")?
        .query_row([sandbox_id], |row| row.get(0))?;
    let claim = DiskClaim::new(disk_mb, (reserved - disk_mb).max(0))?;
    set_dynamic_claim(tx, sandbox_id, claim)?;
    Ok(Some(claim))
}

/// The incarnation's (mount epoch, released MiB); another generation's row counts as none.
fn mount_epoch(tx: &Tx<'_>, sandbox_id: &str, sandbox_generation: i64) -> Result<(i64, i64)> {
    let row: Option<(i64, i64, i64)> = tx
        .connection
        .prepare_cached("SELECT sandbox_generation,mount_epoch,released_mb FROM workspace_capacity WHERE sandbox_id=?")?
        .query_row([sandbox_id], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .optional()?;
    Ok(match row {
        Some((generation, epoch, released)) if generation == sandbox_generation => (epoch, released),
        _ => (0, 0),
    })
}

fn validate_overlap_identity(
    sandbox_id: &str,
    sandbox_generation: i64,
    hibernation_generation: i64,
    manifest_sha256: &str,
) -> Result<()> {
    if sandbox_id.is_empty() || sandbox_generation <= 0 || hibernation_generation <= 0 || !is_digest(manifest_sha256) {
        return Err(RegistryError::invalid("invalid reflink overlap identity"));
    }
    Ok(())
}

fn growth_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<GrowthIntent> {
    Ok(GrowthIntent {
        sandbox_id: row.get(0)?,
        generation: row.get(1)?,
        job_id: row.get(2)?,
        launch_sha256: row.get(3)?,
        memory_bytes: row.get(4)?,
        phase: row.get(5)?,
        request_id: row.get(6)?,
    })
}

fn wake_fenced(tx: &Tx<'_>, sandbox_id: &str, generation: i64, request_id: &str) -> Result<bool> {
    Ok(tx
        .connection
        .prepare_cached("SELECT 1 FROM relay_wake_fences WHERE sandbox_id=? AND generation=? AND request_id=?")?
        .query_row(params![sandbox_id, generation, request_id], |_| Ok(()))
        .optional()?
        .is_some())
}

fn insert_wake_fence(tx: &Tx<'_>, sandbox_id: &str, generation: i64, request_id: &str) -> Result<()> {
    tx.connection
        .prepare_cached("INSERT OR IGNORE INTO relay_wake_fences VALUES (?,?,?)")?
        .execute(params![sandbox_id, generation, request_id])?;
    Ok(())
}

fn save_growth(tx: &Tx<'_>, intent: &GrowthIntent) -> Result<()> {
    tx.connection.prepare_cached("INSERT OR REPLACE INTO managed_growth VALUES (?,?,?,?,?,?,?)")?.execute(params![
        intent.sandbox_id,
        intent.generation,
        intent.job_id,
        intent.launch_sha256,
        intent.memory_bytes,
        intent.phase,
        intent.request_id
    ])?;
    Ok(())
}

/// `_growth_intent`: the forecast for this generation's sole primary.
///
/// A different launch cannot replace an ambiguous (activated) first launch
/// but may replace a still-queued one, which never reached the supervisor.
/// Imported generations start with an unknown job identity. Every
/// `activate` with a request ID also commits that request's wake fence.
fn growth_intent_step(
    tx: &Tx<'_>,
    sandbox_id: &str,
    generation: i64,
    action: GrowthAction,
    job_id: &str,
    launch_sha256: &str,
    request_id: &str,
) -> Result<Option<GrowthIntent>> {
    let owner = tx.require(sandbox_id)?;
    if owner.sandbox_generation != generation || owner.phase != Phase::Owned {
        return Err(RegistryError::conflict("growth intent lost incarnation ownership"));
    }
    let row = tx
        .connection
        .prepare_cached("SELECT * FROM managed_growth WHERE sandbox_id=?")?
        .query_row([sandbox_id], growth_row)
        .optional()?;
    if row.as_ref().is_some_and(|intent| intent.generation != generation) {
        return Err(RegistryError::conflict("growth intent has stale generation"));
    }
    let new_intent = |phase: &str, job_id: &str, launch_sha256: &str, request_id: &str| -> Result<GrowthIntent> {
        let memory_mb =
            owner.spec.memory_mb().ok_or_else(|| RegistryError::invalid("growth intent requires memory_mb"))?;
        Ok(GrowthIntent {
            sandbox_id: sandbox_id.to_owned(),
            generation,
            job_id: job_id.to_owned(),
            launch_sha256: launch_sha256.to_owned(),
            memory_bytes: memory_mb * MIB,
            phase: phase.to_owned(),
            request_id: request_id.to_owned(),
        })
    };
    let mut intent = row.clone();
    match action {
        GrowthAction::Launch | GrowthAction::Bind => {
            if job_id.is_empty() || !is_digest(launch_sha256) {
                return Err(RegistryError::invalid("invalid managed launch identity"));
            }
            match &mut intent {
                Some(existing) if existing.job_id.is_empty() => {
                    // An imported primary binds only through the supervisor.
                    if action == GrowthAction::Bind {
                        existing.job_id = job_id.to_owned();
                        existing.launch_sha256 = launch_sha256.to_owned();
                        save_growth(tx, existing)?;
                    }
                    return Ok(intent);
                }
                Some(existing)
                    if (existing.job_id.as_str(), existing.launch_sha256.as_str()) == (job_id, launch_sha256) =>
                {
                    return Ok(intent);
                }
                Some(existing) if action == GrowthAction::Bind || existing.phase != "queued" => {
                    return Err(RegistryError::ManagedPrimaryOwned { job_id: existing.job_id.clone() });
                }
                Some(existing) => {
                    // A timed-out admission left it queued; nothing was dispatched.
                    existing.job_id = job_id.to_owned();
                    existing.launch_sha256 = launch_sha256.to_owned();
                }
                None => intent = Some(new_intent("queued", job_id, launch_sha256, "")?),
            }
        }
        GrowthAction::Terminal => match &mut intent {
            Some(existing) if !job_id.is_empty() && (existing.job_id.is_empty() || existing.job_id == job_id) => {
                existing.phase = "terminal".into();
            }
            _ => return Ok(intent),
        },
        GrowthAction::Park => match &mut intent {
            // A queued launch has no primary to capture.
            Some(existing) if existing.phase != "terminal" && existing.phase != "queued" => {
                existing.phase = "parked".into()
            }
            _ => return Ok(intent),
        },
        GrowthAction::Wait => {
            if request_id.is_empty() || wake_fenced(tx, sandbox_id, generation, request_id)? {
                return Err(RegistryError::conflict("growth wait was superseded by wake"));
            }
            match &mut intent {
                None => intent = Some(new_intent("safe", "", "", request_id)?),
                Some(existing) if matches!(existing.phase.as_str(), "active" | "safe" | "parked") => {
                    if existing.phase != "parked" {
                        existing.phase = "safe".into();
                    }
                    existing.request_id = request_id.to_owned();
                }
                Some(_) => {}
            }
        }
        GrowthAction::Activate => match &mut intent {
            None => intent = Some(new_intent("active", "", "", request_id)?),
            Some(existing) if existing.phase == "queued" => {
                // Only this launch's own admission charges it.
                if (existing.job_id.as_str(), existing.launch_sha256.as_str()) == (job_id, launch_sha256) {
                    existing.phase = "active".into();
                    existing.request_id = request_id.to_owned();
                }
            }
            Some(existing)
                if existing.phase == "parked"
                    || (existing.phase == "safe"
                        && (existing.request_id.is_empty() || existing.request_id == request_id)) =>
            {
                existing.phase = "active".into();
                existing.request_id = request_id.to_owned();
            }
            Some(_) => {}
        },
    }
    if action == GrowthAction::Activate && !request_id.is_empty() {
        // Admission and revocation of this safe wait are one commit.
        insert_wake_fence(tx, sandbox_id, generation, request_id)?;
    }
    if intent != row
        && let Some(intent) = &intent
    {
        save_growth(tx, intent)?;
    }
    Ok(intent)
}

#[cfg(test)]
mod tests;
