//! Files, connections and transactions (the private half of
//! `DirectSandboxRegistry`): the file and owner-lock checks, the connection
//! pool, validated transactions, the owner's index and its group commit.
//!
//! Ownership: the owner instance holds an exclusive BSD `flock` on the
//! `.owner` sidecar for its life, so Python and Rust owners exclude each
//! other, and serves registration reads from its index. Any other instance,
//! in any process, reads and writes SQLite directly; the owner detects such
//! foreign commits through `PRAGMA data_version` at its next BEGIN (writes)
//! or within a second (reads).
//!
//! Group commit: owner writers queue for one turn and share one open
//! `BEGIN IMMEDIATE` transaction, each under its own SAVEPOINT. A failed
//! writer undoes only its savepoint and returns at once; the others return
//! after the COMMIT holding their changes, which the last queued writer (or
//! the 64th) issues. The index advances only after that COMMIT.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock, TryLockError};
use std::time::{Duration, Instant};

use rusqlite::types::{Value as SqlValue, ValueRef};
use rusqlite::{Connection, OptionalExtension, params};

use super::error::{RegistryError, Result};
use super::index::{Index, Row};
use super::record::{MIB, Registration};
use super::schema::{self, Metadata, Stamp};
use crate::fsutil::{euid, fsync_dir};

/// File and index revalidation interval (`_FILE_RECHECK_SECONDS`).
pub const RECHECK: Duration = Duration::from_secs(1);
/// Writers one owner transaction serves at most (`_GROUP_COMMIT_MAX`).
pub const GROUP_COMMIT_MAX: usize = 64;
/// Idle pooled connections kept (`_IDLE_CONNECTIONS`).
pub const IDLE_CONNECTIONS: usize = 64;
pub const OWNER_SUFFIX: &str = ".owner";

/// One registration's current claim (`_ROW_CLAIM_MB`). Fixed rows carry
/// reserved_mb less any published workspace (it may go negative); dynamic
/// rows carry a workspace claim, dropped while published, plus memory.
pub const ROW_CLAIM_MB: &str = "d.reserved_mb + d.memory_mb + CASE WHEN COALESCE(w.released_mb, 0) > 0 THEN CASE WHEN d.dynamic = 1 THEN 0 ELSE -w.released_mb END ELSE d.workspace_mb END";
pub const CLAIM_JOIN: &str = "registration_disk AS d LEFT JOIN workspace_capacity AS w ON w.sandbox_id = d.sandbox_id AND w.sandbox_generation = d.sandbox_generation";

/// Each registration's charge in MiB by (sandbox ID, generation).
pub type ClaimsMb = HashMap<(String, i64), i64>;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(crate) struct Pooled {
    pub connection: Connection,
    /// The schema stamp this connection last validated.
    stamp: Option<Stamp>,
}

/// The owner's published index and when it was last proven against the file.
#[derive(Default)]
struct View {
    index: Option<Arc<Index>>,
    checked_at: Option<Instant>,
}

#[derive(Default)]
struct FileIdentity {
    identity: Option<(u64, u64)>,
    checked_at: Option<Instant>,
}

/// One open owner transaction that queued writers share, and its outcome.
struct Group {
    revision: i64,
    outcome: Mutex<Option<std::result::Result<(), RegistryError>>>,
    done: Condvar,
}

impl Group {
    fn finish(&self, outcome: std::result::Result<(), RegistryError>) {
        *lock(&self.outcome) = Some(outcome);
        self.done.notify_all();
    }

    fn wait(&self) -> std::result::Result<(), RegistryError> {
        let mut outcome = lock(&self.outcome);
        loop {
            if let Some(outcome) = outcome.as_ref() {
                return outcome.clone();
            }
            outcome = self.done.wait(outcome).unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }
}

/// State behind the writer turn. Holding the turn also guards the owner
/// connection; the staged rows belong to the open group.
#[derive(Default)]
struct Turn {
    owner_lock: Option<File>,
    owner: Option<Pooled>,
    group: Option<Arc<Group>>,
    members: usize,
    staged: HashMap<String, Option<Arc<Row>>>,
    bumps: i64,
}

/// The open group's staging, for writes made through the owner connection.
struct Staging<'a> {
    index: &'a Index,
    staged: &'a mut HashMap<String, Option<Arc<Row>>>,
    bumps: &'a mut i64,
}

/// One transaction's connection. In an owner transaction, reads of
/// registrations come from the index plus staged rows (the BEGIN proved the
/// index equals this snapshot), and writes are staged for after COMMIT.
pub(crate) struct Tx<'a> {
    pub connection: &'a Connection,
    staging: Option<Staging<'a>>,
}

impl Tx<'_> {
    /// `_get`.
    pub fn get(&self, sandbox_id: &str) -> Result<Option<Arc<Registration>>> {
        if let Some(staging) = &self.staging {
            let row = match staging.staged.get(sandbox_id) {
                Some(staged) => staged.clone(),
                None => staging.index.rows.get(sandbox_id).cloned(),
            };
            return Ok(row.map(|row| row.record.clone()));
        }
        let row = self
            .connection
            .prepare_cached("SELECT sandbox_id, image_id, record_json FROM registrations WHERE sandbox_id = ?")?
            .query_row([sandbox_id], |row| {
                Ok((row.get::<_, SqlValue>(0)?, row.get::<_, SqlValue>(1)?, row.get::<_, SqlValue>(2)?))
            })
            .optional()?;
        row.map(|(id, image, encoded)| {
            schema::decode_row(ValueRef::from(&id), ValueRef::from(&image), ValueRef::from(&encoded)).map(Arc::new)
        })
        .transpose()
    }

    /// `_require`.
    pub fn require(&self, sandbox_id: &str) -> Result<Arc<Registration>> {
        self.get(sandbox_id)?.ok_or_else(|| RegistryError::conflict("direct registration is absent"))
    }

    /// `_write`: the row, its fixed disk claim, then staging. The encoding is
    /// decoded back first, so no row Python cannot read is ever written.
    pub fn write(&mut self, record: Registration, insert: bool) -> Result<Arc<Registration>> {
        let encoded = record.encode();
        match Registration::decode(record.sandbox_id(), &record.image_id, &encoded) {
            Ok(decoded) if decoded == record => {}
            _ => return Err(RegistryError::registry("direct registration encoding is invalid")),
        }
        if insert {
            self.connection.prepare_cached("INSERT INTO registrations VALUES (?, ?, ?)")?.execute(params![
                record.sandbox_id(),
                record.image_id,
                encoded
            ])?;
        } else if self
            .connection
            .prepare_cached("UPDATE registrations SET image_id = ?, record_json = ? WHERE sandbox_id = ?")?
            .execute(params![record.image_id, encoded, record.sandbox_id()])?
            != 1
        {
            return Err(RegistryError::registry("direct registration disappeared"));
        }
        schema::write_disk_claim(self.connection, &record)?;
        let record = Arc::new(record);
        if let Some(staging) = &mut self.staging {
            let row = Row { image_id: record.image_id.clone(), encoded, record: record.clone() };
            staging.staged.insert(record.sandbox_id().to_owned(), Some(Arc::new(row)));
        }
        Ok(record)
    }

    /// `_delete`: the row and its disk claim.
    pub fn delete(&mut self, sandbox_id: &str) -> Result<()> {
        self.connection.prepare_cached("DELETE FROM registration_disk WHERE sandbox_id = ?")?.execute([sandbox_id])?;
        if self.connection.prepare_cached("DELETE FROM registrations WHERE sandbox_id = ?")?.execute([sandbox_id])? != 1
        {
            return Err(RegistryError::registry("direct registration disappeared"));
        }
        if let Some(staging) = &mut self.staging {
            staging.staged.insert(sandbox_id.to_owned(), None);
        }
        Ok(())
    }

    /// `_bump_activity`: the node clock moves by exactly one.
    pub fn bump_activity(&mut self) -> Result<()> {
        if self
            .connection
            .prepare_cached("UPDATE registry_metadata SET activity_revision = activity_revision + 1")?
            .execute([])?
            != 1
        {
            return Err(RegistryError::registry("direct registry metadata is invalid"));
        }
        if let Some(staging) = &mut self.staging {
            *staging.bumps += 1;
        }
        Ok(())
    }

    /// `_retire`: this migration ID may never import again.
    pub fn retire(&self, sandbox_id: &str, migration_id: &str) -> Result<()> {
        self.connection
            .prepare_cached("INSERT OR IGNORE INTO migration_tombstones VALUES (?, ?)")?
            .execute([sandbox_id, migration_id])?;
        Ok(())
    }

    pub fn metadata(&self) -> Result<Metadata> {
        schema::metadata(self.connection)
    }

    /// `_reserved_disk_bytes`: every claim plus reflink overlaps, the
    /// admission authority. Negative fixed rows are summed unclamped.
    pub fn reserved_disk_bytes(&self) -> Result<i128> {
        let reserved_mb: i64 = self
            .connection
            .prepare_cached(&format!("SELECT COALESCE(SUM({ROW_CLAIM_MB}),0) FROM {CLAIM_JOIN}"))?
            .query_row([], |row| row.get(0))?;
        let overlap: i64 = self
            .connection
            .prepare_cached("SELECT COALESCE(SUM(allocated_bytes),0) FROM reflink_overlaps")?
            .query_row([], |row| row.get(0))?;
        Ok(i128::from(reserved_mb) * i128::from(MIB) + i128::from(overlap))
    }
}

pub struct Store {
    path: PathBuf,
    pid: u32,
    pool: Mutex<Vec<Pooled>>,
    validated_stamp: Mutex<Option<Stamp>>,
    file: Mutex<FileIdentity>,
    turn: Mutex<Turn>,
    turn_waiters: AtomicUsize,
    owning: AtomicBool,
    view: RwLock<View>,
    pub(crate) hard_disk_capacity_mb: AtomicI64,
    /// `disk_claims_mb` read at the index it is keyed on.
    pub(crate) claims: Mutex<Option<(Arc<Index>, ClaimsMb)>>,
    /// Group COMMITs issued, for tests.
    #[cfg(test)]
    pub(crate) commits: AtomicUsize,
}

impl Store {
    pub fn new(path: PathBuf, hard_disk_capacity_mb: i64) -> Store {
        Store {
            path,
            pid: std::process::id(),
            pool: Mutex::new(Vec::new()),
            validated_stamp: Mutex::new(None),
            file: Mutex::new(FileIdentity::default()),
            turn: Mutex::new(Turn::default()),
            turn_waiters: AtomicUsize::new(0),
            owning: AtomicBool::new(false),
            view: RwLock::new(View::default()),
            hard_disk_capacity_mb: AtomicI64::new(hard_disk_capacity_mb),
            claims: Mutex::new(None),
            #[cfg(test)]
            commits: AtomicUsize::new(0),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn is_owner(&self) -> bool {
        self.owning.load(Ordering::SeqCst)
    }

    fn check_fork(&self) -> Result<()> {
        if std::process::id() != self.pid {
            return Err(RegistryError::registry("reopen direct registry after fork"));
        }
        Ok(())
    }

    /// `_prepare_file`: a private, owned directory and a private regular file.
    fn prepare_file(&self) -> Result<()> {
        let parent =
            self.path.parent().ok_or_else(|| RegistryError::invalid("direct registry path must be absolute"))?;
        std::fs::DirBuilder::new().recursive(true).mode(0o700).create(parent)?;
        let meta = std::fs::symlink_metadata(parent)?;
        if !meta.is_dir() || meta.uid() != euid() || meta.mode() & 0o022 != 0 {
            return Err(RegistryError::registry("direct registry directory must be private and owned"));
        }
        let created = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(&self.path);
        match created {
            Ok(file) => {
                drop(file);
                fsync_dir(parent)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        let meta = std::fs::symlink_metadata(&self.path)?;
        if !meta.is_file() || meta.uid() != euid() || meta.mode() & 0o077 != 0 {
            return Err(RegistryError::registry("direct registry must be private, regular, and owned"));
        }
        Ok(())
    }

    /// `_check_file`: the file's identity, owner and mode on every use; the
    /// directory walk and create probe at most once a second.
    fn check_file(&self) -> Result<()> {
        let now = Instant::now();
        {
            let state = lock(&self.file);
            if let (Some(identity), Some(checked_at)) = (state.identity, state.checked_at)
                && now.duration_since(checked_at) < RECHECK
                && std::fs::symlink_metadata(&self.path).is_ok_and(|meta| {
                    meta.is_file()
                        && meta.uid() == euid()
                        && meta.mode() & 0o077 == 0
                        && (meta.dev(), meta.ino()) == identity
                })
            {
                return Ok(());
            }
        }
        self.prepare_file()?;
        let meta = std::fs::symlink_metadata(&self.path)?;
        let identity = (meta.dev(), meta.ino());
        let mut state = lock(&self.file);
        if state.identity.is_some_and(|known| known != identity) {
            return Err(RegistryError::registry("direct registry file was replaced; reopen it"));
        }
        state.identity = Some(identity);
        state.checked_at = Some(now);
        Ok(())
    }

    /// `_connect`: the first connection of an instance creates, migrates and
    /// validates the schema; later ones start from the validated stamp.
    fn connect(&self) -> Result<Pooled> {
        let connection = schema::open(&self.path)?;
        let validated = lock(&self.validated_stamp).clone();
        let stamp = match validated {
            Some(stamp) => stamp,
            None => {
                schema::ensure_schema(&connection)?;
                let stamp = schema::schema_stamp(&connection)?;
                *lock(&self.validated_stamp) = Some(stamp.clone());
                stamp
            }
        };
        Ok(Pooled { connection, stamp: Some(stamp) })
    }

    /// `_checked_stamp`: validate the open transaction's schema if its stamp
    /// moved, then its metadata; returns (activity revision, data version).
    fn checked_stamp(&self, entry: &mut Pooled) -> Result<(Metadata, i64)> {
        type Stamped = (Stamp, (SqlValue, SqlValue, SqlValue), i64);
        let query = |connection: &Connection| -> rusqlite::Result<Option<Stamped>> {
            connection
                .prepare_cached(schema::STAMPED_METADATA)?
                .query_row([], |row| {
                    Ok((
                        (row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?),
                        (row.get::<_, SqlValue>(4)?, row.get::<_, SqlValue>(5)?, row.get::<_, SqlValue>(6)?),
                        row.get(7)?,
                    ))
                })
                .optional()
        };
        // A failing statement means changed DDL: full validation reports it.
        let mut row = query(&entry.connection).ok().flatten();
        if row.as_ref().is_none_or(|(stamp, _, _)| Some(stamp) != entry.stamp.as_ref()) {
            schema::validate_schema(&entry.connection, None)?;
            let stamp = schema::schema_stamp(&entry.connection)?;
            entry.stamp = Some(stamp.clone());
            *lock(&self.validated_stamp) = Some(stamp);
            row = query(&entry.connection)?;
        }
        let (_, (revision, compatibility, drain), data_version) =
            row.ok_or_else(|| RegistryError::registry("direct registry metadata is invalid"))?;
        let metadata = schema::checked_metadata(Some((
            ValueRef::from(&revision),
            ValueRef::from(&compatibility),
            ValueRef::from(&drain),
        )))?;
        Ok((metadata, data_version))
    }

    /// `_validated`: BEGIN (IMMEDIATE to write), check the stamp, run the
    /// body, COMMIT; ROLLBACK on failure. `durable == false` commits with
    /// `synchronous = NORMAL` and restores FULL afterwards.
    fn validated<T>(
        &self,
        entry: &mut Pooled,
        write: bool,
        durable: bool,
        body: impl FnOnce(&mut Tx<'_>, &Metadata, i64) -> Result<T>,
    ) -> Result<T> {
        if !durable {
            entry.connection.execute_batch("PRAGMA synchronous = NORMAL")?;
        }
        let result = (|| {
            entry.connection.execute_batch(if write { "BEGIN IMMEDIATE" } else { "BEGIN" })?;
            let outcome = (|| {
                let (metadata, data_version) = self.checked_stamp(entry)?;
                let value = body(&mut Tx { connection: &entry.connection, staging: None }, &metadata, data_version)?;
                entry.connection.execute_batch("COMMIT")?;
                Ok(value)
            })();
            if outcome.is_err() && !entry.connection.is_autocommit() {
                // Roll back before the next in-process writer may BEGIN.
                entry.connection.execute_batch("ROLLBACK")?;
            }
            outcome
        })();
        if !durable {
            // A failure here discards the connection, never pools it.
            entry.connection.execute_batch("PRAGMA synchronous = FULL")?;
        }
        result
    }

    /// `_borrow`: a pooled connection to the checked file; one that failed
    /// is closed, never pooled.
    fn borrow<T>(&self, use_: impl FnOnce(&mut Pooled) -> Result<T>) -> Result<T> {
        self.check_fork()?;
        self.check_file()?;
        let pooled = lock(&self.pool).pop();
        let mut entry = match pooled {
            Some(entry) => entry,
            None => self.connect()?,
        };
        let result = use_(&mut entry)?;
        let mut pool = lock(&self.pool);
        if pool.len() < IDLE_CONNECTIONS {
            pool.push(entry);
        }
        Ok(result)
    }

    /// A read transaction on a pooled connection, even on the owner.
    pub(crate) fn read<T>(&self, body: impl FnOnce(&mut Tx<'_>) -> Result<T>) -> Result<T> {
        self.borrow(|entry| self.validated(entry, false, true, |tx, _, _| body(tx)))
    }

    /// A write transaction: the owner's group commit, else a pooled
    /// connection holding the in-process writer turn.
    pub(crate) fn write<T>(&self, durable: bool, body: impl FnOnce(&mut Tx<'_>) -> Result<T>) -> Result<T> {
        if self.is_owner() {
            return self.owner_write(body);
        }
        self.borrow(|entry| {
            let _turn = lock(&self.turn);
            self.validated(entry, true, durable, |tx, _, _| body(tx))
        })
    }

    fn owner_write<T>(&self, body: impl FnOnce(&mut Tx<'_>) -> Result<T>) -> Result<T> {
        self.check_fork()?;
        self.check_file()?;
        self.turn_waiters.fetch_add(1, Ordering::SeqCst);
        let mut turn = lock(&self.turn);
        self.turn_waiters.fetch_sub(1, Ordering::SeqCst);
        let group = match turn.group.clone() {
            Some(group) => group,
            None => self.begin_group(&mut turn)?,
        };
        let mut failure = None;
        let mut value = None;
        let (result, lost) = self.member(&mut turn, body);
        if let Some(cause) = lost
            && turn.group.as_ref().is_some_and(|open| Arc::ptr_eq(open, &group))
        {
            self.abandon_group(&mut turn, cause);
        }
        match result {
            Ok(result) => {
                value = Some(result);
                turn.members += 1;
            }
            Err(error) => failure = Some(error),
        }
        if turn.group.is_some()
            && (self.turn_waiters.load(Ordering::SeqCst) == 0 || turn.members >= GROUP_COMMIT_MAX)
            && let Err(error) = self.commit_group(&mut turn)
        {
            failure.get_or_insert(error);
        }
        drop(turn);
        if let Some(error) = failure {
            return Err(error);
        }
        if group.wait().is_err() {
            return Err(RegistryError::registry("direct registry commit failed"));
        }
        Ok(value.expect("a member without failure has a value"))
    }

    /// One group member under its SAVEPOINT: the writer's result, and the
    /// savepoint statement's error if one failed and so lost the group.
    fn member<T>(
        &self,
        turn: &mut Turn,
        body: impl FnOnce(&mut Tx<'_>) -> Result<T>,
    ) -> (Result<T>, Option<RegistryError>) {
        let lost = |error: RegistryError| (Err(error.clone()), Some(error));
        let released = || RegistryError::registry("direct registry ownership was released");
        let Turn { owner, staged, bumps, .. } = turn;
        let (Some(index), Some(entry)) = (self.current_index(), owner.as_ref()) else {
            return lost(released());
        };
        let connection = &entry.connection;
        if let Err(error) = connection.execute_batch("SAVEPOINT member") {
            return lost(error.into());
        }
        let saved = (staged.clone(), *bumps);
        let mut tx =
            Tx { connection, staging: Some(Staging { index: &index, staged: &mut *staged, bumps: &mut *bumps }) };
        let result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| body(&mut tx))) {
            Ok(result) => result,
            Err(panic) => {
                // Nobody may commit a half-run member, or wait on a group
                // whose committer is gone: abandon it, then keep unwinding.
                let _ = connection.execute_batch("ROLLBACK TO member; RELEASE member");
                (*staged, *bumps) = saved;
                self.abandon_group(turn, RegistryError::registry("direct registry writer panicked"));
                std::panic::resume_unwind(panic);
            }
        };
        match result {
            Ok(value) => match connection.execute_batch("RELEASE member") {
                Ok(()) => (Ok(value), None),
                Err(error) => lost(error.into()),
            },
            Err(failure) => {
                (*staged, *bumps) = saved;
                let undone = connection.execute_batch("ROLLBACK TO member; RELEASE member");
                (Err(failure), undone.err().map(RegistryError::from))
            }
        }
    }

    /// `_begin_group`: BEGIN IMMEDIATE on the owner connection, proving the
    /// index equals this snapshot or rebuilding it from the snapshot first.
    fn begin_group(&self, turn: &mut Turn) -> Result<Arc<Group>> {
        if turn.owner_lock.is_none() {
            return Err(RegistryError::registry("direct registry ownership was released"));
        }
        if turn.owner.is_none() {
            turn.owner = Some(self.connect()?);
        }
        let entry = turn.owner.as_mut().expect("connected above");
        let begun = (|| {
            entry.connection.execute_batch("BEGIN IMMEDIATE")?;
            let (metadata, data_version) = self.checked_stamp(entry)?;
            self.prove_index(&entry.connection, metadata.activity_revision, data_version)?;
            Ok(metadata.activity_revision)
        })();
        let revision = match begun {
            Ok(revision) => revision,
            Err(error) => {
                self.drop_owner_connection(turn);
                return Err(error);
            }
        };
        self.write_view().checked_at = Some(Instant::now());
        turn.staged.clear();
        turn.bumps = 0;
        turn.members = 0;
        let group = Arc::new(Group { revision, outcome: Mutex::new(None), done: Condvar::new() });
        turn.group = Some(group.clone());
        Ok(group)
    }

    /// `_commit_group`: the staged rows reach the index only after COMMIT.
    /// An uncertain COMMIT drops the index and the owner connection.
    fn commit_group(&self, turn: &mut Turn) -> Result<()> {
        let group = turn.group.take().expect("an open group");
        let staged = std::mem::take(&mut turn.staged);
        let bumps = std::mem::take(&mut turn.bumps);
        let committed = match &turn.owner {
            Some(entry) => entry.connection.execute_batch("COMMIT").map_err(RegistryError::from),
            None => Err(RegistryError::registry("direct registry ownership was released")),
        };
        if let Err(error) = committed {
            group.finish(Err(error.clone()));
            self.drop_owner_connection(turn);
            return Err(error);
        }
        #[cfg(test)]
        self.commits.fetch_add(1, Ordering::SeqCst);
        if !staged.is_empty() || bumps > 0 {
            let mut view = self.write_view();
            if let Some(index) = view.index.take() {
                view.index = Some(Arc::new(index.applied(staged, group.revision + bumps)));
            }
        }
        group.finish(Ok(()));
        Ok(())
    }

    fn abandon_group(&self, turn: &mut Turn, error: RegistryError) {
        if let Some(group) = turn.group.take() {
            turn.staged.clear();
            turn.bumps = 0;
            self.drop_owner_connection(turn);
            group.finish(Err(error));
        }
    }

    /// Closing the connection rolls back any open transaction.
    fn drop_owner_connection(&self, turn: &mut Turn) {
        turn.owner = None;
        self.write_view().index = None;
    }

    #[cfg(test)]
    pub(crate) fn turn_waiters(&self) -> usize {
        self.turn_waiters.load(Ordering::SeqCst)
    }

    /// Make the next owner read revalidate its index against the file.
    #[cfg(test)]
    pub(crate) fn expire_index_check(&self) {
        self.write_view().checked_at = None;
    }

    fn write_view(&self) -> std::sync::RwLockWriteGuard<'_, View> {
        self.view.write().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn current_index(&self) -> Option<Arc<Index>> {
        self.view.read().unwrap_or_else(|poisoned| poisoned.into_inner()).index.clone()
    }

    /// Rebuild the index from this snapshot unless (revision, data version) prove it equal.
    fn prove_index(&self, connection: &Connection, revision: i64, data_version: i64) -> Result<()> {
        let previous = self.current_index();
        if previous.as_ref().is_some_and(|index| (index.revision, index.data_version) == (revision, data_version)) {
            return Ok(());
        }
        let index = read_index(connection, revision, data_version, previous.as_deref())?;
        self.write_view().index = Some(Arc::new(index));
        Ok(())
    }

    /// `_owned_index`: the owner's index, revalidated at most once a second;
    /// `None` on a non-owner. While a writer holds the turn, reads get the
    /// current index rather than wait (the writer validated it at BEGIN).
    pub(crate) fn owned_index(&self) -> Result<Option<Arc<Index>>> {
        if !self.is_owner() {
            return Ok(None);
        }
        self.check_fork()?;
        let (index, checked_at) = {
            let view = self.view.read().unwrap_or_else(|poisoned| poisoned.into_inner());
            (view.index.clone(), view.checked_at)
        };
        if index.is_some() && checked_at.is_some_and(|at| at.elapsed() < RECHECK) {
            return Ok(index);
        }
        self.check_file()?;
        let mut turn = if index.is_none() {
            lock(&self.turn)
        } else {
            match self.turn.try_lock() {
                Ok(turn) => turn,
                Err(TryLockError::WouldBlock) => return Ok(index),
                Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            }
        };
        if turn.owner_lock.is_none() {
            return Ok(None);
        }
        if turn.group.is_none() {
            self.refresh_owned_index(&mut turn)?;
        }
        Ok(self.current_index())
    }

    /// `_refresh_owned_index`: a read transaction on the owner connection.
    fn refresh_owned_index(&self, turn: &mut Turn) -> Result<()> {
        if turn.owner_lock.is_none() {
            return Err(RegistryError::registry("direct registry ownership was released"));
        }
        if turn.owner.is_none() {
            turn.owner = Some(self.connect()?);
        }
        let entry = turn.owner.as_mut().expect("connected above");
        let result = self.validated(entry, false, true, |tx, metadata, data_version| {
            self.prove_index(tx.connection, metadata.activity_revision, data_version)?;
            self.write_view().checked_at = Some(Instant::now());
            Ok(())
        });
        if result.is_err() && !entry.connection.is_autocommit() {
            self.drop_owner_connection(turn);
        }
        result
    }

    /// `_own`: take the exclusive owner lock, then build the index.
    pub(crate) fn own(&self) -> Result<()> {
        let result = (|| {
            self.prepare_file()?;
            let mut name = self.path.file_name().unwrap_or_default().to_os_string();
            name.push(OWNER_SUFFIX);
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .mode(0o600)
                .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
                .open(self.path.with_file_name(name))?;
            let meta = file.metadata()?;
            let private = meta.is_file() && meta.uid() == euid() && meta.mode() & 0o077 == 0;
            lock(&self.turn).owner_lock = Some(file);
            self.owning.store(true, Ordering::SeqCst);
            if !private {
                return Err(RegistryError::registry("direct registry owner lock must be private and owned"));
            }
            let turn = lock(&self.turn);
            let fd = turn.owner_lock.as_ref().expect("stored above").as_raw_fd();
            // BSD flock, per open file description: a Python owner, or a second
            // instance in this process, conflicts; the kernel drops it with us.
            // SAFETY: the descriptor is valid while the turn holds the file.
            if unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) } != 0 {
                let error = std::io::Error::last_os_error();
                return Err(if error.raw_os_error() == Some(libc::EWOULDBLOCK) {
                    RegistryError::registry("direct registry has another live owner")
                } else {
                    error.into()
                });
            }
            drop(turn);
            self.owned_index().map(drop)
        })();
        if result.is_err() {
            self.close();
        }
        result
    }

    /// `close`: release ownership and idle connections; stragglers then use SQLite.
    pub(crate) fn close(&self) {
        let owner = {
            let mut turn = lock(&self.turn);
            if turn.group.is_some() {
                self.abandon_group(&mut turn, RegistryError::registry("direct registry was closed"));
            }
            self.owning.store(false, Ordering::SeqCst);
            self.write_view().index = None;
            *lock(&self.claims) = None;
            turn.owner_lock = None;
            turn.owner.take()
        };
        drop(owner);
        lock(&self.pool).clear();
    }

    /// The registrations: the owner's index, or every row read and validated.
    pub(crate) fn view_index(&self) -> Result<Arc<Index>> {
        if let Some(index) = self.owned_index()? {
            return Ok(index);
        }
        self.read(|tx| {
            let revision = tx.metadata()?.activity_revision;
            read_index(tx.connection, revision, 0, None)
        })
        .map(Arc::new)
    }
}

/// `_read_index`: every row, decoding only rows whose stored text changed.
fn read_index(connection: &Connection, revision: i64, data_version: i64, previous: Option<&Index>) -> Result<Index> {
    let mut statement = connection.prepare_cached("SELECT sandbox_id, image_id, record_json FROM registrations")?;
    let mut cursor = statement.query([])?;
    let mut rows = HashMap::new();
    while let Some(row) = cursor.next()? {
        let (sandbox_id, image_id, encoded) = (row.get_ref(0)?, row.get_ref(1)?, row.get_ref(2)?);
        let cached = match (sandbox_id, image_id, encoded) {
            (ValueRef::Text(id), ValueRef::Text(image), ValueRef::Text(text)) => previous
                .and_then(|index| index.rows.get(std::str::from_utf8(id).ok()?))
                .filter(|row| row.image_id.as_bytes() == image && row.encoded.as_bytes() == text)
                .cloned(),
            _ => None,
        };
        let row = match cached {
            Some(row) => row,
            None => {
                let record = schema::decode_row(sandbox_id, image_id, encoded)?;
                Arc::new(Row { image_id: record.image_id.clone(), encoded: record.encode(), record: Arc::new(record) })
            }
        };
        rows.insert(row.record.sandbox_id().to_owned(), row);
    }
    let index = Index::new(revision, data_version, rows);
    index.snapshot()?;
    Ok(index)
}
