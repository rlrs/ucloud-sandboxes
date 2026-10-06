//! The Python registry's scenarios (tests/test_direct_registry*.py,
//! test_registry_index.py, test_registry_hot_paths.py, test_create_pipeline.py)
//! replayed against this port.

use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use rusqlite::Connection;
use serde_json::json;

use super::*;

pub(crate) struct TempDir(pub PathBuf);

impl TempDir {
    pub fn new(name: &str) -> TempDir {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "noded-registry-{name}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
        TempDir(path)
    }

    fn registry(&self) -> PathBuf {
        self.0.join("registry.sqlite")
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

const COMPAT: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn spec(sandbox_id: &str) -> SandboxSpec {
    SandboxSpec::from_dict(&json!({
        "id": sandbox_id,
        "image": format!("registry/image@sha256:{}", "a".repeat(64)),
        "memory_mb": 1024,
        "disk_mb": 2048,
    }))
    .unwrap()
}

fn parkable_spec(sandbox_id: &str) -> SandboxSpec {
    SandboxSpec::from_dict(&json!({
        "id": sandbox_id,
        "image": format!("registry/image@sha256:{}", "a".repeat(64)),
        "memory_mb": 1024,
        "disk_mb": 2048,
        "parkable": true,
    }))
    .unwrap()
}

fn request(spec: SandboxSpec, generation: i64) -> PlanRequest {
    PlanRequest {
        spec,
        sandbox_generation: generation,
        operation_id: format!("create:{generation}"),
        runtime_compatibility_sha256: COMPAT.into(),
        split_memory_backing: false,
        initial_claim: None,
    }
}

fn plan(registry: &Registry, sandbox_id: &str, generation: i64) -> Result<Arc<Registration>> {
    registry.plan(request(spec(sandbox_id), generation))
}

fn quota(root: &Path, sandbox_id: &str, generation: i64) -> Quota {
    Quota {
        project_id: 200_000 + generation,
        total_mb: 4096,
        path: root.join("quota").join(sandbox_id).display().to_string(),
    }
}

fn rootfs(root: &Path, sandbox_id: &str, generation: i64) -> Rootfs {
    Rootfs {
        rootfs_sha256: "d".repeat(64),
        container_id: format!("{generation:064x}"),
        bundle: root.join("bundles").join(sandbox_id).display().to_string(),
        memory_directory: format!("{sandbox_id}.{generation}"),
    }
}

fn image() -> String {
    format!("sha256:{}", "e".repeat(64))
}

/// plan, then quota, rootfs and owned as separate commits.
fn owned(registry: &Registry, root: &Path, sandbox_id: &str, generation: i64) -> Arc<Registration> {
    let planned = plan(registry, sandbox_id, generation).unwrap();
    let ready = registry.commit_quota(sandbox_id, planned.revision, &quota(root, sandbox_id, generation)).unwrap();
    let rootfs = registry
        .commit_rootfs(sandbox_id, ready.revision, &image(), &rootfs(root, sandbox_id, generation), None)
        .unwrap();
    registry.commit_owned(sandbox_id, rootfs.revision).unwrap()
}

fn delete(registry: &Registry, record: &Registration) {
    let deleting = registry.begin_delete(record.sandbox_id(), record.revision, None).unwrap();
    registry.commit_deleted(record.sandbox_id(), record.sandbox_generation, deleting.revision).unwrap();
}

fn message<T>(result: Result<T>) -> String {
    match result {
        Ok(_) => panic!("expected an error"),
        Err(error) => error.message().to_owned(),
    }
}

fn raw(path: &Path) -> Connection {
    let connection = Connection::open(path).unwrap();
    connection.busy_timeout(Duration::from_secs(30)).unwrap();
    connection
}

#[test]
fn create_writes_three_commits_and_the_owner_serves_them() {
    let dir = TempDir::new("create");
    let owner = Registry::owner(dir.registry(), 0).unwrap();
    assert_eq!(owner.activity_revision().unwrap(), 0);
    let planned = plan(&owner, "sandbox", 1).unwrap();
    assert_eq!((planned.phase, planned.revision, planned.version), (Phase::Planned, 1, 3));
    let ready = owner
        .commit_rootfs("sandbox", 1, &image(), &rootfs(&dir.0, "sandbox", 1), Some(&quota(&dir.0, "sandbox", 1)))
        .unwrap();
    let owned = owner.commit_owned("sandbox", ready.revision).unwrap();
    assert_eq!((owned.phase, owned.revision, owned.quota_total_mb), (Phase::Owned, 3, Some(4096)));
    assert_eq!(owner.activity_revision().unwrap(), 3);
    assert_eq!(owner.get("sandbox").unwrap().unwrap(), owned);
    assert!(owner.references_image(&image()).unwrap());
    assert!(!owner.references_image("").unwrap());
    assert_eq!(owner.disk_claims_mb().unwrap(), HashMap::from([(("sandbox".to_string(), 1), 4096)]));
    // Another instance reads the identical journal.
    let reader = Registry::new(dir.registry(), 0).unwrap();
    assert_eq!(reader.list().unwrap(), owner.list().unwrap());
    assert_eq!(reader.activity_revision().unwrap(), 3);
    assert_eq!(reader.disk_claims_mb().unwrap(), owner.disk_claims_mb().unwrap());
    let mode = std::fs::metadata(dir.registry()).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o600);
}

#[test]
fn exact_plan_replay_is_idempotent_but_mismatch_conflicts() {
    let dir = TempDir::new("replay");
    let registry = Registry::new(dir.registry(), 0).unwrap();
    let first = plan(&registry, "sandbox", 1).unwrap();
    assert_eq!(plan(&registry, "sandbox", 1).unwrap(), first);
    let ready = registry.commit_quota("sandbox", 1, &quota(&dir.0, "sandbox", 1)).unwrap();
    // A replay returns the record in any phase, with no write.
    assert_eq!(plan(&registry, "sandbox", 1).unwrap(), ready);
    assert_eq!(registry.activity_revision().unwrap(), 2);
    let mut other = request(spec("sandbox"), 1);
    other.operation_id = "create:other".into();
    let error = registry.plan(other).unwrap_err();
    assert!(matches!(error, RegistryError::RegistrationOwned(_)) && error.is_conflict());
    assert_eq!(error.message(), "sandbox already has another direct registration");
    assert_eq!(message(plan(&registry, "sandbox", 2)), "sandbox already has another direct registration");
    let mut changed = request(spec("sandbox"), 1);
    changed.spec =
        SandboxSpec::from_dict(&json!({"id":"sandbox","image":"other","memory_mb":1024,"disk_mb":2048})).unwrap();
    assert!(matches!(registry.plan(changed), Err(RegistryError::RegistrationOwned(_))));
}

#[test]
fn transitions_are_compare_and_swap() {
    let dir = TempDir::new("cas");
    let registry = Registry::new(dir.registry(), 0).unwrap();
    plan(&registry, "sandbox", 1).unwrap();
    assert_eq!(message(registry.commit_owned("sandbox", 1)), TRANSITION_FENCE);
    assert_eq!(message(registry.commit_quota("sandbox", 2, &quota(&dir.0, "sandbox", 1))), TRANSITION_FENCE);
    assert_eq!(
        message(registry.commit_quota("absent", 1, &quota(&dir.0, "absent", 1))),
        "direct registration is absent"
    );
    let bad = Quota { path: "relative".into(), ..quota(&dir.0, "sandbox", 1) };
    let error = registry.commit_quota("sandbox", 1, &bad).unwrap_err();
    assert_eq!(error, RegistryError::Invalid("direct registration quota path must be absolute".into()));
    // A failed transition changed nothing.
    assert_eq!(registry.activity_revision().unwrap(), 1);
    let ready = registry.commit_quota("sandbox", 1, &quota(&dir.0, "sandbox", 1)).unwrap();
    assert_eq!(message(registry.begin_delete("sandbox", ready.revision, Some(2))), TRANSITION_FENCE);
    let deleting = registry.begin_delete("sandbox", ready.revision, Some(1)).unwrap();
    assert_eq!(
        message(registry.commit_deleted("sandbox", 1, ready.revision)),
        "direct deletion completion lost its ownership fence"
    );
    assert_eq!(
        message(registry.commit_deleted("sandbox", 0, deleting.revision)),
        "sandbox generation must be positive"
    );
    registry.commit_deleted("sandbox", 1, deleting.revision).unwrap();
    // The clock survives deleting the last record.
    assert_eq!(registry.activity_revision().unwrap(), 4);
    assert!(registry.list().unwrap().is_empty());
}

#[test]
fn concurrent_cas_allows_one_transition() {
    let dir = TempDir::new("cas-race");
    let owner = Registry::owner(dir.registry(), 0).unwrap();
    let planned = plan(&owner, "sandbox", 1).unwrap();
    let results: Vec<_> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..8)
            .map(|_| scope.spawn(|| owner.commit_quota("sandbox", planned.revision, &quota(&dir.0, "sandbox", 1))))
            .collect();
        handles.into_iter().map(|handle| handle.join().unwrap()).collect()
    });
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert!(results.iter().filter_map(|result| result.as_ref().err()).all(|error| error.message() == TRANSITION_FENCE));
    assert_eq!(owner.activity_revision().unwrap(), 2);
}

#[test]
fn delete_tombstone_fences_delayed_create() {
    let dir = TempDir::new("tombstone");
    let registry = Registry::new(dir.registry(), 0).unwrap();
    let record = owned(&registry, &dir.0, "sandbox", 7);
    delete(&registry, &record);
    for generation in [6, 7] {
        let error = plan(&registry, "sandbox", generation).unwrap_err();
        assert!(matches!(error, RegistryError::Conflict(_)));
        assert_eq!(error.message(), "direct registration is fenced by a tombstone");
    }
    // Imports ignore generation tombstones; their migration ID's tombstone fences them.
    let migration = Migration { id: "move:1".into(), sha256: "f".repeat(64) };
    let import =
        || registry.plan_import(spec("sandbox"), 7, "import:7".into(), COMPAT.into(), migration.clone(), false);
    let imported = import().unwrap();
    assert_eq!((imported.phase, imported.migration_id.as_str()), (Phase::ImportPlanned, "move:1"));
    registry.abort_import_planned("sandbox", imported.revision, &migration, true).unwrap();
    assert_eq!(message(import()), "migration import is fenced by a tombstone");
    let fresh = Migration { id: "move:2".into(), ..migration.clone() };
    let again =
        registry.plan_import(spec("sandbox"), 7, "import:7".into(), COMPAT.into(), fresh.clone(), false).unwrap();
    registry.abort_import_planned("sandbox", again.revision, &fresh, false).unwrap();
    // Tombstones persist across instances.
    let reopened = Registry::owner(dir.registry(), 0).unwrap();
    assert_eq!(message(plan(&reopened, "sandbox", 7)), "direct registration is fenced by a tombstone");
    assert_eq!(
        message(reopened.plan_import(spec("sandbox"), 7, "import:7".into(), COMPAT.into(), migration, false)),
        "migration import is fenced by a tombstone"
    );
    assert_eq!(plan(&reopened, "sandbox", 8).unwrap().sandbox_generation, 8);
}

#[test]
fn migration_phases_are_generation_and_digest_fenced() {
    let dir = TempDir::new("migration");
    let registry = Registry::new(dir.registry(), 0).unwrap();
    let migration = Migration { id: "move:1".into(), sha256: "1".repeat(64) };
    let stale = Migration { sha256: "2".repeat(64), ..migration.clone() };
    let planned =
        registry.plan_import(spec("sandbox"), 3, "import:3".into(), COMPAT.into(), migration.clone(), true).unwrap();
    assert_eq!((planned.version, planned.memory_allocation_id.as_str()), (4, "sandbox.sandbox-3"));
    let importing = registry.commit_import_quota("sandbox", planned.revision, &quota(&dir.0, "sandbox", 3)).unwrap();
    let rootfs_ready =
        registry.commit_import_rootfs("sandbox", importing.revision, &image(), &rootfs(&dir.0, "sandbox", 3)).unwrap();
    assert_eq!(
        message(registry.commit_import_ready("sandbox", rootfs_ready.revision, &stale)),
        "import readiness lost its ownership fence"
    );
    let ready = registry.commit_import_ready("sandbox", rootfs_ready.revision, &migration).unwrap();
    assert_eq!(
        message(registry.activate_import("sandbox", ready.revision, &stale)),
        "import activation lost its ownership fence"
    );
    let active = registry.activate_import("sandbox", ready.revision, &migration).unwrap();
    assert_eq!((active.phase, active.migration_id.as_str()), (Phase::Owned, "move:1"));
    let outgoing = Migration { id: "move:2".into(), sha256: "3".repeat(64) };
    let moving = registry.begin_move_out("sandbox", active.revision, &outgoing).unwrap();
    assert_eq!((moving.phase, moving.migration_id.as_str()), (Phase::MovingOut, "move:2"));
    assert_eq!(
        message(registry.abort_move_out("sandbox", moving.revision, &migration)),
        "move abort lost its ownership fence"
    );
    let back = registry.abort_move_out("sandbox", moving.revision, &outgoing).unwrap();
    assert_eq!((back.phase, back.migration_id.as_str(), back.migration_sha256.as_str()), (Phase::Owned, "", ""));
    // begin_move_out retired the previous (incoming) migration ID.
    let tombstones: i64 = raw(&dir.registry())
        .query_row("SELECT COUNT(*) FROM migration_tombstones WHERE migration_id = 'move:1'", [], |row| row.get(0))
        .unwrap();
    assert_eq!(tombstones, 1);
    let moving = registry.begin_move_out("sandbox", back.revision, &outgoing).unwrap();
    let deleting = registry.begin_delete_moved("sandbox", moving.revision, &outgoing).unwrap();
    registry.commit_deleted("sandbox", 3, deleting.revision).unwrap();
    assert_eq!(
        message(registry.plan_import(spec("sandbox"), 4, "import:4".into(), COMPAT.into(), outgoing, true)),
        "migration import is fenced by a tombstone"
    );
}

#[test]
fn capacity_admission_and_dynamic_claims() {
    let dir = TempDir::new("capacity");
    let registry = Registry::new(dir.registry(), 10_000).unwrap();
    // A parkable fixed claim is the hibernation reservation: 5184 MiB.
    registry.plan(request(parkable_spec("a"), 1)).unwrap();
    let error = registry.plan(request(parkable_spec("b"), 1)).unwrap_err();
    assert!(matches!(error, RegistryError::CapacityUnavailable(_)) && error.is_conflict());
    assert_eq!(error.message(), "combined workspace and memory backing capacity exhausted");
    assert!(registry.get("b").unwrap().is_none());
    // A split plan charges its initial claim, not the maximum.
    let split = PlanRequest {
        split_memory_backing: true,
        initial_claim: Some(DiskClaim::new(1024, 64).unwrap()),
        ..request(parkable_spec("b"), 1)
    };
    let planned = registry.plan(split).unwrap();
    assert_eq!(planned.workspace_directory, "workspace-b.sandbox-1");
    assert_eq!(registry.disk_claim("b", 1).unwrap(), Some(DiskClaim { workspace_mb: 1024, memory_mb: 64 }));
    assert_eq!(registry.disk_claim("a", 1).unwrap(), None);
    let without_split = PlanRequest { initial_claim: Some(DiskClaim::new(1, 1).unwrap()), ..request(spec("c"), 1) };
    assert_eq!(message(registry.plan(without_split)), "dynamic disk claims require split memory backing");
    // Growth beyond the cap is refused and changes nothing; recorded bytes always land.
    let before = registry.activity_revision().unwrap();
    assert_eq!(
        message(registry.update_disk_claim("b", 1, Some(9000), None, true, false)),
        "physical disk capacity exhausted"
    );
    assert_eq!(registry.activity_revision().unwrap(), before);
    assert_eq!(
        registry.update_disk_claim("b", 1, Some(9000), None, false, false).unwrap(),
        Some(DiskClaim { workspace_mb: 9000, memory_mb: 64 })
    );
    assert_eq!(registry.activity_revision().unwrap(), before + 1);
    // An unchanged claim does not bump; a fixed claim ignores updates.
    registry.update_disk_claim("b", 1, Some(9000), None, false, false).unwrap();
    assert_eq!(registry.activity_revision().unwrap(), before + 1);
    assert_eq!(registry.update_disk_claim("a", 1, Some(1), None, false, false).unwrap(), None);
    assert_eq!(
        message(registry.update_disk_claim("a", 2, Some(1), None, false, false)),
        "disk claim lost incarnation ownership"
    );
    assert_eq!(
        message(registry.update_disk_claim("a", 1, Some(-1), None, false, false)),
        "disk claim components must be non-negative integers"
    );
    // Record writes never reset a dynamic claim.
    registry.commit_quota("b", 1, &quota(&dir.0, "b", 1)).unwrap();
    assert_eq!(registry.disk_claim("b", 1).unwrap(), Some(DiskClaim { workspace_mb: 9000, memory_mb: 64 }));
    let claims = registry.disk_claims_mb().unwrap();
    assert_eq!(claims[&("a".to_string(), 1)], 5184);
    assert_eq!(claims[&("b".to_string(), 1)], 9064);
}

#[test]
fn capacity_zero_bounds_nothing_but_refuses_required_growth() {
    let dir = TempDir::new("cap0");
    let registry = Registry::new(dir.registry(), 0).unwrap();
    let split = PlanRequest {
        split_memory_backing: true,
        initial_claim: Some(DiskClaim::new(1, 1).unwrap()),
        ..request(parkable_spec("a"), 1)
    };
    registry.plan(split).unwrap();
    assert_eq!(
        message(registry.update_disk_claim("a", 1, Some(2), None, true, false)),
        "physical disk capacity exhausted"
    );
    // A decrease never needs capacity.
    assert_eq!(
        registry.update_disk_claim("a", 1, Some(0), None, true, false).unwrap(),
        Some(DiskClaim { workspace_mb: 0, memory_mb: 1 })
    );
}

#[test]
fn adopting_a_split_fixed_claim_keeps_the_total_and_published_workspaces_release() {
    let dir = TempDir::new("adopt");
    let registry = Registry::new(dir.registry(), 100_000).unwrap();
    let planned = registry.plan(PlanRequest { split_memory_backing: true, ..request(parkable_spec("a"), 1) }).unwrap();
    let ready = registry
        .commit_rootfs(
            "a",
            planned.revision,
            &image(),
            &rootfs(&dir.0, "a", 1),
            Some(&Quota { total_mb: 5184, ..quota(&dir.0, "a", 1) }),
        )
        .unwrap();
    registry.commit_owned("a", ready.revision).unwrap();
    let reserved = |registry: &Registry| registry.store.read(|tx| tx.reserved_disk_bytes()).unwrap();
    let before = reserved(&registry);
    assert_eq!(before, i128::from(5184 * MIB));
    let adopted = registry.update_disk_claim("a", 1, None, None, false, true).unwrap();
    assert_eq!(adopted, Some(DiskClaim { workspace_mb: 2048, memory_mb: 3136 }));
    assert_eq!(reserved(&registry), before);
    // A published workspace stops charging its grant; a remount re-charges it.
    let epoch = registry.workspace_mount_epoch("a", 1).unwrap();
    assert_eq!(epoch, 0);
    assert!(registry.release_published_workspace("a", 1, 7, epoch).unwrap());
    assert_eq!(registry.disk_claims_mb().unwrap()[&("a".to_string(), 1)], 3136);
    registry.reserve_workspace_for_mount("a", 1).unwrap();
    assert_eq!(registry.workspace_mount_epoch("a", 1).unwrap(), 1);
    assert_eq!(registry.disk_claims_mb().unwrap()[&("a".to_string(), 1)], 5184);
    // A late publisher holding the old epoch cannot un-charge the remount.
    assert!(!registry.release_published_workspace("a", 1, 7, epoch).unwrap());
    assert_eq!(
        message(registry.release_published_workspace("a", 2, 7, 1)),
        "workspace release lost incarnation ownership"
    );
    assert_eq!(message(registry.release_published_workspace("a", 1, 0, 1)), "invalid released workspace size");
}

#[test]
fn reflink_overlaps_are_exact_and_fence_deletion() {
    let dir = TempDir::new("reflink");
    let registry = Registry::new(dir.registry(), 0).unwrap();
    let record = owned(&registry, &dir.0, "a", 1);
    let digest = "c".repeat(64);
    assert_eq!(
        message(registry.reserve_reflink_overlap("a", 1, 1, 10, &digest)),
        "reflink overlap physical disk capacity exhausted"
    );
    registry.set_hard_disk_capacity_mb(10_000).unwrap();
    registry.reserve_reflink_overlap("a", 1, 1, 10, &digest).unwrap();
    let revision = registry.activity_revision().unwrap();
    registry.reserve_reflink_overlap("a", 1, 1, 10, &digest).unwrap();
    assert_eq!(registry.activity_revision().unwrap(), revision);
    assert_eq!(message(registry.reserve_reflink_overlap("a", 1, 1, 11, &digest)), "reflink overlap source changed");
    assert_eq!(message(registry.reserve_reflink_overlap("a", 1, 0, 11, &digest)), "invalid reflink overlap identity");
    assert_eq!(registry.reflink_overlap_bytes().unwrap(), 10);
    assert_eq!(registry.list_reflink_overlaps(Some(("a", 1))).unwrap().len(), 1);
    let deleting = registry.begin_delete("a", record.revision, None).unwrap();
    assert_eq!(
        message(registry.commit_deleted("a", 1, deleting.revision)),
        "deletion retains unreconciled reflink overlap"
    );
    assert_eq!(
        message(registry.release_reflink_overlap("a", 1, 1, &"d".repeat(64))),
        "reflink overlap release lost source ownership"
    );
    registry.release_reflink_overlap("a", 1, 1, &digest).unwrap();
    registry.release_reflink_overlap("a", 1, 1, &digest).unwrap();
    registry.commit_deleted("a", 1, deleting.revision).unwrap();
    assert!(registry.list_reflink_overlaps(None).unwrap().is_empty());
}

#[test]
fn queued_launch_is_replaceable_but_activated_launch_is_not() {
    let dir = TempDir::new("growth");
    let registry = Registry::owner(dir.registry(), 0).unwrap();
    owned(&registry, &dir.0, "sandbox", 3);
    let revision = registry.activity_revision().unwrap();
    let first = ("first", "1".repeat(64));
    let second = ("second", "2".repeat(64));
    let growth = |action: &str, identity: (&str, &str)| {
        registry.growth_intent("sandbox", 3, GrowthAction::parse(action).unwrap(), identity.0, identity.1, "")
    };
    let phase = |action: &str, identity: (&str, &str)| growth(action, identity).unwrap().unwrap().phase;
    assert_eq!(phase("launch", (first.0, &first.1)), "queued");
    let replaced = growth("launch", (second.0, &second.1)).unwrap().unwrap();
    assert_eq!((replaced.job_id.as_str(), replaced.phase.as_str()), ("second", "queued"));
    assert_eq!(replaced.memory_bytes, 1024 * MIB);
    assert_eq!(phase("activate", (first.0, &first.1)), "queued");
    assert_eq!(phase("activate", ("", "")), "queued");
    assert_eq!(phase("park", ("", "")), "queued");
    assert_eq!(phase("activate", (second.0, &second.1)), "active");
    let error = growth("launch", (first.0, &first.1)).unwrap_err();
    assert_eq!(error, RegistryError::ManagedPrimaryOwned { job_id: "second".into() });
    assert!(error.is_conflict());
    assert_eq!(error.message(), "sandbox generation already owns another primary process");
    assert_eq!(phase("park", ("", "")), "parked");
    assert!(growth("launch", (first.0, &first.1)).is_err());
    assert_eq!(phase("activate", ("", "")), "active");
    let intents = registry.growth_intents().unwrap();
    assert_eq!(
        intents.iter().map(|i| (i.job_id.as_str(), i.phase.as_str())).collect::<Vec<_>>(),
        [("second", "active")]
    );
    assert_eq!(GrowthAction::parse("fly").unwrap_err().message(), "invalid growth action");
    // Growth never moves the clock.
    assert_eq!(registry.activity_revision().unwrap(), revision);
}

#[test]
fn growth_batch_items_commit_together_and_fail_alone() {
    let dir = TempDir::new("growth-batch");
    let registry = Registry::owner(dir.registry(), 0).unwrap();
    owned(&registry, &dir.0, "a", 1);
    owned(&registry, &dir.0, "b", 1);
    let key = |id: &str| (id.to_string(), 1);
    let results = registry
        .growth_intent_batch(&[
            (key("a"), GrowthAction::Wait, "req-1".into()),
            (key("b"), GrowthAction::Activate, "req-2".into()),
            (key("b"), GrowthAction::Wait, "req-2".into()),
            (key("missing"), GrowthAction::Wait, "req-3".into()),
        ])
        .unwrap();
    assert_eq!(results[0].as_ref().unwrap().as_ref().unwrap().phase, "safe");
    assert_eq!(results[1].as_ref().unwrap().as_ref().unwrap().phase, "active");
    assert_eq!(results[2].as_ref().unwrap_err().message(), "growth wait was superseded by wake");
    assert_eq!(results[3].as_ref().unwrap_err().message(), "direct registration is absent");
    // The activation committed its wake fence.
    assert!(registry.relay_wake_fence("b", 1, "req-2", false).unwrap());
    assert!(!registry.relay_wake_fence("a", 1, "req-9", false).unwrap());
    assert!(registry.relay_wake_fence("a", 1, "req-9", true).unwrap());
    assert!(registry.relay_wake_fence("a", 1, "req-9", false).unwrap());
    assert_eq!(message(registry.relay_wake_fence("a", 2, "req-9", false)), "relay lifecycle generation changed");
    assert_eq!(message(registry.relay_wake_fence("a", 1, "bad id", false)), "invalid relay lifecycle identity");
    let record = registry.get("b").unwrap().unwrap();
    delete(&registry, &record);
    assert_eq!(registry.growth_intents().unwrap().len(), 1);
}

#[test]
fn queued_writers_share_one_commit_and_fail_alone() {
    let dir = TempDir::new("group");
    let owner = Registry::owner(dir.registry(), 0).unwrap();
    let planned = plan(&owner, "a", 1).unwrap();
    let commits = owner.store.commits.load(Ordering::SeqCst);
    let (inside_tx, inside_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    std::thread::scope(|scope| {
        let (writer, root) = (&owner, &dir.0);
        let first = scope.spawn(move || {
            writer.store.write(true, |tx| {
                let ready = owner_transition_body(tx, root)?;
                inside_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok(ready)
            })
        });
        inside_rx.recv().unwrap();
        let second = scope.spawn(|| plan(&owner, "b", 1));
        let stale = scope.spawn(|| owner.commit_owned("a", planned.revision));
        let deadline = Instant::now() + Duration::from_secs(10);
        while owner.store.turn_waiters() < 2 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        // Written but not committed: no reader sees it, the owner included.
        owner.store.expire_index_check();
        assert_eq!(owner.get("a").unwrap().unwrap().phase, Phase::Planned);
        assert_eq!(owner.activity_revision().unwrap(), 1);
        assert_eq!(Registry::new(dir.registry(), 0).unwrap().get("a").unwrap().unwrap().phase, Phase::Planned);
        release_tx.send(()).unwrap();
        assert_eq!(first.join().unwrap().unwrap().phase, Phase::QuotaReady);
        assert_eq!(second.join().unwrap().unwrap().phase, Phase::Planned);
        assert_eq!(message(stale.join().unwrap()), TRANSITION_FENCE);
    });
    // One COMMIT (one fsync) for the three writers.
    assert_eq!(owner.store.commits.load(Ordering::SeqCst), commits + 1);
    assert_eq!(owner.activity_revision().unwrap(), 3);
    assert_matches_journal(&owner, &dir.registry());
}

fn owner_transition_body(tx: &mut Tx<'_>, root: &Path) -> Result<Arc<Registration>> {
    let record = tx.require("a")?;
    let mut updated = Registration::clone(&record);
    set_quota(&mut updated, &quota(root, "a", 1));
    updated.phase = Phase::QuotaReady;
    updated.revision += 1;
    let written = tx.write(updated, false)?;
    tx.bump_activity()?;
    Ok(written)
}

fn assert_matches_journal(owner: &Registry, path: &Path) {
    let journal = Registry::new(path, 0).unwrap();
    owner.store.expire_index_check();
    assert_eq!(owner.list().unwrap(), journal.list().unwrap());
    assert_eq!(owner.activity_revision().unwrap(), journal.activity_revision().unwrap());
    assert_eq!(owner.disk_claims_mb().unwrap(), journal.disk_claims_mb().unwrap());
}

#[test]
fn a_failed_member_undoes_only_its_own_changes() {
    let dir = TempDir::new("member");
    let owner = Registry::owner(dir.registry(), 0).unwrap();
    plan(&owner, "a", 1).unwrap();
    let error = owner
        .store
        .write(true, |tx| {
            tx.delete("a")?;
            tx.bump_activity()?;
            Err::<(), _>(RegistryError::conflict("changed my mind"))
        })
        .unwrap_err();
    assert_eq!(error.message(), "changed my mind");
    assert_eq!(owner.get("a").unwrap().unwrap().phase, Phase::Planned);
    assert_eq!(owner.activity_revision().unwrap(), 1);
    assert_matches_journal(&owner, &dir.registry());
}

#[test]
fn a_panicking_member_abandons_its_group_and_leaves_nothing() {
    let dir = TempDir::new("panic");
    let owner = Registry::owner(dir.registry(), 0).unwrap();
    plan(&owner, "a", 1).unwrap();
    let panicked = std::thread::scope(|scope| {
        scope
            .spawn(|| {
                owner.store.write(true, |tx| {
                    tx.delete("a")?;
                    tx.bump_activity()?;
                    if tx.get("a")?.is_none() {
                        panic!("writer bug");
                    }
                    Ok(())
                })
            })
            .join()
    });
    assert!(panicked.is_err());
    assert_eq!(owner.get("a").unwrap().unwrap().phase, Phase::Planned);
    plan(&owner, "b", 1).unwrap();
    assert_eq!(owner.activity_revision().unwrap(), 2);
    assert_matches_journal(&owner, &dir.registry());
}

#[test]
fn concurrent_owner_and_foreign_writers_stay_coherent() {
    let dir = TempDir::new("coherent");
    let owner = Registry::owner(dir.registry(), 0).unwrap();
    let foreign = Registry::new(dir.registry(), 0).unwrap();
    let stop = std::sync::atomic::AtomicBool::new(false);
    let lifecycle = |registry: &Registry, prefix: &str| {
        for index in 0..8 {
            let record = owned(registry, &dir.0, &format!("{prefix}{index}"), 1);
            if index % 2 == 1 {
                delete(registry, &record);
            }
        }
    };
    let read = || {
        let mut last = 0;
        while !stop.load(Ordering::SeqCst) {
            let snapshot = owner.snapshot().unwrap();
            assert!(snapshot.activity_revision >= last, "the clock went back");
            last = snapshot.activity_revision;
            assert!(owner.activity_revision().unwrap() >= last, "clock behind its own snapshot");
            for record in &snapshot.records {
                assert!(snapshot.activity_revision >= record.revision);
                if let Some(current) = owner.get(record.sandbox_id()).unwrap() {
                    assert!(current.revision >= record.revision, "{} went back", record.sandbox_id());
                }
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    };
    std::thread::scope(|scope| {
        let readers: Vec<_> = (0..3).map(|_| scope.spawn(read)).collect();
        let writers: Vec<_> = (0..4)
            .map(|index| {
                let (owner, lifecycle) = (&owner, &lifecycle);
                scope.spawn(move || lifecycle(owner, &format!("o{index}-")))
            })
            .chain([scope.spawn(|| lifecycle(&foreign, "f-"))])
            .collect();
        for writer in writers {
            writer.join().unwrap();
        }
        stop.store(true, Ordering::SeqCst);
        for reader in readers {
            reader.join().unwrap();
        }
    });
    // The foreign writer's last commit may be up to a second behind the owner's reads.
    assert_matches_journal(&owner, &dir.registry());
    assert_eq!(owner.list().unwrap().len(), 20);
    assert_eq!(owner.activity_revision().unwrap(), 5 * (8 * 4 + 4 * 2));
}

#[test]
fn owner_detects_foreign_commits_at_its_next_write_and_read() {
    let dir = TempDir::new("foreign");
    let owner = Registry::owner(dir.registry(), 0).unwrap();
    let record = owned(&owner, &dir.0, "a", 1);
    let foreign = Registry::new(dir.registry(), 0).unwrap();
    foreign.begin_delete("a", record.revision, None).unwrap();
    // The owner's next write proves its index at BEGIN and sees the delete.
    assert_eq!(message(owner.commit_owned("a", record.revision)), TRANSITION_FENCE);
    assert_eq!(owner.get("a").unwrap().unwrap().phase, Phase::Deleting);
    // Raw SQL behind the owner: its write sees the row gone.
    raw(&dir.registry())
        .execute_batch("BEGIN; DELETE FROM registration_disk; DELETE FROM registrations; UPDATE registry_metadata SET activity_revision = activity_revision + 1; COMMIT")
        .unwrap();
    assert_eq!(message(owner.begin_delete("a", record.revision + 1, None)), "direct registration is absent");
    // Reads recheck within a second; a corrupted clock is refused.
    let connection = raw(&dir.registry());
    connection
        .execute_batch("PRAGMA ignore_check_constraints = ON; UPDATE registry_metadata SET activity_revision = -1")
        .unwrap();
    owner.store.expire_index_check();
    assert_eq!(message(owner.activity_revision()), "direct registry metadata is invalid");
}

#[test]
fn ownership_is_exclusive_and_moves_with_reopen() {
    let dir = TempDir::new("ownership");
    let owner = Registry::owner(dir.registry(), 0).unwrap();
    assert!(owner.is_owner());
    assert_eq!(message(Registry::owner(dir.registry(), 0)), "direct registry has another live owner");
    let foreign = Registry::new(dir.registry(), 0).unwrap();
    owned(&foreign, &dir.0, "a", 1);
    owner.close();
    assert!(!owner.is_owner());
    // A closed owner keeps working against SQLite.
    assert_eq!(owner.get("a").unwrap().unwrap().phase, Phase::Owned);
    plan(&owner, "b", 1).unwrap();
    let next = Registry::owner(dir.registry(), 0).unwrap();
    assert_eq!(next.list().unwrap(), foreign.list().unwrap());
    drop(next);
    let lock = dir.0.join("registry.sqlite.owner");
    std::fs::set_permissions(&lock, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!(message(Registry::owner(dir.registry(), 0)), "direct registry owner lock must be private and owned");
}

#[test]
fn replaced_file_and_changed_schema_are_refused() {
    let dir = TempDir::new("replaced");
    let owner = Registry::owner(dir.registry(), 0).unwrap();
    plan(&owner, "a", 1).unwrap();
    let reader = Registry::new(dir.registry(), 0).unwrap();
    reader.list().unwrap();
    raw(&dir.registry()).execute_batch("CREATE TABLE extra (x)").unwrap();
    assert_eq!(message(reader.list()), "direct registry schema is invalid");
    owner.store.expire_index_check();
    assert_eq!(message(owner.list()), "direct registry schema is invalid");
    raw(&dir.registry()).execute_batch("DROP TABLE extra").unwrap();
    reader.list().unwrap();
    let copy = dir.0.join("copy.sqlite");
    std::fs::copy(dir.registry(), &copy).unwrap();
    std::fs::rename(&copy, dir.registry()).unwrap();
    assert_eq!(message(reader.list()), "direct registry file was replaced; reopen it");
    assert_eq!(message(plan(&owner, "b", 1)), "direct registry file was replaced; reopen it");
}

#[test]
fn noncanonical_rows_and_metadata_are_unreadable() {
    let dir = TempDir::new("noncanonical");
    let registry = Registry::new(dir.registry(), 0).unwrap();
    plan(&registry, "a", 1).unwrap();
    let connection = raw(&dir.registry());
    let stored: String = connection.query_row("SELECT record_json FROM registrations", [], |row| row.get(0)).unwrap();
    let pretty = serde_json::to_string_pretty(&serde_json::from_str::<serde_json::Value>(&stored).unwrap()).unwrap();
    connection.execute("UPDATE registrations SET record_json = ?", [&pretty]).unwrap();
    assert_eq!(message(registry.list()), "direct registration encoding is invalid");
    assert_eq!(message(Registry::owner(dir.registry(), 0)), "direct registration encoding is invalid");
    connection.execute("UPDATE registrations SET record_json = ?", [&stored]).unwrap();
    connection
        .execute_batch(r#"UPDATE registry_metadata SET drain_json = '{"admission_open":true,"drain_activity_epoch":0,"draining":"true","token":"t"}'"#)
        .unwrap();
    assert_eq!(message(registry.load_drain()), "direct registry metadata is invalid");
    connection
        .execute_batch(&format!("PRAGMA ignore_check_constraints = ON; UPDATE registry_metadata SET drain_json = '{}', runtime_compatibility_sha256 = '{}'", DrainState::default().encode(), "x".repeat(64)))
        .unwrap();
    assert_eq!(message(registry.activity_revision()), "direct registry metadata is invalid");
}

#[test]
fn non_sqlite_files_are_unreadable() {
    let dir = TempDir::new("json");
    std::fs::write(dir.registry(), b"{\"records\": []}").unwrap();
    std::fs::set_permissions(dir.registry(), std::fs::Permissions::from_mode(0o600)).unwrap();
    let error = Registry::new(dir.registry(), 0).unwrap().list().unwrap_err();
    assert!(matches!(error, RegistryError::Unreadable { .. }), "{error:?}");
    assert_eq!(error.message(), "direct registry is unreadable");
}

#[test]
fn runtime_compatibility_and_drain() {
    let dir = TempDir::new("compat");
    let registry = Registry::new(dir.registry(), 0).unwrap();
    plan(&registry, "a", 1).unwrap();
    assert_eq!(message(registry.bind_runtime_compatibility("x")), "runtime compatibility digest is invalid");
    assert_eq!(
        message(registry.bind_runtime_compatibility(&"c".repeat(64))),
        "direct registry contains another runtime compatibility"
    );
    assert_eq!(registry.bind_runtime_compatibility(COMPAT).unwrap(), COMPAT);
    assert_eq!(registry.bind_runtime_compatibility(COMPAT).unwrap(), COMPAT);
    assert_eq!(
        message(registry.bind_runtime_compatibility(&"c".repeat(64))),
        "node state belongs to another runtime compatibility"
    );
    let mut other = request(spec("b"), 1);
    other.runtime_compatibility_sha256 = "c".repeat(64);
    assert_eq!(message(registry.plan(other)), "direct registration belongs to another runtime compatibility");
    let drain = DrainState { draining: true, token: "t".into(), drain_activity_epoch: 3, admission_open: false };
    registry.save_drain(&drain).unwrap();
    assert_eq!(registry.load_drain().unwrap(), drain);
    let bad = DrainState { admission_open: true, ..drain };
    assert_eq!(message(registry.save_drain(&bad)), "direct registry metadata is invalid");
    // Neither moves the clock.
    assert_eq!(registry.activity_revision().unwrap(), 1);
}

#[test]
fn version7_upgrade_backfills_the_ledger_and_version8_keeps_claims_fixed() {
    for version in [7, 8] {
        let dir = TempDir::new("upgrade");
        let registry = Registry::new(dir.registry(), 0).unwrap();
        let split = PlanRequest {
            split_memory_backing: true,
            initial_claim: Some(DiskClaim::new(1, 1).unwrap()),
            ..request(parkable_spec("a"), 1)
        };
        registry.plan(split).unwrap();
        plan(&registry, "b", 1).unwrap();
        drop(registry);
        let connection = raw(&dir.registry());
        if version == 7 {
            connection.execute_batch("DROP TABLE registration_disk; PRAGMA user_version = 7").unwrap();
        } else {
            connection
                .execute_batch(&format!(
                    "DROP TABLE registration_disk; {}; INSERT INTO registration_disk VALUES ('a', 1, 5184), ('b', 1, 2048); PRAGMA user_version = 8",
                    "CREATE TABLE registration_disk (\n            sandbox_id TEXT PRIMARY KEY,\n            sandbox_generation INTEGER NOT NULL CHECK (sandbox_generation > 0),\n            reserved_mb INTEGER NOT NULL CHECK (reserved_mb >= 0)\n        ) STRICT"
                ))
                .unwrap();
        }
        drop(connection);
        let upgraded = Registry::owner(dir.registry(), 0).unwrap();
        let claims = upgraded.disk_claims_mb().unwrap();
        assert_eq!((claims[&("a".to_string(), 1)], claims[&("b".to_string(), 1)]), (5184, 2048), "v{version}");
        assert_eq!(upgraded.disk_claim("a", 1).unwrap(), None);
        let (_, user_version) = schema::versions(&raw(&dir.registry())).unwrap();
        assert_eq!(user_version, SCHEMA_VERSION);
    }
}

#[test]
fn concurrent_creates_advance_the_clock_exactly() {
    let dir = TempDir::new("burst");
    let owner = Registry::owner(dir.registry(), 0).unwrap();
    std::thread::scope(|scope| {
        for index in 0..32 {
            let (owner, root) = (&owner, &dir.0);
            scope.spawn(move || {
                let id = format!("s{index}");
                let planned = plan(owner, &id, 1).unwrap();
                let ready = owner
                    .commit_rootfs(&id, planned.revision, &image(), &rootfs(root, &id, 1), Some(&quota(root, &id, 1)))
                    .unwrap();
                owner.commit_owned(&id, ready.revision).unwrap();
            });
        }
    });
    assert_eq!(owner.activity_revision().unwrap(), 96);
    assert!(owner.store.commits.load(Ordering::SeqCst) <= 96);
    assert_matches_journal(&owner, &dir.registry());
}
