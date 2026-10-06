//! The overlay prepare and discard against Python's `OverlayRootfsManager`
//! with fake `mount`, `umount` and `mountpoint` binaries (no real mounts, so
//! these run unprivileged). GOLDEN is what Python's `prepare` wrote for the
//! same template in both layouts, with its temp root replaced by `{ROOT}`.

use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

use serde_json::Value;
use ucloud_noded::image::{EnvironmentManifest, HOST_EROFS_ABI, ImageConfig, MaterializedRootfs, MountProbe};
use ucloud_noded::rootfs::{OverlayManager, RootfsError};

struct Fixture {
    root: PathBuf,
    log: PathBuf,
    manager: OverlayManager,
    image: MaterializedRootfs,
}

fn script(path: &Path, body: &str) {
    std::fs::write(path, format!("#!/bin/sh\n{body}")).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

impl Fixture {
    /// `mount_exit`/`umount_exit`: the fake binaries' exit codes. A successful
    /// fake mount leaves a marker the fake `mountpoint` reports.
    fn new(name: &str, mount_exit: i32, umount_exit: i32) -> Fixture {
        let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let root = std::env::temp_dir().join(format!("noded-rootfs-{name}-{}-{nanos}", std::process::id()));
        let bin = root.join("bin");
        std::fs::create_dir_all(&bin).unwrap();
        let log = root.join("calls.log");
        let record = format!("printf '%s\\037' \"$(basename \"$0\")\" \"$@\" >> {log}; printf '\\n' >> {log}\n", log = log.display());
        script(
            &bin.join("mount"),
            &format!("{record}[ {mount_exit} = 0 ] || {{ echo 'mount: bad option' >&2; exit {mount_exit}; }}\neval \"touch \\\"\\${{$#}}.mounted\\\"\"\n"),
        );
        script(&bin.join("umount"), &format!("{record}[ {umount_exit} = 0 ] || {{ echo busy >&2; exit {umount_exit}; }}\nrm -f \"$1.mounted\"\n"));
        script(&bin.join("mountpoint"), "[ -e \"$2.mounted\" ] && exit 0\nexit 1\n");
        let manifest = EnvironmentManifest {
            base: format!("sha256:{}", "1".repeat(64)),
            workspace: None,
            toolkits: vec![format!("sha256:{}", "3".repeat(64))],
        };
        let lower = root.join("cache/images").join(manifest.sha256()).join("rootfs");
        std::fs::create_dir_all(&lower).unwrap();
        std::fs::set_permissions(&lower, std::fs::Permissions::from_mode(0o755)).unwrap();
        let image = MaterializedRootfs {
            image_ref: "ref".into(),
            image_id: format!("sha256:{}", manifest.sha256()),
            rootfs_identity_sha256: manifest.rootfs_fingerprint(),
            rootfs: lower,
            image_config: ImageConfig::default(),
            environment: manifest,
            backend_abi: HOST_EROFS_ABI,
        };
        let mut manager = OverlayManager::new(root.join("mounts"), root.join("bundles"), true).unwrap();
        manager.mount_binary = bin.join("mount");
        manager.umount_binary = bin.join("umount");
        manager.probe = MountProbe::Command(bin.join("mountpoint"));
        Fixture { root, log, manager, image }
    }

    fn calls(&self) -> Vec<Vec<String>> {
        let text = std::fs::read_to_string(&self.log).unwrap_or_default();
        text.lines()
            .map(|line| line.trim_end_matches('\u{1f}').split('\u{1f}').map(|item| item.replace(&self.root.display().to_string(), "{ROOT}")).collect())
            .collect()
    }

    fn writable(&self, name: &str) -> PathBuf {
        let path = self.root.join("mounts").join(name);
        std::fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
        path
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn mode(path: &Path) -> u32 {
    std::fs::symlink_metadata(path).unwrap().mode() & 0o7777
}

fn golden() -> Value {
    serde_json::from_str(GOLDEN).unwrap()
}

#[tokio::test]
async fn prepare_writes_what_python_writes_in_both_layouts() {
    for layout in ["legacy", "split"] {
        let fixture = Fixture::new(layout, 0, 0);
        let expected = &golden()[layout];
        let incarnation = "sbx.sandbox-3";
        let (workspace, allocation) = if layout == "split" {
            (format!("workspace-{incarnation}"), Some(incarnation))
        } else {
            (String::new(), None)
        };
        let writable = fixture.writable(if workspace.is_empty() { incarnation } else { &workspace });
        let lease = fixture
            .manager
            .prepare("sbx", 3, &fixture.image, &expected["template"], &workspace, allocation)
            .await
            .unwrap();
        let bundle = fixture.root.join("bundles").join(incarnation);
        let unroot = |text: String| text.replace(&fixture.root.display().to_string(), "{ROOT}");
        assert_eq!(unroot(std::fs::read_to_string(bundle.join("config.json")).unwrap()), expected["config"].as_str().unwrap());
        assert_eq!(
            unroot(std::fs::read_to_string(bundle.join(".ucloud-overlay.json")).unwrap()),
            expected["overlay"].as_str().unwrap()
        );
        let mut calls = fixture.calls();
        assert_eq!(calls.len(), 1);
        calls[0][0] = "mount".into();
        assert_eq!(serde_json::to_value(&calls).unwrap(), expected["calls"], "{layout}");
        assert_eq!(lease.container_id, expected["container_id"].as_str().unwrap());
        assert_eq!(lease.memory_directory, expected["memory_directory"].as_str().unwrap());
        assert_eq!(unroot(lease.writable.display().to_string()), expected["writable"].as_str().unwrap());
        assert_eq!(lease.writable, writable);
        assert_eq!(lease.rootfs_sha256, expected["rootfs_sha256"].as_str().unwrap());
        assert_eq!(lease.bundle, bundle);
        assert_eq!(lease.merged, bundle.join("rootfs"));
        for path in [&bundle, &lease.work, &lease.merged] {
            assert_eq!(mode(path), 0o700);
        }
        // The upper is the guest's "/": it takes the image root's mode.
        assert_eq!(mode(&lease.upper), 0o755);
        assert_eq!(mode(&bundle.join("config.json")), 0o600);
        assert_eq!(mode(&bundle.join(".ucloud-overlay.json")), 0o600);
        let names: Vec<_> = std::fs::read_dir(&bundle)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .filter(|name| name != "rootfs.mounted")
            .collect();
        assert_eq!(names.len(), 3, "no temporary files remain: {names:?}");
        // A second prepare of the same incarnation is refused.
        let again = fixture.manager.prepare("sbx", 3, &fixture.image, &expected["template"], &workspace, allocation).await;
        assert!(matches!(again, Err(RootfsError::Warden(message)) if message == "overlay sandbox incarnation already exists"));
    }
}

#[tokio::test]
async fn a_failed_mount_removes_the_partial_overlay() {
    let fixture = Fixture::new("mount-fails", 32, 0);
    let writable = fixture.writable("sbx.sandbox-1");
    let error = fixture.manager.prepare("sbx", 1, &fixture.image, &serde_json::json!({}), "", None).await.unwrap_err();
    assert_eq!(error.to_string(), "overlay mount failed: mount: bad option\n");
    assert!(!fixture.root.join("bundles/sbx.sandbox-1").exists());
    // The precreated (quota-owned) writable root stays, empty.
    assert_eq!(std::fs::read_dir(&writable).unwrap().count(), 0);
    assert_eq!(fixture.calls().len(), 1);
}

#[tokio::test]
async fn a_failure_after_the_mount_unmounts_then_removes() {
    let fixture = Fixture::new("late-failure", 0, 0);
    let writable = fixture.writable("sbx.sandbox-1");
    // A split template without its memory allocation annotation.
    let workspace = "workspace-sbx.sandbox-1";
    let split = fixture.writable(workspace);
    let error =
        fixture.manager.prepare("sbx", 1, &fixture.image, &serde_json::json!({}), workspace, Some("sbx.sandbox-1")).await.unwrap_err();
    assert_eq!(error.to_string(), "runtime spec lacks its memory allocation identity");
    let calls = fixture.calls();
    assert_eq!(calls.iter().map(|call| call[0].as_str()).collect::<Vec<_>>(), ["mount", "umount"]);
    assert!(!fixture.root.join("bundles/sbx.sandbox-1").exists());
    assert_eq!(std::fs::read_dir(&split).unwrap().count(), 0);
    assert_eq!(std::fs::read_dir(&writable).unwrap().count(), 0);

    // When the unmount fails too, the distinct error leaves everything for discard.
    let fixture = Fixture::new("stuck", 0, 1);
    fixture.writable("sbx.sandbox-1");
    let error = fixture.manager.prepare("sbx", 1, &fixture.image, &serde_json::json!({"linux": []}), "", None).await.unwrap_err();
    assert!(matches!(&error, RootfsError::UnmountFailed { .. }), "{error}");
    assert!(error.to_string().starts_with("overlay preparation failed and its mount could not be released: busy"));
    assert!(fixture.root.join("bundles/sbx.sandbox-1/rootfs").exists());
}

#[tokio::test]
async fn preconditions_match_python() {
    let fixture = Fixture::new("preconditions", 0, 0);
    let template = serde_json::json!({});
    let prepare = |workspace: &'static str, allocation: Option<&'static str>, id: &'static str, generation: u64| {
        let manager = fixture.manager.clone();
        let image = fixture.image.clone();
        let template = template.clone();
        async move { manager.prepare(id, generation, &image, &template, workspace, allocation).await.unwrap_err().to_string() }
    };
    assert_eq!(prepare("", None, "sbx", 1).await, "quota-owned writable incarnation was not prepared");
    let writable = fixture.writable("sbx.sandbox-1");
    std::fs::write(writable.join("stale"), "").unwrap();
    assert_eq!(prepare("", None, "sbx", 1).await, "quota-owned writable incarnation is not empty");
    std::fs::set_permissions(&writable, std::fs::Permissions::from_mode(0o777)).unwrap();
    assert_eq!(prepare("", None, "sbx", 1).await, "rootfs store directory must be owned and private");
    assert_eq!(prepare("workspace-sbx.sandbox-1", None, "sbx", 1).await, "split rootfs requires both component references");
    assert_eq!(prepare("workspace-other", Some("x"), "sbx", 1).await, "workspace directory belongs to another incarnation");
    assert_eq!(prepare("", None, "-bad", 1).await, "sandbox incarnation is invalid");
    assert_eq!(prepare("", None, "sbx", 0).await, "sandbox incarnation is invalid");
    assert!(fixture.calls().is_empty());
}

#[tokio::test]
async fn discard_unmounts_and_empties_an_unregistered_overlay() {
    let fixture = Fixture::new("discard", 0, 0);
    let writable = fixture.writable("sbx.sandbox-2");
    fixture.manager.prepare("sbx", 2, &fixture.image, &serde_json::json!({}), "", None).await.unwrap();
    std::fs::write(writable.join("upper/file"), "guest data").unwrap();
    fixture.manager.discard_unregistered("sbx", 2, "").await.unwrap();
    let calls = fixture.calls();
    assert_eq!(calls.iter().map(|call| call[0].as_str()).collect::<Vec<_>>(), ["mount", "umount"]);
    assert_eq!(calls[1][1], "{ROOT}/bundles/sbx.sandbox-2/rootfs");
    assert!(!fixture.root.join("bundles/sbx.sandbox-2").exists());
    assert!(writable.exists());
    assert_eq!(std::fs::read_dir(&writable).unwrap().count(), 0);
    // Nothing there: a no-op; an unmounted bundle is removed without umount.
    fixture.manager.discard_unregistered("sbx", 2, "").await.unwrap();
    std::fs::create_dir_all(fixture.root.join("bundles/sbx.sandbox-2/rootfs")).unwrap();
    fixture.manager.discard_unregistered("sbx", 2, "").await.unwrap();
    assert!(!fixture.root.join("bundles/sbx.sandbox-2").exists());
    assert_eq!(fixture.calls().len(), 2);
    assert!(fixture.manager.discard_unregistered("sbx", 2, "workspace-other").await.is_err());
}

/// A real overlay mount, only as root (skipped otherwise).
#[tokio::test]
async fn a_real_overlay_mounts_and_discards_as_root() {
    // SAFETY: geteuid has no preconditions.
    if unsafe { libc::geteuid() } != 0 {
        return;
    }
    let mut fixture = Fixture::new("real", 0, 0);
    fixture.manager.mount_binary = "mount".into();
    fixture.manager.umount_binary = "umount".into();
    fixture.manager.probe = MountProbe::Kernel;
    std::fs::write(fixture.image.rootfs.join("image-file"), "from the image").unwrap();
    let writable = fixture.writable("sbx.sandbox-1");
    let lease = fixture.manager.prepare("sbx", 1, &fixture.image, &serde_json::json!({}), "", None).await.unwrap();
    assert_eq!(std::fs::read_to_string(lease.merged.join("image-file")).unwrap(), "from the image");
    std::fs::write(lease.merged.join("guest-file"), "guest").unwrap();
    assert!(writable.join("upper/guest-file").exists());
    assert_eq!(ucloud_noded::image::statx_mount_root(&lease.merged), Some(true));
    fixture.manager.discard_unregistered("sbx", 1, "").await.unwrap();
    assert!(!lease.bundle.exists());
    assert_eq!(std::fs::read_dir(&writable).unwrap().count(), 0);
}

const GOLDEN: &str = r#"{"legacy": {"calls": [["mount", "-t", "overlay", "overlay", "-o", "lowerdir={ROOT}/cache/images/9da61c54aae0042e073382218178c48a3c6b2e11c5b3a669f09716ed2e5ec5ed/rootfs,upperdir={ROOT}/mounts/sbx.sandbox-3/upper,workdir={ROOT}/mounts/sbx.sandbox-3/work", "{ROOT}/bundles/sbx.sandbox-3/rootfs"]], "config": "{\n  \"annotations\": {\n    \"dev.gvisor.internal.application-memory-directory\": \"sbx.sandbox-3\",\n    \"x\": \"y\"\n  },\n  \"linux\": {\n    \"cgroupsPath\": \"/ucloud-sandboxes/c14bc53ae2daa1de84e947f8b24e1597c04aed56589da184a5c643ba03788c2f\",\n    \"resources\": {\n      \"cpu\": {\n        \"quota\": 1\n      }\n    }\n  },\n  \"mounts\": [],\n  \"process\": {\n    \"args\": [],\n    \"cwd\": \"/w\",\n    \"env\": [\n      \"A=\\u00e9\",\n      \"B=\\ud83d\\ude00\"\n    ]\n  },\n  \"root\": {\n    \"path\": \"rootfs\",\n    \"readonly\": true\n  }\n}\n", "container_id": "c14bc53ae2daa1de84e947f8b24e1597c04aed56589da184a5c643ba03788c2f", "memory_directory": "sbx.sandbox-3", "overlay": "{\"backend_abi\":\"ucloud-host-erofs-environment-v1\",\"environment\":{\"base\":\"sha256:1111111111111111111111111111111111111111111111111111111111111111\",\"composition\":\"oci-overlay-v1\",\"schema\":1,\"toolkits\":[\"sha256:3333333333333333333333333333333333333333333333333333333333333333\"],\"workspace\":null},\"lowerdir\":\"{ROOT}/cache/images/9da61c54aae0042e073382218178c48a3c6b2e11c5b3a669f09716ed2e5ec5ed/rootfs\",\"rootfs_identity_sha256\":\"11a108c67b647857faaf233d87d84a246281f01293011822e2a4012f938b307a\",\"schema\":2}\n", "rootfs_sha256": "11a108c67b647857faaf233d87d84a246281f01293011822e2a4012f938b307a", "template": {"annotations": {"x": "y"}, "linux": {"resources": {"cpu": {"quota": 1}}}, "mounts": [], "process": {"args": [], "cwd": "/w", "env": ["A=\u00e9", "B=\ud83d\ude00"]}, "root": {"path": "x", "readonly": true}}, "writable": "{ROOT}/mounts/sbx.sandbox-3"}, "split": {"calls": [["mount", "-t", "overlay", "overlay", "-o", "lowerdir={ROOT}/cache/images/9da61c54aae0042e073382218178c48a3c6b2e11c5b3a669f09716ed2e5ec5ed/rootfs,upperdir={ROOT}/mounts/workspace-sbx.sandbox-3/upper,workdir={ROOT}/mounts/workspace-sbx.sandbox-3/work", "{ROOT}/bundles/sbx.sandbox-3/rootfs"]], "config": "{\n  \"annotations\": {\n    \"dev.gvisor.internal.application-memory-directory\": \"sbx.sandbox-3\",\n    \"x\": \"y\"\n  },\n  \"linux\": {\n    \"cgroupsPath\": \"/ucloud-sandboxes/c14bc53ae2daa1de84e947f8b24e1597c04aed56589da184a5c643ba03788c2f\",\n    \"resources\": {\n      \"cpu\": {\n        \"quota\": 1\n      }\n    }\n  },\n  \"mounts\": [],\n  \"process\": {\n    \"args\": [],\n    \"cwd\": \"/w\",\n    \"env\": [\n      \"A=\\u00e9\",\n      \"B=\\ud83d\\ude00\"\n    ]\n  },\n  \"root\": {\n    \"path\": \"rootfs\",\n    \"readonly\": true\n  }\n}\n", "container_id": "c14bc53ae2daa1de84e947f8b24e1597c04aed56589da184a5c643ba03788c2f", "memory_directory": "sbx.sandbox-3", "overlay": "{\"backend_abi\":\"ucloud-host-erofs-environment-v1\",\"environment\":{\"base\":\"sha256:1111111111111111111111111111111111111111111111111111111111111111\",\"composition\":\"oci-overlay-v1\",\"schema\":1,\"toolkits\":[\"sha256:3333333333333333333333333333333333333333333333333333333333333333\"],\"workspace\":null},\"lowerdir\":\"{ROOT}/cache/images/9da61c54aae0042e073382218178c48a3c6b2e11c5b3a669f09716ed2e5ec5ed/rootfs\",\"rootfs_identity_sha256\":\"11a108c67b647857faaf233d87d84a246281f01293011822e2a4012f938b307a\",\"schema\":2}\n", "rootfs_sha256": "11a108c67b647857faaf233d87d84a246281f01293011822e2a4012f938b307a", "template": {"annotations": {"dev.gvisor.internal.application-memory-directory": "sbx.sandbox-3", "x": "y"}, "linux": {"resources": {"cpu": {"quota": 1}}}, "mounts": [], "process": {"args": [], "cwd": "/w", "env": ["A=\u00e9", "B=\ud83d\ude00"]}, "root": {"path": "x", "readonly": true}}, "writable": "{ROOT}/mounts/workspace-sbx.sandbox-3"}}"#;
