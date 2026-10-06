//! The warm image path against a cache laid out as Python's
//! `EnvironmentRootfsStore` leaves it, with a fake environment backend on a
//! Unix socket and a fake `mountpoint` (a marker file beside the rootfs).
//!
//! RECEIPT_A and RECEIPT_B are canonical receipts Python wrote for two
//! config-only sibling images over one composition (`environment_root_digest`,
//! `EnvironmentManifest.sha256` and the HOST_EROFS fingerprint from the
//! repository's Python); OVERLAY is Python's `.ucloud-overlay.json` for it.

use std::os::fd::AsRawFd;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use ucloud_noded::image::{
    EnvironmentBackendClient, ImageError, ImageStore, MountProbe, Resolution, environment_root_digest,
};
use ucloud_noded::rootfs::overlay_metadata;

const IMAGE_ID: &str = "sha256:dcf7f0453232511dea3fec26678deca21ed138fb7aafd107a26369912f9a8fdc";
const FINGERPRINT: &str = "ab45da577a6d79d4f165a630a0f6b6f6d9c496e1517bde48f30346c247987d34";
const REF_A: &str = "registry.example:5000/env:task@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const REF_B: &str = "registry.example:5000/env:other@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";

#[derive(Clone, Default)]
struct Backend {
    requests: Arc<Mutex<Vec<Value>>>,
    /// Answer every ensure with this error text instead of a path.
    error: Arc<Mutex<Option<String>>>,
    /// Answer with this raw line instead.
    raw: Arc<Mutex<Option<Vec<u8>>>>,
}

impl Backend {
    fn spawn(&self, path: &Path) {
        let listener = UnixListener::bind(path).unwrap();
        let backend = self.clone();
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let backend = backend.clone();
                tokio::spawn(async move {
                    let (read, mut write) = stream.into_split();
                    let mut line = String::new();
                    BufReader::new(read).read_line(&mut line).await.unwrap();
                    let request: Value = serde_json::from_str(&line).unwrap();
                    // The client sends canonical JSON and a newline.
                    assert_eq!(line, ucloud_noded::pyjson::dumps(&request) + "\n");
                    backend.requests.lock().unwrap().push(request.clone());
                    let raw = backend.raw.lock().unwrap().clone();
                    let response = match (raw, backend.error.lock().unwrap().clone(), request["method"].as_str()) {
                        (Some(raw), _, _) => raw,
                        (None, Some(error), Some("ensure")) => format!("{}\n", json!({"error": error})).into_bytes(),
                        (None, _, Some("ensure")) => {
                            let hex = &request["digest"].as_str().unwrap()[7..];
                            format!("{}\n", json!({"result": format!("/components/{hex}")})).into_bytes()
                        }
                        (None, _, _) => b"{\"result\":true}\n".to_vec(),
                    };
                    write.write_all(&response).await.unwrap();
                });
            }
        });
    }

    fn methods(&self) -> Vec<(String, String)> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .map(|r| (r["method"].as_str().unwrap().to_string(), r["digest"].as_str().unwrap()[7..8].to_string()))
            .collect()
    }
}

struct Fixture {
    root: PathBuf,
    cache: PathBuf,
    backend: Backend,
    store: ImageStore,
}

impl Fixture {
    async fn new(name: &str) -> Fixture {
        let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let root = std::env::temp_dir().join(format!("noded-image-{name}-{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let probe = root.join("mountpoint");
        std::fs::write(&probe, "#!/bin/sh\n[ \"$1\" = --quiet ] || exit 2\n[ -e \"$2.mounted\" ] && exit 0\nexit 1\n").unwrap();
        std::fs::set_permissions(&probe, std::fs::Permissions::from_mode(0o755)).unwrap();
        let backend = Backend::default();
        let socket = root.join("io.sock");
        backend.spawn(&socket);
        let cache = root.join("cache");
        let store = ImageStore::new(&cache, EnvironmentBackendClient::new(&socket), MountProbe::Command(probe)).unwrap();
        Fixture { root, cache, backend, store }
    }

    /// Python's materialization: the receipt, the rootfs directory and (here) its mount marker.
    fn materialize(&self, receipt: &str, mounted: bool) -> PathBuf {
        let target = self.cache.join("images").join(&IMAGE_ID[7..]);
        std::fs::create_dir_all(target.join("rootfs")).unwrap();
        std::fs::write(target.join("environment.json"), receipt).unwrap();
        if mounted {
            std::fs::write(target.join("rootfs.mounted"), "").unwrap();
        }
        target.join("rootfs")
    }

    fn lock_is_shared(&self, digest: &str) -> bool {
        let file = std::fs::File::open(self.cache.join("locks").join(format!("{}.lock", &digest[7..]))).unwrap();
        // SAFETY: a valid descriptor.
        let exclusive = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0;
        !exclusive
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn root_of(receipt: &str) -> String {
    serde_json::from_str::<Value>(receipt).unwrap()["root"].as_str().unwrap().to_string()
}

#[test]
fn identities_match_python() {
    for receipt in [RECEIPT_A, RECEIPT_B] {
        let raw: Value = serde_json::from_str(receipt).unwrap();
        // Receipts are canonical; floats keep Python's repr.
        assert_eq!(ucloud_noded::pyjson::dumps(&raw), receipt);
        let resolution = Resolution::from_receipt(&raw).unwrap();
        assert_eq!(environment_root_digest(&raw["environment"]), raw["root"]);
        assert_eq!(resolution.image_id(), IMAGE_ID);
        assert_eq!(resolution.manifest.rootfs_fingerprint(), FINGERPRINT);
        assert_eq!(resolution.to_receipt(), raw);
    }
    let mut tampered: Value = serde_json::from_str(RECEIPT_A).unwrap();
    tampered["environment"]["image_config"]["Cmd"] = json!(["/bin/evil"]);
    let error = Resolution::from_receipt(&tampered).unwrap_err();
    assert_eq!(error.to_string(), "environment receipt root identity changed");
    let mut extra: Value = serde_json::from_str(RECEIPT_A).unwrap();
    extra["more"] = json!(1);
    assert!(Resolution::from_receipt(&extra).is_err());
}

#[tokio::test]
async fn a_warm_pinned_image_is_leased_and_fenced_until_dropped() {
    let fixture = Fixture::new("warm").await;
    let rootfs = fixture.materialize(RECEIPT_A, true);
    let lease = fixture.store.lease(REF_A, Some(&root_of(RECEIPT_A))).await.unwrap();
    let image = &lease.image;
    assert_eq!(image.image_id, IMAGE_ID);
    assert_eq!(image.rootfs_identity_sha256, FINGERPRINT);
    assert_eq!(image.rootfs, rootfs);
    assert_eq!(image.image_ref, REF_A);
    assert_eq!(image.image_config.command, ["/bin/a"]);
    assert_eq!(image.image_config.env, ["A=\u{e9}"]);
    assert_eq!((image.image_config.working_dir.as_str(), image.image_config.user.as_str()), ("/w", "alice"));
    // Every component in mount order (a toolkit repeats the base): Python's liveness check.
    let expected: Vec<(String, String)> = ["1", "2", "3", "1"].iter().map(|d| ("ensure".to_string(), d.to_string())).collect();
    assert_eq!(fixture.backend.methods(), expected);
    // The image lease holds the GC fence; component leases are released.
    assert!(fixture.lock_is_shared(IMAGE_ID));
    assert!(!fixture.lock_is_shared(&format!("sha256:{}", "1".repeat(64))));
    // Python's wake reads the overlay metadata derived from this image.
    let mut python_view = image.clone();
    python_view.rootfs = PathBuf::from(format!("/cache/images/{}/rootfs", &IMAGE_ID[7..]));
    assert_eq!(overlay_metadata(&python_view), OVERLAY);
    drop(lease);
    assert!(!fixture.lock_is_shared(IMAGE_ID));
    // Without a dispatched root, a receipt cannot say whether its root was the
    // annotation: the agent resolves the pinned reference once.
    assert!(matches!(fixture.store.lease(REF_A, None).await, Err(ImageError::NotMaterialized(_))));
    let resolution: Value = serde_json::from_str(RECEIPT_A).unwrap();
    drop(fixture.store.lease_resolved(REF_A, None, &resolution).await.unwrap());
    let lease = fixture.store.lease(REF_A, None).await.unwrap();
    assert_eq!(lease.image.image_config.command, ["/bin/a"]);
}

#[tokio::test]
async fn cold_unmounted_and_unpinned_images_are_not_leased() {
    let fixture = Fixture::new("cold").await;
    assert!(matches!(fixture.store.lease(REF_A, None).await, Err(ImageError::NotMaterialized(_))));
    assert!(matches!(
        fixture.store.lease("registry.example:5000/env:task", None).await,
        Err(ImageError::MutableReference(_))
    ));
    assert!(matches!(fixture.store.lease(REF_A, Some("sha256:bad")).await, Err(ImageError::Invalid(_))));
    fixture.materialize(RECEIPT_A, false);
    assert!(matches!(fixture.store.lease(REF_A, Some(&root_of(RECEIPT_A))).await, Err(ImageError::NotMaterialized(_))));
    let resolution: Value = serde_json::from_str(RECEIPT_A).unwrap();
    assert!(matches!(fixture.store.lease_resolved(REF_A, None, &resolution).await, Err(ImageError::NotMaterialized(_))));
    assert!(fixture.backend.methods().is_empty());
    // Another root for the same reference is not this receipt's.
    let other = format!("sha256:{}", "9".repeat(64));
    fixture.materialize(RECEIPT_A, true);
    assert!(matches!(fixture.store.lease(REF_A, Some(&other)).await, Err(ImageError::NotMaterialized(_))));
    // A mutable tag with a dispatched root is fine once Python resolved it.
    let mut tagged: Value = serde_json::from_str(RECEIPT_A).unwrap();
    tagged["source"] = json!("registry.example:5000/env:task");
    let tagged_ref = "registry.example:5000/env:task";
    let lease = fixture.store.lease_resolved(tagged_ref, Some(&root_of(RECEIPT_A)), &tagged).await.unwrap();
    drop(lease);
    fixture.store.lease(tagged_ref, Some(&root_of(RECEIPT_A))).await.unwrap();
    assert!(matches!(fixture.store.lease(tagged_ref, None).await, Err(ImageError::MutableReference(_))));
}

#[tokio::test]
async fn a_config_only_sibling_needs_the_agents_resolution_once() {
    let fixture = Fixture::new("sibling").await;
    fixture.materialize(RECEIPT_A, true);
    let root_b = root_of(RECEIPT_B);
    // One receipt per composition: the sibling's root is unknown to the receipts.
    assert!(matches!(fixture.store.lease(REF_B, Some(&root_b)).await, Err(ImageError::NotMaterialized(_))));
    let resolution: Value = serde_json::from_str(RECEIPT_B).unwrap();
    // A resolution must belong to the reference and root it serves.
    assert!(matches!(fixture.store.lease_resolved(REF_A, None, &resolution).await, Err(ImageError::Invalid(_))));
    assert!(matches!(
        fixture.store.lease_resolved(REF_B, Some(&root_of(RECEIPT_A)), &resolution).await,
        Err(ImageError::Invalid(_))
    ));
    let lease = fixture.store.lease_resolved(REF_B, Some(&root_b), &resolution).await.unwrap();
    assert_eq!(lease.image.image_config.command, ["/bin/b"]);
    assert_eq!(lease.image.image_id, IMAGE_ID);
    drop(lease);
    // Remembered for the next create, alongside the receipt's own image.
    assert_eq!(fixture.store.lease(REF_B, Some(&root_b)).await.unwrap().image.image_config.command, ["/bin/b"]);
    assert_eq!(fixture.store.lease(REF_A, Some(&root_of(RECEIPT_A))).await.unwrap().image.image_config.command, ["/bin/a"]);
    // A dispatched root never answers a lease without one (Python would read the annotation).
    assert!(matches!(fixture.store.lease(REF_B, None).await, Err(ImageError::NotMaterialized(_))));
    // A collected image is forgotten: the next lease misses.
    std::fs::remove_dir_all(fixture.cache.join("images").join(&IMAGE_ID[7..])).unwrap();
    assert!(matches!(fixture.store.lease(REF_B, Some(&root_b)).await, Err(ImageError::NotMaterialized(_))));
}

#[tokio::test]
async fn device_exhaustion_drops_every_component_and_is_a_capacity_error() {
    let fixture = Fixture::new("capacity").await;
    fixture.materialize(RECEIPT_A, true);
    let root = root_of(RECEIPT_A);
    *fixture.backend.error.lock().unwrap() = Some("attach failed: no available environment block device".into());
    let error = fixture.store.lease(REF_A, Some(&root)).await.unwrap_err();
    assert!(matches!(error, ImageError::DeviceCapacity(_)), "{error}");
    let methods = fixture.backend.methods();
    assert_eq!(methods[0], ("ensure".to_string(), "1".to_string()));
    let dropped: Vec<_> = methods[1..].iter().map(|(method, digest)| format!("{method}:{digest}")).collect();
    assert_eq!(dropped, ["drop:1", "drop:2", "drop:3"]);
    *fixture.backend.error.lock().unwrap() = Some("environment backend is closed".into());
    assert!(matches!(fixture.store.lease(REF_A, Some(&root)).await, Err(ImageError::Backend(message)) if message == "environment backend is closed"));
    *fixture.backend.raw.lock().unwrap() = Some(vec![b'x'; 70 * 1024]);
    let error = fixture.store.lease(REF_A, Some(&root)).await.unwrap_err();
    assert_eq!(error.to_string(), "invalid environment backend response size");
    *fixture.backend.raw.lock().unwrap() = Some(b"{\"result\":\"/x\"}".to_vec());
    assert!(fixture.store.lease(REF_A, Some(&root)).await.is_err(), "a response needs its newline");
}

#[tokio::test]
async fn the_kernel_probe_reads_statx_mount_root() {
    use ucloud_noded::image::{mount_present, statx_mount_root};
    let directory = std::env::temp_dir().join(format!("noded-image-statx-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    // Kernels before 5.8 cannot answer; production then runs `mountpoint`.
    if let Some(mounted) = statx_mount_root(Path::new("/")) {
        assert!(mounted);
        assert_eq!(statx_mount_root(&directory), Some(false));
        assert!(mount_present(Path::new("/"), &MountProbe::Kernel).await.unwrap());
        assert!(!mount_present(&directory, &MountProbe::Kernel).await.unwrap());
    }
    std::fs::remove_dir_all(&directory).unwrap();
}

const RECEIPT_A: &str = r#"{"environment":{"environment":{"base":"sha256:1111111111111111111111111111111111111111111111111111111111111111","composition":"oci-overlay-v1","schema":1,"toolkits":["sha256:3333333333333333333333333333333333333333333333333333333333333333","sha256:1111111111111111111111111111111111111111111111111111111111111111"],"workspace":"sha256:2222222222222222222222222222222222222222222222222222222222222222"},"image_config":{"Cmd":["/bin/a"],"Env":["A=\u00e9"],"Healthcheck":{"Big":1e+16,"Interval":1.5e-05,"Retries":3},"Labels":null,"User":"alice","WorkingDir":"/w"},"producer_key":"sha256:5555555555555555555555555555555555555555555555555555555555555555","schema":"ucloud-immutable-environment-v1","signature":"c2lnbmF0dXJl","source_image":"sha256:4444444444444444444444444444444444444444444444444444444444444444"},"root":"sha256:2867766e072d4524a08cc1344c4f5f41efbeff45a58d9efef9aeed5ed956e2e3","source":"registry.example:5000/env:task@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}"#;

const RECEIPT_B: &str = r#"{"environment":{"environment":{"base":"sha256:1111111111111111111111111111111111111111111111111111111111111111","composition":"oci-overlay-v1","schema":1,"toolkits":["sha256:3333333333333333333333333333333333333333333333333333333333333333","sha256:1111111111111111111111111111111111111111111111111111111111111111"],"workspace":"sha256:2222222222222222222222222222222222222222222222222222222222222222"},"image_config":{"Cmd":["/bin/b"]},"producer_key":"sha256:5555555555555555555555555555555555555555555555555555555555555555","schema":"ucloud-immutable-environment-v1","signature":"c2lnbmF0dXJl","source_image":"sha256:4444444444444444444444444444444444444444444444444444444444444444"},"root":"sha256:0afdf0ec1010ee89c07323297f956d69cdd19e54ae821a58764a3cda486f2b8b","source":"registry.example:5000/env:other@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"}"#;

const OVERLAY: &str = r#"{"backend_abi":"ucloud-host-erofs-environment-v1","environment":{"base":"sha256:1111111111111111111111111111111111111111111111111111111111111111","composition":"oci-overlay-v1","schema":1,"toolkits":["sha256:3333333333333333333333333333333333333333333333333333333333333333","sha256:1111111111111111111111111111111111111111111111111111111111111111"],"workspace":"sha256:2222222222222222222222222222222222222222222222222222222222222222"},"lowerdir":"/cache/images/dcf7f0453232511dea3fec26678deca21ed138fb7aafd107a26369912f9a8fdc/rootfs","rootfs_identity_sha256":"ab45da577a6d79d4f165a630a0f6b6f6d9c496e1517bde48f30346c247987d34","schema":2}
"#;
