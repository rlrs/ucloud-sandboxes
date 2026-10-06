//! The warm path of the production image store, `EnvironmentRootfsStore`
//! (ucloud_sandboxes/environment_rootfs.py), and its environment backend
//! client (environment_backend.py `EnvironmentBackendClient`). Create-pipeline
//! spec S2.2 and A.8; protocols spec §2.
//!
//! Python resolves an image reference against the managed registry (signed
//! environment metadata over HTTP), composes its components into
//! `<cache>/images/<hex>/rootfs` and writes the receipt
//! `<cache>/images/<hex>/environment.json`. The Rust create only *uses* an
//! image Python already materialized:
//!
//! - it finds the image from the receipts (`source` and the dispatched
//!   `root`), or from a resolution the Python agent hands over
//!   (`lease_resolved`, needed for a config-only sibling image and for a
//!   pinned reference without a dispatched root);
//! - it takes the shared image flock `<cache>/locks/<hex>.lock`, held by the
//!   returned `ImageLease` until the create finishes (the image GC fence);
//! - it requires the composed rootfs to be mounted, and asks the backend to
//!   `ensure` every component under shared component flocks (the liveness
//!   check Python runs on every lease).
//!
//! Anything else is `ImageError::NotMaterialized`: the caller asks the Python
//! agent to materialize (and resolve) the image, retries once with
//! `lease_resolved`, and otherwise forwards the create.
//! A mutable tag without a dispatched root is `ImageError::MutableReference`
//! (Python re-resolves those over HTTP on every create). No registry HTTP and
//! no signature verification here: receipts are written by the local agent
//! after it authenticated them, and the root digest re-derived from a receipt
//! binds its contents.

use std::collections::{BTreeSet, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{self, Read};
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

use crate::fsutil::ensure_private_dir;
use crate::oci::is_digest;
use crate::rootfs::json_canonical;

pub const HOST_EROFS_ABI: &str = "ucloud-host-erofs-environment-v1";
pub const DEFAULT_BACKEND_SOCKET: &str = "/run/ucloud-environment/io.sock";
/// The backend's text for device exhaustion: a capacity error, retryable elsewhere.
pub const NO_BLOCK_DEVICE: &str = "no available environment block device";
pub const MAX_RPC_BYTES: usize = 64 * 1024;
pub const BACKEND_TIMEOUT: Duration = Duration::from_secs(120);
const OCI_IMAGE: &str = "application/vnd.oci.image.manifest.v1+json";
const ENVIRONMENT_MEDIA_TYPE: &str = "application/vnd.ucloud.environment.v1+json";
const ENVIRONMENT_SCHEMA: &str = "ucloud-immutable-environment-v1";
/// A receipt holds one signed environment (its image config at most 16 MiB).
const MAX_RECEIPT_BYTES: u64 = 64 * 1024 * 1024;
const MOUNT_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug)]
pub enum ImageError {
    /// No mounted composition for this image (or this resolution) yet.
    NotMaterialized(String),
    /// A mutable tag with no dispatched root: only Python can resolve it.
    MutableReference(String),
    /// Python `EnvironmentDeviceCapacityError` (a DirectRegistryCapacityUnavailable): 503, retry elsewhere.
    DeviceCapacity(String),
    /// The backend answered `{"error": ...}` (Python RuntimeError): 503.
    Backend(String),
    /// A malformed receipt, reference or response (Python ValueError).
    Invalid(String),
    /// The mount probe failed (Python DirectWardenError).
    Mount(String),
    Io(io::Error),
}

impl std::fmt::Display for ImageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ImageError::NotMaterialized(message) => write!(f, "image is not materialized: {message}"),
            ImageError::MutableReference(image) => write!(f, "mutable image reference needs registry resolution: {image}"),
            ImageError::DeviceCapacity(message)
            | ImageError::Backend(message)
            | ImageError::Invalid(message)
            | ImageError::Mount(message) => f.write_str(message),
            ImageError::Io(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ImageError {}

impl From<io::Error> for ImageError {
    fn from(error: io::Error) -> Self {
        ImageError::Io(error)
    }
}

fn invalid(message: impl Into<String>) -> ImageError {
    ImageError::Invalid(message.into())
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes).iter().map(|b| format!("{b:02x}")).collect()
}

fn require_digest(value: &Value) -> Result<String, ImageError> {
    match value.as_str() {
        Some(text) if is_digest(text) => Ok(text.to_string()),
        _ => Err(invalid("immutable environment requires a SHA256 digest")),
    }
}

/// environment_manifest.py `EnvironmentManifest`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvironmentManifest {
    pub base: String,
    pub workspace: Option<String>,
    pub toolkits: Vec<String>,
}

impl EnvironmentManifest {
    pub fn from_value(raw: &Value) -> Result<Self, ImageError> {
        let raw = raw.as_object().ok_or_else(|| invalid("invalid environment manifest fields"))?;
        let keys: BTreeSet<&str> = raw.keys().map(String::as_str).collect();
        if keys != BTreeSet::from(["schema", "composition", "base", "workspace", "toolkits"]) {
            return Err(invalid("invalid environment manifest fields"));
        }
        let toolkits = raw["toolkits"].as_array().ok_or_else(|| invalid("environment toolkits must be an ordered list"))?;
        if raw["schema"].as_i64() != Some(1) {
            return Err(invalid("unsupported environment manifest schema"));
        }
        if raw["composition"].as_str() != Some("oci-overlay-v1") {
            return Err(invalid("unsupported environment composition"));
        }
        let digest = |value: &Value| {
            value.as_str().filter(|text| is_digest(text)).map(str::to_string).ok_or_else(|| {
                invalid("environment components require immutable sha256 digests")
            })
        };
        Ok(EnvironmentManifest {
            base: digest(&raw["base"])?,
            workspace: if raw["workspace"].is_null() { None } else { Some(digest(&raw["workspace"])?) },
            toolkits: toolkits.iter().map(digest).collect::<Result<_, _>>()?,
        })
    }

    pub fn to_value(&self) -> Value {
        json!({"schema": 1, "composition": "oci-overlay-v1", "base": self.base,
            "workspace": self.workspace, "toolkits": self.toolkits})
    }

    /// The composition's identity: sha256 of its canonical JSON.
    pub fn sha256(&self) -> String {
        sha256_hex(json_canonical(&self.to_value()).as_bytes())
    }

    /// `rootfs_fingerprint(HOST_EROFS_ABI)`.
    pub fn rootfs_fingerprint(&self) -> String {
        sha256_hex(format!("{HOST_EROFS_ABI}\0{}", self.sha256()).as_bytes())
    }

    /// Base, optional workspace seed, then toolkits: the mount order.
    pub fn components(&self) -> Vec<String> {
        std::iter::once(self.base.clone()).chain(self.workspace.clone()).chain(self.toolkits.iter().cloned()).collect()
    }
}

/// image_rootfs.py `DockerImageConfig`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ImageConfig {
    pub entrypoint: Vec<String>,
    pub command: Vec<String>,
    pub env: Vec<String>,
    pub working_dir: String,
    pub user: String,
}

impl ImageConfig {
    /// `DockerImageConfig.from_inspection`: Docker's `Config` object.
    pub fn from_inspection(raw: &Value) -> Result<Self, ImageError> {
        let raw = raw.as_object().ok_or_else(|| invalid("Docker image Config is invalid"))?;
        let strings = |name: &str| -> Result<Vec<String>, ImageError> {
            match raw.get(name) {
                None | Some(Value::Null) => Ok(vec![]),
                Some(Value::Array(items)) => items
                    .iter()
                    .map(|item| item.as_str().map(str::to_string))
                    .collect::<Option<Vec<_>>>()
                    .ok_or_else(|| invalid(format!("Docker image Config.{name} is invalid"))),
                Some(_) => Err(invalid(format!("Docker image Config.{name} is invalid"))),
            }
        };
        let string = |name: &str| -> Result<String, ImageError> {
            match raw.get(name) {
                None | Some(Value::Null) => Ok(String::new()),
                Some(Value::String(text)) => Ok(text.clone()),
                Some(_) => Err(invalid(format!("Docker image Config.{name} is invalid"))),
            }
        };
        let config = ImageConfig {
            entrypoint: strings("Entrypoint")?,
            command: strings("Cmd")?,
            env: strings("Env")?,
            working_dir: string("WorkingDir")?,
            user: string("User")?,
        };
        for (label, values) in [("entrypoint", &config.entrypoint), ("command", &config.command), ("env", &config.env)] {
            if values.iter().any(|value| value.contains('\0')) {
                return Err(invalid(format!("Docker image {label} is invalid")));
            }
        }
        if config.working_dir.contains('\0') || config.user.contains('\0') {
            return Err(invalid("Docker image process configuration is invalid"));
        }
        Ok(config)
    }
}

/// image_rootfs.py `MaterializedRootfs` for the HOST_EROFS store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedRootfs {
    pub image_ref: String,
    /// `"sha256:" + environment.sha256()`.
    pub image_id: String,
    /// `sha256(HOST_EROFS_ABI + "\0" + environment.sha256())`.
    pub rootfs_identity_sha256: String,
    /// `<cache>/images/<hex>/rootfs`, the shared read-only lower.
    pub rootfs: PathBuf,
    pub image_config: ImageConfig,
    pub environment: EnvironmentManifest,
    pub backend_abi: &'static str,
}

/// One signed environment resolution in receipt form:
/// `{"environment": ImmutableEnvironment.to_dict(), "root": <root digest>, "source": <image ref>}`.
#[derive(Debug, Clone)]
pub struct Resolution {
    pub root: String,
    pub source: String,
    pub environment: Value,
    pub manifest: EnvironmentManifest,
    pub image_config: ImageConfig,
}

impl Resolution {
    /// Python `_load`'s checks without the signature: exact key sets,
    /// `ImmutableEnvironment.from_dict`, and the root digest re-derived from
    /// the environment (it binds the image config and component list).
    pub fn from_receipt(raw: &Value) -> Result<Self, ImageError> {
        let receipt = raw.as_object().ok_or_else(|| invalid("invalid immutable image receipt"))?;
        let keys: BTreeSet<&str> = receipt.keys().map(String::as_str).collect();
        if keys != BTreeSet::from(["root", "source", "environment"]) {
            return Err(invalid("invalid immutable image receipt"));
        }
        let source = receipt["source"].as_str().ok_or_else(|| invalid("invalid immutable image receipt"))?.to_string();
        let environment = &receipt["environment"];
        let fields = environment.as_object().ok_or_else(|| invalid("invalid immutable environment metadata"))?;
        let keys: BTreeSet<&str> = fields.keys().map(String::as_str).collect();
        if keys != BTreeSet::from(["schema", "source_image", "environment", "image_config", "producer_key", "signature"])
            || fields["schema"].as_str() != Some(ENVIRONMENT_SCHEMA)
        {
            return Err(invalid("invalid immutable environment metadata"));
        }
        require_digest(&fields["source_image"])?;
        require_digest(&fields["producer_key"])?;
        let image_config = ImageConfig::from_inspection(&fields["image_config"])?;
        let manifest = EnvironmentManifest::from_value(&fields["environment"])?;
        if manifest.toolkits.len() > 32 {
            return Err(invalid("environment component count exceeds mount bound"));
        }
        if !fields["signature"].is_string() {
            return Err(invalid("environment composition signature did not verify"));
        }
        let root = environment_root_digest(environment);
        if receipt["root"].as_str() != Some(root.as_str()) {
            return Err(invalid("environment receipt root identity changed"));
        }
        Ok(Resolution { root, source, environment: environment.clone(), manifest, image_config })
    }

    pub fn image_id(&self) -> String {
        format!("sha256:{}", self.manifest.sha256())
    }

    pub fn to_receipt(&self) -> Value {
        json!({"environment": self.environment, "root": self.root, "source": self.source})
    }
}

/// environment_artifact.py `environment_root_digest`: the digest of the OCI
/// manifest whose config blob is the canonical environment.
pub fn environment_root_digest(environment: &Value) -> String {
    let config = json_canonical(environment);
    let manifest = json!({"schemaVersion": 2, "mediaType": OCI_IMAGE, "layers": [], "config": {
        "mediaType": ENVIRONMENT_MEDIA_TYPE,
        "digest": format!("sha256:{}", sha256_hex(config.as_bytes())),
        "size": config.len(),
    }});
    format!("sha256:{}", sha256_hex(json_canonical(&manifest).as_bytes()))
}

/// managed_registry.py `manifest_digest_from_image_ref`: the normalized
/// `@sha256:` pin of a reference, if any.
pub fn manifest_digest_from_image_ref(image_ref: &str) -> Option<String> {
    let (_, digest) = image_ref.trim().rsplit_once('@')?;
    let digest = digest.trim().to_lowercase();
    is_digest(&digest).then_some(digest)
}

// ---------------------------------------------------------------------------
// flock leases

/// A held flock on `<cache>/locks/<hex>.lock`; released on drop.
#[derive(Debug)]
pub struct Flock {
    file: File,
}

impl Flock {
    /// Python `_lease`: `open(O_CREAT|O_RDWR|O_NOFOLLOW, 0o600)` + `flock(LOCK_SH or LOCK_EX)`. Blocks.
    pub fn acquire_blocking(path: &Path, exclusive: bool) -> io::Result<Flock> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)?;
        let operation = if exclusive { libc::LOCK_EX } else { libc::LOCK_SH };
        loop {
            // SAFETY: a valid descriptor owned by `file`.
            if unsafe { libc::flock(file.as_raw_fd(), operation) } == 0 {
                return Ok(Flock { file });
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }

    /// The same, off the async workers: an exclusive holder (cold
    /// materialization or GC) can keep it for minutes.
    pub async fn acquire(path: PathBuf, exclusive: bool) -> io::Result<Flock> {
        tokio::task::spawn_blocking(move || Flock::acquire_blocking(&path, exclusive))
            .await
            .map_err(|error| io::Error::other(format!("lock task failed: {error}")))?
    }
}

impl Drop for Flock {
    fn drop(&mut self) {
        // SAFETY: as above; closing the file releases the lock anyway.
        unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
    }
}

// ---------------------------------------------------------------------------
// Mount status (mount_status.py, image_rootfs.py `_mount_present`)

/// How to tell whether a path is a mount root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MountProbe {
    /// statx `STATX_ATTR_MOUNT_ROOT`, falling back to `mountpoint --quiet` when
    /// the kernel cannot answer (production).
    Kernel,
    /// Only `<binary> --quiet <path>` (Python's injected runner; tests).
    Command(PathBuf),
}

/// `linux_mount_root`: Some(mounted), or None when statx cannot answer.
pub fn statx_mount_root(path: &Path) -> Option<bool> {
    const AT_SYMLINK_NOFOLLOW: libc::c_int = 0x100;
    const AT_NO_AUTOMOUNT: libc::c_int = 0x800;
    const STATX_BASIC_STATS: libc::c_uint = 0x7ff;
    const STATX_ATTR_MOUNT_ROOT: u64 = 0x2000;
    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    // The full 256-byte struct statx; stx_attributes at 8, stx_attributes_mask at 56.
    let mut buffer = [0u64; 32];
    // SAFETY: a valid C string and a 256-byte, 8-aligned output buffer.
    let result = unsafe {
        libc::syscall(
            libc::SYS_statx,
            libc::AT_FDCWD,
            path.as_ptr(),
            AT_SYMLINK_NOFOLLOW | AT_NO_AUTOMOUNT,
            STATX_BASIC_STATS,
            buffer.as_mut_ptr(),
        )
    };
    if result != 0 {
        return None;
    }
    let (attributes, mask) = (buffer[1], buffer[7]);
    if mask & STATX_ATTR_MOUNT_ROOT == 0 {
        return None;
    }
    Some(attributes & STATX_ATTR_MOUNT_ROOT != 0)
}

/// `_mount_present(path, runner, "mountpoint")`: exit 0 mounted, 1 or 32 not,
/// anything else an error ("could not inspect overlay mount").
pub async fn mount_present(path: &Path, probe: &MountProbe) -> Result<bool, ImageError> {
    let binary = match probe {
        MountProbe::Kernel => {
            if let Some(mounted) = statx_mount_root(path) {
                return Ok(mounted);
            }
            PathBuf::from("mountpoint")
        }
        MountProbe::Command(binary) => binary.clone(),
    };
    let argv = vec![binary.to_string_lossy().into_owned(), "--quiet".into(), path.to_string_lossy().into_owned()];
    let result = crate::runsc::run(&argv, MOUNT_TIMEOUT).await.map_err(|error| ImageError::Mount(error.to_string()))?;
    match result.returncode {
        0 => Ok(true),
        1 | 32 => Ok(false),
        _ => Err(ImageError::Mount(format!(
            "could not inspect overlay mount: {}",
            if result.stderr.is_empty() { &result.stdout } else { &result.stderr }
        ))),
    }
}

// ---------------------------------------------------------------------------
// Environment backend client

/// `EnvironmentBackendClient`: one connection per call, newline-delimited
/// canonical JSON, a 64 KiB response line.
#[derive(Debug, Clone)]
pub struct EnvironmentBackendClient {
    pub path: PathBuf,
    pub timeout: Duration,
}

impl EnvironmentBackendClient {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        EnvironmentBackendClient { path: path.into(), timeout: BACKEND_TIMEOUT }
    }

    /// A full accept queue refuses a non-blocking AF_UNIX connect with EAGAIN;
    /// retry from 5 ms, doubling to 100 ms, within the deadline.
    async fn connect(&self, deadline: Instant) -> io::Result<UnixStream> {
        let mut delay = Duration::from_millis(5);
        loop {
            match UnixStream::connect(&self.path).await {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    if Instant::now() + delay >= deadline {
                        return Err(error);
                    }
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_millis(100));
                }
                other => return other,
            }
        }
    }

    pub async fn call(&self, request: &Value) -> Result<Value, ImageError> {
        let deadline = Instant::now() + self.timeout;
        let exchange = async {
            let mut stream = self.connect(deadline).await?;
            let mut frame = json_canonical(request).into_bytes();
            frame.push(b'\n');
            stream.write_all(&frame).await?;
            // readline(MAX_RPC_BYTES + 1)
            let mut line = Vec::new();
            let mut chunk = [0u8; 8192];
            while line.len() <= MAX_RPC_BYTES && !line.contains(&b'\n') {
                let read = stream.read(&mut chunk).await?;
                if read == 0 {
                    break;
                }
                line.extend_from_slice(&chunk[..read]);
            }
            if let Some(end) = line.iter().position(|b| *b == b'\n') {
                line.truncate(end + 1);
            }
            Ok::<Vec<u8>, io::Error>(line)
        };
        let line = tokio::time::timeout(self.timeout, exchange)
            .await
            .map_err(|_| ImageError::Io(io::Error::new(io::ErrorKind::TimedOut, "environment backend timed out")))??;
        if line.len() > MAX_RPC_BYTES || !line.ends_with(b"\n") {
            return Err(invalid("invalid environment backend response size"));
        }
        let response: Value = serde_json::from_slice(&line).map_err(|_| invalid("invalid environment backend response"))?;
        let response = response.as_object().ok_or_else(|| invalid("invalid environment backend response"))?;
        if let Some(error) = response.get("error") {
            let message = error.as_str().map(str::to_string).unwrap_or_else(|| error.to_string());
            if message.contains(NO_BLOCK_DEVICE) {
                return Err(ImageError::DeviceCapacity(message));
            }
            return Err(ImageError::Backend(message));
        }
        response.get("result").cloned().ok_or_else(|| invalid("invalid environment backend response"))
    }

    /// `ensure(digest)`: attach and mount a component; returns its mount path.
    pub async fn ensure(&self, digest: &str) -> Result<PathBuf, ImageError> {
        require_digest(&json!(digest))?;
        let result = self.call(&json!({"method": "ensure", "digest": digest})).await?;
        result.as_str().map(PathBuf::from).ok_or_else(|| invalid("invalid environment backend response"))
    }

    /// `drop(digest)`: release an unused component; false while still in use.
    pub async fn drop_component(&self, digest: &str) -> Result<bool, ImageError> {
        require_digest(&json!(digest))?;
        let result = self.call(&json!({"method": "drop", "digest": digest})).await?;
        result.as_bool().ok_or_else(|| invalid("invalid environment backend response"))
    }
}

// ---------------------------------------------------------------------------
// The store

/// A materialized image held for one create: the shared image flock stays
/// taken until this is dropped, so GC cannot remove the lower underneath.
#[derive(Debug)]
pub struct ImageLease {
    pub image: MaterializedRootfs,
    _lease: Flock,
}

/// What the store knows how to lease without asking the agent.
#[derive(Default)]
struct Index {
    /// (image reference, signed root): every receipt, and every resolution
    /// the agent handed over.
    by_root: HashMap<(String, String), Resolution>,
    /// Pinned references the agent resolved without a dispatched root (from
    /// the manifest's annotation, immutable for a pinned manifest). Receipts
    /// cannot serve these: a receipt does not say whether its root was
    /// dispatched or annotated.
    annotated: HashMap<String, Resolution>,
}

pub struct ImageStore {
    root: PathBuf,
    images: PathBuf,
    locks: PathBuf,
    backend: EnvironmentBackendClient,
    probe: MountProbe,
    index: Mutex<Index>,
}

impl ImageStore {
    /// The `--image-cache-root` store; its directories must be private.
    pub fn new(root: impl Into<PathBuf>, backend: EnvironmentBackendClient, probe: MountProbe) -> Result<Self, ImageError> {
        let root = root.into();
        if !root.is_absolute() {
            return Err(invalid("environment image store must be absolute"));
        }
        let (images, locks) = (root.join("images"), root.join("locks"));
        for path in [&root, &images, &locks] {
            ensure_private_dir(path)?;
        }
        Ok(ImageStore { root, images, locks, backend, probe, index: Mutex::new(Index::default()) })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn lock_path(&self, digest: &str) -> PathBuf {
        self.locks.join(format!("{}.lock", &digest[7..]))
    }

    fn read_receipt(&self, hex: &str) -> Result<Option<Resolution>, ImageError> {
        let path = self.images.join(hex).join("environment.json");
        let file = match OpenOptions::new().read(true).custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC).open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let mut bytes = Vec::new();
        file.take(MAX_RECEIPT_BYTES + 1).read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_RECEIPT_BYTES {
            return Err(invalid("invalid immutable image receipt"));
        }
        let raw: Value = serde_json::from_slice(&bytes).map_err(|_| invalid("invalid immutable image receipt"))?;
        Ok(Some(Resolution::from_receipt(&raw)?))
    }

    /// Re-read every receipt into the index (after a miss). Unreadable or
    /// foreign entries are skipped: a lease re-validates what it uses.
    fn refresh(&self) -> io::Result<()> {
        let mut found = Vec::new();
        for entry in std::fs::read_dir(&self.images)? {
            let entry = entry?;
            let Some(hex) = entry.file_name().to_str().map(str::to_string) else { continue };
            if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) {
                continue;
            }
            if let Ok(Some(resolution)) = self.read_receipt(&hex)
                && resolution.manifest.sha256() == hex
            {
                found.push(resolution);
            }
        }
        let mut index = self.index.lock().expect("index lock");
        for resolution in found {
            index.by_root.insert((resolution.source.clone(), resolution.root.clone()), resolution);
        }
        Ok(())
    }

    fn lookup(&self, image_ref: &str, environment_root: Option<&str>) -> Option<Resolution> {
        let index = self.index.lock().expect("index lock");
        match environment_root {
            Some(root) => index.by_root.get(&(image_ref.to_string(), root.to_string())).cloned(),
            None => index.annotated.get(image_ref).cloned(),
        }
    }

    fn forget(&self, resolution: &Resolution) {
        let mut index = self.index.lock().expect("index lock");
        index.by_root.remove(&(resolution.source.clone(), resolution.root.clone()));
        if index.annotated.get(&resolution.source).is_some_and(|known| known.root == resolution.root) {
            index.annotated.remove(&resolution.source);
        }
    }

    fn remember(&self, environment_root: Option<&str>, resolution: Resolution) {
        let mut index = self.index.lock().expect("index lock");
        if environment_root.is_none() {
            index.annotated.insert(resolution.source.clone(), resolution.clone());
        }
        index.by_root.insert((resolution.source.clone(), resolution.root.clone()), resolution);
    }

    fn check_reference(image_ref: &str, environment_root: Option<&str>) -> Result<(), ImageError> {
        if let Some(root) = environment_root
            && !is_digest(root)
        {
            return Err(invalid("environment_root must be a sha256 digest."));
        }
        if environment_root.is_none() && manifest_digest_from_image_ref(image_ref).is_none() {
            return Err(ImageError::MutableReference(image_ref.to_string()));
        }
        Ok(())
    }

    /// `operation_lease(image_ref, environment_root)`, warm path only. With a
    /// dispatched root the receipts serve it; without one, only a pinned
    /// reference the agent already resolved in this process (`lease_resolved`).
    pub async fn lease(&self, image_ref: &str, environment_root: Option<&str>) -> Result<ImageLease, ImageError> {
        Self::check_reference(image_ref, environment_root)?;
        let resolution = match self.lookup(image_ref, environment_root) {
            Some(resolution) => resolution,
            None if environment_root.is_none() => {
                return Err(ImageError::NotMaterialized(format!("{image_ref} has no known annotated root yet")));
            }
            None => {
                self.refresh()?;
                self.lookup(image_ref, environment_root).ok_or_else(|| {
                    ImageError::NotMaterialized(format!("no receipt for {image_ref} (root {environment_root:?})"))
                })?
            }
        };
        let result = self.lease_with(image_ref, &resolution).await;
        if matches!(result, Err(ImageError::NotMaterialized(_))) {
            self.forget(&resolution);
        }
        result
    }

    /// A lease from a resolution the Python agent returned (receipt form):
    /// config-only sibling images share one composition and one receipt, so a
    /// receipt alone cannot serve the second sibling's root; and without a
    /// dispatched root only the agent knows the annotated one. Resolutions of
    /// a root or a pinned reference are remembered for later creates.
    pub async fn lease_resolved(
        &self,
        image_ref: &str,
        environment_root: Option<&str>,
        resolution: &Value,
    ) -> Result<ImageLease, ImageError> {
        let resolution = Resolution::from_receipt(resolution)?;
        if resolution.source != image_ref {
            return Err(invalid("environment resolution belongs to another image reference"));
        }
        if environment_root.is_some_and(|root| root != resolution.root) {
            return Err(invalid("environment resolution belongs to another root"));
        }
        let lease = self.lease_with(image_ref, &resolution).await?;
        if Self::check_reference(image_ref, environment_root).is_ok() {
            self.remember(environment_root, resolution);
        }
        Ok(lease)
    }

    async fn lease_with(&self, image_ref: &str, resolution: &Resolution) -> Result<ImageLease, ImageError> {
        let image_id = resolution.image_id();
        let hex = image_id[7..].to_string();
        let lease = Flock::acquire(self.lock_path(&image_id), false).await?;
        let target = self.images.join(&hex);
        let rootfs = target.join("rootfs");
        if !target.join("environment.json").exists() || !mount_present(&rootfs, &self.probe).await? {
            return Err(ImageError::NotMaterialized(format!("{image_id} has no mounted composition")));
        }
        let existing = self
            .read_receipt(&hex)?
            .ok_or_else(|| ImageError::NotMaterialized(format!("{image_id} lost its receipt")))?;
        if existing.image_id() != image_id {
            return Err(invalid("environment image identity changed"));
        }
        // A composition is a filesystem; config-only siblings share it.
        if existing.manifest != resolution.manifest {
            return Err(invalid("environment components changed for an existing composition"));
        }
        self.mount(&image_id, &resolution.manifest, &rootfs).await?;
        Ok(ImageLease {
            image: MaterializedRootfs {
                image_ref: image_ref.to_string(),
                image_id,
                rootfs_identity_sha256: resolution.manifest.rootfs_fingerprint(),
                rootfs,
                image_config: resolution.image_config.clone(),
                environment: resolution.manifest.clone(),
                backend_abi: HOST_EROFS_ABI,
            },
            _lease: lease,
        })
    }

    /// `_mount` for a mounted composition: shared component flocks, then the
    /// backend `ensure` of every component in order (a liveness and fencing
    /// check after a frontend restart). On device exhaustion every component
    /// is dropped under an exclusive flock before the error returns.
    async fn mount(&self, image_id: &str, manifest: &EnvironmentManifest, rootfs: &Path) -> Result<(), ImageError> {
        let components = manifest.components();
        let unique: BTreeSet<&String> = components.iter().collect();
        let result = async {
            let mut leases = Vec::with_capacity(unique.len());
            for digest in &unique {
                leases.push(Flock::acquire(self.lock_path(digest), false).await?);
            }
            match std::fs::DirBuilder::new().mode(0o700).create(rootfs) {
                Err(error) if error.kind() != io::ErrorKind::AlreadyExists => return Err(error.into()),
                _ => {}
            }
            if !mount_present(rootfs, &self.probe).await? {
                // Composing (mounting) the components is the Python agent's.
                return Err(ImageError::NotMaterialized(format!("{image_id} has no mounted composition")));
            }
            for digest in &components {
                self.backend.ensure(digest).await?;
            }
            drop(leases);
            Ok(())
        }
        .await;
        if let Err(ImageError::DeviceCapacity(_)) = &result {
            for digest in &unique {
                let released = async {
                    let _exclusive = Flock::acquire(self.lock_path(digest), true).await?;
                    self.backend.drop_component(digest).await
                }
                .await;
                if let Err(error) = released {
                    eprintln!("ucloud-noded: could not release component after device exhaustion: {digest}: {error}");
                }
            }
        }
        result
    }
}
