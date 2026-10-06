//! The per-sandbox overlay, as ucloud_sandboxes/image_rootfs.py
//! `OverlayRootfsManager` makes it (`discard_unregistered`, `prepare`):
//! create-pipeline spec S7. Python's park, wake and delete read what this
//! writes: `<bundle>/config.json` and `<bundle>/.ucloud-overlay.json`.
//!
//! Also Python's `json.dumps(indent=2, sort_keys=True)`, byte for byte
//! (`ensure_ascii` escapes and float `repr` as in `pyjson`).

use std::fs::OpenOptions;
use std::io::{self, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::fsutil::{euid, fsync_dir};
use crate::image::{MaterializedRootfs, MountProbe, mount_present};
use crate::oci::MEMORY_DIRECTORY_ANNOTATION;
use crate::pyjson::number_repr;

// ---------------------------------------------------------------------------
// Python json.dumps

fn write_string(text: &str, out: &mut String) {
    out.push('"');
    for character in text.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if (' '..='~').contains(&c) => out.push(c),
            c => {
                let mut units = [0u16; 2];
                for unit in c.encode_utf16(&mut units) {
                    out.push_str(&format!("\\u{unit:04x}"));
                }
            }
        }
    }
    out.push('"');
}

fn write_json(value: &Value, indent: Option<usize>, level: usize, out: &mut String) {
    let newline = |out: &mut String, level: usize| {
        if let Some(width) = indent {
            out.push('\n');
            out.push_str(&" ".repeat(width * level));
        }
    };
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(flag) => out.push_str(if *flag { "true" } else { "false" }),
        Value::Number(number) => out.push_str(&number_repr(number)),
        Value::String(text) => write_string(text, out),
        Value::Array(items) if items.is_empty() => out.push_str("[]"),
        Value::Object(map) if map.is_empty() => out.push_str("{}"),
        Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                newline(out, level + 1);
                write_json(item, indent, level + 1, out);
            }
            newline(out, level);
            out.push(']');
        }
        Value::Object(map) => {
            // Code point order is UTF-8 byte order.
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push('{');
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                newline(out, level + 1);
                write_string(key, out);
                out.push_str(if indent.is_some() { ": " } else { ":" });
                write_json(&map[key], indent, level + 1, out);
            }
            newline(out, level);
            out.push('}');
        }
    }
}

/// `json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)`.
pub fn json_canonical(value: &Value) -> String {
    crate::pyjson::dumps(value)
}

/// `json.dumps(value, indent=2, sort_keys=True)`.
pub fn json_indent2(value: &Value) -> String {
    let mut out = String::new();
    write_json(value, Some(2), 0, &mut out);
    out
}

// ---------------------------------------------------------------------------
// The overlay

pub const OVERLAY_METADATA: &str = ".ucloud-overlay.json";
const COMMAND_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug)]
pub enum RootfsError {
    /// Python ValueError (a bad incarnation or workspace reference): 400.
    Invalid(String),
    /// Python DirectWardenError: 503.
    Warden(String),
    /// Rollback after a failed prepare could not unmount the overlay; the
    /// bundle and writable directories were left in place.
    UnmountFailed { message: String, cause: Box<RootfsError> },
    Io(io::Error),
}

impl std::fmt::Display for RootfsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RootfsError::Invalid(message) | RootfsError::Warden(message) => f.write_str(message),
            RootfsError::UnmountFailed { message, cause } => write!(f, "{message} (after: {cause})"),
            RootfsError::Io(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for RootfsError {}

impl From<io::Error> for RootfsError {
    fn from(error: io::Error) -> Self {
        RootfsError::Io(error)
    }
}

impl From<crate::image::ImageError> for RootfsError {
    fn from(error: crate::image::ImageError) -> Self {
        RootfsError::Warden(error.to_string())
    }
}

fn warden(message: impl Into<String>) -> RootfsError {
    RootfsError::Warden(message.into())
}

/// `[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}`.
fn safe_id(value: &str) -> bool {
    let bytes = value.as_bytes();
    (1..=128).contains(&bytes.len())
        && bytes[0].is_ascii_alphanumeric()
        && bytes.iter().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b':' | b'-'))
}

pub fn incarnation(sandbox_id: &str, generation: u64) -> String {
    format!("{sandbox_id}.sandbox-{generation}")
}

/// `sha256(f"{sandbox_id}:{generation}")`, the runsc container id.
pub fn container_id(sandbox_id: &str, generation: u64) -> String {
    Sha256::digest(format!("{sandbox_id}:{generation}").as_bytes()).iter().map(|b| format!("{b:02x}")).collect()
}

fn is_symlink(path: &Path) -> bool {
    std::fs::symlink_metadata(path).map(|meta| meta.file_type().is_symlink()).unwrap_or(false)
}

fn require_real_directory(path: &Path) -> Result<(), RootfsError> {
    if !path.is_dir() || is_symlink(path) {
        return Err(warden("rootfs path must be a real directory"));
    }
    Ok(())
}

fn require_private_directory(path: &Path) -> Result<(), RootfsError> {
    require_real_directory(path)?;
    let meta = std::fs::symlink_metadata(path)?;
    if meta.uid() != euid() || meta.mode() & 0o022 != 0 {
        return Err(warden("rootfs store directory must be owned and private"));
    }
    Ok(())
}

/// `shutil.rmtree`: refuses a symlink, never follows one below.
fn rmtree(path: &Path) -> io::Result<()> {
    if is_symlink(path) {
        return Err(io::Error::other(format!("Cannot call rmtree on a symbolic link: {}", path.display())));
    }
    std::fs::remove_dir_all(path)
}

fn rmtree_ignoring_errors(path: &Path) {
    let _ = rmtree(path);
}

/// image_rootfs.py `_atomic_write`: a private temp file beside `path`
/// (fchmod 0600, write, fsync, rename). The caller fsyncs the directory.
fn write_replace(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let directory = path.parent().ok_or_else(|| io::Error::other("path has no parent"))?;
    let name = path.file_name().and_then(|n| n.to_str()).ok_or_else(|| io::Error::other("invalid file name"))?;
    loop {
        let random = crate::guest::random_bytes::<6>()?;
        let suffix: String = random.iter().map(|b| format!("{b:02x}")).collect();
        let temporary = directory.join(format!(".{name}.{suffix}.tmp"));
        let mut file = match OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(&temporary)
        {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        };
        let result = (|| {
            file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
            file.write_all(bytes)?;
            file.sync_all()?;
            std::fs::rename(&temporary, path)
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&temporary);
        }
        return result;
    }
}

/// The `.ucloud-overlay.json` bytes for a HOST_EROFS image (schema 2):
/// Python's wake decodes exactly these keys and re-derives the fingerprint.
pub fn overlay_metadata(image: &MaterializedRootfs) -> String {
    let metadata = json!({
        "backend_abi": image.backend_abi,
        "environment": image.environment.to_value(),
        "lowerdir": image.rootfs.to_string_lossy(),
        "rootfs_identity_sha256": image.rootfs_identity_sha256,
        "schema": 2,
    });
    json_canonical(&metadata) + "\n"
}

/// What `prepare` made; the registry commit takes these names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OverlayLease {
    pub sandbox_id: String,
    pub sandbox_generation: u64,
    pub container_id: String,
    pub rootfs_sha256: String,
    pub image_id: String,
    pub bundle: PathBuf,
    pub memory_directory: String,
    pub workspace_directory: String,
    pub writable: PathBuf,
    pub upper: PathBuf,
    pub work: PathBuf,
    pub merged: PathBuf,
}

/// `OverlayRootfsManager` (no migration imports: those stay in Python).
#[derive(Debug, Clone)]
pub struct OverlayManager {
    /// The storage daemon's volume mount root (`--volume-mount-root`).
    pub writable_root: PathBuf,
    /// `<state_root>/bundles`.
    pub bundle_root: PathBuf,
    /// Production: the storage daemon creates (and quota-accounts) the
    /// writable root; this manager only fills it.
    pub require_precreated_writable: bool,
    pub mount_binary: PathBuf,
    pub umount_binary: PathBuf,
    pub probe: MountProbe,
}

impl OverlayManager {
    pub fn new(writable_root: PathBuf, bundle_root: PathBuf, require_precreated_writable: bool) -> Result<Self, RootfsError> {
        for path in [&writable_root, &bundle_root] {
            if !path.is_absolute() {
                return Err(RootfsError::Invalid("overlay roots must be absolute".into()));
            }
            std::fs::DirBuilder::new().recursive(true).mode(0o700).create(path)?;
            require_private_directory(path)?;
        }
        Ok(OverlayManager {
            writable_root,
            bundle_root,
            require_precreated_writable,
            mount_binary: "mount".into(),
            umount_binary: "umount".into(),
            probe: MountProbe::Kernel,
        })
    }

    async fn command(&self, argv: Vec<String>) -> Result<crate::runsc::CommandResult, RootfsError> {
        crate::runsc::run(&argv, COMMAND_TIMEOUT).await.map_err(|error| warden(error.to_string()))
    }

    async fn umount(&self, path: &Path) -> Result<crate::runsc::CommandResult, RootfsError> {
        self.command(vec![self.umount_binary.to_string_lossy().into_owned(), path.to_string_lossy().into_owned()]).await
    }

    async fn unmount_if_mounted(&self, path: &Path) -> Result<(), RootfsError> {
        if !mount_present(path, &self.probe).await? {
            return Ok(());
        }
        let result = self.umount(path).await?;
        if result.returncode != 0 {
            return Err(warden(format!("overlay unmount failed: {}", output(&result))));
        }
        Ok(())
    }

    fn writable(&self, incarnation: &str, workspace_directory: &str) -> Result<PathBuf, RootfsError> {
        if !workspace_directory.is_empty() && workspace_directory != format!("workspace-{incarnation}") {
            return Err(RootfsError::Invalid("workspace directory belongs to another incarnation".into()));
        }
        Ok(self.writable_root.join(if workspace_directory.is_empty() { incarnation } else { workspace_directory }))
    }

    /// `discard_unregistered`: remove the overlay of a create that crashed
    /// before `rootfs_ready` (unmount and remove the bundle; empty the
    /// precreated writable root's `upper` and `work`).
    pub async fn discard_unregistered(&self, sandbox_id: &str, generation: u64, workspace_directory: &str) -> Result<(), RootfsError> {
        if !safe_id(sandbox_id) {
            return Err(RootfsError::Invalid("sandbox incarnation is invalid".into()));
        }
        let incarnation = incarnation(sandbox_id, generation);
        let bundle = self.bundle_root.join(&incarnation);
        if bundle.exists() {
            self.unmount_if_mounted(&bundle.join("rootfs")).await?;
            rmtree(&bundle)?;
        }
        let writable = self.writable(&incarnation, workspace_directory)?;
        if writable.exists() {
            if self.require_precreated_writable {
                for name in ["upper", "work"] {
                    let path = writable.join(name);
                    if path.exists() {
                        rmtree(&path)?;
                    }
                }
            } else {
                rmtree(&writable)?;
            }
        }
        Ok(())
    }

    /// `prepare`: mount `<bundle>/rootfs` as an overlay of the image lower and
    /// the writable root's `upper`/`work`, then write `config.json` (indent 2)
    /// and `.ucloud-overlay.json` and fsync the bundle. Split creates pass the
    /// memory allocation id, which the template's annotation must already
    /// carry; the legacy layout gets the incarnation as its memory directory.
    pub async fn prepare(
        &self,
        sandbox_id: &str,
        generation: u64,
        image: &MaterializedRootfs,
        config_template: &Value,
        workspace_directory: &str,
        memory_allocation_id: Option<&str>,
    ) -> Result<OverlayLease, RootfsError> {
        if !safe_id(sandbox_id) || generation < 1 {
            return Err(RootfsError::Invalid("sandbox incarnation is invalid".into()));
        }
        let incarnation = incarnation(sandbox_id, generation);
        let container_id = container_id(sandbox_id, generation);
        if workspace_directory.is_empty() != memory_allocation_id.is_none() {
            return Err(RootfsError::Invalid("split rootfs requires both component references".into()));
        }
        let writable = self.writable(&incarnation, workspace_directory)?;
        let bundle = self.bundle_root.join(&incarnation);
        let (upper, work, merged) = (writable.join("upper"), writable.join("work"), bundle.join("rootfs"));
        if bundle.exists() {
            return Err(warden("overlay sandbox incarnation already exists"));
        }
        let owned = !self.require_precreated_writable;
        if self.require_precreated_writable {
            if !writable.exists() {
                return Err(warden("quota-owned writable incarnation was not prepared"));
            }
            require_private_directory(&writable)?;
            if std::fs::read_dir(&writable)?.next().is_some() {
                return Err(warden("quota-owned writable incarnation is not empty"));
            }
        } else if writable.exists() {
            return Err(warden("overlay sandbox incarnation already exists"));
        }
        let private = || {
            let mut builder = std::fs::DirBuilder::new();
            builder.mode(0o700);
            builder
        };
        if owned {
            private().create(&writable)?;
        }
        private().create(&bundle)?;
        for path in [&upper, &work, &merged] {
            private().create(path)?;
        }
        // Overlayfs exposes the upper directory inode as the mounted root: a
        // private one would stop every non-root OCI user traversing "/".
        let image_root = std::fs::metadata(&image.rootfs)?;
        let upper_meta = std::fs::metadata(&upper)?;
        if upper_meta.uid() != image_root.uid() || upper_meta.gid() != image_root.gid() {
            std::os::unix::fs::chown(&upper, Some(image_root.uid()), Some(image_root.gid()))?;
        }
        std::fs::set_permissions(&upper, std::fs::Permissions::from_mode(image_root.mode() & 0o7777))?;

        let mut mounted = false;
        let result = async {
            let options = format!("lowerdir={},upperdir={},workdir={}", image.rootfs.display(), upper.display(), work.display());
            let argv = vec![
                self.mount_binary.to_string_lossy().into_owned(),
                "-t".into(),
                "overlay".into(),
                "overlay".into(),
                "-o".into(),
                options,
                merged.to_string_lossy().into_owned(),
            ];
            let result = self.command(argv).await?;
            if result.returncode != 0 {
                return Err(warden(format!("overlay mount failed: {}", output(&result))));
            }
            mounted = true;
            let config = finish_config(config_template, &container_id, &incarnation, memory_allocation_id)?;
            write_replace(&bundle.join("config.json"), (json_indent2(&config) + "\n").as_bytes())?;
            write_replace(&bundle.join(OVERLAY_METADATA), overlay_metadata(image).as_bytes())?;
            fsync_dir(&bundle)?;
            Ok(())
        }
        .await;
        if let Err(error) = result {
            if mounted {
                let cleanup = self.umount(&merged).await;
                let failure = match cleanup {
                    Ok(cleanup) if cleanup.returncode == 0 => None,
                    Ok(cleanup) => Some(output(&cleanup)),
                    Err(cleanup) => Some(cleanup.to_string()),
                };
                if let Some(failure) = failure {
                    return Err(RootfsError::UnmountFailed {
                        message: format!("overlay preparation failed and its mount could not be released: {failure}"),
                        cause: Box::new(error),
                    });
                }
            }
            rmtree_ignoring_errors(&bundle);
            if owned {
                rmtree_ignoring_errors(&writable);
            } else {
                rmtree_ignoring_errors(&upper);
                rmtree_ignoring_errors(&work);
            }
            return Err(error);
        }
        Ok(OverlayLease {
            sandbox_id: sandbox_id.to_string(),
            sandbox_generation: generation,
            container_id,
            rootfs_sha256: image.rootfs_identity_sha256.clone(),
            image_id: image.image_id.clone(),
            bundle,
            memory_directory: incarnation,
            workspace_directory: workspace_directory.to_string(),
            writable,
            upper,
            work,
            merged,
        })
    }
}

fn output(result: &crate::runsc::CommandResult) -> String {
    if result.stderr.is_empty() { result.stdout.clone() } else { result.stderr.clone() }
}

fn object<'a>(config: &'a mut serde_json::Map<String, Value>, key: &str) -> Result<&'a mut serde_json::Map<String, Value>, RootfsError> {
    config
        .entry(key)
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .ok_or_else(|| warden(format!("runtime spec {key} must be an object")))
}

/// The config template plus the incarnation's cgroup, root and memory directory.
fn finish_config(
    template: &Value,
    container_id: &str,
    memory_directory: &str,
    memory_allocation_id: Option<&str>,
) -> Result<Value, RootfsError> {
    let mut config = template.clone();
    let map = config.as_object_mut().ok_or_else(|| warden("runtime spec must be an object"))?;
    object(map, "linux")?.insert("cgroupsPath".into(), json!(format!("/ucloud-sandboxes/{container_id}")));
    let root = object(map, "root")?;
    root.insert("path".into(), json!("rootfs"));
    root.entry("readonly").or_insert(json!(false));
    match memory_allocation_id {
        // Legacy layout: the incarnation names the memory directory.
        None => {
            object(map, "annotations")?.insert(MEMORY_DIRECTORY_ANNOTATION.into(), json!(memory_directory));
        }
        Some(allocation) => {
            let annotation = map.get("annotations").and_then(|annotations| annotations.get(MEMORY_DIRECTORY_ANNOTATION));
            if annotation.and_then(Value::as_str) != Some(allocation) {
                return Err(warden("runtime spec lacks its memory allocation identity"));
            }
        }
    }
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoders_match_python_json_dumps() {
        let value: Value =
            serde_json::from_str(r#"{"b": [1, 2.0, {"z": [], "a": {}}], "a": "é😀/\u007f\t\"", "c": null, "d": true, "e": 1e-05}"#)
                .unwrap();
        // python3 -c 'import json; print(json.dumps(v, sort_keys=True, separators=(",", ":")))'
        assert_eq!(
            json_canonical(&value),
            r#"{"a":"\u00e9\ud83d\ude00/\u007f\t\"","b":[1,2.0,{"a":{},"z":[]}],"c":null,"d":true,"e":1e-05}"#
        );
        // json.dumps(v, indent=2, sort_keys=True)
        assert_eq!(
            json_indent2(&value),
            "{\n  \"a\": \"\\u00e9\\ud83d\\ude00/\\u007f\\t\\\"\",\n  \"b\": [\n    1,\n    2.0,\n    {\n      \"a\": {},\n      \"z\": []\n    }\n  ],\n  \"c\": null,\n  \"d\": true,\n  \"e\": 1e-05\n}"
        );
        assert_eq!(json_indent2(&json!([])), "[]");
        assert_eq!(json_indent2(&json!("x")), "\"x\"");
    }

    #[test]
    fn config_gets_cgroup_root_and_memory_directory() {
        let template = json!({"root": {"path": "ignored", "readonly": true}, "annotations": {"a": "b"}});
        let config = finish_config(&template, "cid", "s.sandbox-1", None).unwrap();
        assert_eq!(config["linux"]["cgroupsPath"], "/ucloud-sandboxes/cid");
        assert_eq!(config["root"], json!({"path": "rootfs", "readonly": true}));
        assert_eq!(config["annotations"][MEMORY_DIRECTORY_ANNOTATION], "s.sandbox-1");
        let config = finish_config(&json!({}), "cid", "s.sandbox-1", None).unwrap();
        assert_eq!(config["root"], json!({"path": "rootfs", "readonly": false}));
        // Split: the template must already name the allocation.
        assert!(finish_config(&template, "cid", "s.sandbox-1", Some("s.sandbox-1")).is_err());
        let split = json!({"annotations": {MEMORY_DIRECTORY_ANNOTATION: "s.sandbox-1"}});
        let config = finish_config(&split, "cid", "s.sandbox-1", Some("s.sandbox-1")).unwrap();
        assert_eq!(config["annotations"][MEMORY_DIRECTORY_ANNOTATION], "s.sandbox-1");
        assert!(finish_config(&json!({"linux": []}), "cid", "x", None).is_err());
    }

    #[test]
    fn identities_match_python() {
        // hashlib.sha256(b"sbx:3").hexdigest()
        assert_eq!(container_id("sbx", 3), "c14bc53ae2daa1de84e947f8b24e1597c04aed56589da184a5c643ba03788c2f");
        assert_eq!(incarnation("sbx", 3), "sbx.sandbox-3");
        assert!(safe_id("a:b.c-d_e") && !safe_id("-a") && !safe_id(""));
    }
}
