//! The create pipeline the daemon runs between the agent's admit and finish:
//! direct_provisioner.py `create` and `_advance` (create-pipeline spec S2 to
//! S13), resumable from every registry phase so a gateway replay of a crashed
//! create continues where it stopped.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use hyper::{Method, StatusCode};
use serde_json::{Map, Value, json};

use crate::agent_rpc::AgentClient;
use crate::create::{Admitted, CreateError, Pipeline};
use crate::guest;
use crate::image::{EnvironmentBackendClient, ImageError, ImageLease, ImageStore, MountProbe};
use crate::memory_backing::{ActiveMode, MemoryBackingConfig, MemoryBackingError, MemoryBackingRef, MemoryBackingStore, XfsMemoryQuota};
use crate::network::{NetworkError, NetworkManager, TcpEgress};
use crate::oci::{self, NetworkMode, OciBuilder, OciError};
use crate::pause::{PauseConfig, PauseTier};
use crate::pipeline::{CreateConfig, spec_supported};
use crate::registry::{self, DiskClaim, MIB, Phase, PlanRequest, Quota, Registration, Registry, RegistryError, Rootfs};
use crate::rootfs::{OverlayManager, RootfsError};
use crate::storage::{StorageClient, StorageError, VolumeOwner};
use crate::timings::Timings;
use crate::warden::{MemoryMode, Sandbox, Warden, WardenConfig, WardenError};

impl From<RegistryError> for CreateError {
    fn from(error: RegistryError) -> Self {
        match error {
            RegistryError::RegistrationOwned(message) => CreateError::RegistrationOwned(message),
            RegistryError::CapacityUnavailable(message) => CreateError::Capacity(message),
            RegistryError::Invalid(message) => CreateError::Invalid(message),
            other => CreateError::Unavailable(other.message().to_string()),
        }
    }
}

impl From<MemoryBackingError> for CreateError {
    fn from(error: MemoryBackingError) -> Self {
        match error {
            MemoryBackingError::Busy(message) => CreateError::MemoryDraining(message),
            MemoryBackingError::Invalid(message) => CreateError::Invalid(message),
            // OS, SQLite and xfs_quota failures are the node's: ambiguous.
            other => CreateError::Unavailable(other.to_string()),
        }
    }
}

impl From<StorageError> for CreateError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::Capacity(message) => CreateError::Capacity(message),
            other => CreateError::Unavailable(other.to_string()),
        }
    }
}

impl From<ImageError> for CreateError {
    fn from(error: ImageError) -> Self {
        match error {
            ImageError::DeviceCapacity(message) => CreateError::Capacity(message),
            ImageError::Invalid(message) => CreateError::Invalid(message),
            other => CreateError::Unavailable(other.to_string()),
        }
    }
}

impl From<OciError> for CreateError {
    fn from(error: OciError) -> Self {
        match error {
            OciError::Config(message) | OciError::Unsupported(message) => CreateError::Invalid(message),
            other => CreateError::Unavailable(other.to_string()),
        }
    }
}

impl From<RootfsError> for CreateError {
    fn from(error: RootfsError) -> Self {
        CreateError::Unavailable(error.to_string())
    }
}

impl From<NetworkError> for CreateError {
    fn from(error: NetworkError) -> Self {
        CreateError::Unavailable(error.to_string())
    }
}

impl From<crate::journal::JournalError> for CreateError {
    fn from(error: crate::journal::JournalError) -> Self {
        CreateError::Unavailable(error.to_string())
    }
}

impl From<WardenError> for CreateError {
    fn from(error: WardenError) -> Self {
        CreateError::Unavailable(error.to_string())
    }
}

fn unavailable(message: impl Into<String>) -> CreateError {
    CreateError::Unavailable(message.into())
}

/// Run blocking work (SQLite, flocks, netlink-by-command) off the reactor.
async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, CreateError> + Send + 'static,
) -> Result<T, CreateError> {
    tokio::task::spawn_blocking(work).await.map_err(|error| unavailable(format!("create worker failed: {error}")))?
}

pub struct NodePipeline {
    pause: Option<Arc<PauseTier>>,
    /// The pause policy and local waits, while the daemon owns the pause tier.
    _pause_runtime: Option<crate::pause_runtime::PauseRuntime>,
    config: CreateConfig,
    agent: Arc<AgentClient>,
    registry: Arc<Registry>,
    memory: Option<Arc<MemoryBackingStore>>,
    storage: StorageClient,
    network: Option<Arc<NetworkManager>>,
    images: ImageStore,
    overlays: OverlayManager,
    oci: OciBuilder,
    warden: Arc<Warden>,
}

impl NodePipeline {
    /// Open everything the node's configuration names. The daemon becomes the
    /// registry owner here; the agent runs with `--registry-foreign`.
    pub fn open(config: CreateConfig, agent: Arc<AgentClient>) -> Result<NodePipeline, String> {
        let environment = config.environment.clone().ok_or("no environment image store")?;
        let hard_capacity_mb = if config.split_memory_backing {
            (config.memory_backing_hard_capacity_bytes / MIB as u64) as i64
        } else {
            0
        };
        let registry = Registry::owner(config.state_root.join(registry::REGISTRY_FILE_NAME), hard_capacity_mb)
            .map_err(|error| format!("registry: {error}"))?;
        let bound = registry
            .bind_runtime_compatibility(&config.runtime_compatibility_sha256)
            .map_err(|error| format!("registry compatibility: {error}"))?;
        if bound != config.runtime_compatibility_sha256 {
            return Err("the registry belongs to another runtime compatibility".into());
        }
        let memory = if config.split_memory_backing {
            let store = MemoryBackingStore::open(
                MemoryBackingConfig {
                    root: config.volume_mount_root.clone(),
                    journal: config.state_root.join("memory-backing.sqlite"),
                    hard_capacity_bytes: config.memory_backing_hard_capacity_bytes,
                    active_root: config.application_memory_root.clone(),
                    // The agent validated the tmpfs at assembly; accept either swap policy.
                    ram_swappable: !tmpfs_noswap(config.application_memory_root.as_deref()),
                },
                Box::new(XfsMemoryQuota::new()),
            )
            .map_err(|error| format!("memory backing: {error}"))?;
            Some(Arc::new(store))
        } else {
            None
        };
        let network_mode = if config.network == "none" { NetworkMode::None } else { NetworkMode::Sandbox };
        let network = match network_mode {
            NetworkMode::None => None,
            NetworkMode::Sandbox => {
                let egress = config
                    .direct_network_allow_tcp
                    .iter()
                    .map(|allowed| {
                        allowed
                            .ip
                            .parse()
                            .map(|address| TcpEgress { address, port: allowed.port })
                            .map_err(|_| format!("egress address {} is not IPv4", allowed.ip))
                    })
                    .collect::<Result<Vec<_>, String>>()?;
                Some(Arc::new(NetworkManager::new(
                    config.state_root.join("network-slots.json"),
                    PathBuf::from("/run/netns"),
                    egress,
                    config.relays_configured,
                )))
            }
        };
        let images = ImageStore::new(
            &config.image_cache_root,
            EnvironmentBackendClient::new(&environment.backend_socket),
            MountProbe::Kernel,
        )
        .map_err(|error| format!("image store: {error}"))?;
        let overlays = OverlayManager::new(config.volume_mount_root.clone(), config.bundle_root.clone(), true)
            .map_err(|error| format!("overlays: {error}"))?;
        let oci = OciBuilder::new(Some(config.init_binary.clone()), Some(config.managed_init_binary.clone()), network_mode)
            .map_err(|error| format!("OCI: {error}"))?;
        let warden_config = WardenConfig {
            runsc: config.runsc.clone(),
            runtime_root: config.runtime_root.clone(),
            bundle_root: config.bundle_root.clone(),
            journal_root: config.journal_root.clone(),
            memory_root: config.volume_mount_root.clone(),
            application_memory_root: config.application_memory_root.clone(),
            network: config.network.clone(),
            reflink_memory_restore: config.reflink_memory_restore,
            proc_root: PathBuf::from("/proc"),
            command_timeout: Duration::from_secs(60),
            stop_timeout: Duration::from_secs(30),
        };
        // Phase 3a: the daemon owns the pause tier only when the agent says so.
        let pause = config.pause.as_ref().filter(|pause| pause.rust_pause_enabled).map(|_| {
            let modes = memory.clone().map(|memory| memory as Arc<dyn crate::pause::ModeSource>);
            Arc::new(PauseTier::new(PauseConfig::new(warden_config.clone(), true), modes))
        });
        let warden = Warden::new(warden_config);
        let registry = Arc::new(registry);
        let pause_runtime = pause.as_ref().map(|tier| {
            let settings = config.pause.as_ref().map(|block| block.settings.clone()).unwrap_or_default();
            crate::pause_runtime::PauseRuntime::start(crate::pause_runtime::PauseInputs {
                agent: agent.clone(),
                tier: tier.clone(),
                registry: registry.clone(),
                state_root: config.state_root.clone(),
                warden_locks_dir: config.runtime_root.join("warden-locks"),
                cgroup_root: tier.config().cgroup_root.clone(),
                settings,
            })
        });
        Ok(NodePipeline {
            pause,
            _pause_runtime: pause_runtime,
            storage: StorageClient::new(&config.storage_native_socket),
            config,
            agent,
            registry,
            memory,
            network,
            images,
            overlays,
            oci,
            warden: Arc::new(warden),
        })
    }

    pub fn config(&self) -> &CreateConfig {
        &self.config
    }

    pub fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }

    pub fn warden(&self) -> &Arc<Warden> {
        &self.warden
    }

    /// The pause tier, when the daemon owns it (phase 3a).
    pub fn pause_tier(&self) -> Option<&Arc<PauseTier>> {
        self.pause.as_ref()
    }

    /// S2.2: a warm lease from the store's receipts, else the agent mounts
    /// the image once and hands over its resolution.
    async fn lease_image(&self, image: &str, root: Option<&str>) -> Result<ImageLease, CreateError> {
        match self.images.lease(image, root).await {
            Ok(lease) => return Ok(lease),
            Err(ImageError::NotMaterialized(_) | ImageError::MutableReference(_)) => {}
            Err(error) => return Err(error.into()),
        }
        let request = json!({"image": image, "environment_root": root});
        let reply = self
            .agent
            // A cold materialization pulls the image: the gateway's create budget.
            .call_within(Duration::from_secs(600), Method::POST, "/internal/v1/images/materialize", Some(&request))
            .await
            .map_err(|error| unavailable(error.to_string()))?;
        let body = reply.json().map_err(|error| unavailable(error.to_string()))?;
        if reply.status != StatusCode::OK {
            let message = body.get("error").and_then(Value::as_str).unwrap_or("image materialization failed").to_string();
            return Err(match (reply.status, body.get("error_code").and_then(Value::as_str)) {
                (_, Some("node_active_admission_deferred")) => CreateError::Capacity(message),
                (StatusCode::BAD_REQUEST, _) => CreateError::Invalid(message),
                _ => CreateError::Unavailable(message),
            });
        }
        let resolution = body.get("resolution").ok_or_else(|| unavailable("image materialization has no resolution"))?;
        Ok(self.images.lease_resolved(image, root, resolution).await?)
    }

    async fn run(&self, admitted: &Admitted, timings: &mut Timings) -> Result<bool, CreateError> {
        // The agent validated this spec. If Rust reads it differently, or would
        // store a row whose fingerprint Python does not reproduce, the agent
        // creates it instead: a row Python cannot re-encode breaks its index.
        let spec = registry::SandboxSpec::from_dict(&admitted.spec)
            .map_err(|error| CreateError::Unsupported(format!("spec: {error}")))?;
        if spec.sha256() != admitted.spec_hash {
            return Err(CreateError::Unsupported("the daemon's spec fingerprint differs from the agent's".into()));
        }
        let oci_spec = oci::SandboxSpec::from_value(&admitted.spec)
            .map_err(|error| CreateError::Unsupported(format!("spec: {error}")))?;
        let generation = admitted.generation;
        let started = timings.start();
        let image = self.lease_image(&oci_spec.image, oci_spec.environment_root.as_deref()).await?;
        timings.add("image_resolve", started);

        let started = timings.start();
        let initial_claim = match &admitted.initial_claim {
            Some(claim) => Some(DiskClaim::new(
                claim["workspace_mb"].as_i64().ok_or_else(|| unavailable("initial claim workspace_mb"))?,
                claim["memory_mb"].as_i64().ok_or_else(|| unavailable("initial claim memory_mb"))?,
            )?),
            None => None,
        };
        let request = PlanRequest {
            spec,
            sandbox_generation: generation as i64,
            operation_id: admitted.operation_id.clone(),
            runtime_compatibility_sha256: self.config.runtime_compatibility_sha256.clone(),
            split_memory_backing: admitted.split,
            initial_claim,
        };
        let registry = self.registry.clone();
        let mut registration = blocking(move || Ok(registry.plan(request)?)).await?;
        timings.add("registry_commit", started);
        if registration.runtime_compatibility_sha256 != self.config.runtime_compatibility_sha256 {
            return Err(unavailable("direct registration belongs to another runtime compatibility"));
        }

        // Python ensures the network once per _advance, on every phase.
        let mut networked = false;
        if matches!(registration.phase, Phase::Planned | Phase::QuotaReady) {
            registration = self.root(&registration, &oci_spec, &image, timings).await?;
            networked = true;
        }
        if !networked {
            self.ensure_network(&registration, timings).await?;
        }
        match registration.phase {
            Phase::RootfsReady => self.own(&registration, &oci_spec, timings).await,
            Phase::Owned => {
                let sandbox = self.sandbox(&registration);
                let journal = self.warden.journal().load(&sandbox.sandbox_id, generation)?;
                match journal.as_ref().and_then(|record| record.get("state")).and_then(Value::as_str) {
                    Some("running" | "parked" | "recovery-required") => Ok(false),
                    _ => Err(unavailable("owned sandbox has no settled lifecycle journal")),
                }
            }
            phase => Err(unavailable(format!("direct registration is {}", phase.as_str()))),
        }
    }

    fn sandbox(&self, registration: &Registration) -> Sandbox {
        Sandbox {
            sandbox_id: registration.sandbox_id().to_string(),
            generation: registration.sandbox_generation as u64,
            container_id: registration.container_id.clone(),
            bundle: PathBuf::from(&registration.bundle),
            memory_directory: registration.memory_directory.clone(),
            spec_sha256: registration.spec_sha256(),
        }
    }

    async fn ensure_network(&self, registration: &Registration, timings: &mut Timings) -> Result<Option<PathBuf>, CreateError> {
        let Some(network) = self.network.clone() else { return Ok(None) };
        let started = timings.start();
        let (id, generation) = (registration.sandbox_id().to_string(), registration.sandbox_generation as u64);
        let lease = blocking(move || Ok(network.ensure_direct(&id, generation)?)).await?;
        timings.add("network_ensure", started);
        Ok(Some(lease.namespace_path))
    }

    /// S3 to S8: storage, network, OCI config and rootfs, then `rootfs_ready`.
    async fn root(
        &self,
        registration: &Registration,
        spec: &oci::SandboxSpec,
        image: &ImageLease,
        timings: &mut Timings,
    ) -> Result<Arc<Registration>, CreateError> {
        let id = registration.sandbox_id().to_string();
        let generation = registration.sandbox_generation as u64;
        let split = !registration.memory_allocation_id.is_empty();
        let quota = if registration.phase == Phase::Planned {
            let started = timings.start();
            let quota = self.prepare_storage(registration, split).await?;
            timings.add("storage_prepare", started);
            Some(quota)
        } else {
            None
        };
        let namespace = self.ensure_network(registration, timings).await?;

        let started = timings.start();
        let mut config = self.oci.build(spec, &image.image, namespace.as_deref())?;
        if split {
            config["annotations"][oci::MEMORY_DIRECTORY_ANNOTATION] = json!(registration.memory_allocation_id);
        }
        timings.add("oci_build", started);

        let started = timings.start();
        self.overlays.discard_unregistered(&id, generation, &registration.workspace_directory).await?;
        let allocation = split.then_some(registration.memory_allocation_id.as_str());
        let lease = self
            .overlays
            .prepare(&id, generation, &image.image, &config, &registration.workspace_directory, allocation)
            .await?;
        timings.add("rootfs_prepare", started);
        let quota_path = quota.as_ref().map(|quota| quota.path.clone()).unwrap_or_else(|| registration.quota_path.clone());
        if lease.writable != PathBuf::from(&quota_path) {
            return Err(unavailable("rootfs writable directory does not match its storage quota"));
        }

        let started = timings.start();
        let rootfs = Rootfs {
            rootfs_sha256: image.image.rootfs_identity_sha256.clone(),
            container_id: lease.container_id.clone(),
            bundle: lease.bundle.display().to_string(),
            memory_directory: lease.memory_directory.clone(),
        };
        let (registry, revision, image_id) = (self.registry.clone(), registration.revision, image.image.image_id.clone());
        let committed = blocking(move || Ok(registry.commit_rootfs(&id, revision, &image_id, &rootfs, quota.as_ref())?)).await?;
        timings.add("registry_commit", started);
        Ok(committed)
    }

    /// S3 and S4: the split memory allocation and the workspace volume.
    async fn prepare_storage(&self, registration: &Registration, split: bool) -> Result<Quota, CreateError> {
        let id = registration.sandbox_id().to_string();
        let generation = registration.sandbox_generation;
        let requested_mb = registration.spec.requested_disk_mb().map_err(|error| CreateError::Invalid(error.to_string()))?;
        let disk_mb = registration.spec.disk_mb().ok_or_else(|| CreateError::Invalid("disk_mb is required".into()))?;
        let claim = if split {
            let registry = self.registry.clone();
            let claim_id = id.clone();
            let claim = blocking(move || Ok(registry.disk_claim(&claim_id, generation)?)).await?;
            let memory = self.memory.clone().ok_or_else(|| unavailable("split layout without memory backing"))?;
            let (allocation, quota_bytes) =
                registration.memory_reference().ok_or_else(|| unavailable("split registration has no memory reference"))?;
            let reference = MemoryBackingRef::new(allocation, quota_bytes as u64)?;
            let limit = claim.map(|claim| ((claim.memory_mb as u64) * MIB as u64).min(quota_bytes as u64));
            let memory_id = id.clone();
            blocking(move || Ok(memory.prepare(&reference, &memory_id, generation as u64, limit).map(|_| ())?)).await?;
            claim
        } else {
            None
        };
        let volume_id = registration.workspace_volume_id();
        let owner = VolumeOwner { volume_id: volume_id.clone(), sandbox_id: id.clone(), sandbox_generation: generation as u64 };
        let virtual_size = (if split { disk_mb } else { requested_mb }) as u64 * MIB as u64;
        let granted = claim.map(|claim| claim.workspace_mb as u64 * MIB as u64).filter(|granted| *granted < virtual_size);
        let record = self.storage.prepare_volume(&owner, &registration.operation_id, virtual_size, granted, Map::new()).await?;
        let owner = (volume_id.as_str(), id.as_str(), generation as u64);
        workspace_quota(&record, owner, virtual_size, &self.config.volume_mount_root, requested_mb)
    }

    /// S5 and S9 to S13: guest files, the runtime, then `owned`.
    async fn own(&self, registration: &Registration, spec: &oci::SandboxSpec, timings: &mut Timings) -> Result<bool, CreateError> {
        let sandbox = self.sandbox(registration);
        let generation = sandbox.generation;
        let journaled = self.warden.journal().load(&sandbox.sandbox_id, generation)?;
        let started_runtime = journaled.is_none();
        match journaled.as_ref().and_then(|record| record.get("state")).and_then(Value::as_str) {
            None => {
                let rootfs = sandbox.bundle.join("rootfs");
                // A replay after a reboot finds the overlay unmounted (spec §7).
                if !crate::image::mount_present(&rootfs, &MountProbe::Kernel).await? {
                    return Err(unavailable("rootfs_ready overlay is not mounted; the agent must recover it"));
                }
                let started = timings.start();
                let (guest_spec, guest_root, bundle) = (spec.clone(), rootfs.clone(), sandbox.bundle.clone());
                blocking(move || {
                    guest::prepare_workspace(&guest_root, &guest_spec)?;
                    let config: Value = serde_json::from_slice(&std::fs::read(bundle.join("config.json")).map_err(|e| unavailable(e.to_string()))?)
                        .map_err(|e| unavailable(e.to_string()))?;
                    if let Some(cwd) = config.pointer("/process/cwd").and_then(Value::as_str) {
                        guest::prepare_working_directory(&guest_root, cwd)?;
                    }
                    if guest_spec.network_mode() != NetworkMode::None {
                        guest::prepare_network_files(&guest_root, &guest_spec)?;
                    }
                    Ok(())
                })
                .await?;
                timings.add("guest_files", started);
                let started = timings.start();
                let (init, managed) = (self.config.init_binary.clone(), self.config.managed_init_binary.clone());
                let (init_enabled, managed_enabled) = (
                    spec.security.init && !spec.managed_process,
                    spec.managed_process || spec.filesystem.management_helper == "static",
                );
                blocking(move || {
                    guest::install_init(&rootfs, Some(&init), init_enabled)?;
                    guest::install_managed_init(&rootfs, Some(&managed), managed_enabled)?;
                    Ok(())
                })
                .await?;
                timings.add("init_install", started);

                let warden = self.warden.clone();
                let discard = sandbox.clone();
                warden.discard_unjournaled(&discard).await?;
                let started = timings.start();
                let mode = self.memory_mode(registration)?;
                let memory = self.memory.clone();
                let reference = registration.memory_reference();
                let (id, operation) = (sandbox.sandbox_id.clone(), registration.operation_id.clone());
                let require = move || -> Result<(), WardenError> {
                    if let (Some(memory), Some((allocation, quota))) = (memory, reference) {
                        let reference = MemoryBackingRef::new(allocation, quota as u64)
                            .map_err(|error| WardenError::Warden(error.to_string()))?;
                        memory.require(&reference, &id, generation).map_err(|error| WardenError::Warden(error.to_string()))?;
                    }
                    Ok(())
                };
                let record = self.warden.create(&sandbox, &operation, mode, require, timings).await?;
                timings.add("runtime_create", started);
                if record.get("state").and_then(Value::as_str) != Some("running") {
                    return Err(unavailable("direct sandbox did not settle running"));
                }
            }
            Some("running" | "parked" | "recovery-required") => {}
            Some(state) => return Err(unavailable(format!("direct sandbox lifecycle is {state}"))),
        }
        let started = timings.start();
        let (registry, id, revision) = (self.registry.clone(), sandbox.sandbox_id.clone(), registration.revision);
        blocking(move || Ok(registry.commit_owned(&id, revision).map(|_| ())?)).await?;
        timings.add("registry_commit", started);
        Ok(started_runtime)
    }

    /// S12.3: the allocation's recorded mode, else RAM when the node has a RAM root.
    fn memory_mode(&self, registration: &Registration) -> Result<MemoryMode, CreateError> {
        let recorded = self
            .memory
            .as_ref()
            .and_then(|memory| memory.active_mode(registration.sandbox_id(), registration.sandbox_generation as u64));
        Ok(match recorded {
            Some(ActiveMode::Ram) => MemoryMode::Ram,
            Some(ActiveMode::File) => MemoryMode::File,
            None if self.config.application_memory_root.is_some() => MemoryMode::Ram,
            None => MemoryMode::File,
        })
    }
}

/// `_require_storage_record`: the daemon's record (StorageVolumeRecord.to_json,
/// flat) must be this workspace's, at its size and mount path, with a project.
fn workspace_quota(
    record: &Map<String, Value>,
    (volume_id, sandbox_id, generation): (&str, &str, u64),
    virtual_size: u64,
    mount_root: &std::path::Path,
    total_mb: i64,
) -> Result<Quota, CreateError> {
    let text = |name: &str| record.get(name).and_then(Value::as_str);
    let owned = text("volume_id") == Some(volume_id)
        && text("sandbox_id") == Some(sandbox_id)
        && record.get("sandbox_generation").and_then(Value::as_u64) == Some(generation);
    if !owned || record.get("virtual_size").and_then(Value::as_u64) != Some(virtual_size) {
        return Err(unavailable("storage-native volume belongs to another quota owner"));
    }
    let expected = mount_root.join(volume_id);
    if text("mount_path").map(std::path::Path::new) != Some(expected.as_path()) {
        return Err(unavailable("storage-native service returned an unexpected mount path"));
    }
    let accounting_id = record.get("accounting_id").and_then(Value::as_i64).unwrap_or(0);
    if accounting_id <= 0 {
        return Err(unavailable("storage-native service returned an invalid accounting ID"));
    }
    Ok(Quota { project_id: accounting_id, total_mb, path: expected.display().to_string() })
}

/// Whether the RAM root is mounted `noswap` (the pause tier mounts it swappable).
fn tmpfs_noswap(root: Option<&std::path::Path>) -> bool {
    let Some(root) = root else { return true };
    let Ok(mounts) = std::fs::read_to_string("/proc/self/mounts") else { return true };
    mounts
        .lines()
        .filter_map(|line| {
            let fields: Vec<&str> = line.split(' ').collect();
            (fields.len() > 3 && std::path::Path::new(fields[1]) == root).then(|| fields[3].to_string())
        })
        .last()
        .is_none_or(|options| options.split(',').any(|option| option == "noswap"))
}

impl Pipeline for NodePipeline {
    fn create<'a>(
        &'a self,
        admitted: &'a Admitted,
        timings: &'a mut Timings,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<bool, CreateError>> + Send + 'a>> {
        Box::pin(self.run(admitted, timings))
    }

    fn supports(&self, spec: &Map<String, Value>) -> bool {
        spec_supported(spec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// StorageVolumeRecord(...).to_json() from the repo's Python.
    const RECORD: &str = r#"{"accounting_id": 1234, "cached_layer_paths": [], "device_id": null, "device_owner_id": "o", "device_path": "", "error": "", "granted_size": 536870912, "local_layer_bytes": 0, "mount_path": "/var/lib/ucloud-sandboxes/storage-native/mounts/workspace-box.sandbox-1", "operation_id": "create-1", "published_backend": "", "published_layers": [], "published_manifest_digest": "", "published_repo_blob_url": "", "published_repository": "", "published_tag": "", "revision": 3, "runtime_dir": "/run/x", "runtime_image_config": "", "sandbox_generation": 1, "sandbox_id": "box", "sealed_layer_bytes": 0, "sealed_layer_paths": [], "source_image_config": "/etc/x.json", "state": "mounted", "updated_ns": 0, "virtual_size": 5368709120, "volume_id": "workspace-box.sandbox-1"}"#;

    #[test]
    fn the_storage_daemons_record_is_checked_like_python() {
        let record: Map<String, Value> = serde_json::from_str(RECORD).unwrap();
        let root = std::path::Path::new("/var/lib/ucloud-sandboxes/storage-native/mounts");
        let owner = ("workspace-box.sandbox-1", "box", 1);
        let quota = workspace_quota(&record, owner, 5120 * MIB as u64, root, 7232).unwrap();
        assert_eq!(quota, Quota { project_id: 1234, total_mb: 7232, path: format!("{}/workspace-box.sandbox-1", root.display()) });
        let message = |result: Result<Quota, CreateError>| match result {
            Err(CreateError::Unavailable(message)) => message,
            other => panic!("{other:?}"),
        };
        assert_eq!(message(workspace_quota(&record, ("workspace-box.sandbox-1", "box", 2), 5120 * MIB as u64, root, 1)),
                   "storage-native volume belongs to another quota owner");
        assert_eq!(message(workspace_quota(&record, owner, 1, root, 1)), "storage-native volume belongs to another quota owner");
        assert_eq!(message(workspace_quota(&record, owner, 5120 * MIB as u64, std::path::Path::new("/elsewhere"), 1)),
                   "storage-native service returned an unexpected mount path");
    }
}
