REQUEST_BODY_KEEPALIVE_CAPABILITY = "request-body-keepalive-v1"
ENVIRONMENT_CONTRACT_CAPABILITY = "environment-contract-v1"
STATIC_FILE_MANAGEMENT_CAPABILITY = "static-file-management-v1"
DISK_QUOTA_CAPABILITY = "disk-quota"
HIBERNATE_LOCAL_CAPABILITY = "hibernate-local-v2"
RELAY_WAKE_FENCE_CAPABILITY = "relay-wake-fence-v1"
RESOURCE_PHASE_CAPABILITY = "resource-phase-advice-v1"
MANAGED_PRIMARY_CAPABILITY = "managed-primary-v1"
STORAGE_NATIVE_CAPABILITY = "storage-native-v1"
SPLIT_CHECKPOINT_CAPABILITY = "sandbox-checkpoint-v3"
REFLINK_MEMORY_RESTORE_CAPABILITY = "sandbox-memory-reflink-restore-v1"
HOST_EROFS_CAPABILITY = "immutable-environment-host-erofs-v1"
# Chunk store M2: honours SandboxSpec.environment_root; reads RAFS components
# through a chunk index and store node.
ENVIRONMENT_ROOT_CAPABILITY = "environment-root-dispatch-v1"
ENVIRONMENT_RAFS_CAPABILITY = "environment-rafs-v1"
RUNTIME_COMPATIBILITY_CAPABILITY_PREFIX = "runtime-compatibility-sha256:"
# Checkpoint import compares the full runtime fingerprint, including the CPU
# feature set; same-model VMs can expose different flags (erms, fsrm).
RUNTIME_CPU_CAPABILITY_PREFIX = "runtime-cpu-features-sha256:"
STORAGE_NATIVE_MIGRATION_CAPABILITY = "sandbox-migrate-storage-native-v1"
STORAGE_NATIVE_DETACH_CAPABILITY = "sandbox-detach-published-v1"
COMMIT_EXPORT_CAPABILITY = "sandbox-commit-export-v1"
# PUT /v1/sandboxes/{id}/archive; the gateway answers 501 for a worker without it.
ARCHIVE_UPLOAD_CAPABILITY = "sandbox-archive-upload-v1"


def has_capability(capabilities: tuple[str, ...], capability: str) -> bool:
    return capability in capabilities
