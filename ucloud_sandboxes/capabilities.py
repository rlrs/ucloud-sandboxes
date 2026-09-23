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
RUNTIME_COMPATIBILITY_CAPABILITY_PREFIX = "runtime-compatibility-sha256:"
STORAGE_NATIVE_MIGRATION_CAPABILITY = "sandbox-migrate-storage-native-v1"
STORAGE_NATIVE_DETACH_CAPABILITY = "sandbox-detach-published-v1"


def merge_capabilities(*groups: tuple[str, ...]) -> tuple[str, ...]:
    values: list[str] = []
    for group in groups:
        for capability in group:
            cleaned = capability.strip()
            if cleaned:
                values.append(cleaned)
    return tuple(dict.fromkeys(values))


def has_capability(capabilities: tuple[str, ...], capability: str) -> bool:
    return capability in capabilities
