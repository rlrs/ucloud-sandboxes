DISK_QUOTA_CAPABILITY = "disk-quota"
DYNAMIC_ACTIVE_ADMISSION_CAPABILITY = "dynamic-active-admission-v1"
HIBERNATE_LOCAL_CAPABILITY = "hibernate-local-v1"
MANAGED_PRIMARY_CAPABILITY = "managed-primary-v1"
STORAGE_NATIVE_CAPABILITY = "storage-native-v1"
STORAGE_NATIVE_MIGRATION_CAPABILITY = "sandbox-migrate-storage-native-v1"


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
