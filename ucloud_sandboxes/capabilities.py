from __future__ import annotations

import json
from pathlib import Path


DISK_QUOTA_CAPABILITY = "disk-quota"
RUNTIME_CONFORMANCE_CAPABILITY = "runtime-conformance"
STORAGE_OPT_QUOTA_PROBE = "storage-opt-quota-enforced"
TMPFS_QUOTA_PROBE = "tmpfs-quota-enforced"


def conformance_capabilities_from_file(path: Path | None) -> tuple[str, ...]:
    result_ok = conformance_results_from_file(path)
    if not result_ok:
        return ()
    capabilities = [RUNTIME_CONFORMANCE_CAPABILITY]
    if result_ok.get(STORAGE_OPT_QUOTA_PROBE):
        capabilities.append(DISK_QUOTA_CAPABILITY)
    return tuple(capabilities)


def conformance_results_from_file(path: Path | None) -> dict[str, bool]:
    if path is None or not path.exists():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        return {}
    if not raw.get("ok"):
        return {}
    results = raw.get("results")
    if not isinstance(results, list):
        return {}
    return {
        str(item.get("name")): bool(item.get("ok"))
        for item in results
        if isinstance(item, dict)
    }


def merge_capabilities(*groups: tuple[str, ...]) -> tuple[str, ...]:
    values: list[str] = []
    for group in groups:
        for capability in group:
            cleaned = capability.strip()
            if cleaned:
                values.append(cleaned)
    return tuple(dict.fromkeys(values))


def has_capability(capabilities: tuple[str, ...], capability: str) -> bool:
    return capability in set(capabilities)
