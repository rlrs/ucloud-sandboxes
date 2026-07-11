from __future__ import annotations

import json
from pathlib import Path
import re


DISK_QUOTA_CAPABILITY = "disk-quota"
RUNTIME_CONFORMANCE_CAPABILITY = "runtime-conformance"
FORK_LOCAL_CAPABILITY = "fork-local-v1"
STORAGE_OPT_QUOTA_PROBE = "storage-opt-quota-enforced"
TMPFS_QUOTA_PROBE = "tmpfs-quota-enforced"
GVISOR_LIVE_FORK_PROBE = "gvisor-live-fork-v1"


def conformance_capabilities_from_file(
    path: Path | None,
    *,
    expected_fork_runtime_fingerprint: str | None = None,
) -> tuple[str, ...]:
    result_ok = conformance_results_from_file(
        path,
        expected_fork_runtime_fingerprint=expected_fork_runtime_fingerprint,
    )
    if not result_ok:
        return ()
    capabilities = [RUNTIME_CONFORMANCE_CAPABILITY]
    if result_ok.get(STORAGE_OPT_QUOTA_PROBE):
        capabilities.append(DISK_QUOTA_CAPABILITY)
    if all(
        result_ok.get(probe)
        for probe in (
            GVISOR_LIVE_FORK_PROBE,
            STORAGE_OPT_QUOTA_PROBE,
            TMPFS_QUOTA_PROBE,
        )
    ):
        capabilities.append(FORK_LOCAL_CAPABILITY)
    return tuple(capabilities)


def conformance_results_from_file(
    path: Path | None,
    *,
    expected_fork_runtime_fingerprint: str | None = None,
) -> dict[str, bool]:
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
    result_ok: dict[str, bool] = {}
    for item in results:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name"))
        ok = bool(item.get("ok")) and not bool(item.get("skipped"))
        if name == GVISOR_LIVE_FORK_PROBE and ok:
            fingerprint = str(item.get("runtime_fingerprint") or "")
            ok = re.fullmatch(r"[0-9a-f]{64}", fingerprint) is not None
            if expected_fork_runtime_fingerprint is not None:
                ok = ok and fingerprint == expected_fork_runtime_fingerprint
        result_ok[name] = ok
    return result_ok


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
