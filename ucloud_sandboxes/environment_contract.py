"""Conservative environment preflight, independent of guest environment secrets.

A configured interface is not a successful workload probe. Unknown requirements
are rejected until the deployment has a qualification and admission mechanism.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .sandbox import SandboxSpec


_FEATURES = {
    "linux-kernel": (
        "unsupported",
        "The current backend runs gVisor, not a guest Linux kernel.",
    ),
    "system-boot": ("unsupported", "No booted-system backend is implemented."),
    "framework-network-policy": (
        "unsupported",
        "Framework-aware egress enforcement is not implemented.",
    ),
    "posix-acl": (
        "unqualified",
        "The pinned runtime failed the sampled ACL oracle; filesystem-specific qualification is required.",
    ),
    "filesystem-xattrs": (
        "unqualified",
        "Support depends on the filesystem and runtime version.",
    ),
    "filesystem-locks": (
        "unqualified",
        "Lock behavior has not been qualified across lifecycle operations.",
    ),
    "filesystem-notifications": (
        "unqualified",
        "Notification behavior has not been qualified across lifecycle operations.",
    ),
    "nested-containers": (
        "unqualified",
        "Nested container requirements have not been qualified.",
    ),
}


def describe_environment(spec: SandboxSpec) -> dict[str, Any]:
    """Describe requested settings; do not claim image or live resolution."""
    features = {
        name: {"status": status, "reason": reason}
        for name, (status, reason) in _FEATURES.items()
    }
    features["network-off"] = {
        "status": "configured" if spec.network == "none" else "unsupported",
        "reason": "Selected network mode is " + spec.network + ".",
    }
    features["static-file-management"] = {
        "status": "configured"
        if spec.filesystem.management_helper == "static"
        else "unsupported",
        "reason": "Requires the updated static supervisor artifact on the selected node.",
    }
    problems = []
    for name in spec.required_features:
        feature = features.get(
            name, {"status": "unknown", "reason": "Unknown feature name."}
        )
        if feature["status"] != "configured":
            problems.append({"feature": name, **feature})
    warnings = []
    if spec.filesystem.workspace_is_tmpfs:
        warnings.append(
            "Workspace tmpfs hides image contents at its mount point and consumes memory; it is not a disk quota."
        )
    if spec.filesystem.enforce_disk_quota:
        warnings.append(
            "enforce_disk_quota is a legacy workspace-tmpfs selector; use workspace_storage explicitly."
        )
    if spec.profile in {"linux_host", "linux_session"}:
        warnings.append(
            "Session startup requires image shell utilities; no system init is booted."
        )
    return {
        "schema_version": 1,
        "backend": "gvisor",
        "evidence_level": "configuration",
        "requested": {
            "profile": spec.profile,
            "required_features": list(spec.required_features),
            "working_dir": spec.working_dir,
            "user": spec.security.user,
            "supplementary_groups": list(spec.security.supplementary_groups),
        },
        "configured": {
            "workspace": spec.filesystem.workspace_path,
            "management_helper": spec.filesystem.management_helper,
            "workspace_storage": "tmpfs"
            if spec.filesystem.workspace_is_tmpfs
            else "image",
            "tmpfs_mb": spec.filesystem.tmpfs_mb,
            "run_tmpfs_mb": spec.filesystem.run_tmpfs_mb,
            "shm_mb": spec.filesystem.shm_mb,
            "network": spec.network,
            "dns_servers": list(spec.dns_servers or ("1.1.1.1", "8.8.8.8"))
            if spec.network == "bridge"
            else [],
        },
        "unresolved": [
            "image_digest",
            "runtime_digest",
            "image_identity",
            "effective_cwd",
            "home",
            "live_features",
            "lifecycle_persistence",
        ],
        "features": features,
        "requirements_satisfied": not problems,
        "problems": problems,
        "warnings": warnings,
    }


def validate_requirements(spec: SandboxSpec) -> None:
    if not spec.required_features:
        return
    report = describe_environment(spec)
    if report["problems"]:
        raise ValueError(
            "environment requirements cannot be satisfied: "
            + "; ".join(
                f"{item['feature']} ({item['status']}): {item['reason']}"
                for item in report["problems"]
            )
        )
