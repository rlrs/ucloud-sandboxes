from __future__ import annotations

from typing import Any

from ...models import ProviderInstance
from ..base import InstanceBootstrapAccess


def bootstrap_access(instance: ProviderInstance) -> InstanceBootstrapAccess:
    command = extract_ssh_command(instance.raw)
    if not instance.is_running:
        return InstanceBootstrapAccess(
            instance=instance,
            command=command,
            runnable=False,
            reason=(
                "Instance is not running yet; current provider state is "
                f"{instance.state or 'unknown'}."
            ),
        )
    if not command:
        return InstanceBootstrapAccess(
            instance=instance,
            command=None,
            runnable=False,
            reason="No SSH access command has been announced by UCloud yet.",
            refresh_recommended=True,
        )
    return InstanceBootstrapAccess(
        instance=instance,
        command=command,
        runnable=True,
        reason="Instance is running and SSH access is available.",
        # UCloud announces the SSH tunnel before sshd in the guest is always
        # ready. Keep the already-scheduled bootstrap worker on that endpoint
        # instead of turning transient connection failures into autoscaler
        # retries with increasingly sparse probes.
        startup_probe_seconds=30,
    )


def bootstrap_access_from_payload(payload: dict[str, Any]) -> InstanceBootstrapAccess:
    from .models import instance_from_payload

    return bootstrap_access(instance_from_payload(payload))


def extract_ssh_command(payload: dict[str, Any]) -> str | None:
    updates = payload.get("updates")
    if not isinstance(updates, list):
        return None
    for update in reversed(updates):
        if not isinstance(update, dict):
            continue
        status = update.get("status")
        if not isinstance(status, str):
            continue
        command = extract_ssh_command_from_text(status)
        if command:
            return command
    return None


def extract_ssh_command_from_text(text: str) -> str | None:
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        lower = line.lower()
        prefix = "SSH Access:"
        if line.startswith(prefix):
            candidate = line[len(prefix) :].strip()
            if candidate.lower().startswith("ssh "):
                return candidate
        marker = "Available at:"
        if line.startswith("SSH:") and marker in line:
            candidate = line[line.index(marker) + len(marker) :].strip()
            if candidate.lower().startswith("ssh "):
                return candidate
        if lower.startswith("ssh "):
            return line
    return None
