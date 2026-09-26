"""Physical disk claims that follow demonstrated usage (docs/disk-density.md).

A sandbox's ``disk_mb`` and ``memory_mb`` are maximums. The node charges:

* the workspace filesystem's current grant plus its local sealed layers, grown
  online toward ``disk_mb`` when the guest fills it; and
* in RAM-backed memory mode, a small idle memory allocation, raised at park
  admission to the sandbox's measured memory and settled to the checkpoint's
  allocated bytes once it commits.

Every increase that creates physical bytes is admitted against the node's
hard capacity first; refusal is retryable and never overcommits the disk.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from .sandbox import SandboxSpec

if TYPE_CHECKING:
    from .direct_registry import DiskClaim

MIB = 1024**2
# The allocation directory of a running RAM-backed sandbox holds only its
# ownership marker; this also covers directory blocks and the manifest slack.
IDLE_MEMORY_CLAIM_MB = 64
# Serialized kernel state and page metadata beyond resident memory.
CAPTURE_OVERHEAD_MB = 64
# Blocks a committed checkpoint may still need beyond its measured size.
CAPTURE_SETTLE_SLACK_MB = 1
MIN_WORKSPACE_GRANT_MB = 512
GROWTH_MIN_FREE_MB = 384
GROWTH_MIN_STEP_MB = 1024


@dataclass(frozen=True)
class DiskClaimPolicy:
    """Node policy; ``workspace_grant_mb`` 0 formats full-size workspaces."""

    workspace_grant_mb: int = 0
    demonstrated_memory: bool = False

    def __post_init__(self) -> None:
        if type(self.workspace_grant_mb) is not int or (
            self.workspace_grant_mb and self.workspace_grant_mb < MIN_WORKSPACE_GRANT_MB
        ):
            raise ValueError(
                f"workspace grant must be 0 or at least {MIN_WORKSPACE_GRANT_MB} MiB"
            )

    @property
    def enabled(self) -> bool:
        return bool(self.workspace_grant_mb or self.demonstrated_memory)

    def initial_claim(self, spec: SandboxSpec) -> DiskClaim | None:
        """The claim a new split sandbox starts with, or None for a fixed claim."""
        from .direct_registry import DiskClaim  # the registry imports the Warden

        if not self.enabled or not spec.parkable:
            return None
        assert spec.disk_mb is not None
        memory_ceiling_mb = spec.requested_resources().disk_mb - spec.disk_mb
        return DiskClaim(
            workspace_mb=self.workspace_grant(spec) // MIB,
            memory_mb=IDLE_MEMORY_CLAIM_MB if self.demonstrated_memory else memory_ceiling_mb,
        )

    def workspace_grant(self, spec: SandboxSpec) -> int:
        """Initial filesystem size in bytes (the ceiling when grants are off)."""
        assert spec.disk_mb is not None
        if not self.workspace_grant_mb:
            return spec.disk_mb * MIB
        return min(spec.disk_mb, self.workspace_grant_mb) * MIB

    def advertised(self) -> dict[str, int]:
        """What the gateway needs to charge an in-flight create correctly."""
        return {
            "storage_workspace_grant_mb": self.workspace_grant_mb,
            "storage_memory_idle_claim_mb": (
                IDLE_MEMORY_CLAIM_MB if self.demonstrated_memory else 0
            ),
        }


def capture_claim_mb(*, base_bytes: int, filestore_bytes: int) -> int:
    """Checkpoint space to reserve at park admission, in MiB.

    ``base_bytes`` bounds the application memory image and the sentry's other
    private pages: for RAM-backed owners the tmpfs memory file's allocated
    bytes plus resident memory capped at the memory limit, otherwise the
    formula ceiling. ``filestore_bytes`` is the allocated size of the gVisor
    filestore, whose contents (the guest's rootfs writes) a hibernate capture
    serializes as private pages. A capture that runs out of space can lose
    the sandbox, so this is an upper bound, not an estimate.
    """
    return -(-(base_bytes + filestore_bytes) // MIB) + CAPTURE_OVERHEAD_MB


def settled_claim_mb(allocated_bytes: int) -> int:
    return -(-allocated_bytes // MIB) + CAPTURE_SETTLE_SLACK_MB


def next_grant(*, granted: int, free: int, ceiling: int) -> int | None:
    """The next filesystem size, or None when this one still has headroom.

    Grow before the guest runs out: when free space falls below a quarter of
    the filesystem (at least 384 MiB), add half again (at least 1 GiB).
    """
    if granted >= ceiling:
        return None
    if free >= max(GROWTH_MIN_FREE_MB * MIB, granted // 4):
        return None
    step = max(GROWTH_MIN_STEP_MB * MIB, granted // 2)
    target = min(ceiling, granted + step)
    return target - target % MIB


def cgroup_memory_demand(pid: int, *, proc_root: Path = Path("/proc"),
                         cgroup_root: Path = Path("/sys/fs/cgroup")) -> int | None:
    """memory.current + memory.swap.current of a process's unified cgroup."""
    try:
        memberships = (proc_root / str(pid) / "cgroup").read_text().splitlines()
        unified = [line[3:] for line in memberships if line.startswith("0::")]
        if len(unified) != 1 or not unified[0].startswith("/"):
            return None
        relative = Path(unified[0].removeprefix("/"))
        if any(part in {".", ".."} for part in relative.parts):
            return None
        path = cgroup_root / relative
        demand = int((path / "memory.current").read_text().strip())
        try:
            demand += int((path / "memory.swap.current").read_text().strip())
        except FileNotFoundError:
            pass
        return demand
    except (OSError, ValueError):
        return None
