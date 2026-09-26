"""Keyed locks that serialize threads and processes sharing one host.

Gateway processes (the public replicas and the placement worker) run on one
host and share its state directory. Multi-step sequences that PostgreSQL does
not fence (image-build dispatch, external migration execution, registry lease
changes) hold one of these for their whole duration. A pooled PostgreSQL
advisory lock would instead pin a database connection for minutes of builder
or worker I/O.

Keys hash into a fixed set of stripe files per namespace, so the directory stays
bounded. The in-process lock is per stripe and reentrant: a thread nesting two
keys of one stripe re-enters rather than deadlocking on its own ``flock``.
Unrelated keys sharing a stripe only serialize, which is safe.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
import fcntl
import hashlib
import os
from pathlib import Path
import threading
from typing import Iterator

_STRIPES = 256


@dataclass
class _Stripe:
    lock: threading.RLock = field(default_factory=threading.RLock)
    depth: int = 0
    descriptor: int | None = None


class HostKeyedLocks:
    def __init__(self, directory: Path | None = None) -> None:
        self._guard = threading.Lock()
        self._stripes: dict[tuple[str, int], _Stripe] = {}
        self._directory: Path | None = None
        if directory is not None:
            self.configure(directory)

    def configure(self, directory: Path | None) -> None:
        """Enable cross-process locking below a private, service-owned directory."""
        if directory is not None:
            directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        with self._guard:
            if any(stripe.depth for stripe in self._stripes.values()):
                raise RuntimeError("cannot reconfigure host locks while held")
            self._directory = directory

    @contextmanager
    def hold(self, namespace: str, key: str) -> Iterator[None]:
        if not namespace.replace("-", "").isalnum():
            raise ValueError("host lock namespace must be alphanumeric")
        index = int.from_bytes(hashlib.sha256(key.encode()).digest()[:4], "big") % _STRIPES
        with self._guard:
            stripe = self._stripes.setdefault((namespace, index), _Stripe())
            directory = self._directory
        with stripe.lock:
            if stripe.depth == 0 and directory is not None:
                path = directory / f"{namespace}-{index:03d}.lock"
                descriptor = os.open(
                    path, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600
                )
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_EX)
                except BaseException:
                    os.close(descriptor)
                    raise
                stripe.descriptor = descriptor
            stripe.depth += 1
            try:
                yield
            finally:
                stripe.depth -= 1
                if stripe.depth == 0 and stripe.descriptor is not None:
                    descriptor, stripe.descriptor = stripe.descriptor, None
                    try:
                        fcntl.flock(descriptor, fcntl.LOCK_UN)
                    finally:
                        os.close(descriptor)


HOST_LOCKS = HostKeyedLocks()
