#!/usr/bin/env python3
"""Prove XFS project-quota clone overlap and immutable-source preservation.

Run only on an isolated XFS project-quota work root. A reflink consumes logical
quota even when its extents initially share physical blocks. Both allocations
are created and retired through the production memory backing owner.
"""
import argparse
import errno
import fcntl
import hashlib
import json
import os
from pathlib import Path
import time

from ucloud_sandboxes.checkpoint_components import MemoryBackingRef
from ucloud_sandboxes.memory_backing import MemoryBackingStore


def run(root):
    directory = root / f"reflink-quota-{time.time_ns()}"
    directory.mkdir(mode=0o700)
    store = MemoryBackingStore(
        directory / "allocations", directory / "memory.sqlite",
        hard_capacity_bytes=256 * 1024**2,
    )
    results = []
    for quota_mb in (64, 128):
        sandbox_id = f"quota-{quota_mb}"
        reference = MemoryBackingRef(sandbox_id + ".sandbox-1", quota_mb * 1024**2)
        lease = store.prepare(reference, sandbox_id=sandbox_id, sandbox_generation=1)
        source, target = lease.path / "source", lease.path / "clone"
        try:
            with source.open("wb") as stream:
                for _ in range(40):
                    stream.write(os.urandom(1024**2))
                stream.flush()
                os.fsync(stream.fileno())
            before = hashlib.sha256(source.read_bytes()).hexdigest()
            failure = None
            with source.open("rb") as src, target.open("xb") as dst:
                try:
                    fcntl.ioctl(dst.fileno(), 0x40049409, src.fileno())  # FICLONE
                except OSError as exc:
                    if quota_mb != 64 or exc.errno not in {errno.EDQUOT, errno.ENOSPC}:
                        raise
                    failure = exc.errno
                else:
                    assert quota_mb == 128, "clone bypassed its logical project quota"
            if quota_mb == 64:
                assert failure is not None, "quota failure was not observed"
            else:
                assert target.read_bytes() == source.read_bytes()
                with target.open("r+b") as stream:
                    stream.write(b"changed private candidate")
                    stream.flush()
                    os.fsync(stream.fileno())
            assert hashlib.sha256(source.read_bytes()).hexdigest() == before
            results.append({
                "quota_bytes": reference.quota_bytes, "source_sha256": before,
                "source_allocated_bytes": source.stat().st_blocks * 512,
                "clone_allocated_bytes": target.stat().st_blocks * 512,
                "clone_errno": failure, "immutable_source_preserved": True,
            })
        finally:
            store.delete(reference, sandbox_id=sandbox_id, sandbox_generation=1)
    references = [MemoryBackingRef(f"cross-{i}.sandbox-1", 64 * 1024**2) for i in range(2)]
    allocations = []
    try:
        for index, reference in enumerate(references):
            allocations.append(store.prepare(reference, sandbox_id=f"cross-{index}", sandbox_generation=1))
        source, target = allocations[0].path / "source", allocations[1].path / "clone"
        with source.open("wb") as stream:
            for _ in range(40):
                stream.write(os.urandom(1024**2))
            stream.flush()
            os.fsync(stream.fileno())
        digest = hashlib.sha256(source.read_bytes()).hexdigest()
        with source.open("rb") as src, target.open("xb") as dst:
            fcntl.ioctl(dst.fileno(), 0x40049409, src.fileno())
        assert source.read_bytes() == target.read_bytes()
        with target.open("r+b") as stream:
            stream.write(b"cross-project candidate mutation")
            stream.flush()
            os.fsync(stream.fileno())
        assert hashlib.sha256(source.read_bytes()).hexdigest() == digest
        cross_project = {
            "source_project": allocations[0].project_id,
            "target_project": allocations[1].project_id,
            "quota_each_bytes": 64 * 1024**2,
            "source_allocated_bytes": source.stat().st_blocks * 512,
            "clone_allocated_bytes": target.stat().st_blocks * 512,
            "immutable_source_preserved": True,
        }
    finally:
        for index, reference in enumerate(references[:len(allocations)]):
            store.delete(reference, sandbox_id=f"cross-{index}", sandbox_generation=1)
    return {"status": "passed", "cases": results, "cross_project": cross_project,
            "final_metrics": store.metrics()}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    args.output.write_text(json.dumps(run(args.work_root), indent=2) + "\n")
