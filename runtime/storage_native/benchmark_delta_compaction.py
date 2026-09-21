#!/usr/bin/env python3
"""Qualify delta-only compaction using a pinned backend's export RPCs.

Uses only temporary local layers and export operations: no device, mount,
production journal, publication, or provider lifecycle mutation. Run on an idle
Linux qualification worker with the repository on PYTHONPATH.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import statistics
import struct
import tempfile
import time
from uuid import uuid4

from ucloud_sandboxes.storage_native import AgentEnvUblkClient
from ucloud_sandboxes.storage_native_registry import consume_export_stream


MAGIC = bytes.fromhex("657e63d29444084ca2d2c8ec4fcfae8a")
MASK = (1 << 50) - 1


def header(virtual_size: int, *, trailer=False, offset=0, count=0) -> bytes:
    raw = bytearray(4096)
    struct.pack_into("<Q16sIIQQQ", raw, 0, 0x00020100544D534C, MAGIC, 390,
                     6 if trailer else 7, offset, count, virtual_size)
    raw[56:93] = str(uuid4()).encode() + b"\0"
    raw[132:134] = b"\x01\x01"
    return bytes(raw)


def write_layer(path: Path, virtual_size: int, extents: list[tuple[int, bytes | int]]) -> None:
    """Emit the pinned OverlayBD sealed format; integer extents are discards."""
    mappings = []
    with path.open("wb") as stream:
        first = header(virtual_size)
        stream.write(first)
        for offset, value in sorted(extents):
            length = value if isinstance(value, int) else len(value)
            assert offset % 512 == length % 512 == 0
            # DiskSegmentMapping has a 14-bit sector count.
            for start in range(0, length, 8192 * 512):
                size = min(length - start, 8192 * 512)
                physical = stream.tell() // 512
                zeroed = isinstance(value, int)
                mappings.append(struct.pack("<QQ", (offset + start) // 512 | ((size // 512) << 50),
                                            physical | (int(zeroed) << 55)))
                if not zeroed:
                    stream.write(value[start:start + size])
        index_offset = stream.tell()
        count = ((len(mappings) + 255) // 256) * 256
        stream.write(b"".join(mappings))
        stream.write(struct.pack("<QQ", MASK, 0) * (count - len(mappings)))
        stream.write(bytes((-stream.tell()) % 4096))
        last = bytearray(first)
        struct.pack_into("<IQQ", last, 28, 6, index_offset, count)
        stream.write(last)


def apply_layer(path: Path, image: bytearray) -> None:
    with path.open("rb") as stream:
        stream.seek(-4096, 2)
        trailer = stream.read(4096)
        offset, count, virtual_size = struct.unpack_from("<QQQ", trailer, 32)
        assert virtual_size == len(image)
        stream.seek(offset)
        mappings = [struct.unpack("<QQ", stream.read(16)) for _ in range(count)]
        for low, high in mappings:
            size = (low >> 50) * 512
            if not size:
                continue
            begin = (low & MASK) * 512
            assert begin + size <= len(image)
            if (high >> 55) & 1:
                image[begin:begin + size] = bytes(size)
            else:
                stream.seek((high & ((1 << 55) - 1)) * 512)
                data = stream.read(size)
                assert len(data) == size
                image[begin:begin + size] = data


def run(socket_path: Path, root: Path, base_mib: int, repeat: int) -> dict:
    client = AgentEnvUblkClient(socket_path)
    global_config = root / "global.json"
    global_config.write_text(json.dumps({
        "cacheConfig": {"cacheDir": str(root / "cache"), "cacheSizeGB": 1,
                        "cacheType": "file", "refillSize": 262144},
        "download": {"enable": False}, "nrIoRings": 1, "registryFsVersion": "v2",
    }))
    base_size = base_mib * 1024**2
    virtual_size = base_size + 1024**2
    base = root / "base.lsmt"
    write_layer(base, virtual_size, [(0, b"A" * base_size)])
    deltas = []
    # Repeated overwrite, explicit zero writes, discard over base data,
    # overwrite after discard, and a new block in a previously unmapped hole.
    edits = [[(0, b"B" * 4096)], [(4096, bytes(4096))], [(8192, 4096)],
             [(12288, b"C" * 4096)], [(12288, 4096)], [(8192, b"D" * 4096)],
             [(base_size + 4096, b"E" * 4096)], [(0, 4096)]]
    for index, extents in enumerate(edits):
        path = root / f"delta-{index}.lsmt"
        write_layer(path, virtual_size, extents)
        deltas.append(path)
    expected = bytearray(b"A" * base_size + bytes(1024**2))
    expected[:8192] = bytes(8192)
    expected[8192:12288] = b"D" * 4096
    expected[12288:16384] = bytes(4096)
    expected[base_size + 4096:base_size + 8192] = b"E" * 4096

    def export(name, paths):
        source = root / (name + ".json")
        source.write_text(json.dumps({"lowers": [{"file": str(p)} for p in paths],
                                      "repoBlobUrl": "", "resultFile": "", "upper": {}}))
        output = root / ("result-" + name + ".lsmt")
        started = time.monotonic()
        with output.open("wb") as stream:
            descriptor = consume_export_stream(
                lambda sock: client.export_compacted_image(source_image_config=source,
                    global_config=global_config, stream_socket_path=sock),
                stream_socket_root=root, chunk_bytes=1024**2, timeout_seconds=120,
                consume=stream.write,
            )
        return output, {"seconds": time.monotonic() - started, "bytes": descriptor.size}

    results = {"full": [], "delta": []}
    for iteration in range(repeat):
        for mode in (["full", "delta"] if iteration % 2 == 0 else ["delta", "full"]):
            output, timing = export(f"{mode}-{iteration}", [base, *deltas] if mode == "full" else deltas)
            results[mode].append(timing)
            image = bytearray(virtual_size)
            if mode == "delta":
                apply_layer(base, image)
            apply_layer(output, image)
            assert image == expected, f"{mode} logical data mismatch"
            if mode == "delta":
                restacked, _ = export(f"restacked-{iteration}", [base, output])
                image = bytearray(virtual_size)
                apply_layer(restacked, image)
                assert image == expected, "native restacking changed logical data"
    return {"base_mib": base_mib, "deltas": len(deltas), "runs": results,
            "median_seconds": {k: statistics.median(x["seconds"] for x in v) for k, v in results.items()},
            "correctness": {"overwrites": True, "zero_writes": True, "discards": True,
                            "holes": True, "native_restack": True},
            "scope": "Local warm export only; excludes network upload and concurrent sandbox traffic."}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend-socket", type=Path, required=True)
    parser.add_argument("--work-root", type=Path, default=Path("/tmp"))
    parser.add_argument("--base-mib", type=int, default=128)
    parser.add_argument("--repeat", type=int, default=3)
    args = parser.parse_args()
    if args.base_mib < 1 or args.repeat < 1:
        parser.error("base-mib and repeat must be positive")
    with tempfile.TemporaryDirectory(prefix="delta-compact-", dir=args.work_root) as directory:
        print(json.dumps(run(args.backend_socket.resolve(), Path(directory).resolve(),
                             args.base_mib, args.repeat), indent=2))


if __name__ == "__main__":
    main()
