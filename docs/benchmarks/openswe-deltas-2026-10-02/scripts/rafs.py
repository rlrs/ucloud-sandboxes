"""Minimal RAFS v6 bootstrap reader: superblock, device table and chunk table (nydus v2.4.5 layout)."""
import struct

SB = 1024
EXT = 1024 + 128
DEVT = 1024 + 128 + 256
CHUNK = struct.Struct("<32sIIIIQQQII")  # RafsV5ChunkInfo, 80 bytes


def read(path):
    with open(path, "rb") as f:
        data = f.read()
    magic, = struct.unpack_from("<I", data, SB)
    assert magic == 0xE0F5E1E2, "not EROFS"
    blocks, = struct.unpack_from("<I", data, SB + 36)
    extra_devices, = struct.unpack_from("<H", data, SB + 86)
    build_time, = struct.unpack_from("<Q", data, SB + 24)
    flags, bt_off, bt_size, chunk_size, ct_off, ct_size = struct.unpack_from("<QQIIQQ", data, EXT)
    devices = []
    for i in range(extra_devices):
        blob_id, nblocks, mapped = struct.unpack_from("<64sII", data, DEVT + 128 * i)
        devices.append({"blob_id": blob_id.rstrip(b"\0").decode(), "blocks": nblocks, "mapped_blkaddr": mapped})
    chunks = []
    for off in range(ct_off, ct_off + ct_size, CHUNK.size):
        d, bi, fl, cs, us, co, uo, fo, idx, crc = CHUNK.unpack_from(data, off)
        chunks.append((d.hex(), bi, fl, cs, us, co, uo))
    return {"size": len(data), "blocks": blocks, "build_time": build_time, "chunk_size": chunk_size,
            "devices": devices, "chunks": chunks}
