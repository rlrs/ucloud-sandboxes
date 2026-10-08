"""Content-addressed chunk store formats (docs/chunk-store-design.md §1).

A chunk is at most 256 KiB of a RAFS v6 blob's uncompressed data, named by
the sha256 of those bytes. Packs hold chunks once each; the signed chunk map
places them in one image's device space; the unsigned locator says where a
pack holds them. Every parser here is strict and bounded: packs, locators
and the object store are untrusted, and the chunk map is trusted only after
its signed digest matched.
"""
from __future__ import annotations

from dataclasses import dataclass
import bisect
import ctypes
import ctypes.util
import hashlib
import json
import re
import struct
import threading

from .environment_artifact import CHUNK_BYTES, RAFS_MAX_BOOTSTRAP_BYTES

BLOCK = 4096
MAX_PACK_BYTES = 64 * 1024 ** 2
MAX_BOOTSTRAP_BYTES = RAFS_MAX_BOOTSTRAP_BYTES
MAX_MAP_ENTRIES = 1 << 20
MAX_REGIONS = 254  # RAFS v6's 8-bit blob index (S10).
MAX_CLEN = CHUNK_BYTES + 4096  # zstd's worst case for one chunk, with margin.
RAW, ZSTD = 0, 1
# RAFS v6 chunk flags nydus-image v2.4.5 emits: compressed (0x1), CRC32 (0x10).
_RAFS_CHUNK_FLAGS = 0x11
_RAFS_SHA256, _RAFS_BLAKE3, _RAFS_ZSTD = 0x8, 0x4, 0x80
_HEX64 = re.compile(r"[0-9a-f]{64}\Z")


def round_up(value, unit=BLOCK):
    return -(-value // unit) * unit


def require_hex(value):
    if not isinstance(value, str) or not _HEX64.fullmatch(value):
        raise ValueError("chunk store requires a lowercase sha256 hex digest")
    return value


# zstd: libzstd through ctypes (every node already ships it), so the package
# gains no dependency; Python 3.14's compression.zstd when libzstd is absent.
_ZSTD_LOCK = threading.Lock()
_ZSTD = None


def _zstd():
    global _ZSTD
    with _ZSTD_LOCK:
        if _ZSTD is None:
            try:
                lib = ctypes.CDLL(ctypes.util.find_library("zstd") or "libzstd.so.1")
            except OSError:
                try:
                    from compression import zstd as module  # Python 3.14+
                except ImportError as exc:
                    raise RuntimeError("the chunk store needs libzstd or Python 3.14") from exc
                _ZSTD = module
                return _ZSTD
            size, pointer = ctypes.c_size_t, ctypes.c_void_p
            lib.ZSTD_decompress.restype = lib.ZSTD_compress.restype = lib.ZSTD_compressBound.restype = size
            lib.ZSTD_decompress.argtypes = (pointer, size, pointer, size)
            lib.ZSTD_compress.argtypes = (pointer, size, pointer, size, ctypes.c_int)
            lib.ZSTD_compressBound.argtypes = (size,)
            lib.ZSTD_isError.restype, lib.ZSTD_isError.argtypes = ctypes.c_uint, (size,)
            lib.ZSTD_getFrameContentSize.restype = ctypes.c_ulonglong
            lib.ZSTD_getFrameContentSize.argtypes = (pointer, size)
            _ZSTD = lib
        return _ZSTD


def zstd_decompress(payload, size):
    """Exactly ``size`` bytes or ValueError; output never exceeds ``size``."""
    lib = _zstd()
    if not isinstance(lib, ctypes.CDLL):
        decompressor = lib.ZstdDecompressor()
        data = decompressor.decompress(payload, max_length=size)
        if len(data) != size or not decompressor.eof or decompressor.unused_data:
            raise ValueError("zstd payload does not decode to its declared size")
        return data
    output = ctypes.create_string_buffer(size)
    result = lib.ZSTD_decompress(output, size, payload, len(payload))
    if lib.ZSTD_isError(result) or result != size:
        raise ValueError("zstd payload does not decode to its declared size")
    return output.raw


def zstd_content_size(payload, limit):
    """The size a zstd frame declares, at most ``limit``."""
    lib = _zstd()
    if isinstance(lib, ctypes.CDLL):
        size = lib.ZSTD_getFrameContentSize(payload, len(payload))
    else:
        size = lib.get_frame_info(payload).decompressed_size
    if size is None or not 0 < size <= limit:
        raise ValueError("zstd frame declares no acceptable size")
    return size


def zstd_compress(payload, level=3):
    lib = _zstd()
    if not isinstance(lib, ctypes.CDLL):
        return lib.compress(payload, level)
    capacity = lib.ZSTD_compressBound(len(payload))
    output = ctypes.create_string_buffer(capacity)
    result = lib.ZSTD_compress(output, capacity, payload, len(payload), level)
    if lib.ZSTD_isError(result):
        raise ValueError("zstd compression failed")
    return output.raw[:result]


def decode_chunk(payload, ulen, flags, chunk_id):
    """Verified uncompressed bytes of one stored chunk (design §1.5 step 5)."""
    if flags == ZSTD:
        data = zstd_decompress(payload, ulen)
    elif flags == RAW and len(payload) == ulen:
        data = bytes(payload)
    else:
        raise ValueError("invalid chunk encoding")
    if hashlib.sha256(data).digest() != chunk_id:
        raise ValueError("chunk content identity mismatch")
    return data


def store_encoding(payload, ulen, flags, *, keep=False):
    """(payload, flags) to store: nydus's zstd bytes, or raw when they save
    under 3% or the chunk is under 4 KiB (design §1.1). Never recompresses.
    ``keep`` stores nydus's bytes as they are, so a blob can be rebuilt."""
    if flags & 1 and (keep or ulen >= BLOCK and len(payload) * 100 <= ulen * 97):
        return bytes(payload), ZSTD
    return (zstd_decompress(payload, ulen) if flags & 1 else bytes(payload)), RAW


# --- RAFS v6 bootstrap (nydus-image v2.4.5): device table and chunk table ---

_CHUNK_INFO = struct.Struct("<32sIIIIQQQII")  # RafsV5ChunkInfo, 80 bytes


@dataclass(frozen=True)
class RafsBootstrap:
    size: int
    devices: tuple  # (blob id hex, blocks, mapped_blkaddr), in blob-index order
    chunks: tuple  # (id, blob index, flags, compressed size, size, compressed offset, offset)


def bootstrap_devices(data):
    """A RAFS v6 bootstrap's device table, strictly bounded; the chunk table's
    bounds are checked, not its records (nydusd checks every chunk)."""
    return _bootstrap_tables(data)[0]


def parse_bootstrap(data):
    """The device and chunk tables of a RAFS v6 bootstrap, strictly bounded."""
    devices, (ct_off, ct_size) = _bootstrap_tables(data)
    chunks = []
    for raw in _CHUNK_INFO.iter_unpack(data[ct_off:ct_off + ct_size]):
        digest, blob, chunk_flags, csize, usize, coff, uoff = raw[:7]
        if (blob >= len(devices) or chunk_flags & ~_RAFS_CHUNK_FLAGS or not 0 < usize <= CHUNK_BYTES
                or not 0 < csize <= MAX_CLEN or (not chunk_flags & 1 and csize != usize)
                or uoff % BLOCK or uoff + usize > devices[blob][1] * BLOCK):
            raise ValueError("invalid RAFS chunk record")
        chunks.append((digest, blob, chunk_flags, csize, usize, coff, uoff))
    return RafsBootstrap(len(data), tuple(devices), tuple(chunks))


def _bootstrap_tables(data):
    size = len(data)
    if not 1024 + 128 + 40 <= size <= MAX_BOOTSTRAP_BYTES or size % BLOCK:
        raise ValueError("invalid RAFS bootstrap size")
    magic, = struct.unpack_from("<I", data, 1024)
    blkszbits = data[1024 + 12]
    extra_devices, devt_slotoff = struct.unpack_from("<HH", data, 1024 + 86)
    flags, _bt_off, _bt_size, chunk_size, ct_off, ct_size = struct.unpack_from("<QQIIQQ", data, 1024 + 128)
    if magic != 0xE0F5E1E2 or blkszbits != 12 or chunk_size != CHUNK_BYTES:
        raise ValueError("unsupported RAFS v6 bootstrap")
    if not flags & _RAFS_SHA256 or flags & _RAFS_BLAKE3 or not flags & _RAFS_ZSTD:
        raise ValueError("RAFS bootstrap must use sha256 chunk digests and zstd")
    if (extra_devices > MAX_REGIONS or devt_slotoff * 128 + extra_devices * 128 > size
            or ct_size % _CHUNK_INFO.size or ct_off + ct_size > size
            or ct_size // _CHUNK_INFO.size > MAX_MAP_ENTRIES):
        raise ValueError("RAFS bootstrap tables exceed the bootstrap")
    devices = []
    for index in range(extra_devices):
        tag, blocks, mapped = struct.unpack_from("<64sII", data, devt_slotoff * 128 + index * 128)
        blob_id = tag.rstrip(b"\0").decode("ascii", "replace")
        if not _HEX64.fullmatch(blob_id) or blocks <= 0 or mapped <= 0:
            raise ValueError("invalid RAFS device slot")
        devices.append((blob_id, blocks, mapped))
    return tuple(devices), (ct_off, ct_size)


# --- Chunk map: ucloud-chunk-map-v1 (signed by digest in the component) ---

_MAP_MAGIC = b"UCCMAP\x00\x01"
_MAP_HEADER = struct.Struct("<8sQQII")
_MAP_REGION = struct.Struct("<32sII")
_MAP_ENTRY = struct.Struct("<QI32s")


@dataclass(frozen=True)
class ChunkMap:
    """Block address -> chunk id over one image's unified device space.

    The bootstrap sits at offset 0; blob ``i`` at ``mapped_blkaddr * 4096``.
    Entries are sorted and disjoint; anything else in a region reads as zeros.
    """
    bootstrap_size: int
    device_size: int
    regions: tuple  # (blob id hex, mapped_blkaddr, blocks), by address
    offsets: tuple
    sizes: tuple
    ids: tuple  # 32-byte chunk ids

    def __post_init__(self):
        if (type(self.bootstrap_size) is not int or not 0 < self.bootstrap_size <= MAX_BOOTSTRAP_BYTES
                or len(self.regions) > MAX_REGIONS or len(self.offsets) > MAX_MAP_ENTRIES
                or not len(self.offsets) == len(self.sizes) == len(self.ids)):
            raise ValueError("invalid chunk map header")
        end = round_up(self.bootstrap_size)
        for blob_id, mapped, blocks in self.regions:
            require_hex(blob_id)
            if mapped * BLOCK < end or blocks <= 0:
                raise ValueError("chunk map regions overlap")
            end = (mapped + blocks) * BLOCK
        if self.device_size != end:
            raise ValueError("chunk map device size differs from its regions")
        region, previous = 0, 0
        for offset, size, chunk_id in zip(self.offsets, self.sizes, self.ids):
            if offset % BLOCK or offset < previous or not 0 < size <= CHUNK_BYTES or len(chunk_id) != 32:
                raise ValueError("chunk map entries are not sorted, aligned and disjoint")
            while region < len(self.regions) and offset >= sum(self.regions[region][1:]) * BLOCK:
                region += 1
            if region == len(self.regions) or offset < self.regions[region][1] * BLOCK \
                    or offset + size > sum(self.regions[region][1:]) * BLOCK:
                raise ValueError("chunk map entry lies outside every blob region")
            previous = offset + size

    def encode(self):
        return b"".join((
            _MAP_HEADER.pack(_MAP_MAGIC, self.bootstrap_size, self.device_size, len(self.regions), len(self.ids)),
            *(_MAP_REGION.pack(bytes.fromhex(blob), mapped, blocks) for blob, mapped, blocks in self.regions),
            *(_MAP_ENTRY.pack(*entry) for entry in zip(self.offsets, self.sizes, self.ids))))

    @classmethod
    def decode(cls, payload):
        (bootstrap_size, device_size, parsed), body = _map_header(payload)
        offsets, sizes, ids = zip(*_MAP_ENTRY.iter_unpack(payload[body:])) if len(payload) > body else ((), (), ())
        return cls(bootstrap_size, device_size, parsed, offsets, sizes, ids)

    def overlapping(self, offset, end):
        """Indices of entries overlapping [offset, end)."""
        index = max(bisect.bisect_right(self.offsets, offset) - 1, 0)
        while index < len(self.offsets) and self.offsets[index] < end:
            if self.offsets[index] + self.sizes[index] > offset:
                yield index
            index += 1

    def matches(self, bootstrap):
        """Its header equals the verified bootstrap's size and device table."""
        return (self.bootstrap_size == bootstrap.size and sorted(self.regions) == sorted(
            (blob, mapped, blocks) for blob, blocks, mapped in bootstrap.devices))


def chunk_map_regions(payload):
    """(bootstrap size, device size, regions) of an encoded chunk map, without
    its entries: what nydusd needs, the blobs and where they sit."""
    header, _body = _map_header(payload)
    bootstrap_size, device_size, regions = header
    end = round_up(bootstrap_size) if 0 < bootstrap_size <= MAX_BOOTSTRAP_BYTES else None
    for blob_id, mapped, blocks in regions:
        require_hex(blob_id)
        if end is None or mapped * BLOCK < end or blocks <= 0:
            raise ValueError("chunk map regions overlap")
        end = (mapped + blocks) * BLOCK
    if device_size != end:
        raise ValueError("chunk map device size differs from its regions")
    return header


def _map_header(payload):
    if len(payload) < _MAP_HEADER.size:
        raise ValueError("chunk map is truncated")
    magic, bootstrap_size, device_size, regions, entries = _MAP_HEADER.unpack_from(payload)
    body = _MAP_HEADER.size + regions * _MAP_REGION.size
    if magic != _MAP_MAGIC or regions > MAX_REGIONS or entries > MAX_MAP_ENTRIES \
            or len(payload) != body + entries * _MAP_ENTRY.size:
        raise ValueError("invalid chunk map encoding")
    parsed = tuple((blob.hex(), mapped, blocks) for blob, mapped, blocks
                   in _MAP_REGION.iter_unpack(payload[_MAP_HEADER.size:body]))
    return (bootstrap_size, device_size, parsed), body


def chunk_map_from_bootstrap(bootstrap):
    """Derive the map from a parsed bootstrap's chunk table (design §1.4)."""
    entries = {}
    for chunk_id, blob, _flags, _csize, usize, _coff, uoff in bootstrap.chunks:
        offset = bootstrap.devices[blob][2] * BLOCK + uoff
        if entries.setdefault(offset, (usize, chunk_id)) != (usize, chunk_id):
            raise ValueError("RAFS chunk table places two chunks at one address")
    ordered = sorted(entries.items())
    regions = tuple(sorted(((blob, mapped, blocks) for blob, blocks, mapped in bootstrap.devices),
                           key=lambda region: region[1]))
    # A layer of only directories has no blob: its device is the bootstrap.
    end = (regions[-1][1] + regions[-1][2]) * BLOCK if regions else round_up(bootstrap.size)
    return ChunkMap(bootstrap.size, end, regions,
                    tuple(offset for offset, _ in ordered), tuple(size for _, (size, _) in ordered),
                    tuple(chunk_id for _, (_, chunk_id) in ordered))


# --- Packs: header, address-ordered data, footer sorted by id, trailer ---

PACK_HEADER = struct.Struct("<4sHH8x")
PACK_ENTRY = struct.Struct("<32sIIIB4x")
PACK_TRAILER = struct.Struct("<QI32s4s")
_PACK_MAGIC, _TRAILER_MAGIC = b"UCPK", b"UCPT"


class PackWriter:
    """Write one immutable pack file of at most 64 MiB."""

    def __init__(self, path):
        self.path, self.entries, self._hash = path, [], hashlib.sha256()
        self._data = 0  # Chunk bytes so far: summing the entries per add made filling a pack quadratic.
        self._stream = open(path, "xb")
        self._write(PACK_HEADER.pack(_PACK_MAGIC, 1, 0))

    def _write(self, data):
        self._stream.write(data)
        self._hash.update(data)

    @property
    def size(self):
        return PACK_HEADER.size + self._data

    def fits(self, clen):
        return self.size + clen + (len(self.entries) + 1) * PACK_ENTRY.size + PACK_TRAILER.size <= MAX_PACK_BYTES

    def add(self, chunk_id, payload, ulen, flags):
        if not self.fits(len(payload)) or flags not in (RAW, ZSTD) or (flags == RAW and len(payload) != ulen):
            raise ValueError("chunk does not fit this pack")
        self.entries.append((chunk_id, self.size, len(payload), ulen, flags))
        self._data += len(payload)
        self._write(payload)

    def finish(self):
        """(sha256 hex, size) of the closed pack."""
        footer = b"".join(PACK_ENTRY.pack(*entry) for entry in sorted(self.entries))
        self._write(footer)
        self._write(PACK_TRAILER.pack(len(footer), len(self.entries), hashlib.sha256(footer).digest(),
                                      _TRAILER_MAGIC))
        self._stream.close()
        return self._hash.hexdigest(), self.size + len(footer) + PACK_TRAILER.size


def parse_pack_tail(tail, pack_size):
    """Footer entries (id, offset, clen, ulen, flags) from a pack's last bytes.

    Returns None when ``tail`` is too short; the caller reads the length the
    trailer names. Entries are validated against the pack's data region.
    """
    if len(tail) < PACK_TRAILER.size or not 0 < pack_size <= MAX_PACK_BYTES or len(tail) > pack_size:
        raise ValueError("invalid pack tail")
    footer_len, count, footer_sha, magic = PACK_TRAILER.unpack_from(tail, len(tail) - PACK_TRAILER.size)
    data_end = pack_size - footer_len - PACK_TRAILER.size
    if magic != _TRAILER_MAGIC or footer_len != count * PACK_ENTRY.size or data_end < PACK_HEADER.size:
        raise ValueError("invalid pack trailer")
    if footer_len + PACK_TRAILER.size > len(tail):
        return None
    footer = tail[len(tail) - PACK_TRAILER.size - footer_len:len(tail) - PACK_TRAILER.size]
    if hashlib.sha256(footer).digest() != footer_sha:
        raise ValueError("pack footer digest mismatch")
    entries = list(PACK_ENTRY.iter_unpack(footer))
    if any(entries[index][0] >= entries[index + 1][0] for index in range(len(entries) - 1)):
        raise ValueError("pack footer is not sorted by chunk id")
    end = PACK_HEADER.size
    for _id, offset, clen, ulen, flags in sorted(entries, key=lambda entry: entry[1]):
        if (offset < end or offset + clen > data_end or flags not in (RAW, ZSTD) or not 0 < ulen <= CHUNK_BYTES
                or not 0 < clen <= MAX_CLEN or (flags == RAW and clen != ulen)):
            raise ValueError("invalid pack footer entry")
        end = offset + clen
    return entries


def pack_key(prefix, digest):
    return f"{prefix}/packs/{require_hex(digest)[:2]}/{digest}.pack"


def bootstrap_key(prefix, digest):
    return f"{prefix}/meta/{require_hex(digest)}.boot.zst"


def chunk_map_key(prefix, digest):
    return f"{prefix}/meta/{require_hex(digest)}.map"


def locator_key(prefix, component_hex, epoch):
    """A component's worker locator for one epoch, built once at registration
    (a store node only): attaches read it instead of querying the index."""
    return f"{prefix}/meta/{require_hex(component_hex)}.{int(epoch)}.loc"


def blob_layout_key(prefix, blob_id):
    """A nydus blob's chunk locations, in its tail table's order (an encoded
    Locator), built at registration: virtual blob reads never query the index.
    Compaction (M4) must rewrite it with the locator epochs it bumps."""
    return f"{prefix}/meta/{require_hex(blob_id)}.layout"


def tail_key(prefix, blob_id):
    """A nydus blob's chunk table and tail (encode_tail); only conversions
    for nydusd keep one (spike, docs/benchmarks/nydusd-spike-2026-10-03)."""
    return f"{prefix}/meta/{require_hex(blob_id)}.tail"


TAIL_HEADER = struct.Struct("<8sI")
TAIL_ENTRY = struct.Struct("<QI32s?")  # Blob offset, size, chunk id, zstd.
_TAIL_MAGIC = b"UCTAIL\x00\x01"


def blob_layout(bootstrap, blob_id):
    """((offset, size, chunk id, zstd), ...) of one nydus blob's chunks in a
    parsed layer bootstrap, in blob order; they must fill it from 0."""
    position = next((index for index, device in enumerate(bootstrap.devices) if device[0] == blob_id), None)
    if position is None:
        raise LookupError("the bootstrap names no such blob")
    unique = {}
    for digest, blob, flags, csize, _usize, coff, _uoff in bootstrap.chunks:
        entry = (coff, csize, digest, bool(flags & 1))
        if blob == position and unique.setdefault(coff, entry) != entry:
            raise ValueError("two chunks share one blob offset")
    layout = tuple(sorted(unique.values()))
    if any(left[0] + left[1] != right[0] for left, right in zip(((0, 0),) + layout, layout)):
        raise ValueError("a blob's chunks do not fill it from 0")
    return layout


def encode_tail(layout, tail):
    """The tail object: the blob's chunk table, then the bytes after its last
    chunk (nydus's chunk info, digests and TOC)."""
    return b"".join((TAIL_HEADER.pack(_TAIL_MAGIC, len(layout)), *(TAIL_ENTRY.pack(*entry) for entry in layout), tail))


def decode_tail_table(payload):
    """(chunk table, offset of the tail bytes) from a tail object's prefix."""
    magic, count = TAIL_HEADER.unpack_from(payload)
    table = TAIL_HEADER.size + count * TAIL_ENTRY.size
    if magic != _TAIL_MAGIC or count > MAX_MAP_ENTRIES or len(payload) < table:
        raise ValueError("invalid nydus blob tail")
    layout = tuple(TAIL_ENTRY.iter_unpack(payload[TAIL_HEADER.size:table]))
    if any(left[0] + left[1] != right[0] for left, right in zip(((0, 0),) + layout, layout)):
        raise ValueError("a blob's chunks do not fill it from 0")
    return layout, table


# --- Locator: chunk-map entry i -> (pack, offset, clen, flags); unsigned ---

_LOCATOR_MAGIC = b"UCLOC\x00\x00\x01"
_LOCATOR_HEADER = struct.Struct("<8sQII")
LOCATOR_ENTRY = struct.Struct("<IIIB")
_MAX_LOCATOR_JSON = 16 * 1024 ** 2


@dataclass(frozen=True)
class Locator:
    """A hint only: wrong data means a verified miss, never wrong bytes."""
    epoch: int
    packs: tuple  # (pack sha256 hex, URL)
    entries: tuple  # (pack index, offset, clen, flags), aligned with the chunk map
    meta: dict  # {"bootstrap": URL, "chunk_map": URL}; empty for builder lookups

    def __post_init__(self):
        if type(self.epoch) is not int or not 0 <= self.epoch < 2 ** 63 or len(self.entries) > MAX_MAP_ENTRIES:
            raise ValueError("invalid chunk locator")
        if len({digest for digest, _ in self.packs}) != len(self.packs) or len(self.packs) > 1 << 16:
            raise ValueError("invalid chunk locator pack table")
        for digest, url in self.packs:
            _require_url(url)
            require_hex(digest)
        if set(self.meta) - {"bootstrap", "chunk_map"}:
            raise ValueError("invalid chunk locator metadata")
        for url in self.meta.values():
            _require_url(url)
        for pack, offset, clen, flags in self.entries:
            if (pack >= len(self.packs) or offset < PACK_HEADER.size or not 0 < clen <= MAX_CLEN
                    or offset + clen > MAX_PACK_BYTES or flags not in (RAW, ZSTD)):
                raise ValueError("invalid chunk locator entry")

    def encode(self):
        document = json.dumps({"packs": [list(pack) for pack in self.packs], "meta": self.meta},
                              sort_keys=True, separators=(",", ":")).encode()
        return b"".join((_LOCATOR_HEADER.pack(_LOCATOR_MAGIC, self.epoch, len(self.entries), len(document)),
                         document, *(LOCATOR_ENTRY.pack(*entry) for entry in self.entries)))

    @classmethod
    def decode(cls, payload):
        if len(payload) < _LOCATOR_HEADER.size:
            raise ValueError("chunk locator is truncated")
        magic, epoch, count, size = _LOCATOR_HEADER.unpack_from(payload)
        body = _LOCATOR_HEADER.size + size
        if (magic != _LOCATOR_MAGIC or size > _MAX_LOCATOR_JSON or count > MAX_MAP_ENTRIES
                or len(payload) != body + count * LOCATOR_ENTRY.size):
            raise ValueError("invalid chunk locator encoding")
        try:
            document = json.loads(payload[_LOCATOR_HEADER.size:body])
            packs = tuple((digest, url) for digest, url in document["packs"])
            meta = document["meta"]
        except (ValueError, KeyError, TypeError) as exc:
            raise ValueError("invalid chunk locator pack table") from exc
        if not isinstance(document, dict) or set(document) != {"packs", "meta"} or not isinstance(meta, dict):
            raise ValueError("invalid chunk locator pack table")
        return cls(epoch, packs, tuple(LOCATOR_ENTRY.iter_unpack(payload[body:])), meta)


def _require_url(url):
    if (not isinstance(url, str) or not url.startswith(("http://", "https://")) or len(url) > 8192
            or any(character in url for character in "\0\r\n ")):
        raise ValueError("invalid chunk store URL")
    return url
