"""S12 prototype formats (design §1.2, §1.4): pack files and the combined chunk map + locator.

Pack:    header 16 B  = magic "UCPK", version 1, flags, reserved[10]
         data         = chunk payloads back to back, in conversion (blob/address) order
         footer       = N x 49 B sorted by id: id[32] off:u32 clen:u32 ulen:u32 flags:u8 reserved[4]
         trailer 48 B = footer_len:u64 count:u32 sha256(footer)[32] magic "UCPK"
Map+loc: header JSON line (bootstrap digest/size, device size, pack table), then entries sorted by
         device offset: dev_off:u64 ulen:u32 id[32] pack:u32 off:u32 clen:u32 flags:u8 (57 B).
         The signed chunk map is the first three fields (44 B), the unsigned locator the rest (13 B).
"""
import hashlib
import json
import struct

HEADER = struct.Struct("<4sBB10s")
ENTRY = struct.Struct("<32sIIIB4s")
TRAILER = struct.Struct("<QI32s4s")
MAGIC = b"UCPK"
MAX_PACK = 64 << 20
F_ZSTD = 1
MAPENT = struct.Struct("<QI32sIIIB")


class PackWriter:
    def __init__(self):
        self.buf = bytearray(HEADER.pack(MAGIC, 1, 0, b"\0" * 10))
        self.entries = []  # (id, off, clen, ulen, flags)

    def fits(self, clen):
        return len(self.buf) + clen + (len(self.entries) + 1) * ENTRY.size + TRAILER.size <= MAX_PACK

    def add(self, cid, payload, ulen, flags):
        off = len(self.buf)
        self.buf += payload
        self.entries.append((cid, off, len(payload), ulen, flags))
        return off

    def __len__(self):
        return len(self.entries)

    def finish(self):
        footer = b"".join(ENTRY.pack(c, o, cl, ul, f, b"\0" * 4) for c, o, cl, ul, f in sorted(self.entries))
        data = bytes(self.buf) + footer + TRAILER.pack(len(footer), len(self.entries),
                                                       hashlib.sha256(footer).digest(), MAGIC)
        return hashlib.sha256(data).hexdigest(), data


def read_footer(tail):
    """Parse a pack's footer from its last bytes (a suffix range read). Returns entries or raises."""
    flen, n, fsha, magic = TRAILER.unpack(tail[-TRAILER.size:])
    if magic != MAGIC or flen != n * ENTRY.size or len(tail) < flen + TRAILER.size:
        raise ValueError("bad pack trailer")
    footer = tail[-TRAILER.size - flen:-TRAILER.size]
    if hashlib.sha256(footer).digest() != fsha:
        raise ValueError("footer digest mismatch")
    return [ENTRY.unpack_from(footer, i * ENTRY.size)[:5] for i in range(n)]


def write_map(path, header, entries):
    """entries: (dev_off, ulen, id, pack, off, clen, flags) sorted by dev_off."""
    body = b"".join(MAPENT.pack(*e) for e in entries)
    raw = json.dumps(header).encode() + b"\n" + body
    with open(path, "wb") as f:
        f.write(raw)
    return len(raw)


def parse_map(raw):
    nl = raw.index(b"\n")
    header = json.loads(raw[:nl])
    body = memoryview(raw)[nl + 1:]
    return header, list(MAPENT.iter_unpack(body))
