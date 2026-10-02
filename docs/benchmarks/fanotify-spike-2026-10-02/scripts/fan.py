"""S13: minimal fanotify pre-content (HSM) bindings over ctypes (glibc fanotify_init / fanotify_mark).

Constants are the uapi values of linux/fanotify.h (6.14+); check_header() compares them with the
installed header so a mismatch is loud. Events are parsed from the raw read() buffer:
  struct fanotify_event_metadata { u32 event_len; u8 vers; u8 reserved; u16 metadata_len;
                                   u64 mask; s32 fd; s32 pid; }                       24 B
  struct fanotify_event_info_range { u8 info_type; u8 pad; u16 len; u32 pad; u64 offset; u64 count; }
"""
import ctypes
import ctypes.util
import os
import re
import struct

FAN_CLOEXEC, FAN_NONBLOCK = 0x1, 0x2
FAN_CLASS_NOTIF, FAN_CLASS_CONTENT, FAN_CLASS_PRE_CONTENT = 0x0, 0x4, 0x8
FAN_UNLIMITED_QUEUE, FAN_UNLIMITED_MARKS = 0x10, 0x20
FAN_REPORT_FD_ERROR = 0x2000
FAN_ACCESS, FAN_MODIFY, FAN_OPEN = 0x1, 0x2, 0x20
FAN_OPEN_PERM, FAN_ACCESS_PERM, FAN_OPEN_EXEC_PERM = 0x10000, 0x20000, 0x40000
FAN_PRE_ACCESS = 0x100000
FAN_MARK_ADD, FAN_MARK_REMOVE, FAN_MARK_FLUSH = 0x1, 0x2, 0x80
FAN_MARK_INODE, FAN_MARK_MOUNT, FAN_MARK_FILESYSTEM = 0x0, 0x10, 0x100
FAN_EVENT_INFO_TYPE_RANGE = 6
FAN_ALLOW, FAN_DENY = 0x1, 0x2
FAN_ERRNO_SHIFT, FAN_ERRNO_MASK = 24, 0xff
FAN_NOFD = -1
AT_FDCWD = -100

META = struct.Struct("<IBBHQii")
INFO_HDR = struct.Struct("<BBH")
RANGE = struct.Struct("<BBHIQQ")
RESPONSE = struct.Struct("<iI")

_libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
_libc.fanotify_init.argtypes = (ctypes.c_uint, ctypes.c_uint)
_libc.fanotify_init.restype = ctypes.c_int
_libc.fanotify_mark.argtypes = (ctypes.c_int, ctypes.c_uint, ctypes.c_uint64, ctypes.c_int, ctypes.c_char_p)
_libc.fanotify_mark.restype = ctypes.c_int


def deny_errno(err):
    return FAN_DENY | ((err & FAN_ERRNO_MASK) << FAN_ERRNO_SHIFT)


def init(flags=FAN_CLASS_PRE_CONTENT | FAN_CLOEXEC | FAN_UNLIMITED_QUEUE | FAN_UNLIMITED_MARKS,
         event_f_flags=os.O_RDONLY | os.O_LARGEFILE | os.O_CLOEXEC):
    fd = _libc.fanotify_init(flags, event_f_flags)
    if fd < 0:
        e = ctypes.get_errno()
        raise OSError(e, f"fanotify_init(0x{flags:x}): {os.strerror(e)}")
    return fd


def mark(fd, path, mask=FAN_PRE_ACCESS, flags=FAN_MARK_ADD | FAN_MARK_INODE):
    r = _libc.fanotify_mark(fd, flags, mask, AT_FDCWD, os.fsencode(path) if path is not None else None)
    if r < 0:
        e = ctypes.get_errno()
        raise OSError(e, f"fanotify_mark(0x{flags:x}, 0x{mask:x}, {path}): {os.strerror(e)}")


def unmark(fd, path, mask=FAN_PRE_ACCESS):
    mark(fd, path, mask, FAN_MARK_REMOVE | FAN_MARK_INODE)


def parse(buf):
    """Yield (mask, fd, pid, ranges[(offset, count)], raw_infos[(type, len)]) per event."""
    pos = 0
    while pos + META.size <= len(buf):
        event_len, vers, _, mlen, mask, fd, pid = META.unpack_from(buf, pos)
        if event_len < META.size:
            break
        ranges, infos = [], []
        ip = pos + mlen
        while ip + INFO_HDR.size <= pos + event_len:
            itype, _, ilen = INFO_HDR.unpack_from(buf, ip)
            infos.append((itype, ilen))
            if itype == FAN_EVENT_INFO_TYPE_RANGE and ilen >= RANGE.size:
                _, _, _, _, off, count = RANGE.unpack_from(buf, ip)
                ranges.append((off, count))
            if ilen <= 0:
                break
            ip += ilen
        yield mask, fd, pid, ranges, infos, vers, mlen, event_len
        pos += event_len


def respond(gfd, efd, response=FAN_ALLOW):
    return os.write(gfd, RESPONSE.pack(efd, response))


def check_header(path="/usr/include/linux/fanotify.h"):
    """Compare our constants with the installed uapi header; returns {name: (ours, header)} mismatches."""
    try:
        text = open(path).read()
    except OSError as e:
        return {"error": repr(e)}
    vals = {}
    for m in re.finditer(r"#define\s+(FAN_\w+)\s+(0x[0-9a-fA-F]+|\d+)\b", text):
        vals[m.group(1)] = int(m.group(2), 0)
    ours = {k: v for k, v in globals().items() if k.startswith("FAN_") and isinstance(v, int)}
    out = {"mismatch": {k: (v, vals[k]) for k, v in ours.items() if k in vals and vals[k] != v},
           "missing_in_header": sorted(k for k in ours if k not in vals),
           "has_pre_access": "FAN_PRE_ACCESS" in vals,
           "has_range_info": "FAN_EVENT_INFO_TYPE_RANGE" in vals,
           "deny_errno_macro": bool(re.search(r"FAN_DENY_ERRNO", text)),
           "header_extra": {k: v for k, v in vals.items() if k not in ours and ("RESP" in k or "ERRNO" in k or "PRE" in k
                                                                            or "INFO" in k or "REPORT" in k)}}
    return out
