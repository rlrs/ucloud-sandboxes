"""Read Linux mount-root status without starting a helper process."""

import ctypes
from functools import cache
import os
from pathlib import Path
import sys


class _Statx(ctypes.Structure):
    # Stable Linux statx ABI through stx_attributes_mask (linux/stat.h).
    # Keep the complete 256-byte output buffer, including fields we don't use.
    _fields_ = [
        ("mask", ctypes.c_uint32),
        ("block_size", ctypes.c_uint32),
        ("attributes", ctypes.c_uint64),
        ("metadata", ctypes.c_byte * 40),
        ("attributes_mask", ctypes.c_uint64),
        ("remaining", ctypes.c_byte * 192),
    ]


@cache
def _statx_function():
    if sys.platform != "linux":
        return None
    try:
        function = ctypes.CDLL(None, use_errno=True).statx
    except AttributeError:
        return None
    function.argtypes = [
        ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_uint,
        ctypes.POINTER(_Statx),
    ]
    function.restype = ctypes.c_int
    return function


def linux_mount_root(path: Path) -> bool | None:
    """Return fresh mount status, or None when the helper must be used.

    Unlike comparing st_dev, STATX_ATTR_MOUNT_ROOT also detects same-filesystem
    bind mounts. Never interpret an unsupported or failed query as unmounted.
    Callers retain their mount locks and path ownership checks.
    """
    function = _statx_function()
    if function is None:
        return None
    result = _Statx()
    # AT_FDCWD, AT_SYMLINK_NOFOLLOW | AT_NO_AUTOMOUNT, STATX_BASIC_STATS.
    if function(-100, os.fsencode(path), 0x100 | 0x800, 0x7FF, ctypes.byref(result)):
        return None
    mount_root = 0x2000
    if not result.attributes_mask & mount_root:
        return None
    return bool(result.attributes & mount_root)
