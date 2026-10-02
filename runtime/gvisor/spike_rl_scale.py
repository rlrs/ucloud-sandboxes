#!/usr/bin/env python3
"""RL-scale M0 spikes S1, S2, S4, S6, S7 and S8 (docs/rl-scale-architecture-plan.md, section 9).

Run as root on a disposable Linux qualification VM, never on a node that serves
sandboxes. Each probe writes one JSON section with status
pass|fail|unsupported|error, an answer, raw observations and every command it
ran. "pass" means the probe completed and the answer is the one the plan
assumes (S2 instead reports its duplication verdict in "answer"); "fail" is a
measured contrary answer, "unsupported" means the feature is absent, and
"error" means the probe itself could not complete.

  s1  pinned runsc --overlay2 default and where guest writes land on the host
  s2  page sharing across K Sentries reading one rootfs (plan C0.3)
  s4  whether `runsc tar rootfs-upper` exists, and its output after writes
  s6  memory.reclaim swappiness=, swap/zswap, and a frozen-cgroup swap test
  s7  whether runsc supports --host-uds, and its values
  s8  kernel feature probe: cgroup v2, cpu.idle, memory.reclaim, zswap, EROFS,
      ublk, nbd, overlay, XFS reflink on the work root

Everything the probes create (containers, mounts, cgroups, processes, files)
lives under one run directory and one cgroup that this script creates, and is
removed on exit. On a cleanup failure the run directory is retained and named
in the report. Kernel and cgroup writes go only to cgroups this script created.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
import errno
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import stat
import struct
import subprocess
import sys
import tarfile
import threading
import time
from typing import Any, Callable, Iterator, Sequence
import uuid


SCHEMA = "ucloud-rl-scale-spike/v1"
PROBES = ("s1", "s2", "s4", "s6", "s7", "s8")
VARIANTS = ("gofer", "gofer-shared", "erofs")
STATUSES = ("pass", "fail", "unsupported", "error")
MIB = 1024 * 1024
CGROUP_ROOT = Path("/sys/fs/cgroup")
OWNER_MARKER = ".ucloud-rl-spike-owner"
FICLONE = 0x40049409
GUEST_PATH = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
DEFAULT_READ_COMMAND = (
    "find /usr/lib/python3* /usr/local/lib/python3* -type f -exec cat {} + "
    "2>/dev/null | wc -c"
)
DEFAULT_GUEST_INIT = "exec sleep 2147483647"
QUESTIONS = {
    "s1": "Is the pinned runsc default --overlay2=root:self, and is the filestore "
          "the only copy of guest writes?",
    "s2": "Does the gofer path duplicate image pages per Sentry? (plan C0.3)",
    "s4": "Does `runsc tar rootfs-upper` exist in the pinned runsc?",
    "s6": "Does memory.reclaim swappiness= push tmpfs pages to zswap and swap, "
          "and what does thaw cost?",
    "s7": "Does runsc support --host-uds, and with which values?",
    "s8": "Are cpu.idle, zswap and the memory.reclaim arguments enabled in this kernel?",
}
# Docker's default capability set, so operator images behave as they would in
# a container; these are capabilities inside the Sentry, not on the host.
GUEST_CAPABILITIES = [
    "CAP_AUDIT_WRITE", "CAP_CHOWN", "CAP_DAC_OVERRIDE", "CAP_FOWNER", "CAP_FSETID",
    "CAP_KILL", "CAP_MKNOD", "CAP_NET_BIND_SERVICE", "CAP_NET_RAW", "CAP_SETFCAP",
    "CAP_SETGID", "CAP_SETPCAP", "CAP_SETUID", "CAP_SYS_CHROOT",
]
KERNEL_CONFIG_KEYS = (
    "CONFIG_MEMCG", "CONFIG_MEMCG_SWAP", "CONFIG_CGROUP_SCHED", "CONFIG_FAIR_GROUP_SCHED",
    "CONFIG_PSI", "CONFIG_SWAP", "CONFIG_ZSWAP", "CONFIG_ZSWAP_DEFAULT_ON",
    "CONFIG_ZSWAP_SHRINKER_DEFAULT_ON", "CONFIG_ZSMALLOC", "CONFIG_LRU_GEN",
    "CONFIG_EROFS_FS", "CONFIG_EROFS_FS_XATTR", "CONFIG_EROFS_FS_ZIP",
    "CONFIG_EROFS_FS_ZIP_LZMA", "CONFIG_EROFS_FS_ONDEMAND", "CONFIG_CACHEFILES_ONDEMAND",
    "CONFIG_BLK_DEV_UBLK", "CONFIG_BLK_DEV_NBD", "CONFIG_OVERLAY_FS", "CONFIG_XFS_FS",
    "CONFIG_TRANSPARENT_HUGEPAGE", "CONFIG_USERFAULTFD",
)
MEMINFO_KEYS = ("MemTotal", "MemFree", "MemAvailable", "Cached", "Shmem", "AnonPages",
                "Mapped", "SwapTotal", "SwapFree", "SwapCached", "Zswap", "Zswapped")
SHARING_METRICS = (
    "host_mem_available_drop", "cgroup_memory_current_sum", "cgroup_file_sum",
    "cgroup_anon_sum", "cgroup_shmem_sum", "sentry_rss_sum", "sentry_pss_sum",
    "sentry_uss_sum", "memory_file_allocated_sum",
)


# ---------------------------------------------------------------------------
# Pure parsing and computation helpers (unit-tested without root).

_FLAG_LINE = re.compile(r"^(?: {1,4}|\t)-{1,2}([A-Za-z0-9][\w.-]*)(?:[ \t]+(.*?))?\s*$")
_DEFAULT = re.compile(r"\(default (?:\"((?:[^\"\\]|\\.)*)\"|(.*?))\)\s*$")


def parse_go_flags(text: str) -> dict[str, dict[str, Any]]:
    """Parse Go flag.PrintDefaults output (what `runsc flags` prints)."""
    flags: dict[str, dict[str, Any]] = {}
    current: dict[str, Any] | None = None
    for line in text.splitlines():
        match = None if line.startswith(("    \t", "\t\t")) else _FLAG_LINE.match(line)
        if match:
            name, rest = match.group(1), (match.group(2) or "").strip()
            single_word = bool(rest) and not re.search(r"\s", rest)
            current = {"type": rest if single_word else None,
                       "lines": [] if single_word or not rest else [rest]}
            flags[name] = current
        elif current is not None and line.strip():
            current["lines"].append(line.strip())
    parsed = {}
    for name, entry in flags.items():
        usage = " ".join(entry["lines"])
        parsed[name] = {"type": entry["type"], "usage": usage, "default": flag_default(usage)}
    return parsed


def flag_default(usage: str) -> str | None:
    """Go's trailing "(default X)", else a default stated in the usage text.

    Go omits "(default X)" for zero values, so runsc states some defaults
    inline: "Values: none|open, default: none" or "exclusive (default), shared".
    """
    match = _DEFAULT.search(usage)
    if match is not None:
        if match.group(1) is not None:
            return re.sub(r"\\(.)", r"\1", match.group(1))
        return match.group(2)
    inline = (re.search(r"(?i)\bdefault:\s*[\"']?([\w.:/=-]+)", usage)
              or re.search(r"[\"']?([\w.:/=-]+)[\"']? \(default\)", usage))
    return inline.group(1) if inline else None


def flag_values(usage: str) -> list[str]:
    """Allowed values named in a flag's usage text, e.g. "none"|"open"|"create"."""
    text = re.sub(r"\(default [^)]*\)\s*$", "", usage)
    head = re.split(r"(?i)\bdefault\b", text, maxsplit=1)[0]
    values = re.findall(r"[\"']([A-Za-z0-9][\w.:=-]*)[\"']", head)
    if not values:
        listed = re.search(r"(?i)\bvalues?\s*(?:are|:)\s*(.+)", head)
        if listed:
            values = [item for item in re.split(r"[\s|,/]+", listed.group(1).strip(" .;"))
                      if re.fullmatch(r"[A-Za-z0-9][\w.:=-]*", item)]
    return list(dict.fromkeys(values))


def parse_subcommands(text: str) -> list[str]:
    """Subcommand names from google/subcommands help output (`runsc help`)."""
    names: list[str] = []
    in_section = False
    for line in text.splitlines():
        if re.match(r"^\s*Subcommands(?: for [^:]+)?:\s*$", line):
            in_section = True
            continue
        if in_section:
            if not line.strip():
                in_section = False
                continue
            match = re.match(r"^\s+([A-Za-z0-9][\w.-]*)(?:\s|$)", line)
            if match:
                names.append(match.group(1))
    return list(dict.fromkeys(names))


def mentions_subcommand(text: str, name: str) -> bool:
    """Whether a (group) help text names the subcommand as a whole word."""
    return re.search(rf"(?<![\w-]){re.escape(name)}(?![\w-])", text) is not None


def parse_runsc_version(text: str) -> str | None:
    match = re.search(r"(?m)^runsc version\s+(\S+)", text)
    return match.group(1) if match else None


def effective_flag_value(default: str | None, extra_flags: Sequence[str],
                         name: str) -> str | None:
    value = default
    for flag in extra_flags:
        match = re.fullmatch(rf"--?{re.escape(name)}=(.*)", flag)
        if match:
            value = match.group(1)
    return value


def parse_kv_lines(text: str) -> dict[str, int]:
    """memory.stat, cgroup.events, /proc/vmstat: '<key> <integer>' lines."""
    values = {}
    for line in text.splitlines():
        parts = line.split()
        if len(parts) == 2 and re.fullmatch(r"-?\d+", parts[1]):
            values[parts[0]] = int(parts[1])
    return values


def parse_meminfo(text: str) -> dict[str, int]:
    """/proc/meminfo as bytes (kB lines) or counts (unitless lines)."""
    values = {}
    for line in text.splitlines():
        match = re.match(r"^([\w()]+):\s+(\d+)(?:\s+(kB))?\s*$", line)
        if match:
            values[match.group(1)] = int(match.group(2)) * (1024 if match.group(3) else 1)
    return values


def parse_smaps_rollup(text: str) -> dict[str, int]:
    """/proc/<pid>/smaps_rollup in bytes; Uss = Private_Clean + Private_Dirty."""
    values = {}
    for line in text.splitlines():
        match = re.match(r"^(\w+):\s+(\d+)\s+kB\s*$", line)
        if match:
            values[match.group(1)] = int(match.group(2)) * 1024
    if "Private_Clean" in values and "Private_Dirty" in values:
        values["Uss"] = values["Private_Clean"] + values["Private_Dirty"]
    return values


def parse_proc_swaps(text: str) -> list[dict[str, Any]]:
    entries = []
    for line in text.splitlines()[1:]:
        parts = line.split()
        if len(parts) >= 5 and parts[2].isdigit() and parts[3].isdigit():
            entries.append({"filename": parts[0], "type": parts[1],
                            "size_bytes": int(parts[2]) * 1024,
                            "used_bytes": int(parts[3]) * 1024, "priority": parts[4]})
    return entries


def parse_kernel_config(text: str) -> dict[str, str]:
    """Kconfig as {'CONFIG_X': 'y'|'m'|'n'|value}; '# ... is not set' maps to 'n'."""
    values = {}
    for line in text.splitlines():
        line = line.strip()
        unset = re.fullmatch(r"#\s*(CONFIG_\w+) is not set", line)
        if unset:
            values[unset.group(1)] = "n"
            continue
        match = re.fullmatch(r"(CONFIG_\w+)=(.*)", line)
        if match:
            value = match.group(2)
            if len(value) >= 2 and value[0] == value[-1] == '"':
                value = value[1:-1]
            values[match.group(1)] = value
    return values


def _module_key(name: str) -> str:
    return name.replace("-", "_")


def module_presence(name: str, *, loaded_text: str, builtin_text: str,
                    dep_text: str) -> dict[str, bool]:
    """Whether a kernel module is loaded, built in, or installed as a loadable file."""
    key = _module_key(name)

    def names(text: str) -> set[str]:
        found = set()
        for line in text.splitlines():
            path = line.split(":", 1)[0].strip()
            base = path.rsplit("/", 1)[-1]
            match = re.match(r"^(.+?)\.ko(?:\.\w+)?$", base)
            if match:
                found.add(_module_key(match.group(1)))
        return found

    loaded = {_module_key(line.split()[0]) for line in loaded_text.splitlines() if line.split()}
    return {"loaded": key in loaded, "builtin": key in names(builtin_text),
            "loadable": key in names(dep_text)}


def _unescape_mount_field(value: str) -> str:
    return re.sub(r"\\([0-7]{3})", lambda match: chr(int(match.group(1), 8)), value)


def parse_mountinfo(text: str) -> list[dict[str, str]]:
    mounts = []
    for line in text.splitlines():
        left, separator, right = line.partition(" - ")
        fields = left.split()
        tail = right.split()
        if not separator or len(fields) < 6 or len(tail) < 2:
            continue
        mounts.append({"mount_point": _unescape_mount_field(fields[4]),
                       "options": fields[5], "fstype": tail[0],
                       "source": _unescape_mount_field(tail[1]),
                       "super_options": tail[2] if len(tail) > 2 else ""})
    return mounts


def _is_within(path: str, root: str) -> bool:
    root = root.rstrip("/") or "/"
    return path == root or root == "/" or path.startswith(root + "/")


def mount_for_path(path: str, mounts: Sequence[dict[str, str]]) -> dict[str, str] | None:
    """The mount (last one wins among equal mount points) that contains path."""
    best = None
    for mount in mounts:
        if _is_within(path, mount["mount_point"]) and (
                best is None or len(mount["mount_point"]) >= len(best["mount_point"])):
            best = mount
    return best


def mounts_under(path: str, mounts: Sequence[dict[str, str]]) -> list[str]:
    root = path.rstrip("/")
    return sorted({mount["mount_point"] for mount in mounts
                   if mount["mount_point"] != root and _is_within(mount["mount_point"], root)})


def parse_xfs_info_reflink(text: str) -> bool | None:
    match = re.search(r"\breflink=(\d)", text)
    return None if match is None else match.group(1) == "1"


def elf_interpreter(data: bytes) -> str | None:
    """PT_INTERP of an ELF image, or None for a static executable."""
    if len(data) < 64 or data[:4] != b"\x7fELF":
        raise ValueError("not an ELF executable")
    klass, encoding = data[4], data[5]
    if klass not in (1, 2) or encoding not in (1, 2):
        raise ValueError("unsupported ELF class or encoding")
    order = "<" if encoding == 1 else ">"
    if klass == 2:
        phoff, = struct.unpack_from(order + "Q", data, 0x20)
        phentsize, phnum = struct.unpack_from(order + "HH", data, 0x36)
    else:
        phoff, = struct.unpack_from(order + "I", data, 0x1C)
        phentsize, phnum = struct.unpack_from(order + "HH", data, 0x2A)
    for index in range(phnum):
        offset = phoff + index * phentsize
        if offset + phentsize > len(data):
            raise ValueError("truncated ELF program headers")
        p_type, = struct.unpack_from(order + "I", data, offset)
        if p_type != 3:  # PT_INTERP
            continue
        if klass == 2:
            p_offset, = struct.unpack_from(order + "Q", data, offset + 8)
            p_filesz, = struct.unpack_from(order + "Q", data, offset + 32)
        else:
            p_offset, = struct.unpack_from(order + "I", data, offset + 4)
            p_filesz, = struct.unpack_from(order + "I", data, offset + 16)
        return data[p_offset:p_offset + p_filesz].rstrip(b"\0").decode(errors="replace")
    return None


def classify_fd_link(link: str) -> str | None:
    """Kind of a Sentry file descriptor target relevant to memory accounting."""
    if ".gvisor.filestore" in link:
        return "filestore"
    if re.search(r"runsc-memory|application[_-]memory|memfd:|\.memory(?: \(deleted\))?$", link):
        return "memory_file"
    return None


def page_sharing_ratio(bytes_k: int | float | None, k: int,
                       bytes_1: int | float | None) -> float | None:
    """host bytes for K / (K x bytes for 1): 1.0 = fully duplicated, 1/K = fully shared."""
    if bytes_k is None or bytes_1 is None or k <= 0 or bytes_1 <= 0:
        return None
    return bytes_k / (k * bytes_1)


def classify_sharing(ratio: float | None, k: int) -> str:
    if ratio is None or k <= 1:
        return "unknown"
    if ratio >= 0.8:
        return "duplicated"
    if ratio * k <= 1.2:
        return "shared"
    return "partially_shared"


def aggregate_sandbox_stats(stats: Sequence[dict[str, Any]]) -> dict[str, int | None]:
    """Sum per-sandbox memory figures; a metric missing on any sandbox is None."""
    getters: dict[str, Callable[[dict[str, Any]], Any]] = {
        "cgroup_memory_current_sum": lambda row: row.get("memory_current"),
        "cgroup_file_sum": lambda row: (row.get("memory_stat") or {}).get("file"),
        "cgroup_anon_sum": lambda row: (row.get("memory_stat") or {}).get("anon"),
        "cgroup_shmem_sum": lambda row: (row.get("memory_stat") or {}).get("shmem"),
        "sentry_rss_sum": lambda row: (row.get("sentry_smaps") or {}).get("Rss"),
        "sentry_pss_sum": lambda row: (row.get("sentry_smaps") or {}).get("Pss"),
        "sentry_uss_sum": lambda row: (row.get("sentry_smaps") or {}).get("Uss"),
        "memory_file_allocated_sum": lambda row: row.get("memory_file_allocated"),
    }
    totals: dict[str, int | None] = {}
    for name, getter in getters.items():
        values = [getter(row) for row in stats]
        totals[name] = (sum(values) if stats and all(isinstance(v, int) for v in values)
                        else None)
    return totals


def k_totals(host: dict[str, dict[str, int]], idle: Sequence[dict[str, Any]],
             after_read: Sequence[dict[str, Any]]) -> dict[str, dict[str, int | None]]:
    """Totals for one K run: idle, after_read, and the read increment between them."""
    def available_drop(phase: str) -> int | None:
        before = host.get("before", {}).get("MemAvailable")
        current = host.get(phase, {}).get("MemAvailable")
        return None if before is None or current is None else before - current

    idle_totals = {"host_mem_available_drop": available_drop("idle"),
                   **aggregate_sandbox_stats(idle)}
    read_totals = {"host_mem_available_drop": available_drop("after_read"),
                   **aggregate_sandbox_stats(after_read)}
    increment = {name: (None if idle_totals.get(name) is None or read_totals.get(name) is None
                        else read_totals[name] - idle_totals[name])
                 for name in SHARING_METRICS}
    return {"idle": idle_totals, "after_read": read_totals, "read_increment": increment}


def sharing_ratios(per_k: dict[int, dict[str, dict[str, int | None]]]) -> dict[str, Any]:
    """Ratio and scaling factor per phase and metric, relative to K=1."""
    if 1 not in per_k:
        raise ValueError("page-sharing ratios need a K=1 run")
    result: dict[str, Any] = {}
    for phase in ("after_read", "read_increment"):
        result[phase] = {}
        for metric in SHARING_METRICS:
            base = per_k[1][phase].get(metric)
            rows = {}
            for k in sorted(per_k):
                value = per_k[k][phase].get(metric)
                ratio = page_sharing_ratio(value, k, base)
                rows[str(k)] = {"bytes": value, "ratio": None if ratio is None else round(ratio, 4),
                                "scaling_vs_k1": (None if ratio is None else round(ratio * k, 4)),
                                "classification": classify_sharing(ratio, k)}
            result[phase][metric] = rows
    return result


def evaluate_s1(*, overlay2_default: str | None, overlay2_effective: str | None,
                write_bytes: int, filestore_allocated: int | None,
                guest_file_on_host: bool, upper_other_allocated: int | None) -> tuple[str, str]:
    default_ok = overlay2_default == "root:self"
    holds = filestore_allocated is not None and filestore_allocated >= 0.9 * write_bytes
    only_copy = (holds and not guest_file_on_host and upper_other_allocated is not None
                 and upper_other_allocated < 0.1 * write_bytes)
    parts = [f"--overlay2 default is {overlay2_default!r}"
             + ("" if overlay2_effective == overlay2_default
                else f" (effective {overlay2_effective!r})")]
    if filestore_allocated is None:
        parts.append("no .gvisor.filestore.* file was found")
    else:
        parts.append(f"filestore holds {filestore_allocated} allocated bytes for "
                     f"{write_bytes} written")
    parts.append("guest file is visible on the host" if guest_file_on_host
                 else "guest file is not visible on the host")
    if upper_other_allocated is not None:
        parts.append(f"other host-upper allocation is {upper_other_allocated} bytes")
    status = "pass" if default_ok and only_copy else "fail"
    return status, "; ".join(parts)


def classify_reclaim_probe(plain: int | None, swappiness: int | None) -> dict[str, Any]:
    """Interpret errno results of writing '0' and '0 swappiness=N' to memory.reclaim."""
    if plain is not None:
        return {"memory_reclaim_writable": False, "swappiness_argument": None,
                "detail": f"plain write failed: {errno.errorcode.get(plain, plain)}"}
    if swappiness is None:
        return {"memory_reclaim_writable": True, "swappiness_argument": True, "detail": "accepted"}
    return {"memory_reclaim_writable": True, "swappiness_argument": False,
            "detail": f"swappiness rejected: {errno.errorcode.get(swappiness, swappiness)}"}


def work_root_problem(path: Path, *, euid: int) -> str | None:
    """Refuse anything but an existing, private, empty-or-owned directory."""
    try:
        info = path.lstat()
    except FileNotFoundError:
        return f"{path} does not exist"
    if stat.S_ISLNK(info.st_mode):
        return f"{path} is a symlink"
    if not stat.S_ISDIR(info.st_mode):
        return f"{path} is not a directory"
    if info.st_uid != euid:
        return f"{path} is owned by uid {info.st_uid}, not {euid}"
    if info.st_mode & 0o022:
        return f"{path} is group- or world-writable"
    entries = sorted(entry.name for entry in path.iterdir())
    if entries and OWNER_MARKER not in entries:
        return (f"{path} is not empty and has no {OWNER_MARKER} marker; "
                "use an empty directory")
    return None


def overlay_path_problem(path: Path) -> str | None:
    text = str(path)
    if not path.is_absolute() or re.search(r"[,:\\\s]", text):
        return f"overlay paths must be absolute without commas, colons or whitespace: {text}"
    return None


def oci_config(argv: Sequence[str], *, cgroups_path: str | None = None,
               annotations: dict[str, str] | None = None,
               capabilities: Sequence[str] = GUEST_CAPABILITIES) -> dict[str, Any]:
    linux: dict[str, Any] = {
        "namespaces": [{"type": kind} for kind in ("pid", "network", "ipc", "uts", "mount")],
    }
    if cgroups_path:
        linux["cgroupsPath"] = cgroups_path
    return {
        "ociVersion": "1.0.2",
        "root": {"path": "rootfs", "readonly": False},
        "annotations": dict(sorted((annotations or {}).items())),
        "process": {
            "terminal": False, "user": {"uid": 0, "gid": 0}, "args": list(argv),
            "env": [f"PATH={GUEST_PATH}", "HOME=/root", "LANG=C.UTF-8"], "cwd": "/",
            "noNewPrivileges": True,
            "capabilities": {kind: sorted(capabilities)
                             for kind in ("bounding", "effective", "inheritable", "permitted")},
        },
        "mounts": [
            {"destination": "/proc", "type": "proc", "source": "proc",
             "options": ["nosuid", "noexec", "nodev"]},
            {"destination": "/tmp", "type": "tmpfs", "source": "tmpfs",
             "options": ["nosuid", "nodev", "mode=1777"]},
        ],
        "linux": linux,
    }


def tail(text: str | bytes | None, limit: int = 2000) -> str:
    if text is None:
        return ""
    if isinstance(text, bytes):
        text = text.decode(errors="replace")
    return text[-limit:]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Host plumbing: commands, cleanup, cgroups, runsc.


class ProbeError(RuntimeError):
    pass


class Cleanup:
    """LIFO cleanup actions; every action runs and every failure is kept."""

    def __init__(self) -> None:
        self.actions: list[tuple[str, Callable[[], None]]] = []
        self.lock = threading.Lock()

    def push(self, description: str, action: Callable[[], None]) -> None:
        with self.lock:
            self.actions.append((description, action))

    def run(self) -> list[str]:
        errors = []
        while True:
            with self.lock:
                if not self.actions:
                    break
                description, action = self.actions.pop()
            try:
                action()
            except BaseException as exc:  # Keep cleaning after an interrupt.
                errors.append(f"{description}: {type(exc).__name__}: {exc}")
        return errors


class ProbeContext:
    def __init__(self, name: str, args: argparse.Namespace, run_dir: Path,
                 spike_cgroup: "SpikeCgroup | None") -> None:
        self.name = name
        self.args = args
        self.run_dir = run_dir
        self.dir = run_dir / name
        self.cgroup = spike_cgroup
        self.cleanup = Cleanup()
        self.lock = threading.Lock()
        self.sequence = 0
        self.section: dict[str, Any] = {
            "probe": name, "question": QUESTIONS[name], "status": "error", "answer": None,
            "started_at": utc_now(), "observations": {}, "commands": [], "errors": [],
            "cleanup_errors": [],
        }

    def run(self, argv: Sequence[Any], *, timeout: float | None = None, check: bool = True,
            stdout_path: Path | None = None, note: str | None = None) -> subprocess.CompletedProcess:
        argv = [str(item) for item in argv]
        record: dict[str, Any] = {"argv": argv, "started_unix": time.time()}
        if note:
            record["note"] = note
        started = time.monotonic()
        try:
            if stdout_path is not None:
                # Detached runsc keeps its stdio open for the container's life:
                # pipes would never reach EOF, so log to a file instead.
                with stdout_path.open("ab") as log:
                    completed = subprocess.run(argv, stdin=subprocess.DEVNULL, stdout=log,
                                               stderr=subprocess.STDOUT,
                                               timeout=timeout or self.args.command_timeout)
                completed.stdout = stdout_path.read_bytes()[-4000:].decode(errors="replace")
                completed.stderr = ""
                record["stdio_log"] = str(stdout_path)
            else:
                completed = subprocess.run(argv, stdin=subprocess.DEVNULL, capture_output=True,
                                           text=True, timeout=timeout or self.args.command_timeout)
        except subprocess.TimeoutExpired as exc:
            record.update(seconds=time.monotonic() - started, returncode=None,
                          stdout_tail=tail(exc.stdout), stderr_tail=tail(exc.stderr),
                          error="timeout")
            with self.lock:
                self.section["commands"].append(record)
            raise ProbeError(f"command timed out after {exc.timeout}s: {argv}") from exc
        record.update(seconds=time.monotonic() - started, returncode=completed.returncode,
                      stdout_tail=tail(completed.stdout), stderr_tail=tail(completed.stderr))
        with self.lock:
            self.section["commands"].append(record)
        if check and completed.returncode != 0:
            raise ProbeError(f"command failed ({completed.returncode}): {argv}: "
                             f"{tail(completed.stderr or completed.stdout, 600)}")
        return completed

    def mkdir(self, path: Path) -> Path:
        path.mkdir(mode=0o700, parents=True, exist_ok=False)
        return path

    def next_sequence(self) -> int:
        with self.lock:
            self.sequence += 1
            return self.sequence


def write_cgroup_file(path: Path, value: str) -> None:
    """One write() of an exact payload to a cgroup interface file."""
    descriptor = os.open(path, os.O_WRONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        payload = value.encode("ascii")
        if os.write(descriptor, payload) != len(payload):
            raise OSError(errno.EIO, f"short write to {path}")
    finally:
        os.close(descriptor)


def try_write_errno(path: Path, value: str) -> int | None:
    try:
        write_cgroup_file(path, value)
    except OSError as exc:
        return exc.errno
    return None


def read_text(path: Path) -> str | None:
    try:
        return path.read_text(errors="replace")
    except OSError:
        return None


class SpikeCgroup:
    """A cgroup this script creates; children only, never writes above it."""

    def __init__(self, run_id: str) -> None:
        self.path = CGROUP_ROOT / f"ucloud-rl-spike-{run_id}"
        self.relative = "/" + self.path.name
        self.available: list[str] = []
        self.enabled: list[str] = []
        self.created = False
        self.error: str | None = None

    def create(self) -> None:
        if not (CGROUP_ROOT / "cgroup.controllers").exists():
            self.error = "cgroup v2 is not mounted at /sys/fs/cgroup"
            return
        self.path.mkdir(mode=0o755)
        self.created = True
        self.available = (read_text(self.path / "cgroup.controllers") or "").split()
        for controller in ("cpu", "memory", "pids", "io"):
            if controller in self.available:
                errno_value = try_write_errno(self.path / "cgroup.subtree_control",
                                              "+" + controller)
                if errno_value is None:
                    self.enabled.append(controller)

    def has(self, *controllers: str) -> bool:
        return self.created and all(item in self.enabled for item in controllers)

    def child(self, name: str, cleanup: Cleanup) -> Path:
        path = self.path / name
        path.mkdir(mode=0o755)
        cleanup.push(f"remove cgroup {path}", lambda: remove_cgroup(path))
        return path

    def remove(self) -> None:
        if self.created:
            remove_cgroup(self.path)


def remove_cgroup(path: Path, timeout: float = 10.0) -> None:
    if not path.exists():
        return
    for child in sorted((item for item in path.iterdir() if item.is_dir()), reverse=True):
        remove_cgroup(child, timeout)
    if (path / "cgroup.freeze").exists():
        try_write_errno(path / "cgroup.freeze", "0")
    if (read_text(path / "cgroup.procs") or "").split() and (path / "cgroup.kill").exists():
        try_write_errno(path / "cgroup.kill", "1")
    deadline = time.monotonic() + timeout
    while True:
        try:
            path.rmdir()
            return
        except OSError as exc:
            if exc.errno == errno.ENOENT:
                return
            if exc.errno != errno.EBUSY or time.monotonic() >= deadline:
                raise
        time.sleep(0.1)


def mount_table() -> list[dict[str, str]]:
    return parse_mountinfo(Path("/proc/self/mountinfo").read_text())


def mount_overlay(ctx: ProbeContext, lower: Path, scratch: Path, target: Path) -> Path:
    """Mount lower plus a private upper at target, as production composes a
    bundle rootfs; returns the host upper directory."""
    upper, work = ctx.mkdir(scratch / "upper"), ctx.mkdir(scratch / "work")
    for path in (lower, upper, work, target):
        problem = overlay_path_problem(path)
        if problem:
            raise ProbeError(problem)
    ctx.run(["mount", "-t", "overlay", "overlay", "-o",
             f"lowerdir={lower},upperdir={upper},workdir={work}", target])
    ctx.cleanup.push(f"unmount {target}", lambda: ctx.run(["umount", target]))
    return upper


class Runsc:
    def __init__(self, ctx: ProbeContext) -> None:
        self.ctx = ctx
        self.binary = str(ctx.args.runsc)
        self.root = ctx.dir / "runsc-root"
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.debug: list[str] = []
        if ctx.args.debug:
            logs = ctx.dir / "runsc-debug"
            logs.mkdir(mode=0o700, exist_ok=True)
            self.debug = ["--debug", f"--debug-log={logs}/"]

    def prefix(self) -> list[str]:
        return [self.binary, f"--root={self.root}", *self.debug]

    def create_flags(self, variant_flags: Sequence[str]) -> list[str]:
        return ["--platform=systrap", "--network=none", *variant_flags,
                *self.ctx.args.runsc_flag]

    def launch(self, cid: str, bundle: Path, variant_flags: Sequence[str] = ()) -> dict[str, Any]:
        log = bundle / "stdio.log"
        started = time.monotonic()
        self.ctx.cleanup.push(f"delete container {cid}", lambda: self.delete(cid))
        self.ctx.run([*self.prefix(), *self.create_flags(variant_flags), "create",
                      f"--bundle={bundle}", cid], stdout_path=log)
        created = time.monotonic()
        self.ctx.run([*self.prefix(), "start", cid])
        return {"cid": cid, "create_seconds": created - started,
                "start_seconds": time.monotonic() - created}

    def exec(self, cid: str, argv: Sequence[str], *, timeout: float | None = None,
             check: bool = True) -> subprocess.CompletedProcess:
        return self.ctx.run([*self.prefix(), "exec", cid, *argv], timeout=timeout, check=check)

    def state(self, cid: str) -> dict[str, Any]:
        completed = self.ctx.run([*self.prefix(), "state", cid])
        payload = json.loads(completed.stdout)
        if not isinstance(payload, dict):
            raise ProbeError("runsc state returned non-object JSON")
        return payload

    def delete(self, cid: str) -> None:
        completed = self.ctx.run([*self.prefix(), "delete", "--force", cid], check=False)
        if completed.returncode != 0 and not re.search(
                r"(?i)does not exist|not found|no such", completed.stderr or ""):
            raise ProbeError(f"runsc delete {cid} failed: {tail(completed.stderr, 600)}")


def make_busybox_rootfs(busybox: Path, root: Path) -> Path:
    for name in ("bin", "data", "dev", "proc", "sys", "tmp", "root"):
        (root / name).mkdir(parents=True, exist_ok=True)
    shutil.copyfile(busybox, root / "bin/busybox")
    (root / "bin/busybox").chmod(0o755)
    (root / "bin/sh").symlink_to("busybox")
    (root / "data/original").write_text("lower\n")
    return root


def write_bundle(bundle: Path, config: dict[str, Any]) -> Path:
    bundle.mkdir(mode=0o700, parents=True, exist_ok=True)
    (bundle / "rootfs").mkdir(exist_ok=True)
    (bundle / "config.json").write_text(json.dumps(config, indent=2))
    return bundle


def require_static_busybox(path: Path) -> None:
    try:
        interpreter = elf_interpreter(path.read_bytes())
    except (OSError, ValueError) as exc:
        raise ProbeError(f"--busybox {path} is unusable: {exc}") from exc
    if interpreter is not None:
        raise ProbeError(f"--busybox {path} is dynamically linked ({interpreter}); "
                         "install busybox-static or pass a static binary")


def allocated(path: Path) -> dict[str, int]:
    info = path.stat()
    return {"size": info.st_size, "allocated": info.st_blocks * 512}


def tree_allocation(root: Path, *, exclude: Callable[[Path], bool] = lambda _: False) -> int:
    total = 0
    for directory, _dirs, files in os.walk(root):
        for name in files:
            path = Path(directory) / name
            if exclude(path):
                continue
            try:
                info = path.lstat()
            except FileNotFoundError:
                continue
            if stat.S_ISREG(info.st_mode):
                total += info.st_blocks * 512
    return total


def process_fds(pid: int) -> list[dict[str, Any]]:
    """Memory-relevant fds of a process, stat'ed through /proc (follows the fd)."""
    rows = []
    fd_dir = Path(f"/proc/{pid}/fd")
    try:
        entries = list(fd_dir.iterdir())
    except OSError:
        return rows
    for entry in entries:
        try:
            link = os.readlink(entry)
        except OSError:
            continue
        kind = classify_fd_link(link)
        if kind is None:
            continue
        try:
            info = os.stat(entry)
        except OSError:
            continue
        if stat.S_ISREG(info.st_mode):
            rows.append({"fd": int(entry.name), "link": link, "kind": kind,
                         "size": info.st_size, "allocated": info.st_blocks * 512})
    return sorted(rows, key=lambda row: row["fd"])


def process_memory(pid: int) -> dict[str, Any]:
    comm = (read_text(Path(f"/proc/{pid}/comm")) or "").strip()
    smaps = read_text(Path(f"/proc/{pid}/smaps_rollup"))
    return {"pid": pid, "comm": comm,
            "smaps": parse_smaps_rollup(smaps) if smaps is not None else None,
            "fds": process_fds(pid)}


def meminfo_subset() -> dict[str, int]:
    values = parse_meminfo(Path("/proc/meminfo").read_text())
    return {key: values[key] for key in MEMINFO_KEYS if key in values}


# ---------------------------------------------------------------------------
# Probes.


def container_cgroup(ctx: ProbeContext, cid: str) -> str | None:
    """Pre-create the container's cgroup under the spike cgroup.

    runsc joins an existing cgroup without touching its configuration; for a
    missing one it would write controllers into every ancestor's
    cgroup.subtree_control, including the root's.
    """
    if ctx.cgroup is None or not ctx.cgroup.has("memory"):
        return None
    ctx.cgroup.child(cid, ctx.cleanup)
    return f"{ctx.cgroup.relative}/{cid}"


def busybox_container(ctx: ProbeContext, runsc: Runsc, name: str) -> dict[str, Any]:
    require_static_busybox(ctx.args.busybox)
    root = ctx.mkdir(ctx.dir / name)
    lower = make_busybox_rootfs(ctx.args.busybox, root / "lower")
    bundle = root / "bundle"
    cid = f"rlspike-{ctx.args.run_id}-{ctx.name}-{name}"
    cgroups_path = container_cgroup(ctx, cid)
    write_bundle(bundle, oci_config(["/bin/busybox", "sleep", "2147483647"],
                                    cgroups_path=cgroups_path))
    # Production mounts a host overlay at the bundle rootfs; runsc places the
    # Sentry's root:self filestore in that directory, i.e. in the host upper.
    merged = bundle / "rootfs"
    upper = mount_overlay(ctx, lower, root / "host-overlay", merged)
    launched = runsc.launch(cid, bundle)
    return {"cid": cid, "bundle": bundle, "upper": upper, "merged": merged,
            "lower": lower, "launch": launched, "cgroups_path": cgroups_path}


def require_flags(inventory: dict[str, Any]) -> None:
    if not inventory["flags"]:
        raise ProbeError("could not parse the runsc flag list (`runsc flags`, `runsc --help`)")


def probe_s1(ctx: ProbeContext, inventory: dict[str, Any]) -> None:
    args = ctx.args
    require_flags(inventory)
    runsc = Runsc(ctx)
    default = inventory["flags"].get("overlay2", {}).get("default")
    effective = effective_flag_value(default, args.runsc_flag, "overlay2")
    obs = ctx.section["observations"]
    obs.update(runsc_version=inventory["version"], overlay2_default=default,
               overlay2_effective=effective,
               overlay2_usage=inventory["flags"].get("overlay2", {}).get("usage"))
    container = busybox_container(ctx, runsc, "guest")
    write_bytes = args.s1_write_mib * MIB
    runsc.exec(container["cid"], [
        "/bin/busybox", "sh", "-ec",
        f"/bin/busybox dd if=/dev/urandom of=/spike-write.bin bs=1048576 "
        f"count={args.s1_write_mib} 2>/dev/null; /bin/busybox sync; "
        "/bin/busybox stat -c %s /spike-write.bin"])
    state = runsc.state(container["cid"])
    sentry = int(state["pid"])
    upper, merged = container["upper"], container["merged"]
    by_path = [{"path": str(path), **allocated(path)} for base in (merged, upper)
               for path in sorted(base.glob(".gvisor.filestore.*")) if path.is_file()]
    by_fd = [row for row in process_fds(sentry) if row["kind"] == "filestore"]
    upper_filestores = [row for row in by_path if row["path"].startswith(str(upper) + "/")]
    filestore_allocated = None
    if upper_filestores:
        filestore_allocated = sum(row["allocated"] for row in upper_filestores)
    elif by_fd:
        filestore_allocated = max(row["allocated"] for row in by_fd)
    guest_on_host = any((base / "spike-write.bin").exists() for base in (upper, merged))
    upper_total = tree_allocation(upper)
    upper_other = tree_allocation(upper, exclude=lambda path: ".gvisor.filestore." in path.name)
    obs.update(write_bytes=write_bytes, sentry_pid=sentry,
               filestore_files=by_path, filestore_sentry_fds=by_fd,
               filestore_in_host_upper=bool(upper_filestores),
               guest_file_visible_on_host=guest_on_host,
               host_upper_allocated_bytes=upper_total,
               host_upper_allocated_excluding_filestore=upper_other,
               sentry_memory=process_memory(sentry), container_cgroup=container["cgroups_path"])
    ctx.section["status"], ctx.section["answer"] = evaluate_s1(
        overlay2_default=default, overlay2_effective=effective, write_bytes=write_bytes,
        filestore_allocated=filestore_allocated, guest_file_on_host=guest_on_host,
        upper_other_allocated=upper_other)


def probe_s4(ctx: ProbeContext, inventory: dict[str, Any]) -> None:
    obs = ctx.section["observations"]
    subcommands = inventory["subcommands"]
    obs["runsc_subcommands"] = subcommands
    tar_text = ""
    if "tar" in subcommands:
        # Group usage text differs between releases; any of these lists it.
        for argv in (["help", "tar"], ["tar", "--help"], ["tar"]):
            shown = ctx.run([ctx.args.runsc, *argv], check=False)
            tar_text += (shown.stdout or "") + (shown.stderr or "")
    exists = "tar" in subcommands and mentions_subcommand(tar_text, "rootfs-upper")
    obs.update(tar_subcommand=("tar" in subcommands), rootfs_upper=exists,
               help_tar=tail(tar_text, 4000))
    if not exists:
        ctx.section["status"] = "unsupported"
        ctx.section["answer"] = "runsc has no `tar rootfs-upper` subcommand"
        return
    runsc = Runsc(ctx)
    container = busybox_container(ctx, runsc, "guest")
    runsc.exec(container["cid"], [
        "/bin/busybox", "sh", "-ec",
        "/bin/busybox mkdir -p /spike-upper/dir; echo one > /spike-upper/a; "
        "/bin/busybox dd if=/dev/urandom of=/spike-upper/blob bs=1024 count=256 2>/dev/null; "
        "/bin/busybox rm /data/original; /bin/busybox sync"])
    output = ctx.dir / "rootfs-upper.tar"
    exported = ctx.run([*runsc.prefix(), "tar", "rootfs-upper", f"--file={output}",
                        container["cid"]], check=False)
    obs["export_returncode"] = exported.returncode
    if exported.returncode != 0 or not output.is_file():
        ctx.section["status"] = "fail"
        ctx.section["answer"] = "`runsc tar rootfs-upper` exists but did not produce a tar"
        return
    with tarfile.open(output) as archive:
        names = archive.getnames()
    normalized = [name.lstrip("./") for name in names]
    obs.update(tar_bytes=output.stat().st_size, member_count=len(names),
               members=names[:200], contains_written_file="spike-upper/a" in normalized,
               contains_whiteout=any(name.rsplit("/", 1)[-1].startswith(".wh.")
                                     for name in normalized))
    ok = obs["contains_written_file"]
    ctx.section["status"] = "pass" if ok else "fail"
    ctx.section["answer"] = (f"`runsc tar rootfs-upper` exists; {len(names)} tar members"
                             + ("" if ok else "; the written file is missing"))


def probe_s7(ctx: ProbeContext, inventory: dict[str, Any]) -> None:
    require_flags(inventory)
    flag = inventory["flags"].get("host-uds")
    obs = ctx.section["observations"]
    obs["flags_source"] = inventory["flags_source"]
    if flag is None:
        ctx.section["status"] = "unsupported"
        ctx.section["answer"] = "runsc has no --host-uds flag"
        return
    values = flag_values(flag["usage"])
    obs.update(host_uds=flag, values=values)
    usable = sorted({"open", "create", "all"} & set(values))
    ctx.section["status"] = "pass" if usable else "fail"
    ctx.section["answer"] = (f"--host-uds exists (default {flag['default']!r}); values "
                             f"{values or 'not parseable from help'}")


def cgroup_files(path: Path) -> dict[str, bool]:
    names = ("cpu.idle", "cpu.weight", "memory.reclaim", "memory.swap.max",
             "memory.swap.current", "memory.zswap.max", "memory.zswap.writeback",
             "cgroup.freeze", "cgroup.kill", "memory.pressure")
    return {name: (path / name).exists() for name in names}


def reclaim_probe(path: Path) -> dict[str, Any]:
    """Probe memory.reclaim arguments on an empty cgroup this script created."""
    target = path / "memory.reclaim"
    if not target.exists():
        return {"present": False, "memory_reclaim_writable": False,
                "swappiness_argument": None, "detail": "memory.reclaim is absent"}
    plain = try_write_errno(target, "0")
    low = try_write_errno(target, "0 swappiness=0") if plain is None else None
    high = try_write_errno(target, "0 swappiness=200") if plain is None else None
    result = classify_reclaim_probe(plain, low)
    result.update(present=True, swappiness_200_accepted=(plain is None and high is None))
    return result


def swap_state() -> dict[str, Any]:
    zswap_dir = Path("/sys/module/zswap/parameters")
    params = ({item.name: (read_text(item) or "").strip() for item in sorted(zswap_dir.iterdir())}
              if zswap_dir.is_dir() else None)
    swaps = parse_proc_swaps(read_text(Path("/proc/swaps")) or "")
    return {"swaps": swaps, "active_swap_bytes": sum(row["size_bytes"] for row in swaps),
            "zswap_parameters": params,
            "zswap_enabled": None if params is None else params.get("enabled") in ("Y", "1"),
            "vm_swappiness": (read_text(Path("/proc/sys/vm/swappiness")) or "").strip() or None}


SWAP_CHILD = r'''
import hashlib, os, sys, time
path, size, pattern = sys.argv[1], int(sys.argv[2]), sys.argv[3]
sys.stdin.readline()
block = (b"ucloud-rl-spike compressible page\n" * 32768)[:1 << 20]
digest = hashlib.sha256()
started = time.perf_counter()
with open(path, "wb") as stream:
    written = 0
    while written < size:
        count = min(1 << 20, size - written)
        data = os.urandom(count) if pattern == "random" else block[:count]
        stream.write(data)
        digest.update(data)
        written += count
print("written", time.perf_counter() - started, digest.hexdigest(), flush=True)
sys.stdin.readline()
check = hashlib.sha256()
started = time.perf_counter()
with open(path, "rb") as stream:
    while True:
        data = stream.read(1 << 20)
        if not data:
            break
        check.update(data)
print("read", time.perf_counter() - started, check.hexdigest(), flush=True)
sys.stdin.readline()
'''


def cgroup_memory(path: Path) -> dict[str, Any]:
    current = read_text(path / "memory.current")
    swap = read_text(path / "memory.swap.current")
    return {"memory_current": int(current) if current and current.strip().isdigit() else None,
            "swap_current": int(swap) if swap and swap.strip().isdigit() else None,
            "memory_stat": parse_kv_lines(read_text(path / "memory.stat") or "")}


def wait_frozen(path: Path, frozen: bool, timeout: float = 10.0) -> float:
    started = time.monotonic()
    while time.monotonic() - started < timeout:
        if parse_kv_lines(read_text(path / "cgroup.events") or "").get("frozen") == int(frozen):
            return time.monotonic() - started
        time.sleep(0.005)
    raise ProbeError(f"cgroup {path} did not reach frozen={int(frozen)}")


def swap_test(ctx: ProbeContext) -> dict[str, Any]:
    args = ctx.args
    size = args.swap_test_mib * MIB
    mount = ctx.mkdir(ctx.dir / "tmpfs")
    # A private tmpfs without noswap: its pages are swap-backed like the RAM
    # application-memory tmpfs would be once noswap is dropped (plan C1.1).
    ctx.run(["mount", "-t", "tmpfs", "-o", f"size={args.swap_test_mib + 16}m,mode=0700",
             "tmpfs", mount])
    ctx.cleanup.push(f"unmount {mount}", lambda: ctx.run(["umount", mount]))
    group = ctx.cgroup.child("swap-test", ctx.cleanup)
    child = subprocess.Popen(
        [sys.executable, "-c", SWAP_CHILD, str(mount / "payload"), str(size),
         args.swap_test_pattern],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    def stop_child() -> None:
        if (group / "cgroup.freeze").exists():
            try_write_errno(group / "cgroup.freeze", "0")
        if child.poll() is None:
            child.kill()
        child.wait(timeout=30)
        for stream in (child.stdin, child.stdout, child.stderr):
            stream.close()
    ctx.cleanup.push(f"stop swap-test child {child.pid}", stop_child)
    write_cgroup_file(group / "cgroup.procs", str(child.pid))

    def step(command: str) -> list[str]:
        child.stdin.write(command + "\n")
        child.stdin.flush()
        line = child.stdout.readline().split()
        if not line:
            raise ProbeError("swap-test child exited: " + tail(child.stderr.read(), 600))
        return line

    vmstat_keys = ("pswpin", "pswpout", "zswpin", "zswpout", "zswpwb")

    def vmstat() -> dict[str, int]:
        values = parse_kv_lines(read_text(Path("/proc/vmstat")) or "")
        return {key: values[key] for key in vmstat_keys if key in values}

    written = step("go")
    resident = cgroup_memory(group)
    host_before, vm_before = meminfo_subset(), vmstat()
    freeze_started = time.monotonic()
    write_cgroup_file(group / "cgroup.freeze", "1")
    wait_frozen(group, True)
    freeze_seconds = time.monotonic() - freeze_started
    frozen = cgroup_memory(group)
    reclaim_started = time.monotonic()
    reclaim_errno = try_write_errno(group / "memory.reclaim", f"{size} swappiness=200")
    reclaim_seconds = time.monotonic() - reclaim_started
    reclaimed = cgroup_memory(group)
    host_reclaimed, vm_reclaimed = meminfo_subset(), vmstat()
    thaw_started = time.monotonic()
    write_cgroup_file(group / "cgroup.freeze", "0")
    wait_frozen(group, False)
    thaw_seconds = time.monotonic() - thaw_started
    read_back = step("read")
    after = cgroup_memory(group)
    host_after, vm_after = meminfo_subset(), vmstat()

    def stat_value(snapshot: dict[str, Any], key: str) -> int | None:
        return snapshot["memory_stat"].get(key)

    swap_moved = (None if reclaimed["swap_current"] is None or frozen["swap_current"] is None
                  else reclaimed["swap_current"] - frozen["swap_current"])
    shmem_before, shmem_after = stat_value(frozen, "shmem"), stat_value(reclaimed, "shmem")
    return {
        "ran": True, "bytes": size, "pattern": args.swap_test_pattern,
        "write_seconds": float(written[1]), "freeze_seconds": freeze_seconds,
        "reclaim_request": f"{size} swappiness=200",
        "reclaim_errno": None if reclaim_errno is None else errno.errorcode.get(reclaim_errno),
        "reclaim_seconds": reclaim_seconds, "thaw_seconds": thaw_seconds,
        "fault_back_seconds": float(read_back[1]),
        "integrity_ok": written[2] == read_back[2],
        "swap_bytes_moved": swap_moved,
        "shmem_bytes_evicted": (None if shmem_before is None or shmem_after is None
                                else shmem_before - shmem_after),
        "zswap_bytes_after_reclaim": stat_value(reclaimed, "zswap"),
        "zswapped_bytes_after_reclaim": stat_value(reclaimed, "zswapped"),
        "cgroup": {"resident": resident, "frozen": frozen, "reclaimed": reclaimed,
                   "after_fault_back": after},
        "host_meminfo": {"before": host_before, "reclaimed": host_reclaimed, "after": host_after},
        "vmstat": {"before": vm_before, "reclaimed": vm_reclaimed, "after": vm_after},
    }


def probe_s6(ctx: ProbeContext, _inventory: dict[str, Any]) -> None:
    obs = ctx.section["observations"]
    obs["kernel_release"] = platform.release()
    obs["swap"] = swap_state()
    if ctx.cgroup is None or not ctx.cgroup.has("memory"):
        ctx.section["status"] = "unsupported"
        ctx.section["answer"] = ("the memory controller is not delegated to this script's "
                                 "cgroup; memory.reclaim cannot be probed without writing "
                                 "above it")
        obs["cgroup_error"] = ctx.cgroup.error if ctx.cgroup else "no cgroup"
        return
    probe_group = ctx.cgroup.child("reclaim-probe", ctx.cleanup)
    obs["cgroup_files"] = cgroup_files(probe_group)
    obs["reclaim"] = reclaim_probe(probe_group)
    if not obs["reclaim"]["memory_reclaim_writable"] or not obs["reclaim"]["swappiness_argument"]:
        ctx.section["status"] = "unsupported"
        ctx.section["answer"] = "memory.reclaim swappiness= is unavailable: " + \
            obs["reclaim"]["detail"]
        return
    incomplete = []
    if not obs["swap"]["active_swap_bytes"]:
        obs["swap_test"] = {"ran": False, "reason": "no active swap device (/proc/swaps)"}
    elif not ctx.args.allow_swap_test:
        obs["swap_test"] = {"ran": False, "reason": "--allow-swap-test was not given"}
    elif not (probe_group / "cgroup.freeze").exists():
        obs["swap_test"] = {"ran": False, "reason": "cgroup.freeze is unavailable"}
    else:
        obs["swap_test"] = swap_test(ctx)
    test = obs["swap_test"]
    if not test["ran"]:
        incomplete.append("swap_test: " + test["reason"])
        ctx.section["status"] = "pass"
        ctx.section["answer"] = ("memory.reclaim accepts swappiness=; the swap test did not "
                                 "run: " + test["reason"])
    else:
        moved = max(test["swap_bytes_moved"] or 0, test["shmem_bytes_evicted"] or 0)
        ok = test["integrity_ok"] and moved > 0
        ctx.section["status"] = "pass" if ok else "fail"
        ctx.section["answer"] = (
            f"swappiness=200 moved {moved} of {test['bytes']} tmpfs bytes in "
            f"{test['reclaim_seconds']:.3f}s; thaw {test['thaw_seconds']:.3f}s, fault-back "
            f"{test['fault_back_seconds']:.3f}s; integrity "
            + ("ok" if test["integrity_ok"] else "FAILED"))
    ctx.section["incomplete"] = incomplete


def kernel_config_text(release: str) -> tuple[str | None, str | None]:
    boot = Path(f"/boot/config-{release}")
    if boot.is_file():
        return boot.read_text(errors="replace"), str(boot)
    proc = Path("/proc/config.gz")
    if proc.is_file():
        with gzip.open(proc, "rt", errors="replace") as stream:
            return stream.read(), str(proc)
    return None, None


def reflink_test(directory: Path) -> dict[str, Any]:
    source, target = directory / "reflink-source", directory / "reflink-target"
    data = os.urandom(MIB)
    source.write_bytes(data)
    try:
        with source.open("rb") as src, target.open("wb") as dst:
            fcntl.ioctl(dst.fileno(), FICLONE, src.fileno())
        return {"supported": target.read_bytes() == data, "errno": None}
    except OSError as exc:
        return {"supported": False, "errno": errno.errorcode.get(exc.errno, exc.errno)}
    finally:
        for path in (source, target):
            path.unlink(missing_ok=True)


def probe_s8(ctx: ProbeContext, _inventory: dict[str, Any]) -> None:
    obs = ctx.section["observations"]
    release = platform.release()
    obs.update(kernel_release=release, uname=" ".join(os.uname()))
    text, source = kernel_config_text(release)
    config = parse_kernel_config(text) if text else {}
    obs["kernel_config_source"] = source
    obs["kernel_config"] = {key: config.get(key) for key in KERNEL_CONFIG_KEYS}
    filesystems = sorted({line.split()[-1] for line in
                          (read_text(Path("/proc/filesystems")) or "").splitlines() if line.split()})
    obs["filesystems"] = filesystems
    module_root = Path(f"/lib/modules/{release}")
    texts = {"loaded_text": read_text(Path("/proc/modules")) or "",
             "builtin_text": read_text(module_root / "modules.builtin") or "",
             "dep_text": read_text(module_root / "modules.dep") or ""}
    obs["modules"] = {name: module_presence(name, **texts)
                      for name in ("erofs", "ublk_drv", "nbd", "overlay", "xfs", "zsmalloc")}
    obs["devices"] = {path: Path(path).exists() for path in ("/dev/ublk-control", "/dev/nbd0")}
    cgroup2 = (CGROUP_ROOT / "cgroup.controllers").exists()
    own = "/"
    for line in (read_text(Path("/proc/self/cgroup")) or "").splitlines():
        if line.startswith("0::"):
            own = line[3:] or "/"
    own_path = CGROUP_ROOT / own.lstrip("/")
    obs["cgroup"] = {
        "v2": cgroup2,
        "root_controllers": (read_text(CGROUP_ROOT / "cgroup.controllers") or "").split(),
        "root_subtree_control": (read_text(CGROUP_ROOT / "cgroup.subtree_control") or "").split(),
        "own_cgroup": own, "own_cgroup_files": cgroup_files(own_path),
        "spike_cgroup_enabled": ctx.cgroup.enabled if ctx.cgroup else [],
        "spike_cgroup_error": ctx.cgroup.error if ctx.cgroup else "no cgroup",
    }
    if ctx.cgroup is not None and ctx.cgroup.created:
        probe_group = ctx.cgroup.child("kernel-probe", ctx.cleanup)
        obs["cgroup"]["created_cgroup_files"] = cgroup_files(probe_group)
        obs["reclaim"] = reclaim_probe(probe_group)
        cpu_idle = (obs["cgroup"]["created_cgroup_files"]["cpu.idle"]
                    or obs["cgroup"]["own_cgroup_files"]["cpu.idle"])
    else:
        obs["reclaim"] = {"present": False, "memory_reclaim_writable": False,
                          "swappiness_argument": None, "detail": "no cgroup created"}
        cpu_idle = obs["cgroup"]["own_cgroup_files"]["cpu.idle"]
    obs["swap"] = swap_state()
    zswap = obs["swap"]["zswap_parameters"] is not None or config.get("CONFIG_ZSWAP") == "y"
    erofs = "erofs" in filesystems or any(obs["modules"]["erofs"].values())
    mounts = mount_table()
    work_root = str(ctx.args.work_root.resolve())
    mount = mount_for_path(work_root, mounts)
    filesystem: dict[str, Any] = {"mount": mount}
    if mount and mount["fstype"] == "xfs" and shutil.which("xfs_info"):
        info = ctx.run(["xfs_info", mount["mount_point"]], check=False)
        filesystem["xfs_info_reflink"] = parse_xfs_info_reflink(info.stdout or "")
    filesystem["ficlone"] = reflink_test(ctx.mkdir(ctx.dir / "reflink"))
    obs["work_root_filesystem"] = filesystem
    required = {"cgroup_v2": cgroup2, "cpu.idle": cpu_idle,
                "memory.reclaim": bool(obs["reclaim"].get("memory_reclaim_writable")),
                "memory.reclaim swappiness=": bool(obs["reclaim"].get("swappiness_argument")),
                "zswap": zswap}
    informational = {
        "erofs": erofs,
        "erofs_ondemand": config.get("CONFIG_EROFS_FS_ONDEMAND") == "y",
        "ublk": any(obs["modules"]["ublk_drv"].values()),
        "nbd": any(obs["modules"]["nbd"].values()),
        "overlay": "overlay" in filesystems or any(obs["modules"]["overlay"].values()),
        "work_root_reflink": filesystem["ficlone"]["supported"],
    }
    obs["required"], obs["informational"] = required, informational
    missing = [name for name, present in required.items() if not present]
    ctx.section["status"] = "fail" if missing else "pass"
    ctx.section["answer"] = ("all required features present" if not missing
                             else "missing: " + ", ".join(missing))


S2_VARIANT_NOTES = {
    "gofer": "host overlay rootfs served by the gofer; runsc default flags",
    "gofer-shared": (
        "--file-access=shared on the rootfs. runsc rejects it together with a root "
        "overlay ('overlay flag is incompatible with shared file access for rootfs', "
        "release-20260817.0), so this variant also passes --overlay2=none; the read "
        "command does not write to the rootfs"),
    "erofs": "Sentry-native EROFS rootfs via dev.gvisor.spec.rootfs annotations "
             "(as in qualify_erofs.py), filestore in a per-sandbox host directory",
}


def s2_variant_flags(variant: str) -> list[str]:
    if variant == "gofer-shared":
        return ["--file-access=shared", "--overlay2=none"]
    return []


def s2_launch_one(ctx: ProbeContext, runsc: Runsc, variant: str, k: int,
                  flags: list[str]) -> dict[str, Any]:
    args = ctx.args
    sequence = ctx.next_sequence()
    cid = f"rlspike-{args.run_id}-s2-{variant}-{sequence}"
    root = ctx.mkdir(ctx.dir / variant / f"k{k}-{sequence}")
    bundle = root / "bundle"
    cgroups_path = container_cgroup(ctx, cid)
    argv = [args.guest_shell, "-c", args.guest_init]
    if variant == "erofs":
        runtime_upper = ctx.mkdir(root / "runtime-upper")
        annotations = {"dev.gvisor.spec.rootfs.type": "erofs",
                       "dev.gvisor.spec.rootfs.source": str(args.erofs_image),
                       "dev.gvisor.spec.rootfs.overlay": f"dir={runtime_upper}"}
        write_bundle(bundle, oci_config(argv, cgroups_path=cgroups_path,
                                        annotations=annotations))
    else:
        write_bundle(bundle, oci_config(argv, cgroups_path=cgroups_path))
        mount_overlay(ctx, args.rootfs, root / "host-overlay", bundle / "rootfs")
    launched = runsc.launch(cid, bundle, flags)
    launched["cgroup"] = (CGROUP_ROOT / cgroups_path.lstrip("/")) if cgroups_path else None
    return launched


def s2_sandbox_stats(runsc: Runsc, sandbox: dict[str, Any]) -> dict[str, Any]:
    state = runsc.state(sandbox["cid"])
    sentry = int(state["pid"])
    row: dict[str, Any] = {"sentry_pid": sentry}
    cgroup = sandbox.get("cgroup")
    pids = {sentry}
    if cgroup is not None and cgroup.exists():
        memory = cgroup_memory(cgroup)
        row.update(memory_current=memory["memory_current"], memory_stat=memory["memory_stat"])
        pids |= {int(pid) for pid in (read_text(cgroup / "cgroup.procs") or "").split()}
    processes = [process_memory(pid) for pid in sorted(pids)]
    sentry_memory = next(item for item in processes if item["pid"] == sentry)
    row["sentry_smaps"] = sentry_memory["smaps"]
    memory_files = [fd for fd in sentry_memory["fds"] if fd["kind"] == "memory_file"]
    row["memory_files"] = memory_files
    row["memory_file_allocated"] = sum(fd["allocated"] for fd in memory_files) if memory_files \
        else None
    row["processes"] = processes
    return row


def s2_run_k(ctx: ProbeContext, runsc: Runsc, variant: str, k: int, flags: list[str],
             *, warmup: bool) -> dict[str, Any]:
    args = ctx.args
    host: dict[str, dict[str, int]] = {"before": meminfo_subset()}
    outer = ctx.cleanup
    ctx.cleanup = Cleanup()  # Tear this K down before the next one starts.
    result: dict[str, Any] = {"k": k, "warmup": warmup, "runsc_flags": flags}
    try:
        with ThreadPoolExecutor(max_workers=args.parallel) as pool:
            sandboxes = list(pool.map(
                lambda _index: s2_launch_one(ctx, runsc, variant, k, flags), range(k)))
        time.sleep(args.settle_seconds)
        host["idle"] = meminfo_subset()
        idle = [s2_sandbox_stats(runsc, sandbox) for sandbox in sandboxes]

        def read(sandbox: dict[str, Any]) -> dict[str, Any]:
            started = time.monotonic()
            completed = runsc.exec(sandbox["cid"], [args.guest_shell, "-c", args.read_command],
                                   timeout=args.read_timeout, check=False)
            lines = (completed.stdout or "").strip().splitlines()
            bytes_read = int(lines[-1]) if lines and lines[-1].strip().isdigit() else None
            return {"seconds": time.monotonic() - started, "returncode": completed.returncode,
                    "bytes_read": bytes_read, "stdout_tail": tail(completed.stdout, 400)}

        with ThreadPoolExecutor(max_workers=args.parallel) as pool:
            reads = list(pool.map(read, sandboxes))
        time.sleep(args.settle_seconds)
        host["after_read"] = meminfo_subset()
        after_read = [s2_sandbox_stats(runsc, sandbox) for sandbox in sandboxes]
        result.update(
            sandboxes=[{"cid": sandbox["cid"], "create_seconds": sandbox["create_seconds"],
                        "start_seconds": sandbox["start_seconds"], "read": read_row,
                        "idle": idle_row, "after_read": read_stats}
                       for sandbox, read_row, idle_row, read_stats
                       in zip(sandboxes, reads, idle, after_read)],
            totals=k_totals(host, idle, after_read))
        failures = [row for row in reads if row["returncode"] != 0 or row["bytes_read"] == 0]
        result["read_failures"] = len(failures)
    finally:
        errors = ctx.cleanup.run()
        ctx.cleanup = outer
        ctx.section["cleanup_errors"].extend(errors)
        host["after_teardown"] = meminfo_subset()
        result["host_meminfo"] = host
        if errors:
            raise ProbeError("S2 teardown failed: " + "; ".join(errors))
    return result


def probe_s2(ctx: ProbeContext, _inventory: dict[str, Any]) -> None:
    args = ctx.args
    obs = ctx.section["observations"]
    obs.update(variants={}, k=args.k, read_command=args.read_command,
               rootfs=str(args.rootfs) if args.rootfs else None,
               erofs_image=str(args.erofs_image) if args.erofs_image else None,
               cgroup_metrics=bool(ctx.cgroup and ctx.cgroup.has("memory")),
               ratio_definition="host bytes for K / (K x bytes for 1); 1.0 means fully "
                                "duplicated per Sentry, 1/K fully shared. scaling_vs_k1 = "
                                "ratio x K (the plan's <= 1.2 gate at K=32).")
    runsc = Runsc(ctx)
    verdicts = []
    read_failures = 0
    for variant in args.variant:
        flags = s2_variant_flags(variant)
        section: dict[str, Any] = {"runsc_flags": [*flags, *args.runsc_flag],
                                   "note": S2_VARIANT_NOTES[variant]}
        obs["variants"][variant] = section
        # Unmeasured: warms the host page cache so K=1 is not the only cold run.
        section["warmup"] = (s2_run_k(ctx, runsc, variant, 1, flags, warmup=True)
                             if args.warmup else None)
        per_k = {}
        section["k"] = {}
        for k in args.k:
            run = s2_run_k(ctx, runsc, variant, k, flags, warmup=False)
            section["k"][str(k)] = run
            per_k[k] = run["totals"]
            read_failures += run["read_failures"]
            persist_progress(ctx)
        section["ratios"] = sharing_ratios(per_k)
        primary = ("cgroup_memory_current_sum" if obs["cgroup_metrics"]
                   else "host_mem_available_drop")
        largest = str(max(args.k))
        verdict = section["ratios"]["read_increment"][primary][largest]
        section["primary_metric"] = {"phase": "read_increment", "metric": primary,
                                     "k": int(largest), **verdict}
        verdicts.append(f"{variant}: {verdict['classification']} (ratio {verdict['ratio']}, "
                        f"scaling {verdict['scaling_vs_k1']} at K={largest})")
    ctx.section["status"] = "fail" if read_failures else "pass"
    ctx.section["answer"] = "; ".join(verdicts) + (
        f"; {read_failures} read commands failed or read 0 bytes" if read_failures else "")


PROBE_FUNCTIONS = {"s1": probe_s1, "s2": probe_s2, "s4": probe_s4, "s6": probe_s6,
                   "s7": probe_s7, "s8": probe_s8}
_PROGRESS: dict[str, Any] = {}


def persist_progress(_ctx: ProbeContext | None = None) -> None:
    writer = _PROGRESS.get("write")
    if writer is not None:
        writer()


# ---------------------------------------------------------------------------
# Run orchestration.


def runsc_inventory(args: argparse.Namespace) -> dict[str, Any]:
    commands = []

    def run(argv: list[str]) -> str:
        started = time.monotonic()
        try:
            completed = subprocess.run(argv, capture_output=True, text=True, timeout=60,
                                       stdin=subprocess.DEVNULL)
        except (OSError, subprocess.TimeoutExpired) as exc:
            commands.append({"argv": argv, "error": str(exc)})
            return ""
        commands.append({"argv": argv, "returncode": completed.returncode,
                         "seconds": time.monotonic() - started,
                         "stdout_tail": tail(completed.stdout, 8000),
                         "stderr_tail": tail(completed.stderr, 2000)})
        return (completed.stdout or "") + "\n" + (completed.stderr or "")

    binary = str(args.runsc)
    version = run([binary, "--version"])
    help_text = run([binary, "help"])
    flags, source = {}, None
    for argv in ([binary, "flags"], [binary, "--help"]):
        flags = parse_go_flags(run(argv))
        if flags:
            source = " ".join(argv[1:])
            break
    return {"path": binary, "sha256": hashlib.sha256(args.runsc.read_bytes()).hexdigest(),
            "version_text": version.strip(), "version": parse_runsc_version(version),
            "subcommands": parse_subcommands(help_text), "flags": flags,
            "flags_source": source, "commands": commands}


def _csv(values: str, *, allowed: Sequence[str], name: str) -> list[str]:
    items = [item.strip().lower() for item in values.split(",") if item.strip()]
    unknown = sorted(set(items) - set(allowed))
    if unknown or not items:
        raise argparse.ArgumentTypeError(
            f"{name} must be a comma-separated subset of {','.join(allowed)}")
    return [item for item in allowed if item in items]


def _k_values(text: str) -> list[int]:
    try:
        values = sorted({int(item) for item in text.split(",") if item.strip()})
    except ValueError as exc:
        raise argparse.ArgumentTypeError("--k must be comma-separated integers") from exc
    if not values or values[0] < 1 or values[-1] > 512:
        raise argparse.ArgumentTypeError("--k values must be between 1 and 512")
    return values


def _positive_int(text: str) -> int:
    value = int(text)
    if value <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return value


def _non_negative_float(text: str) -> float:
    value = float(text)
    if not value >= 0:
        raise argparse.ArgumentTypeError("must be a non-negative number")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--runsc", type=Path, required=True, help="pinned runsc binary")
    parser.add_argument("--work-root", type=Path, required=True,
                        help="existing root-owned directory, empty or previously used by this "
                             "script")
    parser.add_argument("--output", type=Path, required=True,
                        help="JSON report path; never overwritten")
    parser.add_argument("--probe", default=",".join(PROBES),
                        type=lambda text: _csv(text, allowed=PROBES, name="--probe"),
                        help="comma-separated probes (default: all)")
    parser.add_argument("--runsc-flag", action="append", default=[], metavar="FLAG",
                        help="extra global runsc flag for create, e.g. --runsc-flag=--overlay2=none")
    parser.add_argument("--debug", action="store_true", help="write runsc debug logs")
    parser.add_argument("--command-timeout", type=_positive_int, default=120)
    guest = parser.add_argument_group("S1/S4 guest")
    guest.add_argument("--busybox", type=Path, default=Path("/usr/bin/busybox"),
                       help="static busybox for the minimal S1/S4 rootfs")
    guest.add_argument("--s1-write-mib", type=_positive_int, default=64)
    s2 = parser.add_argument_group("S2 page sharing (plan C0.3)")
    s2.add_argument("--rootfs", type=Path, help="unpacked image rootfs (gofer variants)")
    s2.add_argument("--erofs-image", type=Path, help="flat EROFS image (erofs variant)")
    s2.add_argument("--variant", default="gofer",
                    type=lambda text: _csv(text, allowed=VARIANTS, name="--variant"),
                    help="comma-separated: gofer, gofer-shared, erofs (default: gofer)")
    s2.add_argument("--k", type=_k_values, default=[1, 8, 32],
                    help="sandbox counts, must include 1 (default: 1,8,32)")
    s2.add_argument("--read-command", default=DEFAULT_READ_COMMAND,
                    help="read-heavy guest shell command; a final integer line is bytes read")
    s2.add_argument("--guest-shell", default="/bin/sh")
    s2.add_argument("--guest-init", default=DEFAULT_GUEST_INIT,
                    help="long-running guest init, run with the guest shell")
    s2.add_argument("--settle-seconds", type=_non_negative_float, default=2.0)
    s2.add_argument("--parallel", type=_positive_int, default=8)
    s2.add_argument("--read-timeout", type=_positive_int, default=600)
    s2.add_argument("--no-warmup", dest="warmup", action="store_false",
                    help="skip the unmeasured K=1 run that warms the host page cache")
    s6 = parser.add_argument_group("S6 swap test")
    s6.add_argument("--allow-swap-test", action="store_true",
                    help="write, freeze and reclaim a tmpfs file in a test cgroup")
    s6.add_argument("--swap-test-mib", type=_positive_int, default=256)
    s6.add_argument("--swap-test-pattern", choices=("random", "compressible"), default="random")
    return parser


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args(argv)
    if "s2" in args.probe:
        if 1 not in args.k:
            parser.error("--k must include 1: the ratio is relative to one sandbox")
        if {"gofer", "gofer-shared"} & set(args.variant):
            if args.rootfs is None or not args.rootfs.is_dir():
                parser.error("S2 gofer variants need --rootfs, an unpacked image directory")
            problem = overlay_path_problem(args.rootfs.resolve())
            if problem:
                parser.error(problem)
            args.rootfs = args.rootfs.resolve()
        if "erofs" in args.variant:
            if args.erofs_image is None or not args.erofs_image.is_file():
                parser.error("S2 erofs variant needs --erofs-image, an EROFS image file")
            args.erofs_image = args.erofs_image.resolve()
    if not args.runsc.is_file():
        parser.error(f"--runsc {args.runsc} is not a file")
    if args.swap_test_mib > 65536:
        parser.error("--swap-test-mib must be at most 65536")
    return args


def write_report(path: Path, report: dict[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(report, indent=2, sort_keys=True, default=str) + "\n")
    temporary.replace(path)


def _raise_interrupt(signum: int, _frame: object) -> None:
    raise KeyboardInterrupt(f"signal {signum}")


@contextmanager
def probe_scope(ctx: ProbeContext) -> Iterator[None]:
    started = time.monotonic()
    try:
        yield
    except KeyboardInterrupt:
        ctx.section["status"] = "error"
        ctx.section["errors"].append("interrupted")
        raise
    except Exception as exc:
        ctx.section["status"] = "error"
        ctx.section["errors"].append(f"{type(exc).__name__}: {exc}")
    finally:
        ctx.section["cleanup_errors"].extend(ctx.cleanup.run())
        if ctx.section["cleanup_errors"] and ctx.section["status"] != "error":
            ctx.section["errors"].append("cleanup failed; see cleanup_errors")
            ctx.section["status"] = "error"
        ctx.section["seconds"] = time.monotonic() - started
        ctx.section["finished_at"] = utc_now()


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parse_args(argv)
    if os.geteuid() != 0:
        parser.error("requires root on a disposable qualification VM")
    work_root = args.work_root
    problem = work_root_problem(work_root, euid=os.geteuid())
    if problem:
        parser.error(f"refusing --work-root: {problem}")
    work_root = work_root.resolve()
    problem = overlay_path_problem(work_root)
    if problem:
        parser.error(f"refusing --work-root: {problem}")
    args.work_root = work_root
    args.run_id = uuid.uuid4().hex[:10]
    output = args.output.resolve()
    if _is_within(str(output), str(work_root)):
        parser.error("--output must be outside --work-root")
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x"):  # Never overwrite another run's evidence.
        pass
    marker = work_root / OWNER_MARKER
    if not marker.exists():
        marker.write_text(json.dumps({"created_by": "runtime/gvisor/spike_rl_scale.py",
                                      "created_at": utc_now()}) + "\n")
    run_dir = work_root / f"run-{args.run_id}"
    run_dir.mkdir(mode=0o700)
    report: dict[str, Any] = {
        "schema": SCHEMA, "run_id": args.run_id, "status": "running", "started_at": utc_now(),
        "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "host": {"uname": " ".join(os.uname()), "kernel_release": platform.release(),
                 "cpu_count": os.cpu_count(),
                 "mem_total_bytes": parse_meminfo(Path("/proc/meminfo").read_text()).get(
                     "MemTotal"),
                 "boot_id": (read_text(Path("/proc/sys/kernel/random/boot_id")) or "").strip()},
        "arguments": {key: (str(value) if isinstance(value, Path) else value)
                      for key, value in sorted(vars(args).items())},
        "run_dir": str(run_dir), "probes": {}, "cleanup_errors": [], "retained_run_dir": None,
    }
    _PROGRESS["write"] = lambda: write_report(output, report)
    previous = signal.signal(signal.SIGTERM, _raise_interrupt)
    spike_cgroup = SpikeCgroup(args.run_id)
    interrupted = False
    try:
        try:
            spike_cgroup.create()
        except OSError as exc:
            spike_cgroup.error = f"cannot create {spike_cgroup.path}: {exc}"
        report["cgroup"] = {"path": str(spike_cgroup.path), "created": spike_cgroup.created,
                            "available": spike_cgroup.available,
                            "enabled": spike_cgroup.enabled, "error": spike_cgroup.error}
        report["runsc"] = runsc_inventory(args)
        persist_progress()
        for name in args.probe:
            ctx = ProbeContext(name, args, run_dir, spike_cgroup if spike_cgroup.created else None)
            report["probes"][name] = ctx.section
            print(json.dumps({"event": "probe_started", "probe": name, "at": utc_now()}),
                  flush=True)
            ctx.dir.mkdir(mode=0o700)
            with probe_scope(ctx):
                PROBE_FUNCTIONS[name](ctx, report["runsc"])
            print(json.dumps({"event": "probe_finished", "probe": name,
                              "status": ctx.section["status"]}), flush=True)
            persist_progress()
    except KeyboardInterrupt:
        interrupted = True
    except Exception as exc:
        report.setdefault("errors", []).append(f"{type(exc).__name__}: {exc}")
    finally:
        try:
            spike_cgroup.remove()
        except BaseException as exc:
            report["cleanup_errors"].append(f"remove cgroup {spike_cgroup.path}: {exc}")
        probe_cleanup = [error for section in report["probes"].values()
                         for error in section["cleanup_errors"]]
        leftover = mounts_under(str(run_dir), mount_table())
        if leftover:
            report["cleanup_errors"].append("mounts remain under the run directory: "
                                            + ", ".join(leftover))
        if report["cleanup_errors"] or probe_cleanup:
            report["retained_run_dir"] = str(run_dir)
        else:
            try:
                shutil.rmtree(run_dir)
            except OSError as exc:
                report["cleanup_errors"].append(f"remove {run_dir}: {exc}")
                report["retained_run_dir"] = str(run_dir)
        signal.signal(signal.SIGTERM, previous)
        statuses = [section["status"] for section in report["probes"].values()]
        report["status"] = "interrupted" if interrupted else "completed"
        report["finished_at"] = utc_now()
        report["summary"] = {name: section["status"] for name, section in report["probes"].items()}
        write_report(output, report)
    print(json.dumps({"event": "spike_finished", "output": str(output),
                      "summary": report["summary"], "retained_run_dir": report["retained_run_dir"]}),
          flush=True)
    if interrupted:
        return 130
    return 1 if ("error" in statuses or report["cleanup_errors"]
                 or report.get("errors")) else 0


if __name__ == "__main__":
    sys.exit(main())
