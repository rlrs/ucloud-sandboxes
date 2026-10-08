"""C3.1 commit: residue policy and strict commit schemas (docs/rl-state-primitives.md §3).

Pure: no registry, runtime or key access. The keyless preparation child runs
``filter_upper`` before anything is signed (W7); the gateway, workers and
builders share the schemas, so every side parses the same bounded documents.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re
import tarfile

COMMIT_MAX_BYTES = 8 * 1024**3
MAX_MEMBERS = 1_000_000
MAX_PATH_BYTES = 4096
MAX_RULES = 256
# With 4,096 secret digests (~270 KiB), a policy fits the keyless child's
# 1 MiB request (environment_prepare.MAX_REQUEST_BYTES).
MAX_POLICY_BYTES = 64 * 1024
MAX_SECRET_DIGESTS = 4096
# Bump whenever a rule table below changes: components record the policy hash.
POLICY_SCHEMA = "ucloud-commit-policy-v1"
EXPORT_SCHEMA = "ucloud-commit-export-v1"
COMMIT_ANNOTATION = "org.ucloud.commit.v1"
REFUSALS = frozenset({"commit_residue_forbidden", "commit_too_large", "commit_secret_residue"})
WHITEOUT, OPAQUE = ".wh.", ".wh..wh..opq"
OPAQUE_XATTR = "trusted.overlay.opaque"
# Fixed-format credentials: class -> length of a maximal lowercase-hex run
# (model_relay.REGISTRATION_TOKEN_RE). secret_digests are SHA-256 of live values.
TOKEN_CLASSES = {"relay-registration": 32}
# Mounted in every sandbox: an entry at or below one means a mount was bypassed.
ESCAPES = ("dev", "proc", "run/ucloud", "sys")
# runsc creates a mount point the image lacks in the upper; that bare directory is no escape.
MOUNT_POINTS = frozenset({"dev", "proc", "sys"})
# Drop rules in table order; dev/shm lies below dev, so it is an escape, not volatile.
HOST_WRITTEN = (".ucloud-init", ".ucloud-job-init", ".ucloud-managed", "etc/hostname", "etc/hosts",
                "etc/resolv.conf")
VOLATILE = ("run", "tmp")
BUILD_RESIDUE = ("root/.cache/pip", "root/.cache/uv", "root/.npm/_cacache")
APT_ARCHIVES = "var/cache/apt/archives/"
# sandbox.OPERATION_ID_RE, and images.IMAGE_ID_RE (= sandbox.SANDBOX_ID_RE), without their imports.
OPERATION_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
IMAGE_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_CODE = re.compile(r"[a-z_]{1,64}\Z")
_HEX64 = re.compile(r"[0-9a-f]{64}\Z")
_HEX = b"0123456789abcdef"


class CommitRefused(ValueError):
    """A refused commit step: ``code`` is its API error code, ``status`` its HTTP status."""

    def __init__(self, code: str, detail: str, *, status: int = 409, retryable: bool = False) -> None:
        super().__init__(f"{code}: {detail}")
        self.code, self.status, self.retryable = code, status, retryable

    def payload(self) -> dict:
        return {"error": str(self), "error_code": self.code, "retryable": self.retryable}


def canonical(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def staging_repository(image_id: str) -> str:
    """Where a worker stages the raw upper; image IDs are not repository names."""
    return "commits/" + hashlib.sha256(image_id.encode("ascii")).hexdigest()[:32]


def _guest_path(value, *, pattern: bool = False) -> str:
    """A clean absolute guest path, returned relative ("" is /)."""
    if (not isinstance(value, str) or not value.startswith("/") or "\0" in value
            or len(value.encode("utf-8", "surrogatepass")) > MAX_PATH_BYTES):
        raise ValueError("commit paths must be clean absolute guest paths")
    body = value[1:-1] if pattern and value.endswith("*") else value[1:]
    if (value != "/" or pattern) and any(part in {"", ".", ".."} for part in body.split("/")):
        raise ValueError("commit paths must be clean absolute guest paths")
    return value[1:]


def _under(path: str, prefix: str) -> bool:
    return not prefix or path == prefix or path.startswith(prefix + "/")


def _rules(raw, *, pattern: bool = False) -> tuple[str, ...]:
    if not isinstance(raw, (list, tuple)) or len(raw) > MAX_RULES:
        raise ValueError("commit path rules must be a bounded list")
    for value in raw:
        _guest_path(value, pattern=pattern)
    return tuple(sorted(set(raw)))


@dataclass(frozen=True)
class CommitPolicy:
    """Caller and platform rules; components record ``sha256``.

    ``identity`` comes from ``DirectOciConfigBuilder.platform_written_paths``;
    a trailing ``*`` there is a name prefix. Lists are sorted and unique.
    """
    include_paths: tuple[str, ...] = ()
    exclude: tuple[str, ...] = ()
    identity: tuple[str, ...] = ()
    max_bytes: int = COMMIT_MAX_BYTES
    # Sandbox builds keep package caches, as Docker images do (docs/sandbox-builds.md).
    keep_build_residue: bool = False

    def __post_init__(self):
        for name in ("include_paths", "exclude", "identity"):
            values = getattr(self, name)
            if not isinstance(values, tuple) or _rules(values, pattern=name == "identity") != values:
                raise ValueError(f"commit policy {name} must be sorted unique guest paths")
        if type(self.max_bytes) is not int or not 0 < self.max_bytes <= 1 << 40:
            raise ValueError("commit max_bytes must be a positive bounded integer")
        if type(self.keep_build_residue) is not bool:
            raise ValueError("commit keep_build_residue must be a boolean")
        if len(canonical(self.to_dict())) > MAX_POLICY_BYTES:
            raise ValueError("commit policy rules exceed their byte bound")

    @classmethod
    def of(cls, *, include_paths=(), exclude=(), identity=(), max_bytes=COMMIT_MAX_BYTES, keep_build_residue=False):
        return cls(_rules(include_paths), _rules(exclude), _rules(identity, pattern=True), max_bytes,
                   keep_build_residue)

    def to_dict(self):
        # Written only when set: the policies (and hashes) of agent commits stay as they were.
        return {"schema": POLICY_SCHEMA, "include_paths": list(self.include_paths),
                "exclude": list(self.exclude), "identity": list(self.identity), "max_bytes": self.max_bytes,
                **({"keep_build_residue": True} if self.keep_build_residue else {})}

    @classmethod
    def from_dict(cls, raw):
        fields = {"schema", "include_paths", "exclude", "identity", "max_bytes"}
        if not isinstance(raw, dict) or set(raw) - {"keep_build_residue"} != fields \
                or raw["schema"] != POLICY_SCHEMA or raw.get("keep_build_residue", True) is not True or not all(
                    isinstance(raw[name], list) for name in ("include_paths", "exclude", "identity")):
            raise ValueError("invalid commit policy")
        return cls(tuple(raw["include_paths"]), tuple(raw["exclude"]), tuple(raw["identity"]), raw["max_bytes"],
                   "keep_build_residue" in raw)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(canonical(self.to_dict())).hexdigest()

    def rule(self, path: str, kind: str) -> str | None:
        """The drop rule for logical ``path``, None to keep it; raises on an escape."""
        if any(_under(path, escape) for escape in ESCAPES):
            if kind in {"dir", "opaque"} and path in MOUNT_POINTS:  # gVisor may mark new directories opaque
                return "volatile"
            raise CommitRefused("commit_residue_forbidden", f"{path!r} lies at or below a sandbox mount")
        if any(_under(path, prefix) for prefix in HOST_WRITTEN):
            return "host_written"
        if any(path.startswith(rule[1:-1]) if rule.endswith("*") else _under(path, rule[1:])
               for rule in self.identity):
            return "identity"
        if any(_under(path, prefix) for prefix in VOLATILE):
            return "volatile"
        if not self.keep_build_residue and (
                any(_under(path, prefix) for prefix in BUILD_RESIDUE) or path.startswith(APT_ARCHIVES)
                and path.endswith(".deb") and "/" not in path[len(APT_ARCHIVES):]):
            return "build_residue"
        if any(_under(path, rule[1:]) for rule in self.exclude):
            return "caller"
        if self.include_paths and not any(_under(path, rule[1:]) for rule in self.include_paths):
            # Ancestors keep their directory metadata, never their opacity:
            # deletions outside include_paths do not propagate.
            if kind == "dir" and any(_under(rule[1:], path) for rule in self.include_paths):
                return None
            return "caller"
        return None


def _bounded_int(value, low, high) -> bool:
    return type(value) is int and low <= value <= high


def _text(value, pattern) -> bool:
    return isinstance(value, str) and pattern.match(value) is not None


def require_secret_digests(values) -> tuple[str, ...]:
    """Sorted unique SHA-256 hex digests of live credentials; never plaintext."""
    if (not isinstance(values, (list, tuple)) or len(values) > MAX_SECRET_DIGESTS
            or not all(_text(value, _HEX64) for value in values) or list(values) != sorted(set(values))):
        raise ValueError("invalid commit secret digests")
    return tuple(values)


@dataclass(frozen=True)
class CommitExportRequest:
    """Gateway -> worker ``POST /v1/sandboxes/{id}/commit-export``; the operation ID is the replay key."""
    operation_id: str
    generation: int
    image_id: str
    resume: bool = True
    max_bytes: int = COMMIT_MAX_BYTES

    def __post_init__(self):
        if not (_text(self.operation_id, OPERATION_ID_RE) and _bounded_int(self.generation, 1, 2**63 - 1)
                and type(self.resume) is bool and _text(self.image_id, IMAGE_ID_RE)
                and _bounded_int(self.max_bytes, 1, 1 << 40)):
            raise ValueError("invalid commit export request")

    def to_dict(self):
        return {"operation_id": self.operation_id, "generation": self.generation, "image_id": self.image_id,
                "resume": self.resume, "max_bytes": self.max_bytes}

    @classmethod
    def from_dict(cls, raw):
        if not isinstance(raw, dict) or set(raw) != {"operation_id", "generation", "image_id", "resume", "max_bytes"}:
            raise ValueError("invalid commit export request")
        return cls(**raw)


@dataclass(frozen=True)
class CommitExport:
    """The worker's replay record and response: ``exporting`` (the intent),
    ``staged`` or ``failed``. ``was_paused`` is the state a replay restores."""
    sandbox_id: str
    request: CommitExportRequest
    state: str
    was_paused: bool
    identity: tuple[str, ...]
    repository: str
    blob_digest: str = ""
    size: int = 0
    error_code: str = ""

    def __post_init__(self):
        staged, failed = self.state == "staged", self.state == "failed"
        if not (_text(self.sandbox_id, IMAGE_ID_RE) and isinstance(self.request, CommitExportRequest)
                and type(self.was_paused) is bool and self.state in {"exporting", "staged", "failed"}
                and self.repository == staging_repository(self.request.image_id)
                and (_text(self.blob_digest, _DIGEST) if staged else self.blob_digest == "")
                and (_bounded_int(self.size, 1, 1 << 40) if staged else self.size == 0)
                and (_text(self.error_code, _CODE) if failed else self.error_code == "")):
            raise ValueError("invalid commit export record")
        _rules(self.identity, pattern=True)

    def to_dict(self):
        return {"schema": EXPORT_SCHEMA, "sandbox_id": self.sandbox_id, "request": self.request.to_dict(),
                "state": self.state, "was_paused": self.was_paused, "identity": list(self.identity),
                "repository": self.repository, "blob_digest": self.blob_digest, "size": self.size,
                "error_code": self.error_code}

    @classmethod
    def from_dict(cls, raw):
        fields = {"schema", "sandbox_id", "request", "state", "was_paused", "identity", "repository",
                  "blob_digest", "size", "error_code"}
        if not isinstance(raw, dict) or set(raw) != fields or raw["schema"] != EXPORT_SCHEMA \
                or not isinstance(raw["identity"], list):
            raise ValueError("invalid commit export record")
        values = {name: raw[name] for name in fields - {"schema"}}
        return cls(**values | {"request": CommitExportRequest.from_dict(raw["request"]),
                               "identity": tuple(raw["identity"])})


@dataclass(frozen=True)
class CommitBuild:
    """The builder's ``commit`` object; the gateway binds every field at ``staged``."""
    sandbox_id: str
    generation: int
    operation_id: str
    image_id: str
    parent_image: str
    parent_root: str
    blob_digest: str
    blob_size: int
    policy: CommitPolicy
    secret_digests: tuple[str, ...] = ()

    def __post_init__(self):
        if not (_text(self.sandbox_id, IMAGE_ID_RE) and _bounded_int(self.generation, 1, 2**63 - 1)
                and _text(self.operation_id, OPERATION_ID_RE) and _text(self.image_id, IMAGE_ID_RE)
                and isinstance(self.parent_image, str) and len(self.parent_image) <= 1024
                and _DIGEST.match(self.parent_image.rpartition("@")[2]) is not None
                and _text(self.parent_root, _DIGEST) and _text(self.blob_digest, _DIGEST)
                and _bounded_int(self.blob_size, 1, 1 << 40) and isinstance(self.policy, CommitPolicy)
                and isinstance(self.secret_digests, tuple)):
            raise ValueError("invalid commit build")
        require_secret_digests(self.secret_digests)

    @property
    def repository(self) -> str:
        return staging_repository(self.image_id)

    def to_dict(self):
        return {"sandbox_id": self.sandbox_id, "generation": self.generation, "operation_id": self.operation_id,
                "image_id": self.image_id, "parent_image": self.parent_image, "parent_root": self.parent_root,
                "blob_digest": self.blob_digest, "blob_size": self.blob_size, "policy": self.policy.to_dict(),
                "secret_digests": list(self.secret_digests)}

    @classmethod
    def from_dict(cls, raw):
        if (not isinstance(raw, dict) or set(raw) != {
                "sandbox_id", "generation", "operation_id", "image_id", "parent_image", "parent_root",
                "blob_digest", "blob_size", "policy", "secret_digests"}
                or not isinstance(raw["secret_digests"], list)):
            raise ValueError("invalid commit build")
        return cls(**raw | {"policy": CommitPolicy.from_dict(raw["policy"]),
                            "secret_digests": tuple(raw["secret_digests"])})


@dataclass(frozen=True)
class FilterResult:
    diff_id: str
    size: int
    members: int
    drops: dict

    def to_dict(self):
        return {"diff_id": self.diff_id, "size": self.size, "members": self.members, "drops": dict(self.drops)}

    @classmethod
    def from_dict(cls, raw):
        rules = {"host_written", "identity", "volatile", "build_residue", "caller"}
        if (not isinstance(raw, dict) or set(raw) != {"diff_id", "size", "members", "drops"}
                or not _text(raw["diff_id"], _DIGEST)
                or not _bounded_int(raw["size"], 1, 1 << 41) or not _bounded_int(raw["members"], 0, 2 * MAX_MEMBERS)
                or not isinstance(raw["drops"], dict) or not set(raw["drops"]) <= rules
                # A directory and its opacity drop separately.
                or any(not _bounded_int(count, 1, 2 * MAX_MEMBERS) for count in raw["drops"].values())):
            raise ValueError("invalid commit filter result")
        return cls(raw["diff_id"], raw["size"], raw["members"], dict(raw["drops"]))


class DigestWriter:
    """A write-only stream that hashes and counts what passes through it."""

    def __init__(self, stream):
        self.stream, self.hash, self.size = stream, hashlib.sha256(), 0

    def write(self, data):
        self.stream.write(data)
        self.hash.update(data)
        self.size += len(data)
        return len(data)

    def tell(self):
        return self.size

    def flush(self):
        self.stream.flush()

    @property
    def digest(self) -> str:
        return "sha256:" + self.hash.hexdigest()


class _Tokens:
    """Maximal lowercase-hex runs of a TOKEN_CLASSES length whose SHA-256 is live."""

    def __init__(self, digests):
        self.digests = frozenset(digests)
        lengths = sorted(set(TOKEN_CLASSES.values()), reverse=True)
        self.carry = lengths[0] + 1
        self.pattern = re.compile(rb"(?<![0-9a-f])(?:" + b"|".join(b"[0-9a-f]{%d}" % n for n in lengths)
                                  + rb")(?![0-9a-f])")

    def check(self, data: bytes) -> None:
        for match in self.pattern.finditer(data):
            if hashlib.sha256(match.group()).hexdigest() in self.digests:
                raise CommitRefused("commit_secret_residue", "a live credential is in the committed upper")


class _ScannedBody:
    """Scan a member body as tarfile copies it; ``finish`` checks the tail."""

    def __init__(self, raw, tokens):
        self.raw, self.tokens, self.tail = raw, tokens, b""

    def read(self, size=-1):
        chunk = self.raw.read(size)
        data = self.tail + chunk
        # A run touching the end may continue: carry it, bounded, to the next read.
        cut = len(data.rstrip(_HEX))
        self.tokens.check(data[:cut])
        self.tail = data[cut:][-self.tokens.carry:]
        return chunk

    def finish(self):
        self.tokens.check(self.tail)


def _member_path(name: str) -> str:
    if "\0" in name:
        raise CommitRefused("commit_residue_forbidden", "NUL in a member path")
    try:
        encoded = name.encode("utf-8")
    except UnicodeEncodeError:
        raise CommitRefused("commit_residue_forbidden", "member path is not UTF-8") from None
    if len(encoded) > MAX_PATH_BYTES or name.startswith("/"):
        raise CommitRefused("commit_residue_forbidden", "member path is absolute or too long")
    while name.startswith("./"):
        name = name[2:]
    name = name.rstrip("/")
    if name in {"", "."}:
        return ""
    if any(part in {"", ".", ".."} for part in name.split("/")):
        raise CommitRefused("commit_residue_forbidden", f"member path {name!r} is not clean")
    return name


def _parse(member):
    """(logical path, kind, opaque, kept xattrs); kinds name what the output holds."""
    path = _member_path(member.name)
    parent, _, base = path.rpartition("/")
    opaque, xattrs = False, {}
    for key, value in member.pax_headers.items():
        if key.startswith("LIBARCHIVE.xattr."):
            raise CommitRefused("commit_residue_forbidden", "unsupported xattr encoding")
        name = key[len("SCHILY.xattr."):] if key.startswith("SCHILY.xattr.") else None
        if name == OPAQUE_XATTR and member.isdir() and value == "y":
            opaque = True
        elif name is not None and name.startswith("trusted."):
            raise CommitRefused("commit_residue_forbidden", f"{path!r} carries {name}")
        elif name is not None and (name == "security.capability" or name.startswith("user.")):
            xattrs[key] = value
    if member.ischr() and member.devmajor == member.devminor == 0 and path:
        return path, "whiteout", False, {}
    if member.ischr() or member.isblk():
        raise CommitRefused("commit_residue_forbidden", f"{path!r} is a device node")
    if base == OPAQUE or base.startswith(WHITEOUT):
        target = base[len(WHITEOUT):]
        if not member.isreg() or not path or (base != OPAQUE and (not target or target.startswith(WHITEOUT))):
            raise CommitRefused("commit_residue_forbidden", f"{path!r} is a malformed whiteout")
        if base == OPAQUE:
            return parent, "opaque", False, {}
        return (parent + "/" if parent else "") + target, "whiteout", False, {}
    if member.isdir():
        return path, "dir", opaque, xattrs
    if not path:
        raise CommitRefused("commit_residue_forbidden", "the upper root is not a directory")
    if member.issym() and "\0" not in member.linkname and len(member.linkname.encode(
            "utf-8", "surrogateescape")) < MAX_PATH_BYTES:  # Linux bounds symlink targets by PATH_MAX
        return path, "symlink", False, xattrs
    if member.islnk():
        return path, "link", False, {}
    for test, kind in ((member.isreg, "file"), (member.isfifo, "fifo")):
        if test():
            return path, kind, False, xattrs
    raise CommitRefused("commit_residue_forbidden", f"{path!r} has an unsupported member type")


def _output_name(path: str, kind: str) -> str:
    parent, _, base = path.rpartition("/")
    prefix = parent + "/" if parent else ""
    if kind == "opaque":
        return (path + "/" if path else "") + OPAQUE
    if kind == "whiteout":
        return prefix + WHITEOUT + base
    return path or "."


def filter_upper(source, destination, policy: CommitPolicy, secret_digests=(), *, check=lambda: None) -> FilterResult:
    """Write runsc's upper tar ``source`` (seekable) as a sorted OCI layer tar.

    Whiteouts become ``.wh.<name>`` and opaque directories ``.wh..wh..opq``
    from either encoding: a 0:0 character device and the opaque xattr, or OCI
    entries. Owners, modes, mtimes (whole seconds), ``security.capability``
    and ``user.*`` xattrs are kept. One upper yields one diff ID whatever its
    member order. Any refusal raises before the caller may read the output.
    """
    tokens = _Tokens(secret_digests) if secret_digests else None
    try:
        with tarfile.open(fileobj=source, mode="r:", encoding="utf-8", errors="surrogateescape") as archive:
            entries, drops, links = _classify(archive, policy, check)
            if tokens is not None:
                for name, (member, _kind, xattrs) in entries.items():
                    tokens.check("\0".join((name, member.linkname, *xattrs, *xattrs.values())).encode(
                        "utf-8", "surrogateescape"))
            # Sorting moves link names before their targets: the first name of
            # each inode in output order carries its data, the rest link to it.
            data_of = {}
            for target, names in links.items():
                if entries.get(target, (None, ""))[1] != "file":  # absent, dropped or not regular
                    raise CommitRefused("commit_residue_forbidden", f"{names[0]!r} links to a dropped or absent target")
                names = sorted((target, *names))
                data_of.update((name, (target, names[0])) for name in names)
            if sum(member.size for member, kind, _ in entries.values() if kind == "file") > policy.max_bytes:
                raise CommitRefused("commit_too_large", "the filtered upper exceeds max_bytes")
            return _write(archive, entries, data_of, destination, policy, tokens, drops, check)
    except (tarfile.TarError, EOFError) as exc:
        raise CommitRefused("commit_residue_forbidden", "the exported upper is not a readable tar") from exc


def _classify(archive, policy, check):
    """Kept entries by output name, drop counts, and hardlink names by target."""
    entries, drops, links = {}, {}, {}
    seen, ancestors, whiteouts = {}, set(), set()
    for count, member in enumerate(archive, 1):
        check()
        if count > MAX_MEMBERS:
            raise CommitRefused("commit_too_large", "the upper has too many members")
        path, kind, opaque, xattrs = _parse(member)
        if kind == "link" and _member_path(member.linkname) in whiteouts:
            # runsc writes each deletion after the first as a hard link to the
            # first whiteout device (they share its inode): a whiteout too.
            kind = "whiteout"
        if kind == "whiteout" and member.ischr():
            whiteouts.add(path)
        # An opaque marker sits below its directory, so that path must be one.
        below = _output_name(path, kind) if kind == "opaque" else path
        parents = [below[:index] for index in range(len(below)) if below[index] == "/"]
        if any(seen.get(p) is False for p in parents) or kind != "opaque" and (
                path in seen or kind != "dir" and path in ancestors):
            raise CommitRefused("commit_residue_forbidden", f"{path!r} is duplicated or below a non-directory")
        if kind != "opaque":
            seen[path] = kind == "dir"
        ancestors.update(parents)
        for logical, item_kind in ((path, kind), *(((path, "opaque"),) if opaque else ())):
            name = _output_name(logical, item_kind)
            if item_kind == "opaque" and name in entries:
                continue  # both opaque encodings on one directory
            rule = policy.rule(logical, item_kind)
            if rule is not None:
                drops[rule] = drops.get(rule, 0) + 1
                continue
            entries[name] = (member, item_kind, xattrs)
            if item_kind == "link":
                links.setdefault(_member_path(member.linkname), []).append(logical)
    return entries, dict(sorted(drops.items())), links


def _write(archive, entries, data_of, destination, policy, tokens, drops, check) -> FilterResult:
    sink = DigestWriter(destination)
    with tarfile.open(fileobj=sink, mode="w", format=tarfile.PAX_FORMAT, encoding="utf-8",
                      errors="surrogateescape", copybufsize=1 << 20) as output:
        for name in sorted(entries, key=lambda item: (item != ".", item)):
            check()
            member, kind, xattrs = entries[name]
            target, first = data_of.get(name, (name, name))
            if kind == "link":  # one inode: its regular member's metadata
                member, kind, xattrs = entries[target]
            info = tarfile.TarInfo(name)
            info.type = {"dir": tarfile.DIRTYPE, "symlink": tarfile.SYMTYPE, "fifo": tarfile.FIFOTYPE}.get(
                kind, tarfile.REGTYPE)
            if kind not in {"whiteout", "opaque"}:  # whiteouts keep TarInfo's fixed defaults
                info.mode, info.uid, info.gid = member.mode & 0o7777, member.uid, member.gid
                info.mtime, info.pax_headers = int(member.mtime), dict(sorted(xattrs.items()))
            body = None
            if kind == "symlink":
                info.linkname = member.linkname
            elif kind == "file" and first != name:
                info.type, info.linkname = tarfile.LNKTYPE, first
            elif kind == "file":
                info.size = member.size
                body = archive.extractfile(member)
                if tokens is not None:
                    body = _ScannedBody(body, tokens)
            output.addfile(info, body)
            if isinstance(body, _ScannedBody):
                body.finish()
            if sink.size > policy.max_bytes:
                raise CommitRefused("commit_too_large", "the filtered upper exceeds max_bytes")
    return FilterResult(sink.digest, sink.size, len(entries), drops)
