"""Sandbox builds (C2.14, docs/sandbox-builds.md).

A recipe whose Dockerfile the prepared catalog splits into a foundation and a
remainder is built without Docker: a sandbox starts from the foundation's
chunk-store root, the remainder runs in it as one generated script, and only
what it changed is committed, as one layer stacked on that root
(``RafsConverter.extend``). No builder, no OCI image, no foundation copy.

- **Plan** (pure): the remainder's instructions become a POSIX shell script
  and the image config Docker would record. ``Unsupported`` sends a recipe to
  the builders instead; recipes reach this only after ``safe_rewrite`` (no
  ARG, ADD, heredocs, ``RUN --`` flags or COPY globs).
- **Run** (``serve-sandbox-builds``, a gateway-host service holding the chunk
  store's S3 key): jobs arrive in a spool directory the gateway writes; each
  is created, run, exported (the worker's ``commit-export``), filtered (the
  keyless commit filter) and stacked; the result file is adopted by ``ensure``.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import base64
import gzip
import hashlib
import io
import json
import logging
import os
from pathlib import Path
import posixpath
import re
import shlex
import socket
import tarfile
from tempfile import TemporaryDirectory
import threading
import time
from urllib import error, parse, request
import uuid

_LOG = logging.getLogger(__name__)
SCHEMA = "ucloud-sandbox-build-v1"
BUILD_DIR = "/var/tmp/.ucloud-build"  # The bundle; removed before the commit.
BUILD_TMP = "/var/tmp/.ucloud-build-tmp"  # TMPDIR for steps: the sandbox's /tmp is a small tmpfs.
DEFAULT_PATH = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
CONFIG_ONLY = frozenset({"LABEL", "EXPOSE", "VOLUME", "STOPSIGNAL", "HEALTHCHECK", "MAINTAINER"})
MAX_EXTERNAL_BYTES = 512 * 1024 ** 2
# Public registries a COPY --from may name; never a private address.
EXTERNAL_REGISTRIES = ("docker.io", "ghcr.io", "quay.io", "gcr.io", "registry.k8s.io", "public.ecr.aws",
                       "mcr.microsoft.com")
_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_CONTINUATION = re.compile(r"\\[ \t]*$")


class Unsupported(ValueError):
    """The recipe needs a builder: a construct sandbox builds do not run."""


class BuildFailed(RuntimeError):
    """The recipe's own step failed (as it would under Docker)."""


# --- Dockerfile: BuildKit's line rules and shell-word processing ---

@dataclass(frozen=True)
class Instruction:
    keyword: str
    flags: tuple[str, ...]
    args: str
    line: int


def parse_dockerfile(text):
    """Instructions with BuildKit's continuation rules: a trailing backslash
    (spaces after it allowed) joins the next line as is, comment and empty
    lines inside a continuation are skipped, leading ``--flags`` split off."""
    lines, out, index = text.splitlines(), [], 0
    while index < len(lines):
        raw, number = lines[index], index + 1
        index += 1
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        line, more = _continued(raw.lstrip())
        while more and index < len(lines):
            following = lines[index]
            index += 1
            if not following.strip() or following.lstrip().startswith("#"):
                continue
            part, more = _continued(following)
            line += part
        keyword, _, rest = line.strip().partition(" ")
        if "\t" in keyword:
            keyword, _, tail = keyword.partition("\t")
            rest = tail + " " + rest
        rest, flags = rest.strip(), []
        while rest.startswith("--"):
            flag, _, rest = rest.partition(" ")
            flags.append(flag)
            rest = rest.lstrip()
        out.append(Instruction(keyword.upper(), tuple(flags), rest.strip(), number))
    return out


def clean_name(name):
    """A tar member name relative to its root ("" for the root itself)."""
    while name.startswith("./"):
        name = name[2:]
    name = posixpath.normpath(name.lstrip("/") or ".")
    if name == ".." or name.startswith("../"):
        raise ValueError(f"tar member {name!r} leaves its root")
    return "" if name == "." else name


def _continued(line):
    return (_CONTINUATION.sub("", line), True) if _CONTINUATION.search(line) else (line, False)


def words(text, env, *, split=True):
    """Docker's shell-word processing: quotes removed, ``$VAR``, ``${VAR}``,
    ``${VAR:-word}`` and ``${VAR:+word}`` (also without the colon) expanded
    from ``env``; split at unquoted blanks unless ``split`` is off."""
    out, word, present, index = [], [], False, 0
    while index < len(text):
        char = text[index]
        if split and char in " \t":
            if present:
                out.append("".join(word))
                word, present = [], False
            index += 1
        elif char == "\\":
            word.append(text[index + 1] if index + 1 < len(text) else char)
            index, present = index + 2, True
        elif char == "'":
            end = text.find("'", index + 1)
            if end < 0:
                raise Unsupported("unterminated quote")
            word.append(text[index + 1:end])
            index, present = end + 1, True
        elif char == '"':
            index += 1
            while index < len(text) and text[index] != '"':
                if text[index] == "\\" and index + 1 < len(text) and text[index + 1] in '"\\$':
                    word.append(text[index + 1])
                    index += 2
                elif text[index] == "$":
                    value, index = _variable(text, index, env)
                    word.append(value)
                else:
                    word.append(text[index])
                    index += 1
            if index >= len(text):
                raise Unsupported("unterminated quote")
            index, present = index + 1, True
        elif char == "$":
            value, index = _variable(text, index, env)
            word.append(value)
            present = True
        else:
            word.append(char)
            index, present = index + 1, True
    if present:
        out.append("".join(word))
    return out if split else "".join(out)


def _variable(text, index, env):
    """The value of the variable at ``text[index] == "$"`` and the index after it."""
    if text.startswith("${", index):
        end = text.find("}", index)
        if end < 0:
            raise Unsupported("unterminated ${")
        body = text[index + 2:end]
        name = _NAME.match(body)
        if name is None:
            raise Unsupported(f"unsupported substitution ${{{body}}}")
        rest, value = body[name.end():], env.get(name[0])
        if not rest:
            return value or "", end + 1
        operator = rest[:2] if rest[:2] in (":-", ":+") else rest[:1]
        if operator not in (":-", ":+", "-", "+") or "$" in rest or "{" in rest:
            raise Unsupported(f"unsupported substitution ${{{body}}}")
        alternative = words(rest[len(operator):], env, split=False)
        empty = value is None or (operator.startswith(":") and value == "")
        if operator.endswith("-"):
            return (alternative if empty else value), end + 1
        return ("" if empty else alternative), end + 1
    name = _NAME.match(text, index + 1)
    if name is None:
        return "$", index + 1
    return env.get(name[0], ""), name.end()


def _json_list(args):
    if not args.startswith("["):
        return None
    try:
        value = json.loads(args)
    except ValueError:
        return None
    return value if isinstance(value, list) and all(isinstance(item, str) for item in value) else None


# --- The plan: a script and an image config ---

@dataclass(frozen=True)
class External:
    """``COPY --from=<reference>``: these absolute ``sources``, fetched by the
    service and shipped in the bundle as ``from-<index>.tar``."""
    reference: str
    sources: tuple[str, ...]


@dataclass
class BuildPlan:
    script: str
    image_config: dict
    externals: list = field(default_factory=list)
    steps: int = 0


@dataclass(frozen=True)
class ContextTree:
    """Path kinds in a build context: ``files`` (regular files and symlinks) and ``dirs``."""
    files: frozenset
    dirs: frozenset

    @classmethod
    def of(cls, members):
        files, dirs = set(), {""}
        for member in members:
            name = clean_name(member.name)
            parts = name.split("/") if name else []
            dirs.update("/".join(parts[:end]) for end in range(len(parts)))
            (dirs if member.isdir() else files).add(name)
        return cls(frozenset(files), frozenset(dirs))

    def kind(self, path):
        path = clean_name(path)
        return "file" if path in self.files else "dir" if path in self.dirs else None


def plan_build(dockerfile, context, base_config, *, apt_proxy=None, build_dir=BUILD_DIR, build_tmp=BUILD_TMP):
    """The remainder of a rewritten Dockerfile (``FROM <foundation>`` first) as
    a build script, with the image config it leaves. ``context`` is a
    ContextTree; ``base_config`` the foundation root's image config.
    ``apt_proxy`` (url, hosts) routes apt for those hosts through the cache.
    ``build_dir`` holds the bundle and ``build_tmp`` is the steps' TMPDIR."""
    paths = (build_dir, build_tmp)
    instructions = parse_dockerfile(dockerfile)
    if not instructions or instructions[0].keyword != "FROM" or instructions[0].flags:
        raise Unsupported("a sandbox build starts from one FROM")
    env = {}
    for item in base_config.get("Env") or []:
        name, _, value = item.partition("=")
        env[name] = value
    config = {"Entrypoint": list(base_config.get("Entrypoint") or []), "Cmd": list(base_config.get("Cmd") or []),
              "WorkingDir": base_config.get("WorkingDir") or "", "User": base_config.get("User") or ""}
    shell, cmd_set, externals, lines = ["/bin/sh", "-c"], False, [], []
    for number, item in enumerate(instructions[1:], 1):
        keyword, args = item.keyword, item.args
        label = " ".join((keyword, *item.flags, args))[:200]
        if keyword == "ENV":
            env.update(_env_pairs(args, env))
        elif keyword == "WORKDIR":
            target = words(args, env, split=False)
            config["WorkingDir"] = posixpath.normpath(posixpath.join(config["WorkingDir"] or "/", target))
            lines.append(_step(number, label, f"mkdir -p -- {shlex.quote(config['WorkingDir'])} || fail $?"))
        elif keyword == "USER":
            config["User"] = words(args, env, split=False)
        elif keyword == "SHELL":
            shell = _json_list(args)
            if not shell:
                raise Unsupported("SHELL needs its JSON form")
        elif keyword in ("CMD", "ENTRYPOINT"):
            value = _json_list(args)
            value = value if value is not None else [*shell, args]
            if keyword == "CMD":
                config["Cmd"], cmd_set = value, True
            else:
                config["Entrypoint"] = value
                if not cmd_set:
                    config["Cmd"] = []  # Docker: an ENTRYPOINT resets an inherited CMD.
        elif keyword == "RUN":
            if item.flags:
                raise Unsupported(f"RUN {' '.join(item.flags)}")
            argv = _json_list(args)
            argv = argv if argv is not None else [*shell, args]
            lines.append(_step(number, label, _run(argv, env, config, apt_proxy, paths)))
        elif keyword == "COPY":
            lines.append(_step(number, label, _copy(item, env, config, context, externals, build_dir)))
        elif keyword not in CONFIG_ONLY:
            raise Unsupported(f"{keyword} is not run in sandbox builds")
    config["Env"] = [f"{name}={value}" for name, value in env.items()]
    return BuildPlan(_script(lines, apt_proxy, paths), config, externals, len(instructions) - 1)


def _env_pairs(args, env):
    """ENV ``a=b c="d e"`` or the legacy ``ENV a b c``; values expand against the env before it."""
    head = args.split(None, 1)
    if head and "=" not in head[0]:
        if len(head) < 2:
            raise Unsupported("ENV needs a value")
        return {head[0]: words(head[1], env, split=False)}
    pairs = {}
    for word in words(args, env):
        name, equals, value = word.partition("=")
        if not equals or not _NAME.fullmatch(name):
            raise Unsupported(f"unsupported ENV {word!r}")
        pairs[name] = value
    return pairs


def _step(number, label, body):
    return f"step {number} {shlex.quote(label)}\n{body}\n"


def _run(argv, env, config, apt_proxy, paths):
    variables = dict(env)
    variables.setdefault("PATH", DEFAULT_PATH)
    variables.setdefault("TMPDIR", paths[1])
    if apt_proxy:
        variables.setdefault("APT_CONFIG", f"{paths[0]}/apt.conf")
    user = config["User"]
    root = user in ("", "root", "0", "0:0", "root:root", "root:0", "0:root")
    if "HOME" not in env:
        variables["HOME"] = "/root" if root else "$(home_of " + shlex.quote(user.split(":")[0]) + ")"
    assignments = " ".join(shlex.quote(f"{name}={value}") if name != "HOME" or "HOME" in env
                           else f'"HOME={value}"' for name, value in variables.items())
    workdir = shlex.quote(config["WorkingDir"] or "/")
    prefix = "" if root else f"as_user {shlex.quote(user)} "
    return f"run_in {workdir} {prefix}env -i {assignments} {shlex.join(argv)}"


def _copy(item, env, config, context, externals, build_dir):
    """Docker's COPY: a directory's contents, a file into ``dest/`` when it ends
    in a slash or several sources share it; parents made; owners 0:0 (the
    bundle's tars carry them); modes, mtimes and symlinks kept."""
    source_image = None
    for flag in item.flags:
        if flag.startswith("--from="):
            source_image = flag[len("--from="):]
        else:
            raise Unsupported(f"COPY {flag}")
    listed = _json_list(item.args)
    parts = [words(part, env, split=False) for part in listed] if listed is not None else words(item.args, env)
    if len(parts) < 2:
        raise Unsupported("COPY needs a source and a destination")
    *sources, dest = parts
    workdir = config["WorkingDir"] or "/"
    dest_dir = dest.endswith("/") or len(sources) > 1
    target = posixpath.normpath(posixpath.join(workdir, dest))
    if source_image is not None:
        if not all(source.startswith("/") for source in sources):
            raise Unsupported("COPY --from takes absolute sources")
        external_reference(source_image)  # Refused before any fetch.
        externals.append(External(source_image, tuple(posixpath.normpath(s) for s in sources)))
        root = f"{build_dir}/from-{len(externals) - 1}"
        kinds = [None] * len(sources)  # Known only once fetched: decided in the sandbox.
    else:
        root = f"{build_dir}/context"
        kinds = [context.kind(source) for source in sources]
        if None in kinds:
            raise Unsupported(f"COPY source {sources[kinds.index(None)]!r} is not in the build context")
    commands = []
    for source, kind in zip(sources, kinds):
        path = shlex.quote(posixpath.normpath(f"{root}/{source.lstrip('/')}"))
        commands.append(f"copy_in {path} {shlex.quote(target)} {'dir' if dest_dir else 'path'} {kind or 'any'}")
    return " &&\n".join(commands) + " || fail $?"


_PRELUDE = r'''#!/bin/sh
# Generated by ucloud-sandboxes (sandbox builds): a Dockerfile remainder, run once as root.
set -u
exec 1>&2
B={build_dir}
T={build_tmp}
step() { printf '##ucloud-step %s %s\n' "$1" "$2"; CURRENT=$1; }
fail() { printf '##ucloud-failed %s %s\n' "$CURRENT" "$1"; printf '%s %s\n' "$CURRENT" "$1" > "$B/failed"; exit 1; }
run_in() { dir=$1; shift; ( cd -- "$dir" && "$@" ) || fail $?; }
home_of() { h=$(getent passwd "$1" 2>/dev/null | cut -d: -f6); printf '%s' "${h:-/}"; }
as_user() {
  u=${1%%:*}; g=; case $1 in *:*) g=${1#*:};; esac; shift
  command -v setpriv >/dev/null 2>&1 || { echo "USER $u needs setpriv" >&2; return 126; }
  uid=$(id -u "$u" 2>/dev/null) || uid=$u
  if [ -n "$g" ]; then gid=$(getent group "$g" 2>/dev/null | cut -d: -f3); gid=${gid:-$g}
  else gid=$(id -g "$u" 2>/dev/null) || gid=$uid; fi
  if [ -z "$g" ] && id -u "$u" >/dev/null 2>&1; then set -- --init-groups "$@"; else set -- --clear-groups "$@"; fi
  setpriv --reuid="$uid" --regid="$gid" "$@"
}
# kind "any": a COPY --from source, a directory or not only once fetched.
copy_in() {
  src=$1; dest=$2; into=$3; kind=$4
  [ "$kind" = any ] && { if [ -d "$src" ] && [ ! -L "$src" ]; then kind=dir; else kind=file; fi; }
  [ -e "$src" ] || [ -L "$src" ] || { echo "COPY source is missing" >&2; return 1; }
  if [ "$kind" = dir ]; then mkdir -p -- "$dest" && cp -a -- "$src/." "$dest/"
  elif [ "$into" = dir ]; then mkdir -p -- "$dest" && cp -a -- "$src" "$dest/"
  else mkdir -p -- "$(dirname -- "$dest")" && cp -a -- "$src" "$dest"; fi
}
CURRENT=0
mkdir -p "$B/context" "$T" && chmod 1777 "$T" || fail $?
tar -x -p --same-owner -f "$B/context.tar" -C "$B/context" || fail $?
for archive in "$B"/from-*.tar; do
  [ -e "$archive" ] || continue
  mkdir -p "${archive%.tar}" && tar -x -p --same-owner -f "$archive" -C "${archive%.tar}" || fail $?
done
'''

_APT = r'''# apt reads APT_CONFIG instead of /etc/apt/apt.conf: keep the image's.
if [ -f /etc/apt/apt.conf ]; then cat /etc/apt/apt.conf "$B/apt.conf" > "$B/apt.conf.new" && mv "$B/apt.conf.new" "$B/apt.conf"; fi
'''

_EPILOGUE = r'''step done 'cleanup'
rm -rf -- "$B" "$T"
printf '##ucloud-done\n'
'''


def _script(lines, apt_proxy, paths):
    prelude = _PRELUDE.replace("{build_dir}", shlex.quote(paths[0])).replace("{build_tmp}", shlex.quote(paths[1]))
    return prelude + (_APT if apt_proxy else "") + "".join(lines) + _EPILOGUE


def apt_config(url, hosts):
    """apt sends its requests for ``hosts`` through the package cache, and no others."""
    return "".join(f'Acquire::http::Proxy::{host} "{url}";\n' for host in sorted(hosts))


# --- COPY --from: files of a public image, pulled anonymously ---

def external_reference(reference):
    """(registry endpoint host, repository, tag or digest) of a public image."""
    name, digest = (reference.split("@", 1) + [""])[:2]
    first, slash, rest = name.partition("/")
    if not slash or ("." not in first and ":" not in first and first != "localhost"):
        host, path = "docker.io", name
    else:
        host, path = first, rest
    path, _, tag = path.partition(":")
    if host == "docker.io" and "/" not in path:
        path = "library/" + path
    if host not in EXTERNAL_REGISTRIES or not re.fullmatch(r"[a-z0-9]+(?:[._/-][a-z0-9]+)*", path):
        raise Unsupported(f"COPY --from={reference} names no public registry image")
    if digest and not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        raise Unsupported(f"COPY --from={reference} has an invalid digest")
    return ("registry-1.docker.io" if host == "docker.io" else host), path, digest or tag or "latest"


class _NoRedirect(request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class ExternalImages:
    """Pulls a public image's layers (linux/amd64) and keeps the named paths,
    cached by manifest digest. Redirects (to blob storage) are followed
    without the registry's token."""

    ACCEPT = ", ".join(("application/vnd.oci.image.index.v1+json",
                        "application/vnd.docker.distribution.manifest.list.v2+json",
                        "application/vnd.oci.image.manifest.v1+json",
                        "application/vnd.docker.distribution.manifest.v2+json"))

    def __init__(self, cache_dir, *, opener=None, timeout=120):
        self.cache_dir, self.timeout = Path(cache_dir), timeout
        self.opener = opener or request.build_opener(_NoRedirect)
        self._tokens = {}

    def files(self, reference, sources):
        """A tar of ``sources`` (absolute paths) from ``reference``, owners 0:0."""
        host, repository, ref = external_reference(reference)
        manifest = self._manifest(host, repository, ref)
        key = hashlib.sha256(json.dumps([hashlib.sha256(manifest).hexdigest(), sorted(sources)]).encode()).hexdigest()
        cached = self.cache_dir / f"{key}.tar"
        if cached.exists():
            return cached.read_bytes()
        document = json.loads(manifest)
        kept, total = {}, 0
        for layer in document.get("layers") or []:
            for member, data in self._layer(host, repository, layer):
                path = "/" + clean_name(member.name)
                base = posixpath.basename(path)
                parent = posixpath.dirname(path)
                if base == ".wh..wh..opq":
                    kept = {p: v for p, v in kept.items() if not p.startswith(parent.rstrip("/") + "/")}
                    continue
                if base.startswith(".wh."):
                    gone = posixpath.join(parent, base[4:])
                    kept = {p: v for p, v in kept.items() if p != gone and not p.startswith(gone + "/")}
                    continue
                if not any(path == source or path.startswith(source.rstrip("/") + "/") for source in sources):
                    continue
                total += len(data or b"")
                if total > MAX_EXTERNAL_BYTES:
                    raise Unsupported(f"COPY --from={reference} copies more than {MAX_EXTERNAL_BYTES} bytes")
                kept[path] = (member, data)
        if not all(any(p == s or p.startswith(s.rstrip("/") + "/") for p in kept) for s in sources):
            raise BuildFailed(f"COPY --from={reference}: a source is not in the image")
        output = io.BytesIO()
        with tarfile.open(fileobj=output, mode="w", format=tarfile.PAX_FORMAT) as archive:
            for path in sorted(kept):
                member, data = kept[path]
                info = tarfile.TarInfo(path.lstrip("/"))
                info.type, info.mode, info.mtime, info.linkname = member.type, member.mode, member.mtime, member.linkname
                info.uid = info.gid = 0
                if member.islnk():
                    info.linkname = clean_name(member.linkname)
                info.size = len(data) if data is not None else 0
                archive.addfile(info, io.BytesIO(data) if data is not None else None)
        payload = output.getvalue()
        self.cache_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        partial = cached.with_suffix(f".{uuid.uuid4().hex}.part")
        partial.write_bytes(payload)
        os.replace(partial, cached)
        return payload

    def _manifest(self, host, repository, ref):
        payload, media = self._get(host, repository, f"/v2/{repository}/manifests/{ref}", self.ACCEPT)
        document = json.loads(payload)
        if document.get("manifests"):
            for entry in document["manifests"]:
                platform = entry.get("platform") or {}
                if platform.get("os") == "linux" and platform.get("architecture") == "amd64":
                    payload, _ = self._get(host, repository, f"/v2/{repository}/manifests/{entry['digest']}",
                                           self.ACCEPT, digest=entry["digest"])
                    return payload
            raise Unsupported(f"{host}/{repository}:{ref} has no linux/amd64 image")
        if ref.startswith("sha256:") and "sha256:" + hashlib.sha256(payload).hexdigest() != ref:
            raise ValueError("external manifest identity mismatch")
        return payload

    def _layer(self, host, repository, descriptor):
        media = descriptor.get("mediaType", "")
        if "zstd" in media:
            raise Unsupported("zstd layers in COPY --from images")
        data, _ = self._get(host, repository, f"/v2/{repository}/blobs/{descriptor['digest']}", "*/*",
                            digest=descriptor["digest"], limit=MAX_EXTERNAL_BYTES)
        raw = gzip.decompress(data) if data[:2] == b"\x1f\x8b" else data
        with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as archive:
            for member in archive:
                yield member, archive.extractfile(member).read() if member.isfile() else None

    def _get(self, host, repository, path, accept, *, digest=None, limit=16 * 1024 ** 2):
        url = f"https://{host}{path}"
        for attempt in range(2):
            headers = {"Accept": accept, "User-Agent": "ucloud-sandboxes"}
            if repository in self._tokens:
                headers["Authorization"] = "Bearer " + self._tokens[repository]
            try:
                response = self.opener.open(request.Request(url, headers=headers), timeout=self.timeout)
            except error.HTTPError as exc:
                if exc.code == 401 and attempt == 0:
                    self._tokens[repository] = self._token(exc.headers.get("WWW-Authenticate", ""))
                    continue
                if exc.code in (301, 302, 303, 307, 308):  # Blob storage: no registry token.
                    response = request.urlopen(request.Request(exc.headers["Location"],
                                                               headers={"User-Agent": "ucloud-sandboxes"}),
                                               timeout=self.timeout)
                else:
                    raise
            with response:
                payload = response.read(limit + 1)
                media = response.headers.get("Content-Type", "")
            if len(payload) > limit:
                raise Unsupported("an external image object exceeds its bound")
            if digest and "sha256:" + hashlib.sha256(payload).hexdigest() != digest:
                raise ValueError("external object identity mismatch")
            return payload, media
        raise ValueError("the registry refused anonymous access")

    def _token(self, challenge):
        fields = dict(re.findall(r'(\w+)="([^"]*)"', challenge))
        if not challenge.lower().startswith("bearer") or not fields.get("realm", "").startswith("https://"):
            raise ValueError("the registry needs credentials")
        query = parse.urlencode({key: fields[key] for key in ("service", "scope") if key in fields})
        with request.urlopen(fields["realm"] + ("?" + query if query else ""), timeout=self.timeout) as response:
            body = json.loads(response.read(1024 * 1024))
        return body.get("token") or body.get("access_token")


# --- The bundle the sandbox receives ---

def context_tar(archive_bytes, changes):
    """The build context (a tar.gz) as a plain tar with owners 0:0 and
    ``changes`` (path -> bytes) applied: what COPY reads."""
    output = io.BytesIO()
    with gzip.GzipFile(fileobj=io.BytesIO(archive_bytes)) as zipped, \
            tarfile.open(fileobj=zipped, mode="r|") as source, \
            tarfile.open(fileobj=output, mode="w", format=tarfile.PAX_FORMAT) as target:
        for member in source:
            if not (member.isfile() or member.isdir() or member.issym()):
                continue
            name = clean_name(member.name)
            if not name:
                continue
            data = source.extractfile(member).read() if member.isfile() else None
            if name in changes:
                data = changes[name]
            info = tarfile.TarInfo(name)
            info.type, info.mode, info.mtime, info.linkname = member.type, member.mode, member.mtime, member.linkname
            info.uid = info.gid = 0
            info.size = len(data) if data is not None else 0
            target.addfile(info, io.BytesIO(data) if data is not None else None)
    return output.getvalue()


def bundle(plan, context, externals, apt):
    """The tar.gz the archive upload writes below BUILD_DIR."""
    output = io.BytesIO()
    with gzip.GzipFile(fileobj=output, mode="wb", mtime=0) as zipped, \
            tarfile.open(fileobj=zipped, mode="w", format=tarfile.PAX_FORMAT) as archive:
        files = {"build.sh": plan.script.encode(), "context.tar": context,
                 **{f"from-{index}.tar": payload for index, payload in enumerate(externals)}}
        if apt:
            files["apt.conf"] = apt.encode()
        for name, payload in files.items():
            info = tarfile.TarInfo(name)
            info.size, info.mode = len(payload), 0o755 if name == "build.sh" else 0o644
            archive.addfile(info, io.BytesIO(payload))
    return output.getvalue()


# --- The gateway, as the build service sees it ---

class GatewayError(RuntimeError):
    def __init__(self, status, body):
        super().__init__(f"gateway answered {status}: {str(body)[:300]}")
        self.status, self.body = status, body


class GatewayApi:
    """The local gateway's HTTP API with its admin token (commit-export is admin-only)."""

    def __init__(self, url, token, *, timeout=120):
        self.url, self.token, self.timeout = url.rstrip("/"), token, timeout

    def call(self, method, path, payload=None, *, body=None, content_type="application/json", timeout=None,
             raw=False):
        data = body if body is not None else (None if payload is None else json.dumps(payload).encode())
        req = request.Request(self.url + path, data=data, method=method,
                              headers={"Authorization": f"Bearer {self.token}", "Content-Type": content_type})
        try:
            with request.urlopen(req, timeout=timeout or self.timeout) as response:
                content = response.read()
                return response.status, (content if raw else json.loads(content or b"{}"))
        except error.HTTPError as exc:
            content = exc.read()
            try:
                parsed = json.loads(content or b"{}")
            except ValueError:
                parsed = {"error": content[:300].decode(errors="replace")}
            return exc.code, parsed

    def create(self, spec, *, deadline):
        """Retries capacity and worker-boot answers (503) until ``deadline``."""
        while True:
            status, body = self.call("POST", "/v1/sandboxes", spec, timeout=600)
            if 200 <= status < 300:
                return body.get("sandbox") or body
            if status != 503 or time.monotonic() > deadline:
                raise GatewayError(status, body)
            time.sleep(5)

    def sandbox_path(self, sandbox_id, suffix=""):
        return f"/v1/sandboxes/{parse.quote(sandbox_id, safe='')}{suffix}"

    def checked(self, method, path, payload=None, **options):
        status, body = self.call(method, path, payload, **options)
        if not 200 <= status < 300:
            raise GatewayError(status, body)
        return body


# --- One build ---

@dataclass(frozen=True)
class BuildResources:
    cpus: float = 4.0
    memory_mb: int = 8192
    disk_mb: int = 32768
    timeout_seconds: int = 1800


class BuildRunner:
    """Runs one spooled job: sandbox, export, filter, stack, with the service's credentials."""

    def __init__(self, *, gateway, registry_client, converter, externals, work_root, resources=BuildResources(),
                 apt_proxy=None, poll=1.0, local_slots=2):
        # ``converter()``: a RafsConverter for one job (they keep per-run metrics).
        self.gateway, self.registry_client, self.converter = gateway, registry_client, converter
        self.externals, self.work_root, self.resources = externals, Path(work_root), resources
        self.apt_proxy, self.poll = apt_proxy, poll
        # Steps run on the workers; filtering and stacking run here, beside the gateway's API.
        self.local = threading.BoundedSemaphore(local_slots)

    def run(self, job):
        """The result for ``job``: ``succeeded`` with the root, or ``failed`` with an error."""
        metrics, started = {}, time.monotonic()
        try:
            result = self._run(job, metrics)
            return {**result, "status": "succeeded", "metrics": metrics}
        except (BuildFailed, Unsupported) as exc:
            return {"status": "failed", "error": str(exc)[:2000], "metrics": metrics}
        except Exception as exc:  # noqa: BLE001 - infrastructure: the recipe's attempt fails, ensure retries.
            _LOG.exception("sandbox build %s failed", job.get("build_id"))
            return {"status": "failed", "error": f"sandbox build infrastructure: {type(exc).__name__}: {exc}"[:2000],
                    "metrics": metrics}
        finally:
            metrics["total_s"] = round(time.monotonic() - started, 3)

    def _run(self, job, metrics):
        from .commit_policy import CommitPolicy
        from .commit_steps import commit_build
        from .environment_prepare import FILTERED_NAME, prepare_commit_in_subprocess
        context = base64.b64decode(job["context_tar"])
        plan = plan_build(job["dockerfile"], ContextTree.of(tarfile.open(fileobj=io.BytesIO(context)).getmembers()),
                          job["base_config"], apt_proxy=self.apt_proxy)
        timer = time.monotonic()
        fetched = [self.externals.files(item.reference, item.sources) for item in plan.externals]
        metrics["externals_s"] = round(time.monotonic() - timer, 3)
        payload = bundle(plan, context, fetched, apt_config(*self.apt_proxy) if self.apt_proxy else "")
        sandbox_id = f"sbuild-{job['image_id'][:40]}-{uuid.uuid4().hex[:8]}"[:63]
        spec = {"id": sandbox_id, "image": job["base_reference"], "cpus": self.resources.cpus,
                "memory_mb": self.resources.memory_mb, "disk_mb": self.resources.disk_mb,
                "ttl_seconds": self.resources.timeout_seconds + 900, "managed_process": True, "parkable": True,
                "security": {"user": "0:0", "cap_drop": [], "no_new_privileges": False, "pids_limit": None},
                "labels": {"sandbox-build": job["image_id"][:63]}}
        deadline = time.monotonic() + self.resources.timeout_seconds
        timer = time.monotonic()
        record = self.gateway.create(spec, deadline=deadline)
        metrics["create_s"] = round(time.monotonic() - timer, 3)
        try:
            generation = int(record.get("generation") or 1)
            self.gateway.checked("PUT", self.gateway.sandbox_path(sandbox_id, "/archive?" + parse.urlencode(
                {"path": BUILD_DIR})), body=payload, content_type="application/gzip", timeout=600)
            timer = time.monotonic()
            self._script(sandbox_id, deadline)
            metrics["steps_s"] = round(time.monotonic() - timer, 3)
            timer = time.monotonic()
            export = self._export(sandbox_id, generation, job["image_id"], deadline)
            metrics["export_s"], metrics["exported_bytes"] = round(time.monotonic() - timer, 3), export.size
        finally:
            self.gateway.call("DELETE", self.gateway.sandbox_path(sandbox_id))
        # Docker's image keeps what the steps cached (pip, uv, npm, apt archives).
        policy = CommitPolicy.of(exclude=(BUILD_DIR, BUILD_TMP), identity=export.identity, keep_build_residue=True)
        commit = commit_build(export, policy, parent_image=job["base_reference"], parent_root=job["parent_root"])
        self.work_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        timer = time.monotonic()
        with self.local, TemporaryDirectory(dir=self.work_root) as scratch:
            metrics["local_wait_s"] = round(time.monotonic() - timer, 3)
            timer = time.monotonic()
            filtered = prepare_commit_in_subprocess(self.registry_client, commit, scratch,
                                                    timeout_seconds=max(60, deadline - time.monotonic()))
            metrics["filter_s"], metrics["layer_bytes"] = round(time.monotonic() - timer, 3), filtered.size
            timer = time.monotonic()
            stacked = self.converter().extend(job["parent_root"], Path(scratch) / FILTERED_NAME, filtered.diff_id,
                                            image_config=plan.image_config, repository=job["repository"])
            metrics["stack_s"] = round(time.monotonic() - timer, 3)
        metrics.update({"rafs_" + key: value for key, value in stacked["metrics"].items()})
        return {"root": stacked["root"], "config": stacked["config"], "diff_id": filtered.diff_id,
                "manifest_digest": born_manifest_digest(stacked["config"], stacked["config_size"], stacked["root"])}

    def _script(self, sandbox_id, deadline):
        job_id = "build-" + uuid.uuid4().hex[:12]
        self.gateway.checked("POST", self.gateway.sandbox_path(sandbox_id, "/jobs"), {
            "job_id": job_id, "argv": ["/bin/sh", f"{BUILD_DIR}/build.sh"], "env": {"PATH": DEFAULT_PATH}, "cwd": "/",
            "max_stdout_bytes": 1024 * 1024, "max_stderr_bytes": 4 * 1024 * 1024})
        path = self.gateway.sandbox_path(sandbox_id, f"/jobs/{job_id}")
        while True:
            job = self.gateway.checked("GET", path).get("job") or {}
            if job.get("state") in ("exited", "signaled", "failed"):
                break
            if time.monotonic() > deadline:
                self.gateway.call("POST", path + "/signal", {"signal": 9})
                raise BuildFailed("the build steps exceeded the build timeout")
            time.sleep(self.poll)
        if job.get("state") == "exited" and job.get("exit_code") == 0:
            return
        status, failed = self.gateway.call("GET", self.gateway.sandbox_path(
            sandbox_id, "/files?" + parse.urlencode({"path": f"{BUILD_DIR}/failed"})), raw=True)
        step = failed.decode(errors="replace").split() if status == 200 and isinstance(failed, bytes) else []
        offset = max(0, int(job.get("stderr_bytes") or 0) - 3000)
        _, logs = self.gateway.call("GET", path + "/logs/stderr?" + parse.urlencode({"offset": offset,
                                                                                    "limit": 3000}))
        tail = base64.b64decode(logs.get("data") or "").decode(errors="replace") if isinstance(logs, dict) else ""
        where = f"step {step[0]} exited {step[1]}" if len(step) >= 2 else f"the build script ended {job.get('state')}"
        raise BuildFailed(f"{where}: {tail.strip()[-1500:]}")

    def _export(self, sandbox_id, generation, image_id, deadline):
        from .commit_policy import CommitExport, CommitExportRequest
        request_ = CommitExportRequest(f"sbuild-{uuid.uuid4().hex[:16]}", generation, image_id[:64], resume=False)
        path = self.gateway.sandbox_path(sandbox_id, "/commit-export")
        while True:
            status, body = self.gateway.call("POST", path, request_.to_dict(), timeout=600)
            if status not in (200, 202):
                raise GatewayError(status, body)
            if body.get("export") is not None:
                export = CommitExport.from_dict(body["export"])
                if export.state == "failed":
                    raise RuntimeError(f"the worker could not export the sandbox: {export.error_code}")
                if export.state == "staged":
                    return export
            if time.monotonic() > deadline:
                raise RuntimeError("the commit export exceeded the build timeout")
            time.sleep(self.poll)


def born_manifest_digest(config_digest, config_size, root):
    """The identity of an image born in the chunk store: the manifest it would
    have, annotated with its root and without layers, never pushed. Its
    ``image_roots`` row is ``released`` from the start."""
    from .environment_artifact import ENVIRONMENT_ANNOTATION, OCI_IMAGE, canonical_bytes, content_digest
    return content_digest(canonical_bytes({
        "schemaVersion": 2, "mediaType": OCI_IMAGE,
        "config": {"mediaType": "application/vnd.oci.image.config.v1+json", "digest": config_digest,
                   "size": config_size},
        "layers": [], "annotations": {ENVIRONMENT_ANNOTATION: root}}))


# --- The spool: jobs from the gateway, results back ---

class Spool:
    """``jobs/<image_id>.json`` (the gateway writes), ``results/<image_id>.json``
    (the service writes), ``running/<image_id>`` (held with flock while a
    build runs). Writes are atomic renames; one image has one job at a time."""

    def __init__(self, root):
        self.root = Path(root)
        for name in ("jobs", "results", "running"):
            (self.root / name).mkdir(mode=0o700, parents=True, exist_ok=True)

    def _write(self, path, value):
        partial = path.with_name(f".{path.name}.{uuid.uuid4().hex}")
        partial.write_text(json.dumps(value, sort_keys=True))
        os.replace(partial, path)

    def submit(self, job):
        (self.root / "results" / f"{job['image_id']}.json").unlink(missing_ok=True)
        self._write(self.root / "jobs" / f"{job['image_id']}.json", {"schema": SCHEMA, **job})

    def status(self, image_id, build_id):
        """("succeeded"|"failed", result), ("running", None) or (None, None): lost."""
        result = self._read(self.root / "results" / f"{image_id}.json")
        if result is not None and result.get("build_id") == build_id:
            return result["status"], result
        job = self._read(self.root / "jobs" / f"{image_id}.json")
        if job is not None and job.get("build_id") == build_id:
            return "running", None
        return None, None

    def finish(self, job, result):
        self._write(self.root / "results" / f"{job['image_id']}.json",
                    {"schema": SCHEMA, **result, "build_id": job["build_id"], "image_id": job["image_id"],
                     "repository": job["repository"], "tag": job["tag"], "finished": time.time()})
        current = self._read(self.root / "jobs" / f"{job['image_id']}.json")
        if current is not None and current.get("build_id") == job["build_id"]:
            (self.root / "jobs" / f"{job['image_id']}.json").unlink(missing_ok=True)

    def pending(self):
        return sorted(self.root.glob("jobs/*.json"), key=lambda path: path.stat().st_mtime)

    @staticmethod
    def _read(path):
        try:
            value = json.loads(path.read_text())
        except (FileNotFoundError, ValueError):
            return None
        return value if isinstance(value, dict) and value.get("schema") == SCHEMA else None


def serve(spool, runner, *, slots=8, poll=2.0, stop=None):
    """Run spooled jobs, at most ``slots`` at once, until ``stop`` is set."""
    import fcntl
    stop, active, guard = stop or threading.Event(), {}, threading.Lock()

    def work(path, job, lock):
        try:
            spool.finish(job, runner.run(job))
        finally:
            os.close(lock)
            with guard:
                active.pop(job["image_id"], None)

    while not stop.is_set():
        for path in spool.pending():
            with guard:
                if len(active) >= slots:
                    break
            job = Spool._read(path)
            if job is None or job["image_id"] in active:
                continue
            lock = os.open(spool.root / "running" / job["image_id"], os.O_CREAT | os.O_RDWR, 0o600)
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                os.close(lock)
                continue
            with guard:
                active[job["image_id"]] = threading.Thread(target=work, args=(path, job, lock), daemon=True,
                                                           name=f"sandbox-build-{job['image_id'][:24]}")
                active[job["image_id"]].start()
        stop.wait(poll)


# --- serve-sandbox-builds: the gateway-host service ---

def serve_command(args):
    """Exit 78 (stay stopped) unless ``immutable_environments.sandbox_builds`` is on."""
    from .chunk_convert import RafsConverter
    from .chunk_index import ChunkIndexClient
    from .config import DeploymentConfig
    from .environment_config import environment_registry_from_deployment, load_signing_key, read_token
    from .gateway.sandbox_builds import apt_proxy, spool_root
    from .managed_registry import RegistryClient
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    config = DeploymentConfig.from_file(args.config)
    selected = config.immutable_environments
    settings = selected.sandbox_builds if selected is not None else None
    if settings is None or not settings.enabled:
        _LOG.info("immutable_environments.sandbox_builds is off")
        return 78
    chunk_store = selected.chunk_store
    with open(chunk_store.nydus_image, "rb") as binary:
        if hashlib.sha256(binary.read()).hexdigest() != chunk_store.nydus_image_sha256:
            raise ValueError(f"{chunk_store.nydus_image} is not the nydus-image chunk_store.nydus_image_sha256 pins")
    store = chunk_store.object_store()  # The S3 key: this service's environment only.
    index = ChunkIndexClient(chunk_store.index_url, read_token(chunk_store.write_token_file).decode())
    registry, key = environment_registry_from_deployment(config), load_signing_key(selected.signing_key_file)
    root = spool_root(config)
    work = root / "work"

    def converter():
        # Builds run side by side in this process: each converter reserves chunks under its own owner.
        return RafsConverter(registry, store, index, key, work / "rafs", nydus_image=chunk_store.nydus_image,
                             layout=chunk_store.mount_granularity, nydusd_blobs=chunk_store.nydusd is not None,
                             owner=f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}")

    gateway = GatewayApi(f"http://127.0.0.1:{config.gateway_port}",
                         read_token(config.gateway_token_file()).decode().strip())
    runner = BuildRunner(gateway=gateway, registry_client=RegistryClient(config.registry_url), converter=converter,
                         externals=ExternalImages(root / "external-images"), work_root=work,
                         resources=BuildResources(settings.cpus, settings.memory_mb, settings.disk_mb,
                                                  settings.timeout_seconds),
                         apt_proxy=apt_proxy(config))
    _LOG.info("sandbox builds: %d slots, spool %s", settings.slots, root)
    serve(Spool(root), runner, slots=settings.slots)
    return 0


def add_commands(subparsers):
    serve_parser = subparsers.add_parser("serve-sandbox-builds",
                                         help="Run sandbox builds (immutable_environments.sandbox_builds).")
    serve_parser.add_argument("--config", type=Path, required=True, help="the gateway's deployment.json")
    serve_parser.set_defaults(func=serve_command)
