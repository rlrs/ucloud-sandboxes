"""Bounded exec-isolated preparation of disposable, authenticated OCI diffs.

No signing key is sent through the protocol, and the child never publishes
registry data. Its output is fixed views under a private parent-owned root.
This is performance isolation, not a security sandbox: the fresh interpreter
runs with the parent's UID and environment and can access the same filesystem.
"""
from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tarfile
import time
from urllib.parse import urlsplit

from .commit_policy import (
    REFUSALS, CommitPolicy, CommitRefused, FilterResult, filter_upper, require_secret_digests,
)
from .environment_artifact import require_digest
from .managed_registry import RegistryClient
from .oci_layer_materialize import (
    FALLBACK_REASONS, MAX_COMPRESSED_BYTES, UnsupportedLayer, materialize_layers,
)

MAX_REQUEST_BYTES = 1024 * 1024
MAX_RESULT_BYTES = 4096
MAX_LAYERS = 1024
MAX_GROUPS = 24
RESULT_NAME = "preparation-result.json"
COMMIT_REQUEST_SCHEMA = 2
FILTERED_NAME = "filtered.tar"
_TIMINGS = ("selective_materialization_ms", "squash_ms")
_MATERIALIZATION_TIMINGS = ("oci_transfer_ms", "oci_decompress_ms", "oci_extract_ms")
_METRIC_LIMITS = {**dict.fromkeys((*_TIMINGS, *_MATERIALIZATION_TIMINGS), 3_600_000),
                  "oci_download_bytes_actual": MAX_COMPRESSED_BYTES + 1}


class PreparationError(RuntimeError):
    """Preparation failed without permitting publication or Docker fallback."""


@dataclass(frozen=True)
class PreparationResult:
    views: tuple[Path, ...]
    metrics: dict[str, float]
    fallback: bool = False
    fallback_reason: str = ""


def _elapsed(started):
    return round((time.monotonic() - started) * 1000, 3)


def _private_root(value, *, empty):
    if not isinstance(value, str) or len(value) > 4096 or "\0" in value:
        raise ValueError("invalid preparation root")
    root = Path(value)
    if not root.is_absolute() or root.resolve(strict=True) != root:
        raise ValueError("preparation root must be canonical and absolute")
    info = root.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
        raise ValueError("preparation root must be a private owned directory")
    if empty and any(root.iterdir()):
        raise ValueError("preparation root must be empty")
    return root


_REGISTRY_FIELDS = {"schema", "root", "registry_url", "registry_timeout_seconds", "repository"}


def _validate_request(value):
    if not isinstance(value, dict) or set(value) != _REGISTRY_FIELDS | {
            "layers", "diff_ids", "group_counts"} or type(value["schema"]) is not int or value["schema"] != 1:
        raise ValueError("invalid preparation request schema")
    _validate_registry(value)
    layers, diff_ids, counts = value["layers"], value["diff_ids"], value["group_counts"]
    if (not isinstance(layers, list) or not isinstance(diff_ids, list)
            or not isinstance(counts, list) or len(layers) != len(diff_ids)):
        raise ValueError("invalid preparation layer binding")
    if not 1 <= len(layers) <= MAX_LAYERS or not 1 <= len(counts) <= MAX_GROUPS:
        raise UnsupportedLayer("selective preparation collection exceeds its bound", reason="collection_budget")
    if any(type(count) is not int or count <= 0 for count in counts) or sum(counts) != len(layers):
        raise ValueError("invalid preparation group binding")
    for layer, diff_id in zip(layers, diff_ids):
        if (not isinstance(layer, dict) or set(layer) != {"digest", "size", "mediaType"}
                or type(layer["size"]) is not int or layer["size"] < 0
                or not isinstance(layer["mediaType"], str) or len(layer["mediaType"]) > 256):
            raise ValueError("invalid preparation descriptor")
        require_digest(layer["digest"])
        require_digest(diff_id)
    if sum(layer["size"] for layer in layers) > MAX_COMPRESSED_BYTES:
        raise UnsupportedLayer("selective preparation byte budget exceeded", reason="compressed_budget")
    return _private_root(value["root"], empty=True)


def _validate_commit_request(value):
    """``commit-upper`` (C3.1): fetch one staged upper, verify it, and filter it."""
    if (not isinstance(value, dict) or set(value) != _REGISTRY_FIELDS | {"blob", "policy", "secret_digests"}
            or value["schema"] != COMMIT_REQUEST_SCHEMA or not isinstance(value["blob"], dict)
            or set(value["blob"]) != {"digest", "size"} or type(value["blob"]["size"]) is not int
            or not 0 < value["blob"]["size"] <= 1 << 40 or not isinstance(value["secret_digests"], list)):
        raise ValueError("invalid commit preparation request")
    _validate_registry(value)
    require_digest(value["blob"]["digest"])
    CommitPolicy.from_dict(value["policy"])
    require_secret_digests(value["secret_digests"])
    return _private_root(value["root"], empty=True)


def _validate_registry(value):
    url = value["registry_url"]
    if not isinstance(url, str) or len(url) > 2048 or any(c in url for c in "\0\r\n"):
        raise ValueError("invalid preparation registry")
    parsed = urlsplit(url)
    if (parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username
            or parsed.password or parsed.query or parsed.fragment):
        raise ValueError("invalid preparation registry")
    timeout = value["registry_timeout_seconds"]
    if type(timeout) not in {int, float} or not math.isfinite(timeout) or not 0 < timeout <= 300:
        raise ValueError("invalid preparation registry timeout")
    repository = value["repository"]
    if (not isinstance(repository, str) or len(repository) > 1024
            or re.fullmatch(r"[a-z0-9]+(?:[._/-][a-z0-9]+)*", repository) is None):
        raise ValueError("invalid preparation repository")


def _prepare(value, root):
    # Import the merger only in this fresh interpreter, after request validation.
    from .environment_builder import squash_layer_diffs

    metrics = dict.fromkeys(_METRIC_LIMITS, 0.0)
    started = time.monotonic()
    try:
        directories = materialize_layers(
            RegistryClient(value["registry_url"], timeout_seconds=value["registry_timeout_seconds"]),
            value["repository"], value["layers"], value["diff_ids"], root / "diffs", metrics=metrics)
    except (UnsupportedLayer, OSError, EOFError, tarfile.TarError) as error:
        # Match the existing extraction-only fallback boundary. Digest errors,
        # registry HTTP errors and later squash failures must fail closed.
        reason = (error.reason if isinstance(error, UnsupportedLayer) else
                  "io" if isinstance(error, OSError) else "archive")
        return {"status": "fallback", "groups": 0, "metrics": metrics,
                "fallback_reason": reason}
    finally:
        metrics["selective_materialization_ms"] = _elapsed(started)
    offset = 0
    for index, count in enumerate(value["group_counts"]):
        view = root / f"view-{index}"
        if count == 1:
            directories[offset].rename(view)
        else:
            started = time.monotonic()
            squash_layer_diffs(directories[offset:offset + count], view, consume_private_diffs=True)
            metrics["squash_ms"] += _elapsed(started)
        offset += count
    return {"status": "ok", "groups": len(value["group_counts"]), "metrics": metrics}


def _result_value(root):
    descriptor = os.open(root / RESULT_NAME, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise ValueError("invalid preparation result file")
        raw = stream.read(MAX_RESULT_BYTES + 1)
    if len(raw) > MAX_RESULT_BYTES:
        raise ValueError("preparation result exceeds its bound")
    return json.loads(raw)


def _read_result(root, groups, elapsed_ms):
    value = _result_value(root)
    if not isinstance(value, dict) or set(value) not in (
            {"status", "groups", "metrics"}, {"status", "groups", "metrics", "fallback_reason"}):
        raise ValueError("invalid preparation result schema")
    reason = value.get("fallback_reason", "unsupported" if value["status"] == "fallback" else "")
    if (not isinstance(reason, str) or
            (reason not in FALLBACK_REASONS if value["status"] == "fallback" else reason != "")):
        raise ValueError("invalid preparation fallback reason")
    metrics = value["metrics"]
    if (not isinstance(metrics, dict) or not set(_TIMINGS) <= set(metrics) <= set(_METRIC_LIMITS)
            or any(type(number) not in {int, float} or not math.isfinite(number)
                   or not 0 <= number <= _METRIC_LIMITS[key] for key, number in metrics.items())):
        raise ValueError("invalid preparation metrics")
    if type(value["groups"]) is not int:
        raise ValueError("invalid preparation result count")
    metrics["selective_subprocess_ms"] = elapsed_ms
    if value["status"] == "fallback" and value["groups"] == 0:
        return PreparationResult((), metrics, fallback=True, fallback_reason=reason)
    if value["status"] != "ok" or value["groups"] != groups:
        raise ValueError("preparation child failed")
    _private_root(str(root), empty=False)
    # Never accept paths supplied by the child. Group names and count derive
    # solely from the parent's already-bound layer groups.
    views = tuple(root / f"view-{index}" for index in range(groups))
    if any(not stat.S_ISDIR(path.lstat().st_mode) or path.resolve(strict=True) != path for path in views):
        raise ValueError("invalid prepared view")
    return PreparationResult(views, metrics)


def prepare_in_subprocess(client, repository, layers, diff_ids, group_counts, root, *, timeout_seconds=600):
    """Prepare private views with a fresh interpreter and bounded metadata IPC."""
    if type(timeout_seconds) not in {int, float} or not math.isfinite(timeout_seconds) or not 0 < timeout_seconds <= 3600:
        raise ValueError("invalid preparation process timeout")
    root = _private_scratch(root)
    value = {"schema": 1, "root": str(root), "registry_url": client.base_url,
             "registry_timeout_seconds": client.timeout_seconds, "repository": repository,
             "layers": [{key: layer.get(key) for key in ("digest", "size", "mediaType")} for layer in layers],
             "diff_ids": list(diff_ids), "group_counts": list(group_counts)}
    root = _validate_request(value)
    data = json.dumps(value, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()
    if len(data) > MAX_REQUEST_BYTES:
        raise UnsupportedLayer("selective preparation request exceeds its bound", reason="request_budget")
    started = time.monotonic()
    try:
        _spawn(data, timeout_seconds)
        return _read_result(root, len(value["group_counts"]), _elapsed(started))
    except subprocess.TimeoutExpired:
        raise PreparationError("selective preparation child timed out") from None
    except (OSError, ValueError):
        raise PreparationError("invalid selective preparation result") from None


def _spawn(data, timeout_seconds):
    # stdout/stderr cannot leak payloads or accumulate unbounded data. The
    # bounded result file is the only response channel; stdin carries no key.
    with subprocess.Popen((sys.executable, "-m", __name__), stdin=subprocess.PIPE,
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, close_fds=True,
                          cwd=Path(__file__).resolve().parent.parent) as process:
        try:
            process.communicate(data, timeout=timeout_seconds)
        except BaseException:
            if process.poll() is None:
                process.kill()
            process.wait()
            raise
        if process.returncode != 0:
            raise PreparationError("selective preparation child failed")


def _private_scratch(root):
    root = Path(root)
    if root.is_symlink():
        raise ValueError("preparation root cannot be a symlink")
    # A configured work-root ancestor may legitimately be a storage symlink;
    # canonicalize it while retaining the real, private scratch-root check.
    return root.resolve(strict=True)


def commit_upper(client, repository, blob, policy, secret_digests, root):
    """Fetch and verify the staged upper, then filter it into ``root/filtered.tar``."""
    upper, digest, size = root / "upper.tar", hashlib.sha256(), 0
    response = client.open_blob(repository, blob["digest"])
    try:
        with upper.open("xb") as stream:
            while chunk := response.read(1 << 20):
                size += len(chunk)
                if size > blob["size"]:
                    raise ValueError("staged commit upper exceeds its recorded size")
                digest.update(chunk)
                stream.write(chunk)
    finally:
        response.close()
    try:
        if size != blob["size"] or "sha256:" + digest.hexdigest() != blob["digest"]:
            raise ValueError("staged commit upper does not match its digest")
        with upper.open("rb") as source, (root / FILTERED_NAME).open("xb") as destination:
            result = filter_upper(source, destination, policy, secret_digests)
    except CommitRefused as refused:
        (root / FILTERED_NAME).unlink(missing_ok=True)
        return {"status": "refused", "error_code": refused.code}
    finally:
        upper.unlink(missing_ok=True)
    return {"status": "ok", "commit": result.to_dict()}


def commit_result(value, root) -> FilterResult:
    """A refusal raises CommitRefused; ``root/filtered.tar`` holds the accepted layer."""
    if (isinstance(value, dict) and set(value) == {"status", "error_code"} and value["status"] == "refused"
            and value["error_code"] in REFUSALS):
        raise CommitRefused(value["error_code"], "the commit policy refused the exported upper")
    if not isinstance(value, dict) or set(value) != {"status", "commit"} or value["status"] != "ok":
        raise ValueError("commit preparation child failed")
    result = FilterResult.from_dict(value["commit"])
    info = (root / FILTERED_NAME).lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_size != result.size:
        raise ValueError("invalid filtered commit layer")
    return result


def prepare_commit_in_subprocess(client, commit, root, *, timeout_seconds=600) -> FilterResult:
    """Fetch, verify and filter one commit upper in a fresh keyless interpreter."""
    value = {"schema": COMMIT_REQUEST_SCHEMA, "root": str(_private_scratch(root)), "registry_url": client.base_url,
             "registry_timeout_seconds": client.timeout_seconds, "repository": commit.repository,
             "blob": {"digest": commit.blob_digest, "size": commit.blob_size},
             "policy": commit.policy.to_dict(), "secret_digests": list(commit.secret_digests)}
    root = _validate_commit_request(value)
    try:
        _spawn(json.dumps(value, separators=(",", ":"), allow_nan=False).encode(), timeout_seconds)
        return commit_result(_result_value(root), root)
    except CommitRefused:
        raise
    except subprocess.TimeoutExpired:
        raise PreparationError("commit preparation child timed out") from None
    except (OSError, ValueError):
        raise PreparationError("invalid commit preparation result") from None


def main():
    root = None
    try:
        raw = sys.stdin.buffer.read(MAX_REQUEST_BYTES + 1)
        if len(raw) > MAX_REQUEST_BYTES:
            return 2
        value = json.loads(raw)
        if isinstance(value, dict) and value.get("schema") == COMMIT_REQUEST_SCHEMA:
            root = _validate_commit_request(value)
            result = commit_upper(
                RegistryClient(value["registry_url"], timeout_seconds=value["registry_timeout_seconds"]),
                value["repository"], value["blob"], CommitPolicy.from_dict(value["policy"]),
                value["secret_digests"], root)
        else:
            root = _validate_request(value)
            result = _prepare(value, root)
    except Exception:
        # Avoid serializing exception text: registry failures may contain URLs
        # or response payloads. Unexpected failures are terminal for the parent.
        result = {"status": "error", "groups": 0, "metrics": dict.fromkeys(_TIMINGS, 0.0)}
    if root is None:
        return 2
    try:
        descriptor = os.open(root / RESULT_NAME, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "w") as stream:
            json.dump(result, stream, separators=(",", ":"), allow_nan=False)
    except Exception:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
