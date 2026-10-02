"""GATEWAY WIRING POINT for C3.1 commit (docs/rl-state-primitives.md §3.2, §3.5).

``gateway/commit.py`` is not written yet: C6.1 is moving ``control_plane.py``.
Its route, the ``image_commits`` row, ``CommitReconciler`` and the builder
dispatch call these steps, one compare-and-set row transition each:

- insert (``exporting``): ``commit_policy(spec, ...)``; the row binds its sha256.
- ``exporting`` -> ``staged``: ``export_step``, replayed until it returns.
- ``staged`` -> ``converting``: ``commit_build``, then the builder runs
  ``FreshEnvironmentBuilder.publish_commit(commit, image_ref=...)``.
"""
from __future__ import annotations

import hashlib
from urllib.parse import quote

from .commit_policy import (
    COMMIT_MAX_BYTES, CommitBuild, CommitExport, CommitExportRequest, CommitPolicy, CommitRefused,
    require_secret_digests,
)
from .direct_oci import DirectOciConfigBuilder


def commit_policy(spec, *, include_paths=(), exclude=(), max_bytes=COMMIT_MAX_BYTES) -> CommitPolicy:
    """The caller's rules plus the paths the platform writes for ``spec``."""
    return CommitPolicy.of(include_paths=include_paths, exclude=exclude, max_bytes=max_bytes,
                           identity=DirectOciConfigBuilder.platform_written_paths(spec))


def secret_digests(tokens) -> tuple[str, ...]:
    """SHA-256 of each live credential, e.g. the sandbox's relay registration tokens."""
    return require_secret_digests(sorted({hashlib.sha256(token.encode("ascii")).hexdigest() for token in tokens}))


def export_step(call, sandbox_id: str, request: CommitExportRequest, policy: CommitPolicy) -> CommitExport | None:
    """One replay of the worker export: the staged record, or None while it runs.

    ``call(method, path, payload) -> (status, body)`` is the node RPC. Worker
    refusals and a failed export raise CommitRefused with the worker's code.
    """
    status, body = call("POST", f"/v1/sandboxes/{quote(sandbox_id, safe='')}/commit-export", request.to_dict())
    if status not in (200, 202):
        code = body.get("error_code") if isinstance(body.get("error_code"), str) else "commit_export_failed"
        raise CommitRefused(code, str(body.get("error") or "worker refused the export"), status=status,
                            retryable=body.get("retryable") is True)
    if body.get("export") is None:
        return None
    export = CommitExport.from_dict(body["export"])
    if (export.sandbox_id, export.request, export.identity) != (sandbox_id, request, policy.identity):
        raise ValueError("the worker's export does not match this commit")
    if export.state == "failed":
        raise CommitRefused(export.error_code, "the worker could not export the sandbox", status=500)
    return export if export.state == "staged" else None


def commit_build(export: CommitExport, policy: CommitPolicy, *, parent_image: str, parent_root: str,
                 secrets=()) -> CommitBuild:
    """The builder's ``commit`` object, every field bound to the staged row."""
    request = export.request
    if export.state != "staged" or export.identity != policy.identity or request.max_bytes != policy.max_bytes:
        raise ValueError("commit build requires the staged export of this policy")
    return CommitBuild(export.sandbox_id, request.generation, request.operation_id, request.image_id, parent_image,
                       parent_root, export.blob_digest, export.size, policy, tuple(secrets))
