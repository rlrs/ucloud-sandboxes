"""Copy a verified public OCI image once into the existing managed registry.

No daemon restart or parallel blob store. Raw config/manifest bytes and layer
hashes are preserved; BuildKit subsequently reads the local immutable digest.
"""
from __future__ import annotations

import fcntl
import hashlib
import io
import json
import sqlite3
import time
from contextlib import closing
from urllib import parse, request

from ucloud_sandboxes.managed_registry import RegistryRequestError

REPOSITORY = "ucloud-upstream"


class BlobSourceIndex:
    """Hints for server-side mounts from already retained image repositories.

    Hints never establish blob availability: the registry must confirm a mount.
    A stale hint falls back to the upstream copy without claiming cache reuse.
    """
    def __init__(self, path):
        self.path = path
        with closing(sqlite3.connect(path, timeout=30)) as db, db:
            db.execute("CREATE TABLE IF NOT EXISTS blobs(digest TEXT PRIMARY KEY, repository TEXT NOT NULL)")

    def remember(self, resolved, repository):
        with closing(sqlite3.connect(self.path, timeout=30)) as db, db:
            db.executemany("INSERT OR IGNORE INTO blobs VALUES (?,?)",
                           [(layer["digest"], repository) for layer in resolved.get("layers", [])])

    def repository(self, digest):
        with closing(sqlite3.connect(self.path, timeout=30)) as db:
            row = db.execute("SELECT repository FROM blobs WHERE digest=?", (digest,)).fetchone()
        return row[0] if row else None


class PublicBlobRedirect(request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if parse.urlparse(newurl).scheme != "https":
            raise ValueError("upstream blob redirect must remain HTTPS")
        redirected = super().redirect_request(req, fp, code, msg, headers, newurl)
        if parse.urlparse(req.full_url).netloc != parse.urlparse(newurl).netloc:
            redirected.remove_header("Authorization")
        return redirected


def verified_chunks(stream, digest, size, *, deadline):
    hashed = hashlib.sha256()
    remaining = size
    while remaining:
        if time.monotonic() >= deadline:
            raise TimeoutError("upstream blob transfer deadline")
        chunk = stream.read(min(1024 * 1024, remaining))
        if not chunk:
            raise ValueError("upstream blob ended before its declared size")
        hashed.update(chunk)
        remaining -= len(chunk)
        yield chunk
    if stream.read(1) or "sha256:" + hashed.hexdigest() != digest:
        raise ValueError("upstream blob digest or size mismatch")


def upload_stream(client, repository, descriptor, stream):
    digest, size = descriptor["digest"], descriptor["size"]
    location = client.start_blob_upload(repository)
    deadline = time.monotonic() + 1800
    try:
        path = client._validate_upload_location(location)
        path += ("&" if "?" in path else "?") + parse.urlencode({"digest": digest})
        response = client._request(path, method="PUT",
            headers={"Content-Type": "application/octet-stream", "Content-Length": str(size)},
            data=verified_chunks(stream, digest, size, deadline=deadline), timeout_seconds=1800)
        try:
            stored = response.headers.get("Docker-Content-Digest", digest)
            if stored != digest:
                raise ValueError("staging registry changed blob digest")
        finally:
            response.close()
    except BaseException:
        try:
            client.abort_blob_upload(location)
        except Exception:
            pass
        raise


def stage_source(resolved, client, lock_root, *, publication_url, protect, opener=None, headers_factory=None,
                 metrics=None, blob_sources=None):
    from prepare_image_pool import public_registry_headers, registry_parts
    host, repository, digest = registry_parts(resolved["reference"])
    manifest = resolved["manifest_json"].encode()
    config = resolved["config_json"].encode()
    if "sha256:" + hashlib.sha256(manifest).hexdigest() != digest:
        raise ValueError("staged manifest identity changed")
    document = json.loads(manifest)
    config_descriptor = document["config"]
    if ("sha256:" + hashlib.sha256(config).hexdigest() != config_descriptor["digest"]
            or len(config) != config_descriptor["size"]):
        raise ValueError("staged config identity changed")
    descriptors = [config_descriptor, *document["layers"]]
    for descriptor in descriptors:
        if (not isinstance(descriptor.get("size"), int) or descriptor["size"] < 0
                or len(descriptor.get("digest", "")) != 71
                or not descriptor["digest"].startswith("sha256:")
                or any(c not in "0123456789abcdef" for c in descriptor["digest"][7:])):
            raise ValueError("invalid upstream blob descriptor")
    tag = "sha256-" + digest[7:]
    # Gateway clients use loopback; the published FROM must be reachable by builders.
    authority = parse.urlparse(publication_url).netloc
    if not authority or parse.urlparse(publication_url).path not in {"", "/"}:
        raise ValueError("staged source requires a fleet-reachable registry URL")
    reference = authority + "/" + REPOSITORY + ":" + tag + "@" + digest
    metrics = metrics if metrics is not None else {}
    metrics.update(downloaded_bytes=0, reused_blob_bytes=0, mounted_blob_bytes=0)
    # Retain before publication; pruning must not race the publication commit.
    if not protect(reference, "upstream-source:" + digest):
        raise RuntimeError("could not retain staged source")
    opener = opener or request.build_opener(PublicBlobRedirect()).open
    headers_factory = headers_factory or public_registry_headers
    headers = None
    for descriptor in descriptors:
        blob = descriptor["digest"]
        with (lock_root / ("upstream-blob-" + blob[7:] + ".lock")).open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            if client.blob_exists(REPOSITORY, blob):
                metrics["reused_blob_bytes"] += descriptor["size"]
                continue
            source_repository = blob_sources.repository(blob) if blob_sources else None
            if source_repository and client.mount_blob(REPOSITORY, source_repository, blob):
                metrics["mounted_blob_bytes"] += descriptor["size"]
                continue
            if blob == config_descriptor["digest"]:
                with io.BytesIO(config) as stream:
                    upload_stream(client, REPOSITORY, descriptor, stream)
                continue
            if headers is None:
                headers = headers_factory(host, repository)
            endpoint = "registry-1.docker.io" if host == "docker.io" else host
            req = request.Request(f"https://{endpoint}/v2/{repository}/blobs/{blob}", headers=headers)
            with opener(req, timeout=60) as stream:
                upload_stream(client, REPOSITORY, descriptor, stream)
            metrics["downloaded_bytes"] += descriptor["size"]
    try:
        existing = client.manifest_digest(REPOSITORY, tag)
    except RegistryRequestError as error:
        if error.status_code != 404:
            raise
        existing = None
    if existing is not None and existing != digest:
        raise ValueError("immutable staged-source tag conflict")
    if existing is None:
        actual = client.put_manifest(REPOSITORY, tag, manifest,
            media_type=document.get("mediaType", "application/vnd.docker.distribution.manifest.v2+json"))
        if actual != digest:
            raise ValueError("staging registry changed manifest digest")
    return reference
