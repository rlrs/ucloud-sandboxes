"""Fixtures for chunk-store tests: an S3 stand-in that checks presigned URLs,
an in-process ucloud-chunk-index, OCI images in a memory registry, and a
fake ``nydus-image`` that writes the RAFS v6 tables our parsers read.

The fake converts tars without shadowing (merge keeps every layer's chunks);
real nydus-image runs are separate tests that skip without the binary.
"""
from __future__ import annotations

import calendar
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import shlex
import struct
import sys
import tarfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from urllib.parse import parse_qs, unquote, urlsplit

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from tests.test_environment_artifact import MemoryRegistry
from ucloud_sandboxes.chunk_index import (ChunkIndex, ChunkIndexClient, ChunkIndexServer, ChunkIndexService,
                                          ChunkObjectStore, S3Presigner)
from ucloud_sandboxes.chunk_store import round_up, zstd_compress
from ucloud_sandboxes.environment_artifact import CHUNK_BYTES, EnvironmentArtifactRegistry, OCI_IMAGE, content_digest

REPOSITORY = "library/app"
NYDUS = os.environ.get("UCLOUD_TEST_NYDUS_IMAGE", "")


class Registry(MemoryRegistry):
    def open_blob(self, repository, digest):
        return io.BytesIO(self.blobs[digest])


class ObjectServer:
    """S3 over HTTP for presigned GETs (path style), with recorded requests."""

    def __init__(self, bucket="chunks"):
        self.objects, self.requests, self.bucket, self.corrupt = {}, [], bucket, {}
        # fault(key, range) -> None, ("status", code), ("stall", seconds) before
        # the headers, or ("body_stall", seconds) after half the body.
        self.fault, self.inflight, self.peak = None, 0, 0
        self.guard = threading.Lock()
        handler = type("Handler", (_ObjectHandler,), {"store": self})
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.server.daemon_threads = True
        self.server.handle_error = lambda *_: None  # Hedged losers hang up mid-response.
        self.endpoint = f"http://127.0.0.1:{self.server.server_address[1]}"
        self.presigner = S3Presigner(self.endpoint, bucket, "hel1", "AKIDTEST", "secret-test", path_style=True)
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": .05}, daemon=True)
        self.thread.start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()

    # The Boto3S3ObjectClient surface the index and builders use.
    def stat(self, key):
        return SimpleNamespace(size=len(self.objects[key])) if key in self.objects else None

    def put_file(self, key, path, *, sha256):
        self.objects[key] = Path(path).read_bytes()

    def put_bytes(self, key, payload, *, sha256):
        self.objects[key] = bytes(payload)

    def gets(self, kind=".pack"):
        with self.guard:
            return [request for request in self.requests if request[0].endswith(kind)]


class _ObjectHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    store = None

    def log_message(self, *args):
        pass

    def do_GET(self):  # noqa: N802
        parsed = urlsplit(self.path)
        key = unquote(parsed.path).removeprefix(f"/{self.store.bucket}/")
        query = {name: values[0] for name, values in parse_qs(parsed.query).items()}
        try:
            signed_at = calendar.timegm(time.strptime(query["X-Amz-Date"], "%Y%m%dT%H%M%SZ"))
            expected = self.store.presigner.url(key, expires=int(query["X-Amz-Expires"]), now=signed_at)
            valid = expected.endswith(parsed.query) and time.time() <= signed_at + int(query["X-Amz-Expires"])
        except (KeyError, ValueError):
            valid = False
        if not valid or key not in self.store.objects:
            return self._send(403 if not valid else 404, b"denied")
        data = self.store.corrupt.get(key, self.store.objects[key])
        spec = self.headers.get("Range", "")
        with self.store.guard:
            self.store.requests.append((key, spec))
            self.store.inflight += 1
            self.store.peak = max(self.store.peak, self.store.inflight)
        try:
            fault = self.store.fault(key, spec) if self.store.fault else None
            if fault and fault[0] == "status":
                return self._send(fault[1], b"fault")
            if fault and fault[0] == "stall":
                time.sleep(fault[1])
            if not spec:
                return self._send(200, data)
            first, _, last = spec.removeprefix("bytes=").partition("-")
            start = max(0, len(data) - int(last)) if first == "" else int(first)
            if start >= len(data):
                return self._send(416, b"", {"Content-Range": f"bytes */{len(data)}"})
            end = len(data) - 1 if first == "" or not last else min(int(last), len(data) - 1)
            self._send(206, data[start:end + 1], {"Content-Range": f"bytes {start}-{end}/{len(data)}"},
                       stall=fault[1] if fault and fault[0] == "body_stall" else 0)
        finally:
            with self.store.guard:
                self.store.inflight -= 1

    def _send(self, status, payload, headers=(), stall=0):
        self.send_response(status)
        for name, value in dict(headers).items():
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        try:
            if stall:
                self.wfile.write(payload[:len(payload) // 2])
                self.wfile.flush()
                time.sleep(stall)
                payload = payload[len(payload) // 2:]
            self.wfile.write(payload)
        except OSError:
            self.close_connection = True  # A hedge's loser hung up.


class IndexFixture:
    """A real ucloud-chunk-index on localhost over SQLite and ObjectServer."""

    def __init__(self, root, objects, *, prefix="test/chunks", url_seconds=3600):
        self.store = ChunkObjectStore(objects, objects.presigner, prefix, url_seconds=url_seconds)
        self.index = ChunkIndex(Path(root) / "index.sqlite")
        self.service = ChunkIndexService(self.index, self.store)
        self.server = ChunkIndexServer(("127.0.0.1", 0), self.service, read_token="r" * 32, write_token="w" * 32)
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": .05}, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        self.writer, self.reader = ChunkIndexClient(self.url, "w" * 32), ChunkIndexClient(self.url, "r" * 32)

    def close(self):
        self.server.shutdown()
        self.server.server_close()


class ChunkStoreFixture:
    """Registry, object store, index and a converter, all in one directory."""

    def __init__(self, test, *, layout="image", nydus=None, signer=None):
        from tempfile import TemporaryDirectory
        from ucloud_sandboxes.chunk_convert import RafsConverter
        directory = TemporaryDirectory()
        test.addCleanup(directory.cleanup)
        self.root = Path(directory.name).resolve()
        self.objects = ObjectServer()
        test.addCleanup(self.objects.close)
        self.index = IndexFixture(self.root, self.objects)
        test.addCleanup(self.index.close)
        self.client = Registry()
        self.key, self.trusted = signer or signing()
        self.registry = environment_registry(self.client, self.trusted)
        self.converter = RafsConverter(self.registry, self.index.store, self.index.writer, self.key,
                                       self.root / "work", nydus_image=nydus or fake_nydus(self.root),
                                       layout=layout, owner="test-builder")

    def roots(self):
        return sorted(tag for tag in self.client.tags if tag.startswith("rafs-root-"))

    def packs(self):
        return sorted(key for key in self.objects.objects if key.endswith(".pack"))


def sample_images(client):
    """Images A and B share base layer L1; their top layers share one file."""
    shared = pseudo_random("shared", 300_000)
    base = layer([("etc", "dir"), ("etc/hosts", b"127.0.0.1 localhost\n"), ("etc/gone", b"x" * 5000),
                  ("usr", "dir"), ("usr/lib", "dir"), ("usr/lib/text", b"lorem ipsum " * 90_000),
                  ("usr/lib/noise", pseudo_random("noise", 700_000)), ("usr/lib/link", ("symlink", "text")),
                  ("usr/lib/dotted", ("symlink", ".././lib/text")),  # Rollback keeps '.' (M1 gate).
                  ("usr/lib/hard", ("link", "usr/lib/text")), ("home", "dir"), ("home/user", "dir"), ("home/user/.p", b"x")])
    top_a = layer([("etc/.wh.gone", b""), ("opt", "dir"), ("opt/a", shared), ("opt/a-only", pseudo_random("a", 9000))])
    top_b = layer([("opt", "dir"), ("opt/b", shared), ("opt/b-only", pseudo_random("b", 70_000))])
    return push_image(client, "a", [base, top_a]), push_image(client, "b", [base, top_b])


def signing():
    key = Ed25519PrivateKey.generate()
    public = key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    return key, {content_digest(public): public}


def environment_registry(client, trusted):
    return EnvironmentArtifactRegistry(client, "environments", trusted)


def layer(entries, *, compress=True):
    """A deterministic OCI layer: entries are (name, bytes | ("symlink", target) | ("link", target) | "dir")."""
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w", format=tarfile.PAX_FORMAT) as writer:
        for name, value in entries:
            info = tarfile.TarInfo(name)
            info.mtime, info.mode, info.uid = 1_600_000_000, 0o644, 1000 if name.startswith("home/user") else 0
            payload = None
            if value == "dir":
                info.type, info.mode = tarfile.DIRTYPE, 0o755
            elif isinstance(value, tuple):
                info.type = tarfile.SYMTYPE if value[0] == "symlink" else tarfile.LNKTYPE
                info.linkname = value[1]
            else:
                info.size, payload = len(value), io.BytesIO(value)
            writer.addfile(info, payload)
    tar = raw.getvalue()
    blob = gzip.compress(tar, mtime=0) if compress else tar
    return blob, "sha256:" + hashlib.sha256(tar).hexdigest()


def push_image(client, tag, layers, *, repository=REPOSITORY):
    config = json.dumps({"architecture": "amd64", "os": "linux", "config": {"Env": ["PATH=/bin"], "Cmd": ["sh"]},
                         "rootfs": {"type": "layers", "diff_ids": [diff_id for _, diff_id in layers]}},
                        sort_keys=True).encode()
    client.blobs[content_digest(config)] = config
    for blob, _ in layers:
        client.blobs[content_digest(blob)] = blob
    manifest = json.dumps({"schemaVersion": 2, "mediaType": OCI_IMAGE,
                           "config": {"mediaType": "application/vnd.oci.image.config.v1+json",
                                      "digest": content_digest(config), "size": len(config)},
                           "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                                       "digest": content_digest(blob), "size": len(blob)} for blob, _ in layers]},
                          sort_keys=True).encode()
    client.put_manifest(repository, tag, manifest, media_type=OCI_IMAGE)
    return content_digest(manifest), content_digest(config)


def pseudo_random(seed, size):
    output, counter = bytearray(), 0
    while len(output) < size:
        output += hashlib.sha256(f"{seed}:{counter}".encode()).digest()
        counter += 1
    return bytes(output[:size])


# --- A fake nydus-image: create (targz-rafs, tar-rafs) and merge ---

def fake_nydus(root):
    """An executable path that runs ``main`` below under this interpreter."""
    script = Path(root) / "nydus-image"
    script.write_text("#!/bin/sh\nexec " + shlex.quote(sys.executable) + " -m tests.chunk_store_support \"$@\"\n")
    script.chmod(0o755)
    return str(script)


def write_bootstrap(devices, chunks):
    """RAFS v6 superblock, extension, device table and chunk table only."""
    table = round_up(1408 + 128 * len(devices))
    size = max(round_up(table + 80 * len(chunks)), 8192)
    data = bytearray(size)
    struct.pack_into("<I", data, 1024, 0xE0F5E1E2)
    data[1024 + 12] = 12
    struct.pack_into("<HH", data, 1024 + 86, len(devices), 11)
    struct.pack_into("<QQIIQQ", data, 1024 + 128, 0x1000088, 4096, 256 * len(devices), CHUNK_BYTES, table, 80 * len(chunks))
    for index, (blob, blocks, mapped) in enumerate(devices):
        struct.pack_into("<64sII", data, 1408 + 128 * index, blob.encode(), blocks, mapped)
    for index, chunk in enumerate(chunks):
        struct.pack_into("<32sIIIIQQQII", data, table + 80 * index, *chunk, 0, index, 0)
    return bytes(data)


def _mapped(bootstrap_size, cursor_blocks=0):
    return max(128, round_up(max(bootstrap_size // 4096, cursor_blocks), 128))


def _create(arguments, blob_toc=False):
    options = dict(zip(arguments[::2], arguments[1::2]))
    source = arguments[-1]
    opener = gzip.open if options["-t"] == "targz-rafs" else open
    chunks, blob, seen, offset = [], bytearray(), {}, 0
    with opener(source, "rb") as stream, tarfile.open(fileobj=io.BytesIO(stream.read())) as reader:
        for member in reader:
            if not member.isfile() or member.name.rsplit("/", 1)[-1].startswith(".wh."):
                continue
            data = reader.extractfile(member).read()
            for start in range(0, len(data), CHUNK_BYTES):
                piece = data[start:start + CHUNK_BYTES]
                digest = hashlib.sha256(piece).digest()
                if digest in seen:
                    continue
                packed = zstd_compress(piece)
                flags, stored = (0x11, packed) if len(packed) < len(piece) else (0x10, piece)
                seen[digest] = True
                chunks.append([digest, 0, flags, len(stored), len(piece), len(blob), offset])
                blob += stored
                offset = round_up(offset + len(piece))
    blob_ids = []
    if chunks:
        if blob_toc:  # Stands in for chunk info, digests and the TOC.
            blob += b"nydus-tail:" + hashlib.sha256(bytes(blob)).digest() * 200
        blob_id = hashlib.sha256(bytes(blob)).hexdigest()
        Path(options["-D"], blob_id).write_bytes(bytes(blob))
        if os.environ.get("FAKE_NYDUS_KEEP"):  # Tests compare rebuilt blobs with these.
            Path(os.environ["FAKE_NYDUS_KEEP"], blob_id).write_bytes(bytes(blob))
        blob_ids.append(blob_id)
    size = len(write_bootstrap([("0" * 64, 1, 128)] * len(blob_ids), chunks))
    devices = [(blob_ids[0], max(1, offset // 4096), _mapped(size))] if blob_ids else []
    Path(options["-B"]).write_bytes(write_bootstrap(devices, [tuple(chunk) for chunk in chunks]))
    Path(options["-J"]).write_text(json.dumps({"blobs": blob_ids}))


def _merge(arguments):
    """``--parent-bootstrap`` keeps a merged parent's blobs and chunks first."""
    from ucloud_sandboxes.chunk_store import parse_bootstrap
    options, index = {}, 0
    while arguments[index].startswith("-"):
        options[arguments[index]] = arguments[index + 1]
        index += 2
    sources = [parse_bootstrap(Path(path).read_bytes()) for path in arguments[index:]]
    ids = options["--original-blob-ids"].split(",")
    inherited = []
    if "--parent-bootstrap" in options:
        parent = parse_bootstrap(Path(options["--parent-bootstrap"]).read_bytes())
        inherited = [(device[0], [chunk for chunk in parent.chunks if chunk[1] == position], device[1])
                     for position, device in enumerate(parent.devices)]
    layers = inherited + [(blob_id, source.chunks, source.devices[0][1])
                          for blob_id, source in zip(ids, sources) if source.devices]
    count = sum(len(layer_chunks) for _, layer_chunks, _ in layers)
    size = len(write_bootstrap([("0" * 64, 1, 128)] * len(layers), [(b"\0" * 32, 0, 0, 1, 1, 0, 0)] * count))
    devices, chunks, cursor = [], [], 0
    for position, (blob_id, layer_chunks, blocks) in enumerate(layers):
        mapped = _mapped(size, cursor)
        devices.append((blob_id, blocks, mapped))
        cursor = mapped + blocks
        chunks += [(chunk[0], position, *chunk[2:]) for chunk in layer_chunks]
    merged = write_bootstrap(devices, chunks)
    if not any(source.devices for source in sources):
        # As nydus-image v2.4.5: with no blob among the sources, merge writes its
        # 1 MiB default chunk size (measured on the gateway, 2026-10-10).
        merged = bytearray(merged)
        struct.pack_into("<I", merged, 1024 + 128 + 20, 0x100000)
    Path(options["-B"]).write_bytes(bytes(merged))
    Path(options["-J"]).write_text(json.dumps({"blobs": [blob_id for blob_id, _, _ in layers]}))


def main(argv):
    command, arguments = argv[0], argv[1:]
    if command == "create":
        flags = {"--repeatable"}
        cleaned, skip = [], False
        for index, value in enumerate(arguments):
            if skip:
                skip = False
                continue
            if value in flags:
                continue
            if value.startswith("-") and index + 1 < len(arguments) and value not in ("-t", "-D", "-B", "-J"):
                skip = True
                continue
            cleaned.append(value)
        _create(cleaned, blob_toc="blob-toc" in arguments)
    elif command == "merge":
        _merge(arguments)
    else:
        raise SystemExit(f"fake nydus-image does not implement {command}")


if __name__ == "__main__":
    main(sys.argv[1:])
