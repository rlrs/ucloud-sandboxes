#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import re
from urllib.parse import unquote, urlparse
from uuid import uuid4


_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--listen", default="127.0.0.1:18080")
    parser.add_argument("--metrics", required=True, type=Path)
    return parser.parse_args()


class RangeBlobHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_HEAD(self) -> None:
        self._serve(send_body=False)

    def do_GET(self) -> None:
        self._serve(send_body=True)

    def do_POST(self) -> None:
        if not urlparse(self.path).path.endswith("/blobs/uploads/"):
            self.send_error(404)
            return
        upload_id = uuid4().hex
        (self.server.uploads / upload_id).write_bytes(b"")  # type: ignore[attr-defined]
        self.send_response(202)
        self.send_header(
            "Location",
            f"/v2/snapshots/blobs/uploads/{upload_id}",
        )
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_PATCH(self) -> None:
        upload_id = urlparse(self.path).path.rsplit("/", 1)[-1]
        path = self.server.uploads / upload_id  # type: ignore[attr-defined]
        if not path.is_file():
            self.send_error(404)
            return
        length = int(self.headers.get("Content-Length", "0"))
        chunk = self.rfile.read(length)
        if len(chunk) != length:
            self.send_error(400)
            return
        with path.open("ab") as target:
            target.write(chunk)
        self.send_response(202)
        self.send_header("Location", self.path)
        self.send_header("Range", f"0-{path.stat().st_size - 1}")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_PUT(self) -> None:
        parsed = urlparse(self.path)
        if "/blobs/uploads/" in parsed.path:
            self._finish_upload(parsed)
            return
        if "/manifests/" in parsed.path:
            self._put_manifest(parsed)
            return
        self.send_error(404)

    def log_message(self, _format: str, *_args: object) -> None:
        return

    def _serve(self, *, send_body: bool) -> None:
        parsed_path = urlparse(self.path).path
        reference = unquote(parsed_path.rsplit("/", 1)[-1])
        if "/manifests/" in parsed_path:
            self._serve_manifest(reference, send_body=send_body)
            return
        digest = reference
        if not _DIGEST.fullmatch(digest):
            self.send_error(404)
            return
        path = self.server.root / digest  # type: ignore[attr-defined]
        if not path.is_file():
            self.send_error(404)
            return
        size = path.stat().st_size
        start = 0
        end = size - 1
        raw_range = self.headers.get("Range", "")
        if raw_range:
            match = re.fullmatch(r"bytes=(\d+)-(\d*)", raw_range)
            if match is None:
                self.send_error(416)
                return
            start = int(match.group(1))
            if match.group(2):
                end = min(end, int(match.group(2)))
            if start >= size or end < start:
                self.send_error(416)
                return
        count = end - start + 1
        self.send_response(206 if raw_range else 200)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Length", str(count))
        self.send_header("Docker-Content-Digest", digest)
        if raw_range:
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.end_headers()
        if send_body:
            with path.open("rb") as source:
                source.seek(start)
                remaining = count
                while remaining:
                    chunk = source.read(min(1024 * 1024, remaining))
                    if not chunk:
                        break
                    self.wfile.write(chunk)
                    remaining -= len(chunk)
        self.server.requests += 1  # type: ignore[attr-defined]
        self.server.bytes_served += count if send_body else 0  # type: ignore[attr-defined]
        self.server.metrics.write_text(  # type: ignore[attr-defined]
            json.dumps(
                {
                    "bytes_served": self.server.bytes_served,  # type: ignore[attr-defined]
                    "requests": self.server.requests,  # type: ignore[attr-defined]
                },
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

    def _serve_manifest(self, reference: str, *, send_body: bool) -> None:
        if not re.fullmatch(r"[A-Za-z0-9_.:+-]{1,128}", reference):
            self.send_error(404)
            return
        path = self.server.manifests / reference  # type: ignore[attr-defined]
        if not path.is_file():
            self.send_error(404)
            return
        payload = path.read_bytes()
        digest = f"sha256:{hashlib.sha256(payload).hexdigest()}"
        self.send_response(200)
        self.send_header(
            "Content-Type",
            "application/vnd.oci.image.manifest.v1+json",
        )
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Docker-Content-Digest", digest)
        self.end_headers()
        if send_body:
            self.wfile.write(payload)

    def _finish_upload(self, parsed: object) -> None:
        upload_id = parsed.path.rsplit("/", 1)[-1]  # type: ignore[attr-defined]
        upload = self.server.uploads / upload_id  # type: ignore[attr-defined]
        query = parsed.query  # type: ignore[attr-defined]
        digest = unquote(query.removeprefix("digest="))
        if not upload.is_file() or not _DIGEST.fullmatch(digest):
            self.send_error(400)
            return
        payload = upload.read_bytes()
        observed = f"sha256:{hashlib.sha256(payload).hexdigest()}"
        if observed != digest:
            self.send_error(400)
            return
        target = self.server.root / digest  # type: ignore[attr-defined]
        upload.replace(target)
        self.send_response(201)
        self.send_header("Location", f"/v2/snapshots/blobs/{digest}")
        self.send_header("Docker-Content-Digest", digest)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _put_manifest(self, parsed: object) -> None:
        reference = unquote(parsed.path.rsplit("/", 1)[-1])  # type: ignore[attr-defined]
        if not re.fullmatch(r"[A-Za-z0-9_.:+-]{1,128}", reference):
            self.send_error(400)
            return
        length = int(self.headers.get("Content-Length", "0"))
        payload = self.rfile.read(length)
        if len(payload) != length:
            self.send_error(400)
            return
        digest = f"sha256:{hashlib.sha256(payload).hexdigest()}"
        (self.server.manifests / reference).write_bytes(payload)  # type: ignore[attr-defined]
        (self.server.manifests / digest).write_bytes(payload)  # type: ignore[attr-defined]
        self.send_response(201)
        self.send_header("Docker-Content-Digest", digest)
        self.send_header("Content-Length", "0")
        self.end_headers()


def main() -> int:
    args = parse_args()
    host, raw_port = args.listen.rsplit(":", 1)
    root = args.root.resolve()
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    server = ThreadingHTTPServer((host, int(raw_port)), RangeBlobHandler)
    server.root = root  # type: ignore[attr-defined]
    server.uploads = root.parent / "uploads"  # type: ignore[attr-defined]
    server.manifests = root.parent / "manifests"  # type: ignore[attr-defined]
    server.uploads.mkdir(mode=0o700, exist_ok=True)  # type: ignore[attr-defined]
    server.manifests.mkdir(mode=0o700, exist_ok=True)  # type: ignore[attr-defined]
    server.metrics = args.metrics.resolve()  # type: ignore[attr-defined]
    server.requests = 0  # type: ignore[attr-defined]
    server.bytes_served = 0  # type: ignore[attr-defined]
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
