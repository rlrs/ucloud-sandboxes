"""Authenticated, read-only git fetches from GitHub for builds.

GitHub limits anonymous git downloads from some networks ("temporarily limiting
some unauthenticated downloads ... authenticate", HTTP 401 mid-clone; UCloud's
address, 2026-10-10), and every OpenSWE recipe clones from GitHub. The package
cache serves ``/github/<owner>/<repo>/info/refs?service=git-upload-pack`` and
``POST /github/<owner>/<repo>/git-upload-pack`` (git's smart HTTP, fetch only)
from ``https://github.com`` with a token; build steps reach it through git's
``url.<proxy>/github/.insteadOf https://github.com/``, so no recipe changes and
the token never enters a build. Nothing else is forwarded: no pushes
(receive-pack), no other paths or hosts.
"""
from __future__ import annotations

import base64
from http import HTTPStatus
import http.client
import re
import threading
from pathlib import Path

GITHUB = "github.com"
_PATH = re.compile(r"/github/([A-Za-z0-9-]{1,39})/([A-Za-z0-9._-]{1,100})/(info/refs|git-upload-pack)")
MAX_REQUEST_BYTES = 64 * 1024 ** 2  # A fetch's wants/haves; git sends far less.
TIMEOUT_SECONDS = 300
COPY_CHUNK = 1024 * 1024
FORWARDED_REQUEST = ("Accept", "Content-Type", "Content-Encoding", "Git-Protocol", "User-Agent")
FORWARDED_RESPONSE = ("Content-Type", "Cache-Control", "Expires", "Pragma", "Content-Encoding")


def git_insteadof(proxy_url):
    """The environment that sends git's GitHub fetches through ``proxy_url`` (git 2.31+)."""
    base = proxy_url.rstrip("/") + "/github/"
    return {"GIT_CONFIG_COUNT": "2",
            "GIT_CONFIG_KEY_0": f"url.{base}.insteadOf", "GIT_CONFIG_VALUE_0": f"https://{GITHUB}/",
            "GIT_CONFIG_KEY_1": f"url.{base}.insteadOf", "GIT_CONFIG_VALUE_1": f"http://{GITHUB}/"}


class GitHubProxy:
    def __init__(self, token_file, *, connect=None):
        self.token_file = Path(token_file)
        self.connect = connect or (lambda: http.client.HTTPSConnection(GITHUB, timeout=TIMEOUT_SECONDS))
        self._token, self._mtime, self._guard = None, None, threading.Lock()
        self.metrics = {"requests": 0, "refused": 0, "errors": 0, "bytes": 0}

    def _count(self, name, value=1):
        with self._guard:
            self.metrics[name] += value

    def _authorization(self):
        """``Basic`` credentials from the token file, reread when it changes."""
        mtime = self.token_file.stat().st_mtime
        with self._guard:
            if mtime != self._mtime:
                token = self.token_file.read_text().strip()
                if not token or not token.isprintable() or len(token) > 1024:
                    raise ValueError("the GitHub token file holds no usable token")
                self._token, self._mtime = token, mtime
            raw = f"x-access-token:{self._token}".encode()
        return "Basic " + base64.b64encode(raw).decode()

    @staticmethod
    def route(path):
        """(owner, repo, endpoint, query) of an allowed fetch, else None."""
        target, _, query = path.partition("?")
        match = _PATH.fullmatch(target)
        if match is None or ".." in match.group(2):
            return None
        owner, repo, endpoint = match.groups()
        if (endpoint == "info/refs") != (query == "service=git-upload-pack") or (endpoint != "info/refs" and query):
            return None
        return owner, repo, endpoint, query

    def handle(self, handler):
        """Answer ``handler``'s request (a BaseHTTPRequestHandler)."""
        self._count("requests")
        route = self.route(handler.path)
        if route is None or handler.command != ("GET" if route[2] == "info/refs" else "POST"):
            self._count("refused")
            return _reply(handler, HTTPStatus.FORBIDDEN, b"only git fetches from github.com\n")
        owner, repo, endpoint, query = route
        body = None
        if handler.command == "POST":
            body = _request_body(handler)
            if body is None:
                self._count("refused")
                return _reply(handler, HTTPStatus.REQUEST_ENTITY_TOO_LARGE, b"request too large\n")
        headers = {name: handler.headers[name] for name in FORWARDED_REQUEST if handler.headers.get(name)}
        headers["Authorization"] = self._authorization()
        headers["Host"] = GITHUB
        if body is not None:
            headers["Content-Length"] = str(len(body))
        connection = self.connect()
        try:
            connection.request(handler.command, f"/{owner}/{repo}/{endpoint}" + (f"?{query}" if query else ""),
                               body=body, headers=headers)
            response = connection.getresponse()
            handler.send_response(response.status)
            for name in FORWARDED_RESPONSE:
                if response.getheader(name):
                    handler.send_header(name, response.getheader(name))
            location = response.getheader("Location") or ""
            if location.startswith(f"https://{GITHUB}/"):
                # A renamed repository: git follows the redirect through the proxy too.
                host = handler.headers.get("Host") or ""
                handler.send_header("Location", f"http://{host}/github/" + location[len(f"https://{GITHUB}/"):])
            handler.send_header("Transfer-Encoding", "chunked")
            handler.end_headers()
            while chunk := response.read(COPY_CHUNK):
                handler.wfile.write(b"%x\r\n%s\r\n" % (len(chunk), chunk))
                self._count("bytes", len(chunk))
            handler.wfile.write(b"0\r\n\r\n")
        except (OSError, http.client.HTTPException):
            self._count("errors")
            handler.close_connection = True
        finally:
            connection.close()


def _request_body(handler):
    """The request's body (Content-Length or chunked), or None past MAX_REQUEST_BYTES."""
    if handler.headers.get("Transfer-Encoding", "").lower() == "chunked":
        parts, total = [], 0
        while True:
            size = int(handler.rfile.readline().split(b";", 1)[0].strip() or b"0", 16)
            if size == 0:
                while handler.rfile.readline() not in (b"\r\n", b"\n", b""):
                    pass
                return b"".join(parts)
            total += size
            if total > MAX_REQUEST_BYTES:
                return None
            parts.append(handler.rfile.read(size))
            handler.rfile.readline()
    length = int(handler.headers.get("Content-Length") or 0)
    return None if length > MAX_REQUEST_BYTES else handler.rfile.read(length)


def _reply(handler, status, body):
    handler.send_response(status)
    handler.send_header("Content-Type", "text/plain")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)
