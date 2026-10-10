"""The package cache's GitHub git proxy: read-only fetches with a token."""
import base64
import subprocess
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
import unittest

from ucloud_sandboxes.git_proxy import GitHubProxy, git_insteadof
from ucloud_sandboxes.package_cache import PackageCache, PackageCacheConfig, _Handler

TOKEN = "github_pat_test_token_0123456789"


class FakeGitHub:
    """github.com's smart-HTTP endpoints, recording what reaches it."""

    def __init__(self):
        self.requests = []
        github = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def _answer(self, status, body, headers=()):
                self.send_response(status)
                for name, value in headers:
                    self.send_header(name, value)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):  # noqa: N802
                github.requests.append(("GET", self.path, dict(self.headers), b""))
                if self.path.startswith("/old/name/"):
                    return self._answer(301, b"", [("Location", "https://github.com/new/name" + self.path[9:])])
                self._answer(200, b"001e# service=git-upload-pack\n0000refs",
                             [("Content-Type", "application/x-git-upload-pack-advertisement")])

            def do_POST(self):  # noqa: N802
                body = self.rfile.read(int(self.headers["Content-Length"]))
                github.requests.append(("POST", self.path, dict(self.headers), body))
                self._answer(200, b"PACK" + body[::-1], [("Content-Type", "application/x-git-upload-pack-result")])

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.port = self.server.server_address[1]


class GitProxyTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.github = FakeGitHub()
        self.addCleanup(self.github.server.shutdown)
        self.token = Path(self.temp.name) / "github-token"
        self.token.write_text(TOKEN + "\n")
        self.cache = self.serve(str(self.token))
        self.cache.github.connect = lambda: http.client.HTTPConnection("127.0.0.1", self.github.port, timeout=10)

    def serve(self, token_file):
        config = PackageCacheConfig.from_dict({
            "listen": "127.0.0.1:3142", "url": "http://127.0.0.1:3142",
            "cache_dir": str(Path(self.temp.name) / f"c{len(os.listdir(self.temp.name))}"),
            "max_bytes": 1024 ** 3, "upstreams": {"archive.ubuntu.com": "http://127.0.0.1:9"},
            "github_token_file": token_file})
        cache = PackageCache(config)
        server = ThreadingHTTPServer(("127.0.0.1", 0), type("H", (_Handler,), {"cache": cache}))
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.shutdown)
        cache.port = server.server_address[1]
        return cache

    def call(self, method, path, body=None, *, chunked=False, cache=None):
        connection = http.client.HTTPConnection("127.0.0.1", (cache or self.cache).port, timeout=10)
        headers = {"Git-Protocol": "version=2", "Content-Type": "application/x-git-upload-pack-request"}
        if chunked:
            connection.request(method, path, body=iter([body[:3], body[3:]]), headers=headers, encode_chunked=True)
        else:
            connection.request(method, path, body=body, headers=headers)
        response = connection.getresponse()
        return response.status, dict(response.getheaders()), response.read()

    def test_fetches_reach_github_with_the_token_and_nothing_leaks(self):
        status, headers, body = self.call("GET", "/github/octo/repo.git/info/refs?service=git-upload-pack")
        self.assertEqual((status, body[-4:]), (200, b"refs"))
        self.assertEqual(headers["Content-Type"], "application/x-git-upload-pack-advertisement")
        status, _, body = self.call("POST", "/github/octo/repo.git/git-upload-pack", b"0032want abc", chunked=True)
        self.assertEqual((status, body), (200, b"PACK" + b"0032want abc"[::-1]))
        (method, path, sent, _), (_, post_path, _, posted) = self.github.requests
        self.assertEqual((method, path), ("GET", "/octo/repo.git/info/refs?service=git-upload-pack"))
        self.assertEqual((post_path, posted), ("/octo/repo.git/git-upload-pack", b"0032want abc"))
        self.assertEqual(sent["Authorization"], "Basic " + base64.b64encode(f"x-access-token:{TOKEN}".encode()).decode())
        self.assertEqual((sent["Host"], sent["Git-Protocol"]), ("github.com", "version=2"))
        self.assertNotIn(TOKEN.encode(), body)

    def test_only_git_fetches_are_forwarded(self):
        for method, path in (("POST", "/github/octo/repo/git-receive-pack"),
                             ("GET", "/github/octo/repo/info/refs?service=git-receive-pack"),
                             ("GET", "/github/octo/repo/archive/main.zip"),
                             ("GET", "/github/octo/../etc/info/refs?service=git-upload-pack"),
                             ("POST", "/github/octo/repo/info/refs?service=git-upload-pack"),
                             ("GET", "/github/octo/repo/git-upload-pack")):
            self.assertEqual(self.call(method, path, b"x" if method == "POST" else None)[0], 403, path)
        self.assertEqual(self.github.requests, [])
        unconfigured = self.serve("")
        self.assertEqual(self.call("GET", "/github/octo/repo/info/refs?service=git-upload-pack",
                                   cache=unconfigured)[0], 404)

    def test_a_renamed_repository_redirects_through_the_proxy(self):
        status, headers, _ = self.call("GET", "/github/old/name/info/refs?service=git-upload-pack")
        self.assertEqual(status, 301)
        self.assertEqual(headers["Location"],
                         f"http://127.0.0.1:{self.cache.port}/github/new/name/info/refs?service=git-upload-pack")

    def test_a_replaced_token_is_used(self):
        self.token.write_text("github_pat_second\n")
        os.utime(self.token, (1, 1))
        self.call("GET", "/github/octo/repo/info/refs?service=git-upload-pack")
        self.assertIn(base64.b64encode(b"x-access-token:github_pat_second").decode(),
                      self.github.requests[-1][2]["Authorization"])

    def test_git_rewrites_only_github_urls_to_the_proxy(self):
        env = git_insteadof("http://10.0.0.5:3142")
        self.assertEqual(env["GIT_CONFIG_COUNT"], "2")
        self.assertEqual({env["GIT_CONFIG_KEY_0"], env["GIT_CONFIG_KEY_1"]},
                         {"url.http://10.0.0.5:3142/github/.insteadOf"})
        self.assertEqual({env["GIT_CONFIG_VALUE_0"], env["GIT_CONFIG_VALUE_1"]},
                         {"https://github.com/", "http://github.com/"})
        self.assertIsNone(GitHubProxy.route("/github/octo/repo/info/refs"))
        self.assertEqual(GitHubProxy.route("/github/octo/repo.git/git-upload-pack"),
                         ("octo", "repo.git", "git-upload-pack", ""))
        with self.assertRaises(ValueError):
            PackageCacheConfig.from_dict({"listen": "127.0.0.1:1", "url": "http://x:1", "cache_dir": "/c",
                                          "max_bytes": 1024 ** 3, "upstreams": {"a.b": "http://a.b"},
                                          "github_token_file": "relative/token"})


class HttpBackendGitHub:
    """github.com served by git's own smart-HTTP CGI over a real bare repository."""

    def __init__(self, root):
        self.root = Path(root)
        work = self.root / "work"
        subprocess.run(["git", "init", "-q", "-b", "main", str(work)], check=True)
        (work / "README").write_text("hello through the proxy\n" * 2000)
        subprocess.run(["git", "-C", str(work), "-c", "user.name=t", "-c", "user.email=t@t", "add", "README"],
                       check=True)
        subprocess.run(["git", "-C", str(work), "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m",
                        "first"], check=True)
        (self.root / "octo").mkdir()
        subprocess.run(["git", "clone", "-q", "--bare", str(work), str(self.root / "octo" / "repo.git")], check=True)
        self.authorizations = []
        backend = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def _cgi(self):
                backend.authorizations.append(self.headers.get("Authorization"))
                path, _, query = self.path.partition("?")
                body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                env = {"GIT_PROJECT_ROOT": str(backend.root), "GIT_HTTP_EXPORT_ALL": "1", "PATH_INFO": path,
                       "REQUEST_METHOD": self.command, "QUERY_STRING": query,
                       "CONTENT_TYPE": self.headers.get("Content-Type", ""), "CONTENT_LENGTH": str(len(body)),
                       "HTTP_GIT_PROTOCOL": self.headers.get("Git-Protocol", ""), "PATH": os.environ["PATH"]}
                if self.headers.get("Content-Encoding"):
                    env["HTTP_CONTENT_ENCODING"] = self.headers["Content-Encoding"]
                out = subprocess.run(["git", "http-backend"], input=body, env=env, capture_output=True).stdout
                head, _, payload = out.partition(b"\r\n\r\n")
                status, headers = 200, []
                for line in head.decode().splitlines():
                    name, _, value = line.partition(": ")
                    if name.lower() == "status":
                        status = int(value.split()[0])
                    else:
                        headers.append((name, value))
                self.send_response(status)
                for name, value in headers:
                    self.send_header(name, value)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            do_GET = do_POST = _cgi  # noqa: N815

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.port = self.server.server_address[1]


@unittest.skipUnless(__import__("shutil").which("git"), "needs git")
class RealGitTests(GitProxyTests):
    def test_git_clones_a_github_url_through_the_proxy(self):
        github = HttpBackendGitHub(Path(self.temp.name) / "gh")
        self.addCleanup(github.server.shutdown)
        self.cache.github.connect = lambda: http.client.HTTPConnection("127.0.0.1", github.port, timeout=30)
        target = Path(self.temp.name) / "clone"
        env = {"PATH": os.environ["PATH"], "HOME": self.temp.name, "GIT_TERMINAL_PROMPT": "0",
               **git_insteadof(f"http://127.0.0.1:{self.cache.port}")}
        subprocess.run(["git", "clone", "-q", "https://github.com/octo/repo.git", str(target)], env=env, check=True,
                       timeout=60)
        self.assertEqual((target / "README").read_text(), "hello through the proxy\n" * 2000)
        # The clone remembers GitHub, not the proxy; every request carried the token.
        stored = {"PATH": os.environ["PATH"], "HOME": self.temp.name}  # the image's environment: no rewrite
        remote = subprocess.run(["git", "-C", str(target), "config", "--get", "remote.origin.url"],
                                capture_output=True, text=True, env=stored).stdout.strip()
        self.assertEqual(remote, "https://github.com/octo/repo.git")
        self.assertTrue(github.authorizations)
        self.assertTrue(all(auth and auth.startswith("Basic ") for auth in github.authorizations))
