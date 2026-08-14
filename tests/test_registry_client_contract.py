from __future__ import annotations

import json
import unittest
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from unittest.mock import patch

from ucloud_sandboxes import managed_registry
from ucloud_sandboxes.managed_registry import (
    MANIFEST_ACCEPT,
    RegistryClient,
    RegistryRequestError,
)

Response = tuple[int, dict[str, str], bytes]
Responder = Callable[[str, str, dict[str, str], bytes], Response]
RecordedRequest = tuple[str, str, dict[str, str], bytes]


class _RegistryHTTPServer:
    def __init__(self, responder: Responder) -> None:
        self.responder = responder
        self.requests: list[RecordedRequest] = []
        contract = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                self._dispatch()

            def do_HEAD(self) -> None:
                self._dispatch()

            def do_POST(self) -> None:
                self._dispatch()

            def do_PATCH(self) -> None:
                self._dispatch()

            def do_PUT(self) -> None:
                self._dispatch()

            def _dispatch(self) -> None:
                length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(length)
                headers = {key.lower(): value for key, value in self.headers.items()}
                contract.requests.append((self.command, self.path, headers, body))
                status, response_headers, response_body = contract.responder(
                    self.command,
                    self.path,
                    headers,
                    body,
                )
                self.send_response(status)
                for key, value in response_headers.items():
                    self.send_header(key, value)
                self.send_header("Content-Length", str(len(response_body)))
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(response_body)

            def log_message(self, _format: str, *args: object) -> None:
                del _format, args

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = Thread(
            target=self.server.serve_forever,
            kwargs={"poll_interval": 0.01},
            daemon=True,
        )

    @property
    def base_url(self) -> str:
        host, port = self.server.server_address
        return f"http://{host}:{port}"

    def __enter__(self) -> _RegistryHTTPServer:  # noqa: PYI034
        self.thread.start()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=1)
        if self.thread.is_alive():
            raise RuntimeError("registry contract server did not stop")


def _json_response(payload: object, **headers: str) -> Response:
    return (
        200,
        {"Content-Type": "application/json", **headers},
        json.dumps(payload).encode("utf-8"),
    )


class RegistryClientHTTPContractTests(unittest.TestCase):
    def test_paths_headers_and_pagination_over_real_http(self) -> None:
        catalog_first = "/v2/_catalog?n=1000"
        catalog_second = "/v2/_catalog?n=1000&last=team%20space%2Fimage%2Bname"
        tags_first = "/v2/team%20space/image%2Bname/tags/list?n=1000"
        tags_second = (
            "/v2/team%20space/image%2Bname/tags/list?n=1000&last=v1%2Fcandidate"
        )
        manifest_path = (
            "/v2/team%20space/image%2Bname/manifests/release%2F1%20%2B%20candidate"
        )
        digest = "sha256:" + "a" * 64

        def respond(
            method: str,
            path: str,
            _headers: dict[str, str],
            _body: bytes,
        ) -> Response:
            responses = {
                ("GET", catalog_first): _json_response(
                    {"repositories": ["team space/image+name"]},
                    link=('<?n=1000&last=team%20space%2Fimage%2Bname>; rel="next"'),
                ),
                ("GET", catalog_second): _json_response(
                    {"repositories": ["team space/image+name", "other"]}
                ),
                ("GET", tags_first): _json_response(
                    {"tags": ["v1/candidate", "stable"]},
                    link='<list?n=1000&last=v1%2Fcandidate>; rel="next"',
                ),
                ("GET", tags_second): _json_response({"tags": ["stable", "v2"]}),
                ("HEAD", manifest_path): (
                    200,
                    {"Docker-Content-Digest": digest},
                    b"",
                ),
            }
            return responses.get((method, path), (404, {}, b"missing route"))

        with _RegistryHTTPServer(respond) as server:
            client = RegistryClient(server.base_url)
            self.assertEqual(
                client.catalog(),
                ["team space/image+name", "other"],
            )
            self.assertEqual(
                client.tags("team space/image+name"),
                ["v1/candidate", "stable", "v2"],
            )
            self.assertEqual(
                client.manifest_digest(
                    "team space/image+name",
                    "release/1 + candidate",
                ),
                digest,
            )

        self.assertEqual(
            [(method, path) for method, path, _headers, _body in server.requests],
            [
                ("GET", catalog_first),
                ("GET", catalog_second),
                ("GET", tags_first),
                ("GET", tags_second),
                ("HEAD", manifest_path),
            ],
        )
        manifest_headers = server.requests[-1][2]
        self.assertEqual(manifest_headers["accept"], MANIFEST_ACCEPT)

    def test_json_response_headers_remain_case_insensitive(self) -> None:
        digest = "sha256:" + "a" * 64

        with _RegistryHTTPServer(
            lambda _method, _path, _headers, _body: _json_response(
                {"schemaVersion": 2, "layers": []},
                **{"docker-content-digest": digest},
            )
        ) as server:
            client = RegistryClient(server.base_url)
            layers = client.manifest_layers("repo/a", "latest")
            _manifest, headers = client.manifest_document("repo/a", "latest")

        self.assertEqual(layers.manifest_digest, digest)
        self.assertIsInstance(headers, dict)
        self.assertEqual(headers.get("Docker-Content-Digest"), digest)
        self.assertEqual(headers.get("docker-content-digest"), digest)

    def test_pagination_links_cannot_escape_registry_origin_or_base(self) -> None:
        cases = (
            ("", "<http://example.invalid/v2/_catalog?n=1000>; rel=next"),
            ("", "</admin?n=1000>; rel=next"),
            ("", "</v2/%2e%2e/admin?n=1000>; rel=next"),
            ("/registry", "</v2/_catalog?n=1000>; rel=next"),
        )
        for base_suffix, link in cases:
            with (
                self.subTest(base_suffix=base_suffix, link=link),
                _RegistryHTTPServer(
                    lambda _method, _path, _headers, _body, link=link: _json_response(
                        {"repositories": ["repo/a"]}, link=link
                    )
                ) as server,
                self.assertRaisesRegex(ValueError, "pagination Link"),
            ):
                RegistryClient(f"{server.base_url}{base_suffix}").catalog()
            self.assertEqual(len(server.requests), 1)

    def test_json_responses_reject_malformed_and_non_object_documents(self) -> None:
        cases = (
            (b'{"repositories":', "Expecting value"),
            (b'["repo/a"]', "non-object JSON"),
        )
        for response_body, message in cases:
            with (
                self.subTest(response_body=response_body),
                _RegistryHTTPServer(
                    lambda _method, _path, _headers, _body, response_body=response_body: (
                        200,
                        {"Content-Type": "application/json"},
                        response_body,
                    )
                ) as server,
                self.assertRaisesRegex(ValueError, message),
            ):
                RegistryClient(server.base_url).catalog()

    def test_json_response_size_limit_is_enforced_over_real_http(self) -> None:
        body = b'{"repositories":["' + (b"x" * 100) + b'"]}'
        with (
            _RegistryHTTPServer(
                lambda _method, _path, _headers, _body: (
                    200,
                    {"Content-Type": "application/json"},
                    body,
                )
            ) as server,
            patch.object(
                managed_registry,
                "MAX_REGISTRY_JSON_RESPONSE_BYTES",
                32,
            ),
            self.assertRaisesRegex(ValueError, "response is too large"),
        ):
            RegistryClient(server.base_url).catalog()

    def test_http_error_body_is_read_with_a_bounded_truncated_preview(self) -> None:
        with (
            _RegistryHTTPServer(
                lambda _method, _path, _headers, _body: (
                    503,
                    {"Content-Type": "text/plain"},
                    b"x" * 100,
                )
            ) as server,
            patch.object(
                managed_registry,
                "MAX_REGISTRY_ERROR_PREVIEW_BYTES",
                16,
            ),
            self.assertRaises(RegistryRequestError) as raised,
        ):
            RegistryClient(server.base_url).tags("repo/a")

        error = raised.exception
        self.assertEqual(error.status_code, 503)
        self.assertEqual(error.method, "GET")
        self.assertEqual(error.path, "/v2/repo/a/tags/list?n=1000")
        self.assertEqual(error.body, ("x" * 16) + "...<truncated>")

    def test_manifest_rejects_invalid_layer_descriptors_over_real_http(self) -> None:
        valid_digest = "sha256:" + "a" * 64
        invalid_manifests = (
            ({"layers": [None]}, "invalid layer"),
            (
                {"layers": [{"digest": "sha256:invalid", "size": 1}]},
                "invalid layer",
            ),
            (
                {"layers": [{"digest": valid_digest, "size": "1"}]},
                "size must be an integer",
            ),
            (
                {"layers": [{"digest": valid_digest, "size": 1.5}]},
                "size must be an integer",
            ),
            (
                {"layers": [{"digest": valid_digest, "size": True}]},
                "size must be an integer",
            ),
            (
                {"layers": [{"digest": valid_digest, "size": -1}]},
                "invalid layer",
            ),
        )
        for manifest, message in invalid_manifests:
            with (
                self.subTest(manifest=manifest),
                _RegistryHTTPServer(
                    lambda _method, _path, _headers, _body, manifest=manifest: (
                        _json_response(
                            manifest,
                            **{"Docker-Content-Digest": valid_digest},
                        )
                    )
                ) as server,
                self.assertRaisesRegex(ValueError, message),
            ):
                RegistryClient(server.base_url).manifest_layers(
                    "repo/a",
                    "latest",
                )

    def test_manifest_index_requires_a_valid_linux_amd64_descriptor(self) -> None:
        valid_digest = "sha256:" + "b" * 64
        invalid_indexes = (
            (
                {
                    "manifests": [
                        {
                            "digest": valid_digest,
                            "platform": {"os": "linux", "architecture": "arm64"},
                        }
                    ]
                },
                "no Linux/amd64 image",
            ),
            (
                {
                    "manifests": [
                        {
                            "digest": "sha512:invalid",
                            "platform": {"os": "linux", "architecture": "amd64"},
                        }
                    ]
                },
                "entry is missing its digest",
            ),
        )
        for index, message in invalid_indexes:
            with (
                self.subTest(index=index),
                _RegistryHTTPServer(
                    lambda _method, _path, _headers, _body, index=index: _json_response(
                        index
                    )
                ) as server,
                self.assertRaisesRegex(ValueError, message),
            ):
                RegistryClient(server.base_url).manifest_layers(
                    "repo/a",
                    "latest",
                )

    def test_manifest_index_nesting_is_bounded(self) -> None:
        first_digest = "sha256:" + "1" * 64
        second_digest = "sha256:" + "2" * 64

        def index(digest: str) -> dict[str, object]:
            return {
                "manifests": [
                    {
                        "digest": digest,
                        "platform": {"os": "linux", "architecture": "amd64"},
                    }
                ]
            }

        def respond(
            _method: str,
            path: str,
            _headers: dict[str, str],
            _body: bytes,
        ) -> Response:
            if path.endswith("/manifests/latest"):
                return _json_response(index(first_digest))
            if path.endswith(f"/manifests/{first_digest}"):
                return _json_response(index(second_digest))
            return (500, {}, b"nesting limit was not enforced")

        with (
            _RegistryHTTPServer(respond) as server,
            self.assertRaisesRegex(ValueError, "nesting is too deep"),
        ):
            RegistryClient(server.base_url).manifest_layers("repo/a", "latest")

        self.assertEqual(len(server.requests), 2)
        self.assertTrue(
            all(
                headers["accept"] == MANIFEST_ACCEPT
                for _method, _path, headers, _body in server.requests
            )
        )

    def test_blob_upload_accepts_same_origin_locations_and_preserves_state(
        self,
    ) -> None:
        digest = "sha256:" + "d" * 64

        def respond(
            method: str,
            path: str,
            _headers: dict[str, str],
            _body: bytes,
        ) -> Response:
            if method == "POST":
                return (
                    202,
                    {
                        "Location": (
                            f"{server.base_url}/v2/team%20space/image/"
                            "blobs/uploads/id;session?state=first"
                        )
                    },
                    b"",
                )
            if method == "PATCH":
                return (
                    202,
                    {
                        "Location": (
                            "/v2/team%20space/image/blobs/uploads/id;session?state=next"
                        )
                    },
                    b"",
                )
            if method == "PUT":
                return (201, {"Docker-Content-Digest": digest}, b"")
            return (404, {}, b"")

        server = _RegistryHTTPServer(respond)
        with server:
            client = RegistryClient(server.base_url)
            location = client.start_blob_upload("team space/image")
            self.assertEqual(
                location,
                "/v2/team%20space/image/blobs/uploads/id;session?state=first",
            )
            location = client.upload_blob_chunk(location, b"chunk")
            self.assertEqual(
                location,
                "/v2/team%20space/image/blobs/uploads/id;session?state=next",
            )
            self.assertEqual(client.finish_blob_upload(location, digest), digest)

        expected_finish_path = (
            "/v2/team%20space/image/blobs/uploads/id;session?"
            f"state=next&digest=sha256%3A{'d' * 64}"
        )
        self.assertEqual(
            [(method, path) for method, path, _headers, _body in server.requests],
            [
                ("POST", "/v2/team%20space/image/blobs/uploads/"),
                (
                    "PATCH",
                    "/v2/team%20space/image/blobs/uploads/id;session?state=first",
                ),
                ("PUT", expected_finish_path),
            ],
        )
        self.assertEqual(
            server.requests[1][2]["content-type"],
            "application/octet-stream",
        )
        self.assertEqual(server.requests[1][3], b"chunk")

    def test_blob_upload_accepts_equivalent_default_port_origin(self) -> None:
        class FakeResponse:
            headers = {"Location": "/v2/uploads/id;session?state=next"}

            def close(self) -> None:
                return None

        client = RegistryClient("http://registry")
        with patch.object(client, "_request", return_value=FakeResponse()) as upload:
            location = client.upload_blob_chunk(
                "http://registry:80/v2/uploads/id;session?state=first",
                b"chunk",
            )

        self.assertEqual(location, "/v2/uploads/id;session?state=next")
        upload.assert_called_once_with(
            "/v2/uploads/id;session?state=first",
            method="PATCH",
            headers={"Content-Type": "application/octet-stream"},
            data=b"chunk",
        )

    def test_blob_upload_rejects_untrusted_or_escaping_locations(self) -> None:
        def respond(
            method: str,
            _path: str,
            _headers: dict[str, str],
            _body: bytes,
        ) -> Response:
            if method == "POST":
                return (
                    202,
                    {"Location": "https://example.invalid/v2/uploads/id"},
                    b"",
                )
            return (500, {}, b"location validation was bypassed")

        with _RegistryHTTPServer(respond) as server:
            client = RegistryClient(server.base_url)
            with self.assertRaisesRegex(ValueError, "another origin"):
                client.start_blob_upload("repo/a")
            request_count = len(server.requests)
            invalid_locations = (
                "http://example.invalid/v2/uploads/id",
                "/outside/uploads/id",
                "/v2/uploads/../admin",
                "/v2/uploads/%2e%2e/admin",
                "/v2/uploads/%252e%252e/admin",
                "/v2/uploads/%2525252525252525252e%2525252525252525252e/admin",
                "/v2/uploads/id#unexpected",
                "/v2/uploads/..\\admin",
            )
            for location in invalid_locations:
                with (
                    self.subTest(location=location),
                    self.assertRaisesRegex(ValueError, "registry upload"),
                ):
                    client.upload_blob_chunk(location, b"chunk")
            self.assertEqual(len(server.requests), request_count)


if __name__ == "__main__":
    unittest.main()
