import unittest
from unittest.mock import patch
from pathlib import Path
from tempfile import TemporaryDirectory

from ucloud_sandboxes.ucloud import (
    MAX_UCLOUD_ERROR_PREVIEW_BYTES,
    MAX_UCLOUD_JSON_RESPONSE_BYTES,
    UCloudClient,
    UCloudError,
    UCloudTransportError,
    SessionState,
    SessionStore,
)


class FakeUCloudClient(UCloudClient):
    def __init__(self) -> None:
        self.calls = []

    def request_json(
        self, method, path, *, project_id=None, params=None, json_body=None
    ):
        self.calls.append(
            {
                "method": method,
                "path": path,
                "project_id": project_id,
                "params": params,
                "json_body": json_body,
            }
        )
        return {"responses": [{"session": {"redirectClientTo": "https://example.org"}}]}


class UCloudClientTests(unittest.TestCase):
    def test_session_store_is_private_atomic_and_rejects_invalid_data(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "session.json"
            store = SessionStore(path)
            session = SessionState(
                cookies={"refreshToken": "refresh"},
                headers={"Authorization": "Bearer access"},
            )

            store.save(session)

            self.assertEqual(store.load().cookies, session.cookies)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(list(path.parent.glob("*.tmp")), [])
            path.write_text("not-json", encoding="utf-8")
            with self.assertRaisesRegex(UCloudError, "Invalid UCloud session"):
                store.load()

    def test_browse_all_jobs_reads_every_page(self) -> None:
        class PaginatedClient(FakeUCloudClient):
            def request_json(
                self,
                method,
                path,
                *,
                project_id=None,
                params=None,
                json_body=None,
            ):
                self.calls.append(dict(params or {}))
                cursor = (params or {}).get("next")
                if cursor is None:
                    return {"items": [{"id": "job-1"}], "next": "page-2"}
                if cursor == "page-2":
                    return {"items": [{"id": "job-2"}], "next": "page-3"}
                return {"items": [{"id": "job-3"}], "next": None}

        client = PaginatedClient()

        jobs = client.browse_all_jobs("project-1")

        self.assertEqual([job["id"] for job in jobs], ["job-1", "job-2", "job-3"])
        self.assertEqual(
            [call.get("next") for call in client.calls], [None, "page-2", "page-3"]
        )

    def test_browse_all_jobs_fails_closed_on_repeated_cursor(self) -> None:
        class RepeatingCursorClient(FakeUCloudClient):
            def request_json(self, *args, **kwargs):
                del args, kwargs
                return {"items": [{"id": "job-1"}], "next": "same"}

        with self.assertRaisesRegex(UCloudError, "repeated a cursor"):
            RepeatingCursorClient().browse_all_jobs("project-1")

    def test_complete_browse_rejects_page_limit_before_end(self) -> None:
        class MorePagesClient(FakeUCloudClient):
            def request_json(self, *args, **kwargs):
                del args, kwargs
                return {"items": [{"id": "job-1"}], "next": "more"}

        with self.assertRaisesRegex(UCloudError, "max_pages"):
            MorePagesClient().browse_jobs(
                "project-1",
                max_pages=1,
                require_complete=True,
            )

    def test_browse_ssh_keys_reads_every_page_and_rejects_cursor_loops(self) -> None:
        class PaginatedClient(FakeUCloudClient):
            def request_json(self, method, path, **kwargs):
                del method, path
                params = kwargs.get("params") or {}
                self.calls.append(dict(params))
                if params.get("next") is None:
                    return {"items": [{"id": "key-1"}], "next": "page-2"}
                return {"items": [{"id": "key-2"}], "next": None}

        client = PaginatedClient()
        self.assertEqual(
            [item["id"] for item in client.browse_ssh_keys()],
            ["key-1", "key-2"],
        )

        class RepeatingClient(FakeUCloudClient):
            def request_json(self, *args, **kwargs):
                del args, kwargs
                return {"items": [], "next": "same"}

        with self.assertRaisesRegex(UCloudError, "repeated a cursor"):
            RepeatingClient().browse_ssh_keys()

    def test_open_json_bounds_success_and_error_responses(self) -> None:
        class Response:
            status = 200

            def __init__(self, payload: bytes) -> None:
                self.payload = payload

            def __enter__(self):
                return self

            def __exit__(self, *args):
                del args

            def read(self, amount: int) -> bytes:
                return self.payload[:amount]

        client = FakeUCloudClient()
        with patch(
            "ucloud_sandboxes.ucloud.request.urlopen",
            return_value=Response(b"x" * (MAX_UCLOUD_JSON_RESPONSE_BYTES + 1)),
        ):
            with self.assertRaisesRegex(UCloudTransportError, "exceeded"):
                client._open_json(object())

        # The smaller diagnostic bound is independently enforced for HTTP errors.
        from io import BytesIO
        from urllib.error import HTTPError

        http_error = HTTPError(
            "https://example.test",
            500,
            "failure",
            {},
            BytesIO(b"x" * (MAX_UCLOUD_ERROR_PREVIEW_BYTES + 1)),
        )
        with patch(
            "ucloud_sandboxes.ucloud.request.urlopen", side_effect=http_error
        ):
            with self.assertRaisesRegex(UCloudTransportError, "exceeded"):
                client._open_json(object())

    def test_open_interactive_session_includes_vm_web_port(self) -> None:
        client = FakeUCloudClient()

        response = client.open_interactive_session(
            "project-1",
            "job-1",
            session_type="WEB",
            rank=0,
            port=8090,
        )

        self.assertEqual(
            response["responses"][0]["session"]["redirectClientTo"],
            "https://example.org",
        )
        self.assertEqual(
            client.calls[0],
            {
                "method": "POST",
                "path": "/api/jobs/interactiveSession",
                "project_id": "project-1",
                "params": None,
                "json_body": {
                    "type": "bulk",
                    "items": [
                        {
                            "id": "job-1",
                            "rank": 0,
                            "sessionType": "WEB",
                            "port": 8090,
                        }
                    ],
                },
            },
        )


if __name__ == "__main__":
    unittest.main()
