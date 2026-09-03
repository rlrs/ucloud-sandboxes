import unittest
from unittest.mock import patch
from pathlib import Path
from tempfile import TemporaryDirectory

from ucloud_sandboxes.providers.ucloud.api import (
    MAX_UCLOUD_ERROR_PREVIEW_BYTES,
    MAX_UCLOUD_JSON_RESPONSE_BYTES,
    UCloudClient,
    UCloudError,
    UCloudTransportError,
    SessionState,
    SessionStore,
)
from ucloud_sandboxes.providers.ucloud.adapter import (
    UCloudCreateProfile,
    UCloudProvider,
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
        self.assertTrue(
            all(call.get("itemsPerPage") == "1000" for call in client.calls)
        )

    def test_browse_all_jobs_applies_state_filter_to_every_page(self) -> None:
        class EmptyClient(FakeUCloudClient):
            def request_json(self, *args, **kwargs):
                self.calls.append({"params": kwargs.get("params")})
                return {"items": [], "next": None}

        client = EmptyClient()

        client.browse_all_jobs("project-1", filter_state="RUNNING")

        self.assertEqual(client.calls[0]["params"]["filterState"], "RUNNING")

    def test_browse_all_jobs_fails_closed_on_repeated_cursor(self) -> None:
        class RepeatingCursorClient(FakeUCloudClient):
            def request_json(self, *args, **kwargs):
                del args, kwargs
                return {"items": [{"id": "job-1"}], "next": "same"}

        with self.assertRaisesRegex(UCloudError, "repeated a cursor"):
            RepeatingCursorClient().browse_all_jobs("project-1")

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
            "ucloud_sandboxes.providers.ucloud.api.request.urlopen",
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
            "ucloud_sandboxes.providers.ucloud.api.request.urlopen",
            side_effect=http_error,
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

    def test_provider_uses_narrow_active_inventory_between_full_censuses(self) -> None:
        deployment_id = "deployment-1"

        def job(job_id: str, state: str, *, managed: bool = True) -> dict:
            return {
                "id": job_id,
                "createdAt": 1,
                "specification": {
                    "labels": (
                        {
                            "ucloud-sandboxes/deployment": deployment_id,
                            "ucloud-sandboxes/node": "true",
                        }
                        if managed
                        else {}
                    )
                },
                "status": {
                    "state": state,
                    "startedAt": 1 if state != "IN_QUEUE" else None,
                },
                "updates": [],
            }

        class InventoryClient:
            def __init__(self) -> None:
                self.calls: list[str | None] = []

            def browse_all_jobs(self, _project_id, **kwargs):
                state = kwargs.get("filter_state")
                self.calls.append(state)
                if state is None:
                    return [
                        job("node-1", "RUNNING"),
                        job("other", "RUNNING", managed=False),
                        job("old", "SUCCESS"),
                    ]
                return [job("node-1", "RUNNING")] if state == "RUNNING" else []

        now = [0.0]
        client = InventoryClient()
        profile = UCloudCreateProfile(None, require_private_network=False)
        provider = UCloudProvider(
            "project-1",
            client=client,  # type: ignore[arg-type]
            sandbox_profile=profile,
            builder_profile=profile,
            deployment_id=deployment_id,
            monotonic=lambda: now[0],
        )

        first = provider.list_instances()
        now[0] = 1.0
        second = provider.list_instances()

        self.assertEqual([item.id for item in first], ["node-1"])
        self.assertEqual([item.id for item in second], ["node-1"])
        self.assertEqual(
            client.calls,
            [None, "IN_QUEUE", "RUNNING", "SUSPENDED"],
        )

    def test_provider_avoids_idle_inventory_calls_until_next_census(self) -> None:
        class EmptyInventoryClient:
            def __init__(self) -> None:
                self.calls = 0

            def browse_all_jobs(self, _project_id, **_kwargs):
                self.calls += 1
                return []

        now = [0.0]
        client = EmptyInventoryClient()
        profile = UCloudCreateProfile(None, require_private_network=False)
        provider = UCloudProvider(
            "project-1",
            client=client,  # type: ignore[arg-type]
            sandbox_profile=profile,
            builder_profile=profile,
            deployment_id="deployment-1",
            full_inventory_refresh_seconds=300,
            monotonic=lambda: now[0],
        )

        self.assertEqual(provider.list_instances(), [])
        now[0] = 299.0
        self.assertEqual(provider.list_instances(), [])
        now[0] = 300.0
        self.assertEqual(provider.list_instances(), [])

        self.assertEqual(client.calls, 2)

    def test_provider_retries_full_census_until_created_job_is_visible(self) -> None:
        deployment_id = "deployment-1"

        class EventuallyConsistentClient:
            def __init__(self) -> None:
                self.browse_calls = 0

            def submit_jobs(self, _project_id, _request):
                return {"responses": [{"id": "node-1"}]}

            def browse_all_jobs(self, _project_id, **_kwargs):
                self.browse_calls += 1
                if self.browse_calls == 1:
                    return []
                return [
                    {
                        "id": "node-1",
                        "createdAt": 1,
                        "specification": {
                            "labels": {
                                "ucloud-sandboxes/deployment": deployment_id,
                                "ucloud-sandboxes/node": "true",
                            }
                        },
                        "status": {"state": "IN_QUEUE", "startedAt": None},
                        "updates": [],
                    }
                ]

        now = [0.0]
        client = EventuallyConsistentClient()
        profile = UCloudCreateProfile(None, require_private_network=False)
        provider = UCloudProvider(
            "project-1",
            client=client,  # type: ignore[arg-type]
            sandbox_profile=profile,
            builder_profile=profile,
            deployment_id=deployment_id,
            monotonic=lambda: now[0],
        )

        self.assertEqual(provider.create({}).status, "accepted")
        self.assertEqual(provider.list_instances(), [])
        now[0] = 1.0
        observed = provider.list_instances()

        self.assertEqual([item.id for item in observed], ["node-1"])
        self.assertEqual(client.browse_calls, 2)


if __name__ == "__main__":
    unittest.main()
