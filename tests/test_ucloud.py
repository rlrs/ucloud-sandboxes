import unittest

from ucloud_sandboxes.ucloud import UCloudClient


class FakeUCloudClient(UCloudClient):
    def __init__(self) -> None:
        self.calls = []

    def request_json(self, method, path, *, project_id=None, params=None, json_body=None):
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
    def test_open_interactive_session_includes_vm_web_port(self) -> None:
        client = FakeUCloudClient()

        response = client.open_interactive_session(
            "project-1",
            "job-1",
            session_type="WEB",
            rank=0,
            port=8090,
        )

        self.assertEqual(response["responses"][0]["session"]["redirectClientTo"], "https://example.org")
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
