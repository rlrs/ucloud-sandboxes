import unittest

from ucloud_sandboxes.http_contract import match_sandbox_http_route


class SandboxHttpContractTests(unittest.TestCase):
    def test_public_routes_have_one_exact_wake_contract(self) -> None:
        cases = (
            ("GET", "/v1/sandboxes/s1/environment", "environment", False),
            ("DELETE", "/v1/sandboxes/s1", "delete", False),
            ("GET", "/v1/sandboxes/s1/files", "files", True),
            ("PUT", "/v1/sandboxes/s1/files", "files", True),
            ("GET", "/v1/sandboxes/s1/ssh", "ssh", True),
            ("POST", "/v1/sandboxes/s1/exec", "exec", True),
            ("POST", "/v1/sandboxes/s1/jobs", "job_create", True),
            ("GET", "/v1/sandboxes/s1/jobs/j1", "job_status", False),
            ("POST", "/v1/sandboxes/s1/jobs/j1/signal", "job_signal", True),
            (
                "GET",
                "/v1/sandboxes/s1/jobs/j1/logs/stdout",
                "job_logs",
                True,
            ),
        )
        for method, path, action, wakes in cases:
            with self.subTest(method=method, path=path):
                route = match_sandbox_http_route(method, path)
                self.assertIsNotNone(route)
                assert route is not None
                self.assertEqual(route.action, action)
                self.assertTrue(route.sdk_public)
                self.assertEqual(route.wakes, wakes)

    def test_internal_lifecycle_routes_are_not_public(self) -> None:
        park = match_sandbox_http_route("POST", "/v1/sandboxes/s1/park")
        wake = match_sandbox_http_route("POST", "/v1/sandboxes/s1/wake")

        self.assertIsNotNone(park)
        self.assertIsNotNone(wake)
        assert park is not None and wake is not None
        self.assertFalse(park.sdk_public)
        self.assertFalse(park.wakes)
        self.assertFalse(wake.sdk_public)
        self.assertTrue(wake.wakes)

    def test_suffixes_and_encoded_slashes_cannot_smuggle_a_public_route(self) -> None:
        for method, path in (
            ("POST", "/v1/sandboxes/s1/exec/extra"),
            ("GET", "/v1/sandboxes/s1/jobs/j1/logs/stdout/extra"),
            ("POST", "/v1/sandboxes/s1%2Fother/exec"),
            ("GET", "/v1/sandboxes//files"),
            ("POST", "/v1/sandboxes/s1/snapshot"),
        ):
            with self.subTest(method=method, path=path):
                self.assertIsNone(match_sandbox_http_route(method, path))


if __name__ == "__main__":
    unittest.main()
