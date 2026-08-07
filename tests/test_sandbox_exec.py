import unittest

from ucloud_sandboxes.sandbox_exec import (
    SandboxExecSpec,
    exec_session_route,
    new_exec_session_id,
)


class SandboxExecProtocolTests(unittest.TestCase):
    def test_session_ids_always_carry_validated_route_identity(self) -> None:
        session_id = new_exec_session_id(
            "sandbox-one",
            node_id="node-one",
            job_id="job-one",
        )

        route = exec_session_route(session_id)
        self.assertIsNotNone(route)
        assert route is not None
        self.assertEqual(route.sandbox_id, "sandbox-one")
        self.assertEqual(route.node_id, "node-one")
        self.assertEqual(route.job_id, "job-one")
        with self.assertRaises(ValueError):
            new_exec_session_id("sandbox-one")
        self.assertIsNone(exec_session_route("exec-deadbeef"))

    def test_exec_payload_requires_the_canonical_schema(self) -> None:
        payload = {
            "command": ["/bin/echo", "ok"],
            "env": {"MODE": "test"},
            "working_dir": "/workspace",
            "stdin": False,
            "tty": False,
        }

        spec = SandboxExecSpec.from_dict(payload, sandbox_id="sandbox-one")

        self.assertEqual(spec.command, ("/bin/echo", "ok"))
        self.assertEqual(spec.env, {"MODE": "test"})
        for invalid in (
            {**payload, "command": "/bin/echo"},
            {**payload, "env": {"MODE": 1}},
            {**payload, "stdin": 0},
            {**payload, "sandbox_id": "sandbox-one"},
        ):
            with self.assertRaises(ValueError):
                SandboxExecSpec.from_dict(invalid, sandbox_id="sandbox-one")


if __name__ == "__main__":
    unittest.main()
