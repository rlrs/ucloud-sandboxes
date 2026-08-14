import base64
import os
from pathlib import Path
import shutil
import subprocess
import sys
from tempfile import TemporaryDirectory
import time
import unittest

from ucloud_sandboxes.managed_process import (
    DEFAULT_MAX_STDERR_BYTES,
    DEFAULT_MAX_STDOUT_BYTES,
    MANAGED_PROCESS_PROTOCOL_VERSION,
    ManagedProcessError,
    ManagedProcessLogChunk,
    ManagedProcessRecord,
    ManagedProcessStart,
    control_request_bytes,
    parse_control_response,
)
from ucloud_sandboxes.sandbox import SandboxSpec


class ManagedProcessProtocolTests(unittest.TestCase):
    def test_managed_sandbox_contract_requires_parkable_empty_primary_slot(
        self,
    ) -> None:
        with self.assertRaisesRegex(ValueError, "parkable"):
            SandboxSpec(
                id="managed",
                image="busybox",
                memory_mb=128,
                disk_mb=128,
                managed_process=True,
            ).validate()
        with self.assertRaisesRegex(ValueError, "command"):
            SandboxSpec(
                id="managed",
                image="busybox",
                command=("sleep", "1"),
                memory_mb=128,
                disk_mb=128,
                parkable=True,
                managed_process=True,
            ).validate()

    def test_start_payload_is_bounded_and_deterministic(self) -> None:
        start = ManagedProcessStart.from_dict(
            {
                "job_id": "rollout-1",
                "argv": ["python", "harness.py"],
                "env": {"Z": "last", "A": "first"},
                "cwd": "/workspace",
            }
        )

        self.assertEqual(start.max_stdout_bytes, DEFAULT_MAX_STDOUT_BYTES)
        self.assertEqual(start.max_stderr_bytes, DEFAULT_MAX_STDERR_BYTES)
        self.assertEqual(
            start.control_payload(uid=1000, gid=1001)["env"],
            {"A": "first", "Z": "last"},
        )
        for field, value in (
            ("argv", ["true"] * 4097),
            ("max_stdout_bytes", 0),
            ("max_stderr_bytes", -1),
        ):
            with (
                self.subTest(field=field),
                self.assertRaisesRegex(
                    ValueError,
                    "bounded|positive",
                ),
            ):
                ManagedProcessStart.from_dict(
                    {
                        "job_id": "rollout-1",
                        "argv": ["true"],
                        field: value,
                    }
                )

    @unittest.skipUnless(
        sys.platform.startswith("linux"),
        "requires a Linux managed-process binary",
    )
    def test_python_start_payload_is_accepted_by_go_supervisor(self) -> None:
        go = shutil.which("go")
        if go is None:
            self.skipTest("Go toolchain is unavailable")
        helper_dir = Path(__file__).resolve().parents[1] / "runtime/managed_process"
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            binary = root / "managed-process"
            build_env = dict(os.environ)
            for inherited_go_setting in ("GOARCH", "GOFLAGS", "GOOS"):
                build_env.pop(inherited_go_setting, None)
            build_env.update(
                {
                    "CGO_ENABLED": "0",
                    "GOCACHE": str(root / "go-build-cache"),
                    "GOTOOLCHAIN": "local",
                }
            )
            build = subprocess.run(
                [go, "build", "-trimpath", "-o", str(binary), "."],
                cwd=helper_dir,
                env=build_env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=120,
                check=False,
            )
            self.assertEqual(
                build.returncode,
                0,
                build.stderr.decode("utf-8", errors="replace"),
            )
            state_dir = root / "state"
            supervisor = subprocess.Popen(
                [str(binary), "supervise", "--state-dir", str(state_dir)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            try:
                socket_path = state_dir / "control.sock"
                deadline = time.monotonic() + 5
                while not socket_path.exists():
                    if supervisor.poll() is not None:
                        stderr = supervisor.stderr.read() if supervisor.stderr else b""
                        self.fail(
                            "Go supervisor exited before creating its socket: "
                            + stderr.decode("utf-8", errors="replace")
                        )
                    if time.monotonic() >= deadline:
                        self.fail("Go supervisor did not create its control socket")
                    time.sleep(0.01)

                start = ManagedProcessStart.from_dict(
                    {
                        "job_id": "python-go-compat",
                        "argv": ["/bin/sh", "-c", "exit 0"],
                        "env": {"PATH": "/usr/bin:/bin"},
                        "cwd": "/",
                        "max_stdout_bytes": 1024,
                        "max_stderr_bytes": 1024,
                    }
                )
                control = subprocess.run(
                    [str(binary), "ctl", "--socket", str(socket_path)],
                    input=control_request_bytes(
                        start.control_payload(uid=os.getuid(), gid=os.getgid())
                    ),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=10,
                    check=False,
                )
                self.assertEqual(
                    control.returncode,
                    0,
                    control.stderr.decode("utf-8", errors="replace"),
                )
                response = parse_control_response(control.stdout)
                self.assertEqual(
                    response.get("version"),
                    MANAGED_PROCESS_PROTOCOL_VERSION,
                )
                record = ManagedProcessRecord.from_control_response(
                    response,
                    sandbox_id="sandbox-1",
                    sandbox_generation=1,
                )
                self.assertEqual(record.job_id, start.job_id)
                self.assertEqual(record.state, "running")
                self.assertEqual(record.sequence, 2)
            finally:
                if supervisor.poll() is None:
                    supervisor.kill()
                supervisor.wait(timeout=5)

    def test_invalid_environment_and_identity_fail_closed(self) -> None:
        for payload in (
            {"job_id": "../job", "argv": ["true"]},
            {"job_id": "job", "argv": []},
            {"job_id": "job", "argv": ["true"], "env": {"BAD-KEY": "x"}},
        ):
            with self.subTest(payload=payload):
                with self.assertRaises(ValueError):
                    ManagedProcessStart.from_dict(payload)

    def test_control_response_is_bound_to_sandbox_generation(self) -> None:
        record = ManagedProcessRecord.from_control_response(
            {
                "ok": True,
                "job": {
                    "job_id": "rollout-1",
                    "spec_sha256": "a" * 64,
                    "state": "running",
                    "pid": 42,
                    "sequence": 2,
                },
            },
            sandbox_id="sandbox-1",
            sandbox_generation=3,
        )

        self.assertEqual(record.sandbox_generation, 3)
        self.assertFalse(record.terminal)
        with self.assertRaises(ManagedProcessError):
            ManagedProcessRecord.from_control_response(
                {"ok": False, "error": "conflict"},
                sandbox_id="sandbox-1",
                sandbox_generation=3,
            )

    def test_log_chunk_validates_base64_and_offsets(self) -> None:
        chunk = ManagedProcessLogChunk.from_control_response(
            {
                "ok": True,
                "stream": "stdout",
                "offset": 2,
                "next_offset": 5,
                "data": base64.b64encode(b"abc").decode("ascii"),
                "eof": True,
            }
        )

        self.assertEqual(chunk.data, b"abc")
        cases = (
            {
                "ok": True,
                "stream": "stdout",
                "offset": 0,
                "next_offset": 1,
                "data": "not-base64!",
            },
            {
                "ok": True,
                "stream": "stdout",
                "offset": -1,
                "next_offset": 2,
                "data": base64.b64encode(b"abc").decode("ascii"),
            },
            {
                "ok": True,
                "stream": "stdout",
                "offset": 2,
                "next_offset": 6,
                "data": base64.b64encode(b"abc").decode("ascii"),
            },
            {
                "ok": True,
                "stream": "combined",
                "offset": 0,
                "next_offset": 0,
                "data": "",
            },
        )
        for response in cases:
            with (
                self.subTest(response=response),
                self.assertRaises(ManagedProcessError),
            ):
                ManagedProcessLogChunk.from_control_response(response)


if __name__ == "__main__":
    unittest.main()
