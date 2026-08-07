import base64
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from ucloud_sandboxes.managed_process import (
    DEFAULT_MAX_STDERR_BYTES,
    DEFAULT_MAX_STDOUT_BYTES,
    ManagedProcessError,
    ManagedProcessLogChunk,
    ManagedProcessRecord,
    ManagedProcessStart,
)
from ucloud_sandboxes.sandbox import SandboxSpec
from ucloud_sandboxes.direct_warden import DirectRunscWarden, DirectSandbox


class ManagedProcessProtocolTests(unittest.TestCase):
    def test_managed_sandbox_contract_requires_parkable_empty_primary_slot(self) -> None:
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
        with self.assertRaises(ManagedProcessError):
            ManagedProcessLogChunk.from_control_response(
                {
                    "ok": True,
                    "stream": "stdout",
                    "offset": 0,
                    "next_offset": 1,
                    "data": "not-base64!",
                }
            )

    def test_checkpoint_identity_binds_the_guest_job_ledger(self) -> None:
        with TemporaryDirectory() as raw:
            bundle = Path(raw) / "bundle"
            ledger = bundle / "rootfs" / ".ucloud-managed" / "state.json"
            ledger.parent.mkdir(parents=True)
            (bundle / "config.json").write_text(
                json.dumps(
                    {
                        "annotations": {
                            "dev.ucloud-sandboxes.managed-process": "v1"
                        }
                    }
                ),
                encoding="utf-8",
            )
            ledger.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "job_id": "rollout-1",
                        "spec_sha256": "a" * 64,
                        "state": "running",
                        "sequence": 2,
                    }
                ),
                encoding="utf-8",
            )
            sandbox = DirectSandbox(
                sandbox_id="managed",
                sandbox_generation=1,
                container_id="b" * 64,
                spec_sha256="c" * 64,
                rootfs_sha256="d" * 64,
                bundle=bundle,
                memory_directory="managed.memory",
            )

            before = DirectRunscWarden._managed_process_ledger_digest(sandbox)
            ledger.write_text(
                ledger.read_text(encoding="utf-8").replace(
                    '"sequence": 2', '"sequence": 3'
                ),
                encoding="utf-8",
            )
            after = DirectRunscWarden._managed_process_ledger_digest(sandbox)

        self.assertNotEqual(before, after)


if __name__ == "__main__":
    unittest.main()
