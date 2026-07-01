from pathlib import Path
from datetime import timedelta
from tempfile import TemporaryDirectory
import unittest

from ucloud_sandboxes.sandbox import (
    DockerGvisorRuntime,
    RecordingExecutor,
    SandboxFilesystemSpec,
    SandboxManager,
    SandboxSecuritySpec,
    SandboxSpec,
    SandboxStore,
)


class SandboxRuntimeTests(unittest.TestCase):
    def test_builds_docker_gvisor_run_command(self) -> None:
        runtime = DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True)
        spec = SandboxSpec(
            id="abc-123",
            image="python:3.12-slim",
            command=("python", "-c", "print('ok')"),
            env={"B": "2", "A": "1"},
            memory_mb=512,
            cpus=1.5,
            disk_mb=2048,
            labels={"purpose": "test"},
        )

        argv = runtime.create_command(spec)

        self.assertEqual(
            argv[:7],
            (
                "docker",
                "run",
                "-d",
                "--name",
                "ucloud-sandbox-abc-123",
                "--runtime",
                "runsc",
            ),
        )
        self.assertIn("--network", argv)
        self.assertIn("none", argv)
        self.assertIn("--memory", argv)
        self.assertIn("512m", argv)
        self.assertIn("--cpus", argv)
        self.assertIn("1.5", argv)
        self.assertIn("--storage-opt", argv)
        self.assertIn("size=2048m", argv)
        self.assertIn("--init", argv)
        self.assertIn("--user", argv)
        self.assertIn("1000:1000", argv)
        self.assertIn("--security-opt", argv)
        self.assertIn("no-new-privileges", argv)
        self.assertIn("--cap-drop", argv)
        self.assertIn("ALL", argv)
        self.assertIn("--pids-limit", argv)
        self.assertIn("256", argv)
        self.assertIn("--tmpfs", argv)
        self.assertIn("/tmp:rw,nosuid,nodev,size=64m", argv)
        self.assertIn("/run:rw,nosuid,nodev,size=16m", argv)
        self.assertIn("-e", argv)
        self.assertIn("A=1", argv)
        self.assertIn("B=2", argv)
        self.assertEqual(argv[-4:], ("python:3.12-slim", "python", "-c", "print('ok')"))

    def test_disk_request_requires_validated_storage_quota_support(self) -> None:
        runtime = DockerGvisorRuntime(dry_run=True)
        spec = SandboxSpec(
            id="disk",
            image="busybox",
            disk_mb=2048,
        )

        with self.assertRaises(ValueError):
            runtime.create_command(spec)

    def test_tmpfs_workspace_requires_validated_runtime_support(self) -> None:
        runtime = DockerGvisorRuntime(dry_run=True)
        spec = SandboxSpec(
            id="tmpfs",
            image="busybox",
            disk_mb=2048,
            filesystem=SandboxFilesystemSpec(enforce_disk_quota=True),
        )

        with self.assertRaises(ValueError):
            runtime.create_command(spec)

    def test_can_request_tmpfs_workspace_on_validated_runtime(self) -> None:
        runtime = DockerGvisorRuntime(dry_run=True, allow_tmpfs_workspace=True)
        spec = SandboxSpec(
            id="tmpfs",
            image="busybox",
            disk_mb=2048,
            filesystem=SandboxFilesystemSpec(enforce_disk_quota=True),
        )

        argv = runtime.create_command(spec)

        self.assertNotIn("--storage-opt", argv)
        self.assertIn("--read-only", argv)
        self.assertIn("--tmpfs", argv)
        self.assertIn("/workspace:rw,nosuid,nodev,size=2048m", argv)
        self.assertIn("/tmp:rw,nosuid,nodev,size=64m", argv)
        self.assertIn("/run:rw,nosuid,nodev,size=16m", argv)
        self.assertIn("--workdir", argv)
        self.assertIn("/workspace", argv)

    def test_compatibility_security_profile_can_opt_out_of_hardening(self) -> None:
        runtime = DockerGvisorRuntime(dry_run=True)
        spec = SandboxSpec(
            id="compat",
            image="busybox",
            memory_mb=128,
            security=SandboxSecuritySpec(
                user=None,
                cap_drop=(),
                no_new_privileges=False,
                pids_limit=None,
                init=False,
            ),
        )

        argv = runtime.create_command(spec)

        self.assertNotIn("--user", argv)
        self.assertNotIn("--security-opt", argv)
        self.assertNotIn("--cap-drop", argv)
        self.assertNotIn("--pids-limit", argv)
        self.assertNotIn("--init", argv)

    def test_rejects_invalid_sandbox_id(self) -> None:
        with self.assertRaises(ValueError):
            SandboxSpec(id="../bad", image="busybox").validate()

    def test_rejects_missing_resource_request(self) -> None:
        with self.assertRaisesRegex(ValueError, "resources are required"):
            SandboxSpec(id="no-resources", image="busybox").validate()

    def test_manager_records_planned_sandbox_in_dry_run_mode(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            executor = RecordingExecutor()
            runtime = DockerGvisorRuntime(executor=executor, dry_run=True)
            manager = SandboxManager(store, runtime)
            spec = SandboxSpec(
                id="one",
                image="busybox",
                command=("true",),
                memory_mb=128,
            )

            record, result = manager.create(spec)

            self.assertEqual(record.state, "planned")
            self.assertEqual(result.exit_code, 0)
            self.assertEqual(executor.commands, [])
            self.assertEqual(len(manager.list()), 1)

    def test_manager_sums_requested_resources(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            runtime = DockerGvisorRuntime(dry_run=True, allow_storage_opt_quota=True)
            manager = SandboxManager(store, runtime)
            manager.create(
                SandboxSpec(
                    id="one",
                    image="busybox",
                    cpus=0.5,
                    memory_mb=256,
                    disk_mb=1024,
                )
            )
            manager.create(
                SandboxSpec(
                    id="two",
                    image="busybox",
                    cpus=1.0,
                    memory_mb=512,
                    disk_mb=2048,
                )
            )

            resources = manager.requested_resources()

            self.assertEqual(resources.vcpu, 1.5)
            self.assertEqual(resources.memory_mb, 768)
            self.assertEqual(resources.disk_mb, 3072)

    def test_manager_cleans_up_expired_sandboxes(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            executor = RecordingExecutor()
            runtime = DockerGvisorRuntime(executor=executor, dry_run=True)
            manager = SandboxManager(store, runtime)
            manager.create(
                SandboxSpec(
                    id="short",
                    image="busybox",
                    ttl_seconds=1,
                    memory_mb=128,
                )
            )

            expired = manager.cleanup_expired()

            self.assertEqual(expired, [])
            records = store.load()
            record = records["short"]
            expired = manager.cleanup_expired(
                now=record.created_at.replace(microsecond=0)
            )
            self.assertEqual(expired, [])
            expired = manager.cleanup_expired(
                now=record.created_at.replace(microsecond=0) + timedelta(seconds=2)
            )

            self.assertEqual([record.spec.id for record in expired], ["short"])
            self.assertEqual(store.load(), {})

    def test_ssh_enabled_sandbox_gets_port_and_publish_flag(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = SandboxStore(Path(raw_dir) / "sandboxes.json")
            runtime = DockerGvisorRuntime(dry_run=True)
            manager = SandboxManager(
                store,
                runtime,
                ssh_port_range=(23000, 23001),
            )
            spec = SandboxSpec.from_dict(
                {
                    "id": "ssh-one",
                    "image": "sandbox-ssh:latest",
                    "memory_mb": 128,
                    "network": "bridge",
                    "ssh": {
                        "enabled": True,
                        "user": "sandbox",
                        "authorized_keys": ["ssh-ed25519 AAAA test"],
                    },
                }
            )

            record, result = manager.create(spec)

            self.assertEqual(record.spec.ssh.host_port, 23000)
            self.assertIn("-p", result.argv)
            self.assertIn("127.0.0.1:23000:22", result.argv)
            self.assertEqual(
                record.to_dict()["ssh"]["command"],
                "ssh -p 23000 sandbox@127.0.0.1",
            )

    def test_ssh_requires_bridge_network(self) -> None:
        spec = SandboxSpec.from_dict(
            {
                "id": "bad-ssh",
                "image": "sandbox-ssh:latest",
                "memory_mb": 128,
                "ssh": {"enabled": True, "host_port": 23000},
            }
        )

        with self.assertRaises(ValueError):
            spec.validate()

    def test_builds_docker_exec_command(self) -> None:
        runtime = DockerGvisorRuntime(dry_run=True)

        argv = runtime.exec_command(
            "abc-123",
            ("python", "-c", "print('ok')"),
            env={"B": "2", "A": "1"},
            working_dir="/workspace",
            interactive=True,
        )

        self.assertEqual(
            argv,
            (
                "docker",
                "exec",
                "-i",
                "-w",
                "/workspace",
                "-e",
                "A=1",
                "-e",
                "B=2",
                "ucloud-sandbox-abc-123",
                "python",
                "-c",
                "print('ok')",
            ),
        )

    def test_builds_docker_file_copy_commands(self) -> None:
        with TemporaryDirectory() as raw_dir:
            source = Path(raw_dir) / "payload.txt"
            target = Path(raw_dir) / "download.txt"
            source.write_bytes(b"hello")
            runtime = DockerGvisorRuntime(dry_run=True)

            upload = runtime.copy_to_container("abc-123", source, "/workspace/payload.txt")
            download = runtime.copy_from_container(
                "abc-123",
                "/workspace/payload.txt",
                target,
            )

        self.assertEqual(
            upload.argv,
            (
                "docker",
                "cp",
                str(source),
                "ucloud-sandbox-abc-123:/workspace/payload.txt",
            ),
        )
        self.assertEqual(
            download.argv,
            (
                "docker",
                "cp",
                "ucloud-sandbox-abc-123:/workspace/payload.txt",
                str(target),
            ),
        )

    def test_streams_file_upload_and_download_through_exec(self) -> None:
        executor = RecordingExecutor(stdout_bytes=b"downloaded bytes\n")
        runtime = DockerGvisorRuntime(executor=executor)

        upload = runtime.write_file_to_container(
            "abc-123",
            "/workspace/payload.txt",
            b"uploaded bytes\n",
            owner="1000:1000",
        )
        content, download = runtime.read_file_from_container(
            "abc-123",
            "/workspace/payload.txt",
        )

        self.assertEqual(executor.inputs[0], b"uploaded bytes\n")
        self.assertIsNone(executor.inputs[1])
        self.assertEqual(content, b"downloaded bytes\n")
        self.assertEqual(
            upload.argv[:9],
            (
                "docker",
                "exec",
                "-i",
                "-e",
                "UCLOUD_SANDBOX_FILE=/workspace/payload.txt",
                "-e",
                "UCLOUD_SANDBOX_OWNER=1000:1000",
                "-u",
                "0",
            ),
        )
        self.assertEqual(
            download.argv[:6],
            (
                "docker",
                "exec",
                "-e",
                "UCLOUD_SANDBOX_FILE=/workspace/payload.txt",
                "-u",
                "0",
            ),
        )

    def test_container_file_copy_rejects_directory_paths(self) -> None:
        runtime = DockerGvisorRuntime(dry_run=True)

        with TemporaryDirectory() as raw_dir:
            source = Path(raw_dir) / "payload.txt"
            source.write_bytes(b"hello")
            with self.assertRaises(ValueError):
                runtime.copy_to_container("abc-123", source, "/workspace/")


if __name__ == "__main__":
    unittest.main()
