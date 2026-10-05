"""PUT /v1/sandboxes/{id}/archive: one helper exec writes a tar's files."""

from contextlib import contextmanager
from dataclasses import replace
from http.client import HTTPConnection
from io import BytesIO
import json
from pathlib import Path
import stat
import subprocess
import tarfile
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import tests.test_direct_provisioner as fixtures
from tests.test_control_plane import _gateway_server, _running_server, _seed_gateway_node
from tests.test_guest_agent import build_agent
from ucloud_sandboxes.capabilities import ARCHIVE_UPLOAD_CAPABILITY
from ucloud_sandboxes.direct_service import DirectExecResult, DirectSandboxService
from ucloud_sandboxes.http_server import TRANSFER_CHUNK_BYTES
from ucloud_sandboxes.sandbox import SandboxFileTooLargeError, SandboxFilesystemSpec
from ucloud_sandboxes import upload_archive
from ucloud_sandboxes.upload_archive import (
    ArchivePlan, archive_helper_unsupported, normalized_archive, plan_archive, sandbox_archive_extract_script,
)

TEST_TIER = "contract"
REG, DIR = tarfile.REGTYPE, tarfile.DIRTYPE


def _tar(*members, compress=False, pax=None) -> bytes:
    """members: (name, data[, type[, mode]])."""
    buffer = BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz" if compress else "w", format=tarfile.PAX_FORMAT) as archive:
        for name, data, *rest in members:
            info = tarfile.TarInfo(name)
            info.type, info.mode = (rest + [REG, 0o644][len(rest):])[:2]
            info.linkname = "/etc/passwd" if info.type in {tarfile.SYMTYPE, tarfile.LNKTYPE} else ""
            info.size, info.pax_headers = (len(data) if info.isreg() else 0), dict(pax or {})
            archive.addfile(info, BytesIO(data) if info.isreg() else None)
    return buffer.getvalue()


def _normalized(archive: bytes) -> tuple[ArchivePlan, bytes]:
    source = BytesIO(archive)
    plan = plan_archive(source, max_bytes=1024 ** 2)
    with normalized_archive(source, plan, None) as stdin:
        return plan, stdin["input_bytes"]


class ArchivePlanTests(unittest.TestCase):
    def test_plan_keeps_canonical_files_and_rejects_unsafe_members(self):
        archive = _tar(("./", b"", DIR), ("./tools/run.sh", b"#!", REG, 0o4755), ("tools", b"", DIR),
                       ("empty//leaf/", b"", DIR), compress=True)
        plan, normalized = _normalized(archive)
        self.assertEqual(plan, ArchivePlan(files=1, directory_members=2, total_bytes=2,
                                           empty_directories=("empty/leaf",)))
        with tarfile.open(fileobj=BytesIO(normalized)) as members:
            self.assertEqual([(m.name, m.type, m.mode, m.uid) for m in members], [("tools/run.sh", REG, 0o755, 0)])
        unsafe = {
            "symlink": _tar(("link", b"", tarfile.SYMTYPE)),
            "hardlink": _tar(("link", b"", tarfile.LNKTYPE)),
            "fifo": _tar(("fifo", b"", tarfile.FIFOTYPE)),
            "device": _tar(("tty", b"", tarfile.CHRTYPE)),
            "absolute": _tar(("/etc/passwd", b"x")),
            "parent": _tar(("a/../../escape", b"x")),
            "control": _tar(("a\nb", b"x")),
            "duplicate": _tar(("a", b"x"), ("./a", b"y")),
            "file parent": _tar(("a", b"x"), ("a/b", b"y")),
            "file and directory": _tar(("a", b"x"), ("a", b"", DIR)),
            "pax bomb": _tar(("a", b"x"), pax={"comment": "x" * 70_000}),
            "not a tar": b"x" * 1024,
            "empty": b"",
        }
        for name, archive in unsafe.items():
            with self.subTest(name), self.assertRaises(ValueError):
                plan_archive(BytesIO(archive), max_bytes=1024)
        with self.assertRaises(SandboxFileTooLargeError):
            plan_archive(BytesIO(_tar(("a", b"abc"), ("b", b"abc"))), max_bytes=5)
        with patch.object(upload_archive, "MAX_ARCHIVE_MEMBERS", 2), self.assertRaisesRegex(ValueError, "members"):
            plan_archive(BytesIO(_tar(("a", b""), ("b", b""), ("c", b""))), max_bytes=5)

    def test_shell_and_static_helpers_extract_the_normalized_archive(self):
        plan, payload = _normalized(_tar(("run.sh", b"#!/bin/sh\n", REG, 0o4755), ("lib/a.py", b"a"),
                                         ("old", b"new", REG, 0o600), ("empty/leaf", b"", DIR)))
        with TemporaryDirectory() as raw:
            binary = str(build_agent(Path(raw)))
            for helper in ("shell", "static"):
                with self.subTest(helper), TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    (root / "dest/lib").mkdir(parents=True, mode=0o700)
                    (root / "outside").write_bytes(b"keep")
                    (root / "dest/old").symlink_to(root / "outside")
                    argv = (["/bin/sh", "-c", sandbox_archive_extract_script(), "ucloud-extract", str(root / "dest")]
                            if helper == "shell" else [binary, "files", "extract", str(root / "dest"), "64"])
                    result = subprocess.run([*argv, *plan.empty_directories], input=payload, capture_output=True)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    modes = {name: stat.S_IMODE((root / "dest" / name).lstat().st_mode)
                             for name in ("run.sh", "lib/a.py", "old", "lib", "empty/leaf")}
                    self.assertEqual(modes, {"run.sh": 0o755, "lib/a.py": 0o644, "old": 0o600,
                                             "lib": 0o700, "empty/leaf": 0o755})
                    self.assertEqual(((root / "dest/old").read_bytes(), (root / "outside").read_bytes()),
                                     (b"new", b"keep"))
            # Unsupported means "upload one by one", never a failed write.
            invalid = subprocess.run([binary, "files", "extract", "relative", "8"], capture_output=True)
            self.assertFalse(archive_helper_unsupported(invalid.returncode, invalid.stderr, static=True))
        old = b"file operation failed: invalid file request: unsupported file operation\n"
        self.assertTrue(archive_helper_unsupported(3, old, static=True))
        with TemporaryDirectory() as empty:
            no_tar = subprocess.run(["/bin/sh", "-c", sandbox_archive_extract_script(), "x", empty],
                                    input=payload, capture_output=True, env={"PATH": empty})
            self.assertTrue(archive_helper_unsupported(no_tar.returncode, no_tar.stderr, static=False))


class _Runner:
    def __init__(self):
        self.calls, self.exit = [], (0, b"")

    def run(self, argv, *, input_bytes, timeout_seconds, max_stdout_bytes, max_stderr_bytes, input_file=None):
        start = next(i for i, item in enumerate(argv) if item in {"/bin/sh", "/.ucloud-job-init"})
        stdin = input_file.read() if input_file is not None else input_bytes
        self.calls.append((tuple(argv[start:]), stdin, input_file is not None))
        return DirectExecResult(tuple(argv), self.exit[0], b"", self.exit[1])


class ArchiveEndpointTests(unittest.TestCase):
    @contextmanager
    def servers(self, *, capable=True):
        with TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            (root / "node").mkdir(mode=0o700)
            fixture = fixtures.DirectProvisionerTests()
            provisioner, *_ = fixture.make(root / "node")
            # Its `files ready` probe must pass to create a static-helper sandbox.
            provisioner.oci = replace(provisioner.oci, managed_init_binary=Path("/bin/true").resolve())
            runner = _Runner()
            service = DirectSandboxService(provisioner, process_runner=runner)
            fixture.create(service, replace(fixture.spec(), id="shell"), generation=1)
            fixture.create(service, replace(fixture.spec(), id="static", filesystem=SandboxFilesystemSpec(
                management_helper="static")), generation=1)
            node = fixtures.build_direct_node_agent_server(
                "127.0.0.1", 0, service=service, image_file=root / "node/images.json", job_id="job-1", node_id="node-1",
            )
            capabilities = ("sandbox", ARCHIVE_UPLOAD_CAPABILITY) if capable else ("sandbox",)
            with _running_server(node) as node_url:
                for sid in ("shell", "static"):
                    heartbeats, routes = _seed_gateway_node(root, node_url=node_url, sandbox_id=sid,
                                                            capabilities=capabilities)
                with _running_server(_gateway_server(root, heartbeat_file=heartbeats, routing_file=routes)) as url:
                    yield url, service, runner

    @staticmethod
    def put(url, sandbox_id, body, path="/work"):
        connection = HTTPConnection(url.removeprefix("http://"), timeout=10)
        try:
            connection.request("PUT", f"/v1/sandboxes/{sandbox_id}/archive?path={path}", body=body)
            response = connection.getresponse()
            return response.status, json.load(response)
        finally:
            connection.close()

    def test_one_exec_extracts_small_and_streamed_archives(self):
        large = b"\0x" * TRANSFER_CHUNK_BYTES
        with self.servers() as (url, service, runner), \
                patch.object(upload_archive, "_IN_MEMORY_ARCHIVE_BYTES", TRANSFER_CHUNK_BYTES):
            for archive, staged in ((_tar(("a.py", b"a"), ("lib/", b"", DIR), ("bin/x", b"#!", REG, 0o755),
                                          compress=True), False),
                                    (_tar(("big.bin", large), ("e/", b"", DIR)), True)):
                status, result = self.put(url, "shell", archive)
                self.assertEqual(status, 200, result)
                argv, stdin, from_file = runner.calls[-1]
                with tarfile.open(fileobj=BytesIO(stdin)) as members:
                    names = {m.name: (m.size, m.mode) for m in members}
                if staged:
                    self.assertEqual(result, {"ok": True, "sandbox_id": "shell", "path": "/work", "size": len(archive),
                                              "files": 1, "directories": 1, "bytes": len(large)})
                    self.assertEqual((argv[4:], names, from_file), (("/work", "e"), {"big.bin": (len(large), 0o644)},
                                                                    True))
                else:
                    self.assertEqual((result["files"], result["directories"], result["bytes"]), (2, 1, 3))
                    self.assertEqual(argv[:2] + argv[3:], ("/bin/sh", "-c", "ucloud-extract", "/work", "lib"))
                    self.assertEqual(names, {"a.py": (1, 0o644), "bin/x": (2, 0o755)})
            self.assertEqual(len(runner.calls), 2)
            self.assertEqual(service.upload_spool._unwritten_bytes, 0)
            self.assertEqual(list(service.upload_spool.directory.iterdir()), [])

    def test_unsafe_archives_and_replaced_generations_dispatch_nothing(self):
        with self.servers() as (url, service, runner):
            self.assertEqual(self.put(url, "shell", _tar(("link", b"", tarfile.SYMTYPE)))[0], 400)
            self.assertEqual(self.put(url, "shell", _tar(("a", b"x")), path="relative")[0], 400)
            original = service._require_registration
            calls = 0

            def replaced(sid):
                nonlocal calls
                calls += 1
                registration = original(sid)
                return registration if calls == 1 else replace(registration, sandbox_generation=2)

            with patch.object(service, "_require_registration", replaced):
                self.assertEqual(self.put(url, "shell", _tar(("a", b"x")))[0], 409)
            self.assertEqual(runner.calls, [])

    def test_static_helper_and_unsupported_workers_and_sandboxes(self):
        with self.servers() as (url, service, runner):
            status, _ = self.put(url, "static", _tar(("a", b"xyz"), ("d/", b"", DIR)))
            self.assertEqual(status, 200)
            self.assertEqual(runner.calls[-1][0], ("/.ucloud-job-init", "files", "extract", "/work", "3", "d"))
            runner.exit = (3, b"file operation failed: invalid file request: unsupported file operation\n")
            status, result = self.put(url, "static", _tar(("a", b"xyz")))
            self.assertEqual((status, result["error_code"]), (501, "archive_upload_unsupported"))
        with self.servers(capable=False) as (url, service, runner):
            status, result = self.put(url, "shell", _tar(("a", b"xyz")))
            self.assertEqual((status, result["error_code"], runner.calls), (501, "archive_upload_unsupported", []))
