"""C3.1 worker export over the local fleet (docs/rl-state-primitives.md §3.2, §3.5).

The gateway route is not wired yet, so these drive the node agent's
``commit-export`` through ``commit_steps.export_step``, as the route will,
with the node-control token. The node stages into the in-memory registry
fake; the fake runsc's ``tar rootfs-upper`` exports the overlay difference.
"""

from dataclasses import replace
from http.client import HTTPConnection
import io
import json
import tarfile
import time
import unittest
from unittest.mock import patch
from urllib.parse import urlparse

from tests.harness import LocalFleet
from tests.test_environment_artifact import MemoryRegistry
from ucloud_sandboxes import node_commit
from ucloud_sandboxes.commit_policy import CommitExport, CommitExportRequest, CommitPolicy, filter_upper, staging_repository
from ucloud_sandboxes.commit_steps import export_step

TEST_TIER = "contract"


class CommitExportScenarioTests(unittest.TestCase):
    def setUp(self):
        self.fleet = LocalFleet()
        self.fleet.start()
        self.addCleanup(self.fleet.close)
        self.fleet.create("src", parkable=True)
        self.node = self.fleet.node_for("src")
        self.registry = MemoryRegistry()
        self.serve_commits()
        self.generation = self.node.registration("src").sandbox_generation

    def serve_commits(self):
        """The harness models no checkpoint registry; stage into the fake one."""
        self.node.server.RequestHandlerClass.commit_exports.registry = self.registry

    def call(self, method, path, payload):
        parsed = urlparse(self.node.url)
        connection = HTTPConnection(parsed.hostname, parsed.port, timeout=30)
        try:
            connection.request(method, path, body=json.dumps(payload).encode(), headers={
                "Authorization": f"Bearer {self.fleet.tokens.node_control}", "Content-Type": "application/json"})
            response = connection.getresponse()
            return response.status, json.loads(response.read() or b"{}")
        finally:
            connection.close()

    def export(self, operation_id, *, resume=True, generation=None):
        request = CommitExportRequest(operation_id, generation or self.generation, "img-1", resume)
        deadline = time.monotonic() + 15
        while (export := export_step(self.call, "src", request, CommitPolicy())) is None:
            self.assertLess(time.monotonic(), deadline, "export never staged")
            time.sleep(0.02)
        return export

    def status(self):
        return self.node.runsc_state("src")["fake"]["status"]

    def paused(self):
        return self.node.service.warden.is_paused("src", self.generation)

    def pauses(self):
        return self.node.service.warden.pause_stats.snapshot()["pauses"]

    def layer(self, export):
        blob = self.registry.blobs[export.blob_digest]
        self.assertEqual((len(blob), export.repository), (export.size, staging_repository("img-1")))
        output = io.BytesIO()
        result = filter_upper(io.BytesIO(blob), output, CommitPolicy())
        output.seek(0)
        with tarfile.open(fileobj=output) as archive:
            return result, {member.name: archive.extractfile(member).read() if member.isreg() else member.type
                            for member in archive}

    def test_export_round_trip_and_replay(self):
        fleet = self.fleet
        self.assertEqual(fleet.write_file("src", "/workspace/kept.txt", b"kept").status, 200)
        self.assertEqual(fleet.write_file("src", "/tmp/memory.txt", b"tmpfs").status, 200)
        self.assertEqual(fleet.exec("src", ["rm", "/opt/greeting"]).exit_code, 0)
        export = self.export("c-1")
        self.assertEqual((export.state, export.was_paused, export.identity), ("staged", False, ()))
        result, members = self.layer(export)
        self.assertEqual(members["workspace/kept.txt"], b"kept")
        self.assertIn("opt/.wh.greeting", members)
        self.assertFalse([name for name in members if name.startswith(("tmp", ".ucloud"))])
        self.assertIn("host_written", result.drops)
        # Thawed and untouched by the export; a replay pauses nothing again.
        self.assertEqual((self.status(), self.paused(), self.pauses()), ("running", False, 1))
        self.assertEqual(self.export("c-1"), export)
        self.assertEqual(self.pauses(), 1)
        self.assertFalse(any((self.node.state_root / "commit-staging").iterdir()))
        status, body = self.call("POST", "/v1/sandboxes/src/commit-export",
                                 CommitExportRequest("c-1", self.generation, "img-2").to_dict())
        self.assertEqual((status, body["error_code"]), (409, "commit_conflict"))
        status, body = self.call("POST", "/v1/sandboxes/src/commit-export",
                                 CommitExportRequest("c-2", self.generation + 1, "img-1").to_dict())
        self.assertEqual((status, body["error_code"]), (409, "commit_generation_mismatch"))
        status, body = self.call("POST", "/v1/sandboxes/src/commit-export", {"operation_id": "c-3"})
        self.assertEqual((status, body["error_code"]), (400, "invalid_request"))

    def test_paused_state_is_restored_and_exec_thaws(self):
        held = self.export("c-hold", resume=False)
        self.assertEqual((held.was_paused, self.status(), self.paused()), (False, "paused", True))
        # A paused sandbox stays paused, whatever resume says.
        again = self.export("c-again")
        self.assertEqual((again.was_paused, self.status(), self.paused()), (True, "paused", True))
        self.assertEqual(again.blob_digest, held.blob_digest)
        self.assertEqual(self.fleet.exec("src", ["cat", "/opt/greeting"]).stdout, "hello from the image\n")
        self.assertEqual((self.status(), self.paused()), ("running", False))

    def test_agent_restart_mid_export_restores_the_recorded_state(self):
        self.assertEqual(self.fleet.write_file("src", "/workspace/a.txt", b"a").status, 200)
        # The crash point: the intent says "running", the marker holds the sandbox paused.
        self.export("c-pause", resume=False)
        request = CommitExportRequest("c-crash", self.generation, "img-1")
        intent = CommitExport("src", request, "exporting", False, (), staging_repository("img-1"))
        (self.node.state_root / "commit-exports" / "c-crash.json").write_text(json.dumps(intent.to_dict()))
        (self.node.state_root / "commit-staging" / "c-crash.tar").write_bytes(b"partial")
        (self.node.state_root / "commit-staging" / "tmp-killed-tar").mkdir()  # a killed runsc's scratch
        self.node.restart()
        self.serve_commits()
        self.assertFalse(any((self.node.state_root / "commit-staging").iterdir()))
        self.assertEqual(self.status(), "paused")
        export = self.export("c-crash")
        self.assertEqual((export.was_paused, self.status(), self.paused()), (False, "running", False))
        self.assertEqual(self.layer(export)[1]["workspace/a.txt"], b"a")

    def test_lifecycle_owners_fence_the_export(self):
        request = CommitExportRequest("c-1", self.generation, "img-1").to_dict()
        with self.node.service._lock("src", self.generation):  # as a park, delete or migration holds it
            status, body = self.call("POST", "/v1/sandboxes/src/commit-export", request)
        self.assertEqual((status, body["error_code"], body["retryable"]), (409, "commit_source_busy", True))
        self.assertEqual(self.fleet.park("src").status, 200)
        status, body = self.call("POST", "/v1/sandboxes/src/commit-export", request)
        self.assertEqual((status, body["error_code"], body["retryable"]), (409, "commit_source_not_running", True))
        self.assertEqual(self.pauses(), 0)
        self.assertFalse(any((self.node.state_root / "commit-exports").iterdir()))

    def test_unstageable_worker_refuses_before_pausing(self):
        self.node.server.RequestHandlerClass.commit_exports.registry = None
        status, body = self.call("POST", "/v1/sandboxes/src/commit-export",
                                 CommitExportRequest("c-1", self.generation, "img-1").to_dict())
        self.assertEqual((status, body["error_code"]), (503, "commit_export_unavailable"))
        self.assertEqual((self.status(), self.pauses()), ("running", 0))
        # Only a node with a staging registry advertises the capability.
        self.assertNotIn("sandbox-commit-export-v1", self.node.server.RequestHandlerClass.capabilities)

    def test_size_scratch_and_migration_refusals_precede_any_pause(self):
        # The harness models no gVisor filestore; give the guest's writes a size.
        tiny = CommitExportRequest("c-1", self.generation, "img-1", max_bytes=4095).to_dict()
        with patch.object(self.node.service.warden, "_filestore_bytes", return_value=4096):
            status, body = self.call("POST", "/v1/sandboxes/src/commit-export", tiny)
        self.assertEqual((status, body["error_code"], body["retryable"]), (413, "commit_too_large", False))
        request = CommitExportRequest("c-2", self.generation, "img-1").to_dict()
        with patch.object(node_commit, "MIN_FREE_BYTES", 1 << 60):
            status, body = self.call("POST", "/v1/sandboxes/src/commit-export", request)
        self.assertEqual((status, body["error_code"], body["retryable"]), (503, "commit_capacity_unavailable", True))
        registry = self.node.service.provisioner.registry
        migrating = replace(registry.get("src"), migration_id="m-1", migration_sha256="a" * 64)
        with patch.object(registry, "get", return_value=migrating):
            status, body = self.call("POST", "/v1/sandboxes/src/commit-export", request)
        self.assertEqual((status, body["error_code"], body["retryable"]), (409, "commit_source_busy", True))
        self.assertEqual((self.status(), self.pauses()), ("running", 0))
        self.assertFalse(any((self.node.state_root / "commit-exports").iterdir()))
        self.assertEqual(self.node.server.RequestHandlerClass.commit_exports._reserved, 0)

    def test_a_stale_marker_on_a_running_runtime_still_freezes_it(self):
        # A crash between the C1.1 marker write and `runsc pause` leaves this.
        warden = self.node.service.warden
        warden._pause_marker("src", self.generation).write_bytes(b"stale")
        export = self.export("c-1")
        self.assertEqual((export.was_paused, self.pauses(), self.status(), self.paused()),
                         (False, 1, "running", False))


if __name__ == "__main__":
    unittest.main()
