"""The exec admission lease replaces an unfenced inventory reread."""

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from tests import test_direct_provisioner as fixtures
from ucloud_sandboxes.direct_service import DirectSandboxService
from ucloud_sandboxes.direct_warden import DirectWardenError
from ucloud_sandboxes.models import NodeRuntimeMetrics, ResourceQuantity, utc_now
from ucloud_sandboxes.node_runtime import DirectNodeRuntime
from ucloud_sandboxes.sandbox import SandboxAdmissionClosedError
from ucloud_sandboxes.sandbox_exec import ExecSessionManager, SandboxExecSpec


class ExecAdmissionContractTests(TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        fixture = fixtures.DirectProvisionerTests()
        provisioner, self.registry, _, _, self.warden = fixture.make(
            Path(directory.name).resolve()
        )
        self.service = DirectSandboxService(provisioner)
        self.created = fixture.create(self.service, fixture.spec())
        self.runtime = DirectNodeRuntime(self.service)
        self.sessions = ExecSessionManager(self.runtime)
        self.spec = SandboxExecSpec(sandbox_id=self.created.spec.id, command=("true",))

    def test_start_does_not_materialize_an_unfenced_inventory_record(self):
        with (
            patch.object(
                self.service, "get", side_effect=AssertionError("inventory read")
            ),
            patch.object(self.warden, "inspect", wraps=self.warden.inspect) as inspect,
            patch.object(
                self.warden,
                "running_process_alive",
                wraps=self.warden.running_process_alive,
            ) as alive,
            patch.object(
                self.sessions,
                "_start_process",
                side_effect=lambda session: self.runtime.runtime.exec_started(
                    session.spec.sandbox_id
                ),
            ),
        ):
            session = self.sessions.start(self.spec)
        try:
            inspect.assert_called_once()
            alive.assert_called_once()
            self.assertEqual(self.service.activity_snapshot().active_exec_operations, 1)
        finally:
            self.sessions._complete(session, 0)
        self.assertEqual(self.service.activity_snapshot().active_exec_operations, 0)
        self.assertTrue(self.runtime.lifecycle.is_idle(self.created.spec.id))

    def test_capacity_checks_owned_registration_once_without_sampling(self):
        with patch.object(self.registry, "get", wraps=self.registry.get) as get:
            token = self.service.acquire_exec_capacity(
                self.created.spec.id, self.created.generation
            )
        get.assert_called_once_with(self.created.spec.id)
        self.service.release_exec_capacity(token)

    def test_owner_revoked_during_sampling_cannot_dispatch(self):
        self.assert_sampling_revocation("owner", DirectWardenError)

    def test_drain_during_sampling_cannot_dispatch(self):
        self.assert_sampling_revocation("drain", SandboxAdmissionClosedError)

    def assert_sampling_revocation(self, change, expected):
        def sample():
            if change == "owner":
                registration = self.registry.get(self.created.spec.id)
                self.registry.begin_delete(
                    registration.sandbox_id, expected_revision=registration.revision
                )
            else:
                self.service.close_admission()
            return NodeRuntimeMetrics(
                collected_at=utc_now(),
                cpu_percent=0,
                cpu_count=4,
                memory_total_mb=8192,
                memory_available_mb=8192,
            )

        self.service.configure_active_capacity(
            ResourceQuantity(vcpu=4, memory_mb=8192), runtime_metrics_provider=sample
        )
        with patch.object(
            self.warden, "exec_lease", side_effect=AssertionError("dispatched")
        ):
            with self.assertRaises(expected):
                self.sessions.start(self.spec)
        self.assertTrue(self.runtime.lifecycle.is_idle(self.created.spec.id))
        self.assertEqual(self.service.activity_snapshot().active_exec_operations, 0)

    def test_final_warden_failure_releases_admission_and_shared_activity(self):
        with patch.object(
            self.warden,
            "exec_lease",
            side_effect=DirectWardenError("live authority revoked"),
        ):
            with self.assertRaisesRegex(DirectWardenError, "live authority"):
                self.sessions.start(self.spec)
        self.assertTrue(self.runtime.lifecycle.is_idle(self.created.spec.id))
        self.assertEqual(self.service.activity_snapshot().active_exec_operations, 0)
        self.assertEqual(self.runtime._exec_start_users, {})
