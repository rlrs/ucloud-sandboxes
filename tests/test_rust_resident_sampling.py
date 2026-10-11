"""Phase 3b, the agent's half (docs/admission-port-3c.md, step 3c.0(a)).

With --rust-admission runtime/noded samples owned, parkable, managed sandboxes
(and paused ones) every second and publishes them to resident.json; the agent
stops sampling and every resident reader, and the growth forecasts, read
noded's observations instead (resident_memory.PublishedResidentMemory). The
forecast refresh that followed each of the agent's passes now follows each
new (session, seq) of the file. Stale observations (older than the 30 s
growth age) and missing ones charge whole bounds. Field parity with
ResidentMemorySampler on the same cgroup is runtime/noded's
tests/resident_python_parity.rs.
"""

from dataclasses import asdict, replace
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import time
import unittest
from unittest.mock import Mock, patch

from tests import test_direct_provisioner as fixtures
from tests.test_pause_tier import _PausingWarden
from ucloud_sandboxes import pause_handoff
from ucloud_sandboxes.direct_service import DirectSandboxService
from ucloud_sandboxes.managed_process import ManagedProcessStart
from ucloud_sandboxes.models import NodeRuntimeMetrics, ResourceQuantity, utc_now
from ucloud_sandboxes.node_runtime import DirectNodeRuntime
from ucloud_sandboxes.resident_memory import (
    PublishedResidentMemory, ResidentMemorySample, ResidentMemorySampler,
)

TEST_TIER = "contract"


def sample(*, current=3 << 30, age=0.0, **changes) -> ResidentMemorySample:
    return ResidentMemorySample(
        current_bytes=current, anonymous_bytes=0, file_bytes=current, dirty_bytes=0, writeback_bytes=0,
        refault_file_pages=0, cgroup_path="/ucloud-sandboxes/c", cgroup_device=1, cgroup_inode=2,
        sentry_pid=10, sentry_start_time_ticks=20, sampled_at=time.monotonic() - age,
        shared_memory_bytes=current, **changes)


def publish(path: Path, seq: int, samples: dict, *, session: str = "session-a", mode: int = 0o600) -> None:
    """What noded's ResidentFile writes: replaced by rename, 0600."""
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    document = {"seq": seq, "session": session, "samples": [
        {"sandbox_id": key[0], "generation": key[1], **asdict(item)} for key, item in samples.items()]}
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(document))
    os.chmod(temporary, mode)
    os.replace(temporary, path)


class PublishedResidentMemoryTests(unittest.TestCase):
    def setUp(self) -> None:
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "noded" / "resident.json"
        self.own = ResidentMemorySampler(cgroup_root=Path(directory.name), proc_root=Path(directory.name))
        self.published = PublishedResidentMemory(self.path, self.own)
        self.key = ("one", 7)

    def test_a_new_pass_is_read_once_and_every_field_round_trips(self) -> None:
        self.assertFalse(self.published.load())  # No file yet: nothing changed, nothing known.
        self.assertIsNone(self.published.historical(self.key))
        observed = sample(current=1000, peak_bytes=4096, swap_bytes=7)
        publish(self.path, 1, {self.key: observed})
        self.assertTrue(self.published.load())
        self.assertEqual(self.published.historical(self.key), observed)
        self.assertEqual(self.published.get(self.key), observed)
        self.assertFalse(self.published.load())  # The same file again.
        publish(self.path, 1, {self.key: observed})
        self.assertFalse(self.published.load())  # Rewritten, same (session, seq).
        publish(self.path, 1, {self.key: observed}, session="session-b")
        self.assertTrue(self.published.load())  # A restarted noded.
        publish(self.path, 2, {})
        self.assertTrue(self.published.load())
        self.assertIsNone(self.published.historical(self.key))  # Not measured: unknown.

    def test_age_fences_stay_with_the_readers(self) -> None:
        publish(self.path, 1, {self.key: sample(age=3.0)})
        self.published.load()
        self.assertIsNone(self.published.get(self.key))  # The live 2.5 s fence.
        self.assertIsNotNone(self.published.historical(self.key))  # Growth applies its own 30 s.

    def test_missing_invalid_or_foreign_files_are_no_observations(self) -> None:
        publish(self.path, 1, {self.key: sample()})
        self.assertTrue(self.published.load())
        self.path.unlink()
        self.assertTrue(self.published.load())
        self.assertIsNone(self.published.historical(self.key))
        good = {"sandbox_id": "one", "generation": 7, **asdict(sample())}
        with self.assertLogs("ucloud_sandboxes.resident_memory", "WARNING"):  # Once a minute.
            for document in ([], {"seq": -1, "session": "s", "samples": []},
                             {"seq": 1, "session": "", "samples": []},
                             {"seq": 1, "session": "s"}, {"seq": True, "session": "s", "samples": []}):
                publish(self.path, 1, {self.key: sample()})
                self.published.load()
                self.path.write_text(json.dumps(document))
                self.assertTrue(self.published.load(), document)
                self.assertIsNone(self.published.historical(self.key), document)
        # One invalid sample is unknown; the rest of the pass stands.
        for seq, broken in enumerate((
                {**good, "current_bytes": -1}, {**good, "current_bytes": True}, {**good, "extra": 1},
                {**good, "generation": 0}, {**good, "cgroup_path": "relative"},
                {**good, "sampled_at": "now"}, {k: v for k, v in good.items() if k != "swap_bytes"}), start=10):
            other = {"sandbox_id": "two", "generation": 1, **asdict(sample())}
            self.path.write_text(json.dumps({"seq": seq, "session": "s", "samples": [broken, other]}))
            self.assertTrue(self.published.load())
            self.assertIsNone(self.published.historical(self.key), broken)
            self.assertIsNotNone(self.published.historical(("two", 1)))
        publish(self.path, 3, {self.key: sample()}, mode=0o622)  # Others may write it.
        self.published.load()
        self.assertIsNone(self.published.historical(self.key))
        self.path.unlink()
        self.path.symlink_to(self.path.with_name("elsewhere.json"))
        publish(self.path.with_name("elsewhere.json"), 4, {self.key: sample()})
        self.published.load()
        self.assertIsNone(self.published.historical(self.key))

    def test_forget_hides_older_observations_until_a_newer_pass(self) -> None:
        before = sample(current=1 << 30)
        publish(self.path, 1, {self.key: before})
        self.published.load()
        self.published.forget(self.key)  # A park or restore.
        self.assertIsNone(self.published.historical(self.key))
        # A pass that sampled before the forget but published after it.
        publish(self.path, 2, {self.key: before})
        self.published.load()
        self.assertIsNone(self.published.historical(self.key))
        after = sample(current=2 << 30)
        publish(self.path, 3, {self.key: after})
        self.published.load()
        self.assertEqual(self.published.historical(self.key), after)
        self.published.retain(set())
        self.assertEqual(self.published.historical(self.key), after)  # Retain prunes fences only.

    def test_the_agents_own_measurements_overlay_noded_by_age(self) -> None:
        published = sample(current=1 << 30, age=0.5)
        publish(self.path, 1, {self.key: published})
        self.published.load()
        own = sample(current=1 << 29)
        with patch.object(self.own, "sample", side_effect=lambda key, **_: self.own._samples.setdefault(key, own)):
            self.assertEqual(self.published.sample(self.key, pid=10, start_time_ticks=20, container_id="c"), own)
        self.assertEqual(self.published.get(self.key), own)  # Newer than noded's.
        newer = sample(current=3 << 29)
        publish(self.path, 2, {self.key: newer})
        self.published.load()
        self.assertEqual(self.published.get(self.key), newer)
        # A failed measurement is unknown until a later observation, as the
        # sampler drops its own.
        with patch.object(self.own, "sample", return_value=None):
            self.assertIsNone(self.published.sample(self.key, pid=10, start_time_ticks=20, container_id="c"))
        self.assertIsNone(self.published.get(self.key))
        self.assertIs(self.published.cgroup_root, self.own.cgroup_root)


class PublishedGrowthTests(unittest.TestCase):
    """The growth forecasts credit noded's passes; nothing samples in the agent."""

    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.fixture = fixtures.DirectProvisionerTests()
        self.provisioner, self.registry, *_ = self.fixture.make(Path(self.tmp.name).resolve())
        self.provisioner.oci = replace(self.provisioner.oci, managed_init_binary=Path("/bin/sh").resolve())
        self.service = DirectSandboxService(self.provisioner)
        self.fixture.create(self.service, replace(self.fixture.spec(), id="one", memory_mb=4096,
                                                 parkable=True, managed_process=True))
        self.service.configure_active_capacity(
            ResourceQuantity(vcpu=4, memory_mb=8192),
            runtime_metrics_provider=lambda: NodeRuntimeMetrics(
                collected_at=utc_now(), cpu_count=4, cpu_percent=0, memory_total_mb=8192,
                memory_available_mb=6144))
        patch.object(self.service, "_managed_control", side_effect=lambda registration, payload, **_: {
            "version": 1, "ok": True, "job": {"job_id": payload["job_id"], "spec_sha256": "a" * 64,
                                              "state": "running", "pid": 10, "sequence": 1}}).start()
        self.addCleanup(patch.stopall)
        self.path = pause_handoff.noded_directory(self.service) / pause_handoff.RESIDENT_FILE
        self.key = ("one", self.registry.get("one").sandbox_generation)
        self.service.start_managed_process("one", ManagedProcessStart("primary", ("/bin/agent",), cwd="/workspace"))

    def demand(self) -> int:
        return self.service.warm_park_demand().physical_bytes

    def test_without_the_flag_the_agent_samples_and_ignores_the_file(self) -> None:
        publish(self.path, 1, {self.key: sample()})
        self.assertIsInstance(self.service._resident_memory, ResidentMemorySampler)
        with patch.object(self.service, "_sample_resident") as sampled:
            self.service.refresh_resident_memory()
        sampled.assert_called_once()
        self.assertEqual(self.demand(), 4 << 30)

    def test_the_forecasts_follow_each_published_pass(self) -> None:
        self.service.use_published_resident_memory(self.path)
        with patch.object(self.service, "_sample_resident", side_effect=AssertionError("sampled")):
            self.service.refresh_resident_memory()  # No pass yet: the whole bound.
            self.assertEqual(self.demand(), 4 << 30)
            publish(self.path, 1, {self.key: sample(current=3 << 30)})
            self.assertEqual(self.demand(), 4 << 30)  # Credited with the pass, not before.
            self.assertTrue(self.service.refresh_published_resident_memory())
            self.assertEqual(self.demand(), 1 << 30)
            self.assertFalse(self.service.refresh_published_resident_memory())  # Same pass.
            # Older than the growth observation age: missing, the whole bound.
            publish(self.path, 2, {self.key: sample(current=3 << 30, age=40)})
            self.service.refresh_resident_memory()
            self.assertEqual(self.demand(), 4 << 30)
            # A noded that stops publishing: its last pass ages out by itself.
            publish(self.path, 3, {self.key: sample(current=3 << 30, age=29.9)})
            self.service.refresh_resident_memory()
            self.assertEqual(self.demand(), 1 << 30)
            time.sleep(0.2)
            self.assertEqual(self.demand(), 4 << 30)
        # Every reader sees noded's observation.
        observed = sample(current=2 << 30)
        publish(self.path, 4, {self.key: observed})
        self.service.refresh_resident_memory()
        self.assertEqual(self.service.resident_memory_sample(*self.key), observed)
        self.assertEqual(self.service.cached_resident_memory_sample(*self.key), observed)

    def test_a_park_forgets_an_observation_a_late_pass_still_carries(self) -> None:
        self.service.use_published_resident_memory(self.path)
        before = sample(current=3 << 30)
        publish(self.path, 1, {self.key: before})
        self.service.refresh_resident_memory()
        self.assertEqual(self.demand(), 1 << 30)
        self.service._resident_memory.forget(self.key)  # What _observe_managed_park does.
        publish(self.path, 2, {self.key: before})
        self.service.refresh_resident_memory()
        self.assertIsNone(self.service.cached_resident_memory_sample(*self.key))
        self.assertEqual(self.demand(), 4 << 30)

    def test_the_pass_prunes_intents_without_a_registration(self) -> None:
        self.service.use_published_resident_memory(self.path)
        self.service._growth_intents[("gone", 1)] = self.service._growth_intents[self.key]
        publish(self.path, 1, {})
        self.service.refresh_resident_memory()
        self.assertNotIn(("gone", 1), self.service._growth_intents)


class RustAdmissionRuntimeTests(unittest.TestCase):
    def runtime(self, **flags) -> tuple[Path, DirectSandboxService, DirectNodeRuntime]:
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name).resolve()
        with patch.object(fixtures, "FakeWarden", _PausingWarden):
            provisioner, _, _, _, warden = fixtures.DirectProvisionerTests().make(root)
        warden.config.runtime_root = root / "runsc"
        (root / "runsc" / "warden-locks").mkdir(parents=True)
        service = DirectSandboxService(provisioner, process_runner=fixtures.FakeProcessRunner())
        runtime = DirectNodeRuntime(service, rust_execs=True, **flags)
        self.addCleanup(runtime.stop)
        return root, service, runtime

    def test_the_flag_installs_noded_observations_and_needs_the_rust_pause_tier(self) -> None:
        root, service, runtime = self.runtime(rust_pause_tier=True, rust_admission=True)
        self.assertTrue(runtime.rust_admission and service.rust_admission)
        self.assertIsInstance(service._resident_memory, PublishedResidentMemory)
        self.assertEqual(service._resident_memory.path, root / "noded" / "resident.json")
        _, service, runtime = self.runtime(rust_pause_tier=True)
        self.assertFalse(runtime.rust_admission or service.rust_admission)
        self.assertIsInstance(service._resident_memory, ResidentMemorySampler)
        with self.assertRaisesRegex(ValueError, "Rust admission needs the Rust pause tier"):
            self.runtime(rust_admission=True)

    def test_the_parking_loop_looks_for_a_pass_every_tick_only_with_the_flag(self) -> None:
        for flag, calls in ((True, 4), (False, 1)):
            with self.subTest(rust_admission=flag):
                _, service, runtime = self.runtime(rust_pause_tier=True, rust_admission=flag)
                refresh = Mock()
                with patch.object(service, "refresh_resident_memory", refresh), \
                        patch.object(runtime, "_recheck_relay_parks"), \
                        patch.object(runtime._background_stop, "wait", side_effect=[False] * 4 + [True]):
                    runtime._relay_parking_loop()
                self.assertEqual(refresh.call_count, calls)

    def test_the_pause_block_tells_noded_where_to_publish(self) -> None:
        # Whether noded samples is the admission block's rust_admission_enabled
        # (tests/test_noded_client.py); the pause block names the file.
        root, service, runtime = self.runtime(rust_pause_tier=True, rust_admission=True)
        service.warden.config.runsc = root / "runsc-binary"
        pause = pause_handoff.pause_config(service, rust_pause_enabled=True)
        self.assertEqual(pause["resident_path"], str(root / "noded" / "resident.json"))
        self.assertNotIn("rust_admission_enabled", pause)


if __name__ == "__main__":
    unittest.main()
