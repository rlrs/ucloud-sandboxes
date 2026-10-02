from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
from threading import Event, Thread
from types import SimpleNamespace
import unittest
from unittest.mock import Mock
from urllib import error, request

from ucloud_sandboxes import control_plane
from ucloud_sandboxes.gateway.image_resolution import RegistryManifestResolutionCache
from ucloud_sandboxes.config import DeploymentConfig
from ucloud_sandboxes.deploy import (
    REGISTRY_STORAGE_SYSTEMD_UNITS,
    SYSTEMD_UNIT_NAMES,
    packaged_systemd_units,
)
from ucloud_sandboxes.image_import import import_image_id
from ucloud_sandboxes.managed_registry import (
    RegistryMaintenanceBusy,
    registry_maintenance_lock,
)
from ucloud_sandboxes.registry_disk import (
    REGISTRY_DISK_PRESSURE_ERROR_CODE,
    RegistryDiskMonitor,
    RegistryDiskUsage,
    measure_registry_disk,
    read_registry_maintenance_state,
    record_image_evictions,
    record_registry_gc,
    record_registry_prune,
    registry_disk_usage,
)
from ucloud_sandboxes.registry_sweep import RegistrySweepResult
from ucloud_sandboxes.systemd import (
    REGISTRY_BLOB_CACHE_ENV,
    registry_gc_command,
    registry_process_environment,
    registry_run_command,
    reconcile_gateway_services,
    run_registry_gc,
    run_registry_pressure_cleanup,
    run_registry_sweep,
)

from tests import test_control_plane as gateway_fixtures
from tests.gateway_support import gateway_services

TEST_TIER = "contract"


GIB = 1024**3


def statvfs_at(percent: float, *, total_blocks: int = 1000):
    used = int(total_blocks * percent / 100)
    return lambda _path: SimpleNamespace(
        f_frsize=GIB,
        f_bsize=GIB,
        f_blocks=total_blocks,
        f_bfree=total_blocks - used,
        f_bavail=total_blocks - used,
    )


def usage_at(percent: float) -> RegistryDiskUsage:
    return measure_registry_disk(
        ("/registry",),
        cleanup_percent=70,
        refuse_percent=90,
        target_percent=60,
        statvfs=statvfs_at(percent),
    )


def filesystem_config(root: Path, **overrides) -> DeploymentConfig:
    raw = DeploymentConfig.default("project").to_dict()
    raw["data_root"] = str(root / "state")
    raw["registry_store"]["mount_point"] = str(root)
    raw["registry_store"]["data_root"] = str(root / "registry")
    raw.update(overrides)
    return DeploymentConfig.from_dict(raw)


class RegistryDiskUsageTests(unittest.TestCase):
    def test_thresholds_follow_df_semantics(self) -> None:
        reserved = lambda _path: SimpleNamespace(  # noqa: E731
            f_frsize=GIB, f_bsize=GIB, f_blocks=1000, f_bfree=300, f_bavail=250,
        )
        usage = measure_registry_disk(
            ("/missing", "/registry"),
            cleanup_percent=70, refuse_percent=90, target_percent=60,
            statvfs=lambda path: (_ for _ in ()).throw(FileNotFoundError(path))
            if path == "/missing" else reserved(path),
        )

        self.assertEqual(usage.path, "/registry")
        self.assertAlmostEqual(usage.used_percent, 700 / 950 * 100)
        self.assertTrue(usage.cleanup_needed)
        self.assertFalse(usage.refusing_writes)
        self.assertEqual(usage.state, "cleanup")
        self.assertEqual(usage.target_used_bytes, int(950 * GIB * 0.6))
        self.assertEqual(usage_at(95).state, "refusing")
        self.assertEqual(usage_at(10).state, "ok")

    def test_s3_registry_store_has_no_disk_guard(self) -> None:
        raw = DeploymentConfig.default("project").to_dict()
        raw["registry_store"] = {
            "kind": "s3", "mount_point": "", "data_root": "",
            "endpoint": "https://hel1.your-objectstorage.com", "bucket": "sandboxes",
            "region": "hel1", "prefix": "oci", "access_key_id_env": "A",
            "secret_access_key_env": "B", "force_path_style": False,
        }
        config = DeploymentConfig.from_dict(raw)
        self.assertIsNone(registry_disk_usage(config))
        self.assertIsNone(RegistryDiskMonitor.from_config(config))

    def test_monitor_reports_the_configured_eviction_target(self) -> None:
        raw = DeploymentConfig.default("project").to_dict()
        raw["registry_disk_target_percent"] = 55.0
        config = DeploymentConfig.from_dict(raw)
        monitor = RegistryDiskMonitor.from_config(config)
        assert monitor is not None
        monitor._statvfs = statvfs_at(10)
        self.assertEqual(monitor.status()["target_percent"], 55.0)

    def test_monitor_caches_statvfs_and_reports_maintenance_state(self) -> None:
        calls = []
        now = [0.0]
        with TemporaryDirectory() as raw:
            state = Path(raw) / "registry-maintenance.json"
            record_registry_prune(state, deleted=3)
            record_image_evictions(state, [{"image_id": "task-1", "tag": "r/x:latest"}])

            def statvfs(path):
                calls.append(path)
                return statvfs_at(95)(path)

            monitor = RegistryDiskMonitor(
                ("/registry",), cleanup_percent=70, refuse_percent=90,
                maintenance_state_file=state, statvfs=statvfs, clock=lambda: now[0],
            )
            with self.assertLogs("ucloud_sandboxes.registry_disk", level="WARNING"):
                self.assertIsNotNone(monitor.refusal())
            monitor.usage()
            now[0] = 5.0
            status = monitor.status()

        self.assertEqual(len(calls), 2)
        self.assertEqual(status["state"], "refusing")
        self.assertEqual(status["maintenance"]["deleted_since_gc"], 3)
        self.assertEqual(status["maintenance"]["evicted_image_count"], 1)
        self.assertNotIn("evicted_images", status["maintenance"])
        self.assertEqual(monitor.evicted_image("task-1")["tag"], "r/x:latest")

    def test_maintenance_state_counts_deletions_until_gc(self) -> None:
        with TemporaryDirectory() as raw:
            state = Path(raw) / "state.json"
            record_registry_prune(state, deleted=2)
            record_registry_prune(state, deleted=5)
            pending = read_registry_maintenance_state(state)["deleted_since_gc"]
            record_registry_gc(state, kind="online", deleted_bytes=10)
            after = read_registry_maintenance_state(state)

        self.assertEqual(pending, 7)
        self.assertEqual(after["deleted_since_gc"], 0)
        self.assertEqual(after["last_gc_kind"], "online")


class GatewayDiskPressureTests(unittest.TestCase):
    def _post_build(self, monitor) -> tuple[int, dict, dict]:
        with TemporaryDirectory() as raw:
            server = gateway_fixtures._gateway_server(
                Path(raw), registry_disk_monitor=monitor,
            )
            with gateway_fixtures._running_server(server) as base:
                req = request.Request(
                    f"{base}/v1/images/build",
                    data=json.dumps({"id": "task-image", "tag": "example/task:1",
                                     "context_path": "."}).encode(),
                    method="POST",
                    headers={"Content-Type": "application/json"},
                )
                try:
                    with request.urlopen(req, timeout=5) as response:
                        return response.status, json.loads(response.read()), {}
                except error.HTTPError as exc:
                    return exc.code, json.loads(exc.read()), dict(exc.headers)

    def test_builds_are_refused_before_dispatch_above_the_refuse_threshold(self) -> None:
        full = RegistryDiskMonitor(
            ("/registry",), cleanup_percent=70, refuse_percent=90,
            statvfs=statvfs_at(93),
        )
        status, body, headers = self._post_build(full)

        self.assertEqual(status, 503)
        self.assertEqual(body["error_code"], REGISTRY_DISK_PRESSURE_ERROR_CODE)
        self.assertTrue(body["retryable"])
        self.assertEqual(headers["Retry-After"], "60")
        self.assertEqual(body["registry_disk"]["state"], "refusing")

    def test_builds_proceed_below_the_refuse_threshold(self) -> None:
        busy = RegistryDiskMonitor(
            ("/registry",), cleanup_percent=70, refuse_percent=90,
            statvfs=statvfs_at(80),
        )
        status, body, _headers = self._post_build(busy)

        # The request passed admission and reached ordinary validation.
        self.assertEqual(status, 400)
        self.assertNotEqual(body.get("error_code"), REGISTRY_DISK_PRESSURE_ERROR_CODE)
        self.assertIn("context_archive_digest", body["error"])

    def test_imports_are_not_submitted_while_the_registry_is_full(self) -> None:
        subject = object.__new__(control_plane.ControlPlaneHandler)
        subject.services = gateway_services(
            registry_url="http://registry.internal:5000",
            registry_worker_url="http://registry.internal:5000",
            registry_disk_monitor=Mock(refusal=Mock(return_value=usage_at(95))),
        )
        subject.image_import_submitter = Mock()
        subject.services.images.resolve = Mock(
            return_value=("x", {"error_code": "image_id_not_found"}),
        )
        external = "docker.io/library/python:3.12"

        image, waited = subject._external_image_import(external, wait=True)
        _image, background = subject._external_image_import(external, wait=False)

        self.assertEqual(image, external)
        self.assertEqual(waited["error_code"], REGISTRY_DISK_PRESSURE_ERROR_CODE)
        self.assertEqual(waited["import_id"], import_image_id(external))
        self.assertIsNone(background)
        subject.image_import_submitter.ensure_submitted.assert_not_called()

    def test_evicted_images_fail_clearly_and_flush_manifest_resolutions(self) -> None:
        monitor = Mock()
        monitor.maintenance_state.return_value = {"last_eviction_at": "2026-09-27T12:00:00"}
        monitor.evicted_image.side_effect = lambda image_id: (
            {"tag": "r/x:latest", "evicted_at": "2026-09-27T12:00:00"}
            if image_id == "task-1" else None
        )
        subject = gateway_services(registry_disk_monitor=monitor).images
        cache = subject.manifest_cache = RegistryManifestResolutionCache()
        cache.put("ucloud-managed/x", "latest", "sha256:" + "1" * 64)

        error_payload = subject.evicted_image_error("task-1")
        flushed = subject.manifest_cache_current()

        self.assertEqual(error_payload["error_code"], "image_evicted")
        self.assertFalse(error_payload["retryable"])
        self.assertTrue(error_payload["rebuild_required"])
        self.assertIsNone(subject.evicted_image_error("task-2"))
        self.assertEqual(flushed.get("ucloud-managed/x", "latest"), "")


class PressureCleanupTests(unittest.TestCase):
    def run_cleanup(self, usages, *, state=None, evicted=0, pending_after_prune=0):
        prunes: list[bool] = []
        gcs: list[int] = []
        readings = list(usages)

        with TemporaryDirectory() as raw:
            root = Path(raw)
            config = filesystem_config(root)
            state_file = root / "state.json"
            state_file.write_text(json.dumps(state or {}))

            def usage():
                return usage_at(readings.pop(0) if len(readings) > 1 else readings[0])

            def prune(evict_lru):
                prunes.append(evict_lru)
                deleted = evicted if evict_lru else pending_after_prune
                record_registry_prune(state_file, deleted=deleted)
                return {
                    "deleted_manifest_count": deleted,
                    "lru_eviction": {"evicted_images": evicted} if evict_lru else None,
                }

            def gc():
                gcs.append(1)
                record_registry_gc(state_file, kind="online")
                return True

            result = run_registry_pressure_cleanup(
                config=config, lock_file=root / "maintenance", prune=prune, gc=gc,
                usage=usage, maintenance_state_file=state_file,
            )
        return result, prunes, len(gcs)

    def test_below_the_cleanup_threshold_nothing_runs(self) -> None:
        result, prunes, gcs = self.run_cleanup([50])
        self.assertEqual((result["action"], prunes, gcs), ("none", [], 0))

    def test_prune_then_sweep_frees_enough_without_eviction(self) -> None:
        result, prunes, gcs = self.run_cleanup(
            [75, 75, 75, 55], pending_after_prune=4,
        )
        self.assertEqual(prunes, [False])
        self.assertEqual(gcs, 1)
        self.assertEqual(result["action"], "prune")

    def test_sweeps_are_rate_limited_without_eviction(self) -> None:
        recent = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        result, prunes, gcs = self.run_cleanup(
            [75, 75, 75, 65], state={"last_gc_at": recent},
        )
        self.assertEqual(prunes, [False])
        self.assertEqual(gcs, 0)
        self.assertFalse(result["gc"]["due"])

    def test_eviction_sweeps_pending_garbage_first_and_again_after(self) -> None:
        recent = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        result, prunes, gcs = self.run_cleanup(
            [85, 85, 85, 85, 85, 62],
            state={"last_gc_at": recent},
            pending_after_prune=10,
            evicted=7,
        )
        # Not due after the prune, but garbage must be swept before the
        # eviction projects from measured usage, and again after evicting.
        self.assertEqual(prunes, [False, True])
        self.assertEqual(gcs, 2)
        self.assertEqual(result["action"], "evict")

    def test_waits_for_the_maintenance_lock_instead_of_failing(self) -> None:
        with TemporaryDirectory() as raw:
            lock = Path(raw) / "maintenance"
            release = Event()
            held = Event()

            def holder() -> None:
                with registry_maintenance_lock(lock):
                    held.set()
                    release.wait(timeout=5)

            holding = Thread(target=holder)
            holding.start()
            held.wait(timeout=5)
            clock = [0.0]

            def advance(seconds: float) -> None:
                clock[0] += seconds

            with self.assertRaises(RegistryMaintenanceBusy):
                with registry_maintenance_lock(
                    lock, timeout_seconds=5, sleep=advance, clock=lambda: clock[0],
                ):
                    pass
            self.assertGreaterEqual(clock[0], 5)

            polls: list[float] = []

            def holder_finishes(seconds: float) -> None:
                polls.append(seconds)
                release.set()
                holding.join(timeout=5)

            with registry_maintenance_lock(lock, timeout_seconds=30, sleep=holder_finishes):
                acquired = True
        self.assertTrue(acquired)
        self.assertEqual(len(polls), 1)


class RegistryGcTests(unittest.TestCase):
    def test_filesystem_registry_runs_without_the_blob_descriptor_cache(self) -> None:
        with TemporaryDirectory() as raw:
            config = filesystem_config(Path(raw))
            environment = registry_process_environment(config, environ={})
            command = registry_run_command(config)
        self.assertEqual(environment[REGISTRY_BLOB_CACHE_ENV], "none")
        self.assertIn(REGISTRY_BLOB_CACHE_ENV, command)

    def test_quiescent_sweep_holds_both_locks_and_records_state(self) -> None:
        calls = []
        with TemporaryDirectory() as raw:
            root = Path(raw)
            config = filesystem_config(root)
            state = root / "state.json"

            def sweep(path, *, grace_seconds, writers_stopped):
                self.assertTrue(writers_stopped)
                with self.assertRaises(RegistryMaintenanceBusy):
                    with registry_maintenance_lock(root / "writer", blocking=False):
                        pass
                calls.append((path, grace_seconds))
                with self.assertRaises(RegistryMaintenanceBusy):
                    with registry_maintenance_lock(root / "maintenance", blocking=False):
                        pass
                return RegistrySweepResult(deleted_blobs=2, deleted_bytes=123)

            result = run_registry_sweep(
                config=config, lock_file=root / "maintenance",
                maintenance_state_file=state, sweep=sweep, writer_lock=root / "writer",
                runner=lambda command, **kw: subprocess.CompletedProcess(command, 0, "", ""),
            )
            recorded = read_registry_maintenance_state(state)

        self.assertEqual(calls, [(config.registry_data_dir(), 7200)])
        self.assertEqual(result.deleted_bytes, 123)
        self.assertEqual(recorded["last_gc_kind"], "quiescent")
        self.assertEqual(recorded["last_gc_deleted_bytes"], 123)

    def test_offline_gc_stops_and_always_restarts_the_registry(self) -> None:
        calls: list[list[str]] = []

        def runner(command, *, check, text, env=None, capture_output=False):
            calls.append(command)
            if command[:2] == ["docker", "run"]:
                raise subprocess.CalledProcessError(1, command)
            return subprocess.CompletedProcess(command, 0, "", "")

        with TemporaryDirectory() as raw:
            config = filesystem_config(Path(raw))
            (config.registry_data_dir() / "docker/registry/v2/repositories").mkdir(parents=True)
            with self.assertRaises(subprocess.CalledProcessError):
                run_registry_gc(
                    config=config, lock_file=Path(raw) / "maintenance", runner=runner,
                    writer_lock=Path(raw) / "writer",
                )

        self.assertEqual(calls[0], ["systemctl", "stop", "ucloud-sandbox-registry.service"])
        self.assertEqual(calls[2], registry_gc_command(config))
        self.assertEqual(calls[3], ["systemctl", "start", "ucloud-sandbox-registry.service"])


class MaintenanceUnitTests(unittest.TestCase):
    def test_prune_waits_for_the_lock_hourly_and_pressure_runs_every_minute(self) -> None:
        units = packaged_systemd_units()
        prune = units["ucloud-sandbox-registry-prune.service"]
        self.assertIn("flock --exclusive --wait 1800 "
                      "/run/lock/ucloud-sandbox-registry-maintenance.lock", prune)
        self.assertNotIn("--nonblock", prune)
        self.assertNotIn("Conflicts=", prune + units["ucloud-sandbox-registry-gc.service"])
        self.assertIn("OnCalendar=hourly", units["ucloud-sandbox-registry-prune.timer"])
        self.assertIn("OnUnitInactiveSec=1min", units["ucloud-sandbox-registry-pressure.timer"])
        self.assertIn("registry-pressure", units["ucloud-sandbox-registry-pressure.service"])
        for name in ("gc", "pressure"):
            self.assertIn("registry-recover", units[f"ucloud-sandbox-registry-{name}.service"])
        self.assertNotIn("ExecStartPre=", units["ucloud-sandbox-registry.service"])
        # Restarting the registry must never stop the maintenance units.
        for name in ("ucloud-sandbox-registry-pressure.service",
                     "ucloud-sandbox-registry-gc.service",
                     "ucloud-sandbox-registry-prune.service"):
            self.assertNotIn("Requires=ucloud-sandbox-registry.service", units[name])
        self.assertIn("ucloud-sandbox-registry-pressure.timer", SYSTEMD_UNIT_NAMES)
        self.assertIn("ucloud-sandbox-registry-pressure.service", REGISTRY_STORAGE_SYSTEMD_UNITS)
        installer = (Path(__file__).parents[1] / "scripts/install_hetzner_gateway.sh").read_text()
        self.assertIn("ucloud-sandbox-registry-pressure.timer", installer)

    def test_gateway_reconcile_enables_the_pressure_timer(self) -> None:
        commands: list[list[str]] = []
        reconcile_gateway_services(
            config=DeploymentConfig.default("project"),
            runner=lambda command, *, check, text: commands.append(command),
            wait_for=lambda _name, _url: None,
        )
        self.assertIn(
            ["systemctl", "enable", "--now", "ucloud-sandbox-registry-pressure.timer"],
            commands,
        )


class RegistryGuardConfigTests(unittest.TestCase):
    def test_guard_fields_default_round_trip_and_validate(self) -> None:
        raw = DeploymentConfig.default("project").to_dict()
        for name in ("registry_disk_cleanup_percent", "registry_disk_target_percent",
                     "registry_disk_refuse_percent", "registry_disk_gc_interval_seconds",
                     "registry_reference_grace_seconds", "registry_blob_grace_seconds"):
            raw.pop(name)
        config = DeploymentConfig.from_dict(raw)
        self.assertEqual(
            (config.registry_disk_target_percent, config.registry_disk_cleanup_percent,
             config.registry_disk_refuse_percent, config.registry_reference_grace_seconds,
             config.registry_blob_grace_seconds),
            (60.0, 70.0, 90.0, 3600, 7200),
        )
        custom = config.to_dict() | {"registry_disk_cleanup_percent": 80,
                                     "registry_disk_refuse_percent": 95}
        self.assertEqual(DeploymentConfig.from_dict(custom).to_dict(),
                         DeploymentConfig.from_dict(custom).to_dict())
        self.assertEqual(DeploymentConfig.from_dict(custom).registry_disk_refuse_percent, 95)
        for invalid in (
            {"registry_disk_cleanup_percent": 95},  # above refuse
            {"registry_disk_target_percent": 75},  # above cleanup
            {"registry_disk_refuse_percent": 101},
            {"registry_blob_grace_seconds": 60},
        ):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                DeploymentConfig.from_dict(config.to_dict() | invalid)
        self.assertTrue(config.registry_maintenance_state_file().name.endswith(".json"))
        self.assertTrue(os.path.isabs(config.registry_maintenance_state_file()))


if __name__ == "__main__":
    unittest.main()
