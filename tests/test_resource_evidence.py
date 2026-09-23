from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import os
import unittest
from unittest.mock import patch

from ucloud_sandboxes.models import NodeRuntimeMetrics, utc_now
from ucloud_sandboxes.resource_evidence import (
    MemoryBackingCapacity,
    ResourceEvidenceSampler,
    read_memory_pressure,
    sample_memory_backing,
)
from ucloud_sandboxes.runtime_metrics import read_proc_stat_cpu


class ResourceEvidenceTests(unittest.TestCase):
    def test_ram_backing_uses_exact_mount_available_bytes_and_detects_replacement(self):
        backing = self.root / 'active'
        backing.mkdir()
        fdinfo = self.proc / 'self/fdinfo'
        fdinfo.mkdir()
        info = backing.stat()
        device = f'{os.major(info.st_dev)}:{os.minor(info.st_dev)}'
        mountinfo = self.proc / 'self/mountinfo'
        mountinfo.write_text(f'42 1 {device} / {backing} rw,nosuid - tmpfs tmpfs rw,noswap\n')
        original = os.open

        def opened(path, flags):
            descriptor = original(path, flags)
            (fdinfo / str(descriptor)).write_text('mnt_id:\t42\n')
            return descriptor

        space = SimpleNamespace(f_blocks=100, f_bavail=4, f_bfree=9, f_frsize=4096)
        with patch('ucloud_sandboxes.resource_evidence.os.open', side_effect=opened), \
                patch('ucloud_sandboxes.resource_evidence.os.fstatvfs', return_value=space):
            first = sample_memory_backing(backing, proc_root=self.proc)
            self.assertEqual(first.total_bytes, 409600)
            self.assertEqual(first.available_bytes, 16384)
            self.assertEqual(first.identity, f'42:{device}:{info.st_ino}')
            # Same identity needs no repeated potentially large mount-table scan.
            mountinfo.unlink()
            self.assertEqual(sample_memory_backing(backing, proc_root=self.proc), first)
            backing.rename(self.root / 'old-active')
            backing.mkdir()
            self.assertEqual(sample_memory_backing(backing, proc_root=self.proc), MemoryBackingCapacity())

    def test_disabled_backing_differs_from_missing_or_wrong_filesystem(self):
        self.assertIsNone(sample_memory_backing(None))
        self.assertEqual(sample_memory_backing(self.root / 'missing', proc_root=self.proc),
                         MemoryBackingCapacity())
        backing = self.root / 'ordinary'
        backing.mkdir()
        self.assertEqual(sample_memory_backing(backing, proc_root=self.proc), MemoryBackingCapacity())

    def test_backing_capacity_wire_retains_unknown_and_old_worker_defaults(self):
        metrics = NodeRuntimeMetrics(collected_at=utc_now(), memory_backing=MemoryBackingCapacity())
        self.assertEqual(NodeRuntimeMetrics.from_dict(metrics.to_dict()), metrics)
        raw = metrics.to_dict()
        raw.pop('memory_backing')
        self.assertIsNone(NodeRuntimeMetrics.from_dict(raw).memory_backing)
        for invalid in ({'total_bytes': 1, 'available_bytes': 2, 'identity': 'mount'},
                        {'total_bytes': 10**1000, 'available_bytes': 1, 'identity': 'mount'},
                        {'total_bytes': True, 'available_bytes': 1, 'identity': 'mount'},
                        {'total_bytes': None, 'available_bytes': 1, 'identity': None}):
            raw['memory_backing'] = invalid
            self.assertIsNone(NodeRuntimeMetrics.from_dict(raw))

    def setUp(self):
        temp = TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.proc = self.root / "proc"
        self.sys = self.root / "sys"
        (self.proc / "sys/kernel/random").mkdir(parents=True)
        (self.proc / "sys/kernel/random/boot_id").write_text("boot-one")
        (self.proc / "self").mkdir()
        (self.proc / "self/cgroup").write_text("0::/agent\n")
        (self.sys / "dev/block").mkdir(parents=True)
        self.cg = self.sys / "fs/cgroup/agent"
        self.cg.mkdir(parents=True)
        self.now = [0.0]
        self.sampler = ResourceEvidenceSampler(
            self.proc, self.sys, clock=lambda: self.now[0]
        )

    def disk(
        self, name="vda", number="254:0", sequence=1, virtual=False, partition=False
    ):
        path = self.sys / "devices" / ("virtual" if virtual else "pci") / name
        (path / "slaves").mkdir(parents=True, exist_ok=True)
        (path / "diskseq").write_text(str(sequence))
        if partition:
            (path / "partition").write_text("1")
        link = self.sys / "dev/block" / number
        if not link.exists():
            link.symlink_to(path)
        return path

    def test_cpu_cache_requires_recent_comparable_samples_and_never_waits(self):
        def collect(at, stat):
            self.now[0] = at
            (self.proc / "stat").write_text(stat)
            self.sampler.collect()
            return self.sampler.cached_cpu_percent()

        with patch("ucloud_sandboxes.resource_evidence.threading.Thread") as thread:
            self.assertIsNone(self.sampler.cached_cpu_percent())
            self.assertIsNone(collect(0, "cpu 100 0 0 100 0 0 0 0\n"))
            self.assertEqual(collect(1, "cpu 175 0 0 125 0 0 0 0\n"), 75)
            self.now[0] = 3.01
            self.assertIsNone(self.sampler.cached_cpu_percent())
            # Recovery after an outage must establish a new short baseline.
            self.assertIsNone(collect(4, "cpu 200 0 0 200 0 0 0 0\n"))
            self.assertEqual(collect(5, "cpu 200 0 0 300 0 0 0 0\n"), 0)
            self.assertIsNone(collect(6, "cpu invalid\n"))
            self.assertIsNone(collect(7, "cpu 300 0 0 400 0 0 0 0\n"))
            self.assertEqual(collect(8, "cpu 400 0 0 400 0 0 0 0\n"), 100)
            self.assertIsNone(collect(9, "cpu 10 0 0 10 0 0 0 0\n"))
            self.assertIsNone(collect(10, "cpu 10 0 0 10 0 0 0 0\n"))
            self.assertEqual(thread.call_count, 1)

    def test_failed_collection_does_not_refresh_cpu_freshness(self):
        with patch("ucloud_sandboxes.resource_evidence.threading.Thread"):
            (self.proc / "stat").write_text("cpu 100 0 0 100\n")
            self.sampler.collect()
            self.now[0] = 1
            (self.proc / "stat").write_text("cpu 150 0 0 150\n")
            self.sampler.collect()
            self.assertEqual(self.sampler.cached_cpu_percent(), 50)
            self.now[0] = 4
            with patch("ucloud_sandboxes.resource_evidence.read_leaf_disks", side_effect=OSError):
                with self.assertRaises(OSError):
                    self.sampler.collect()
            self.assertIsNone(self.sampler.cached_cpu_percent())

    def test_cumulative_cpu_excludes_wait_steal_and_duplicate_guest_charge(self):
        (self.proc / "stat").write_text("cpu 100 20 30 400 50 6 7 8 90 10\n")
        with patch("ucloud_sandboxes.resource_evidence.os.sysconf", return_value=100):
            sample = self.sampler.collect()
        self.assertEqual(sample.host_cpu_usage_usec, 1_630_000)
        self.assertEqual(sample.host_cpu_steal_usec, 80_000)
        self.assertEqual(read_proc_stat_cpu(self.proc / "stat"), (621, 450))
        metrics = NodeRuntimeMetrics(collected_at=utc_now(), resource_evidence=sample)
        raw = metrics.to_dict()
        raw["resource_evidence"].pop("host_cpu_usage_usec")
        raw["resource_evidence"].pop("host_cpu_steal_usec")
        older = NodeRuntimeMetrics.from_dict(raw)
        self.assertIsNotNone(older)
        self.assertIsNone(older.resource_evidence.host_cpu_usage_usec)
        for bad in (-1, True, 1.5, "100"):
            raw = metrics.to_dict()
            raw["resource_evidence"]["host_cpu_usage_usec"] = bad
            self.assertIsNone(NodeRuntimeMetrics.from_dict(raw))
        (self.proc / "stat").write_text("cpu malformed\n")
        self.assertIsNone(self.sampler.collect().host_cpu_usage_usec)

    def test_rates_units_and_exclusion_of_stacked_devices(self):
        self.disk()
        self.disk("vda1", "254:1", partition=True)
        self.disk("ublkb0", "259:0", virtual=True)
        self.disk("dm-0", "253:0", virtual=True)
        stacked = self.disk("stacked", "252:0")
        (stacked / "slaves/other").touch()

        def counters(values):
            (self.proc / "diskstats").write_text(
                "\n".join(
                    f"{number.replace(':', ' ')} {name} {values}"
                    for name, number in (
                        ("vda", "254:0"),
                        ("vda1", "254:1"),
                        ("ublkb0", "259:0"),
                        ("dm-0", "253:0"),
                        ("stacked", "252:0"),
                    )
                )
            )

        counters("10 0 100 20 5 0 200 30 2 10 30")
        first = self.sampler.collect()
        self.assertEqual(len(first.devices), 1)
        self.assertIsNone(first.devices[0].read_iops)
        self.assertEqual(first.devices[0].read_bytes, 100 * 512)
        self.assertEqual(first.devices[0].write_bytes, 200 * 512)
        self.now[0] = 2
        counters("14 0 108 32 7 0 204 40 0 510 1030")
        device = self.sampler.collect().devices[0]
        self.assertEqual(device.read_bytes_per_second, 2048)
        self.assertEqual(device.write_bytes_per_second, 1024)
        self.assertEqual(device.read_iops, 2)
        self.assertEqual(device.write_iops, 1)
        self.assertEqual(device.read_await_ms, 3)
        self.assertEqual(device.write_await_ms, 5)
        self.assertEqual(device.average_queue_depth, 0.5)
        self.assertEqual(device.busy_percent, 25)
        self.assertEqual(device.read_bytes, 108 * 512)
        self.assertEqual(device.write_bytes, 204 * 512)

    def test_device_totals_accept_older_readers_payload_but_not_partial_totals(self):
        self.disk()
        (self.proc / "diskstats").write_text("254 0 vda 10 0 100 20 5 0 200 30 0 10 30")
        metrics = NodeRuntimeMetrics(collected_at=utc_now(), resource_evidence=self.sampler.collect())
        raw = metrics.to_dict()
        device = raw["resource_evidence"]["devices"][0]
        device.pop("read_bytes")
        self.assertIsNone(NodeRuntimeMetrics.from_dict(raw))
        device.pop("write_bytes")
        restored = NodeRuntimeMetrics.from_dict(raw)
        self.assertIsNotNone(restored)
        self.assertIsNone(restored.resource_evidence.devices[0].read_bytes)
        self.assertIsNone(restored.resource_evidence.devices[0].write_bytes)
        for value in (-1, True, 1.5, "123"):
            raw = metrics.to_dict()
            raw["resource_evidence"]["devices"][0]["write_bytes"] = value
            self.assertIsNone(NodeRuntimeMetrics.from_dict(raw))

    def test_device_reset_replacement_and_disappearance_reset_baseline(self):
        disk = self.disk()
        for index, (sequence, counters) in enumerate(
            (
                (1, "10 0 100 20 5 0 200 30 0 10 30"),
                (1, "1 0 1 1 1 0 1 1 0 1 1"),
                (2, "20 0 200 40 10 0 400 60 0 20 60"),
            )
        ):
            self.now[0] = index
            (disk / "diskseq").write_text(str(sequence))
            (self.proc / "diskstats").write_text(f"254 0 vda {counters}")
            self.assertIsNone(self.sampler.collect().devices[0].read_iops)
        (self.proc / "diskstats").unlink()
        self.now[0] = 3
        self.assertIsNone(self.sampler.collect().devices)
        (self.proc / "diskstats").write_text(
            "254 0 vda 30 0 300 60 20 0 500 80 0 40 80"
        )
        self.now[0] = 4
        self.assertIsNone(self.sampler.collect().devices[0].read_iops)

    def test_unknown_is_not_zero_and_cgroup_scope_is_explicit(self):
        first = self.sampler.collect()
        self.assertIsNone(first.memory_dirty_bytes)
        self.assertIsNone(first.cgroup_cpu_usage_usec)
        self.assertEqual(first.cgroup_path, "/agent")
        (self.proc / "meminfo").write_text(
            "Dirty: 0 kB\nWriteback: 12 kB\nMapped: 20 kB\n"
        )
        (self.proc / "vmstat").write_text("pgmajfault 9\nworkingset_refault_file 17\n")
        (self.cg / "cpu.stat").write_text(
            "usage_usec 1000\nthrottled_usec 3\nnr_throttled 2\n"
        )
        (self.cg / "memory.stat").write_text(
            "anon 100\nfile 200\nfile_dirty 20\npgmajfault 3\n"
        )
        (self.cg / "memory.current").write_text("350")
        result = self.sampler.collect()
        self.assertEqual(result.memory_dirty_bytes, 0)
        self.assertEqual(result.memory_writeback_bytes, 12 * 1024)
        self.assertEqual(result.host_major_faults, 9)
        self.assertEqual(result.cgroup_cpu_usage_usec, 1000)
        self.assertEqual(result.cgroup_cpu_throttled_usec, 3)
        self.assertEqual(result.cgroup_memory_current_bytes, 350)
        self.assertEqual(result.cgroup_memory_file_bytes, 200)
        self.assertIsNone(result.cgroup_refault_file)

    def test_wire_roundtrip_and_gateway_first_rolling_upgrade(self):
        metrics = NodeRuntimeMetrics(
            collected_at=utc_now(), resource_evidence=self.sampler.collect()
        )
        self.assertEqual(NodeRuntimeMetrics.from_dict(metrics.to_dict()), metrics)
        legacy = metrics.to_dict()
        legacy.pop("resource_evidence")
        self.assertEqual(
            NodeRuntimeMetrics.from_dict(legacy),
            replace(metrics, resource_evidence=None),
        )
        for invalid in (-1, True, float("nan"), "0", 10**1000):
            raw = metrics.to_dict()
            raw["resource_evidence"]["cgroup_cpu_usage_usec"] = invalid
            self.assertIsNone(NodeRuntimeMetrics.from_dict(raw))
        raw = metrics.to_dict()
        raw["resource_evidence"]["collected_at"] = "yesterday"
        self.assertIsNone(NodeRuntimeMetrics.from_dict(raw))

    def test_requests_only_start_one_background_collector_and_expire_stale_data(self):
        with patch("ucloud_sandboxes.resource_evidence.threading.Thread") as thread:
            self.assertIsNone(self.sampler.cached())
            self.assertIsNone(self.sampler.cached())
            self.assertEqual(thread.call_count, 1)
            self.assertEqual(thread.return_value.start.call_count, 1)
            current = self.sampler.collect()
            self.assertIs(self.sampler.cached(), current)
            self.now[0] = 6
            self.assertIsNone(self.sampler.cached())

    def test_canonical_pressure_uses_named_some_field_not_line_order(self):
        (self.proc / "pressure").mkdir()
        (self.proc / "pressure/memory").write_text("full avg10=1.0\nsome avg10=3.0\n")
        evidence = read_memory_pressure(self.proc)
        self.assertEqual(evidence.memory_psi, {"full": 1, "some": 3})
        self.assertEqual(evidence.io_psi, {})

    def test_guest_cpu_time_is_not_counted_twice(self):
        (self.proc / "stat").write_text("cpu 100 20 30 40 0 0 0 0 10 5\n")
        self.assertEqual(read_proc_stat_cpu(self.proc / "stat"), (190, 40))
