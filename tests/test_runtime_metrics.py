from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from hypothesis import given, settings, strategies as st

from ucloud_sandboxes.runtime_metrics import (
    cpu_percent_from_samples,
    read_proc_meminfo,
    read_proc_pressure,
    read_proc_stat_cpu,
    sample_node_runtime_metrics,
)


class RuntimeMetricsTests(unittest.TestCase):
    @settings(max_examples=100, deadline=None, derandomize=True)
    @given(
        total=st.integers(min_value=0, max_value=10**12),
        idle=st.integers(min_value=0, max_value=10**12),
        total_delta=st.integers(min_value=1, max_value=10**9),
        idle_delta=st.integers(min_value=0, max_value=10**9),
    )
    def test_monotonic_cpu_samples_are_bounded_or_rejected(
        self,
        total: int,
        idle: int,
        total_delta: int,
        idle_delta: int,
    ) -> None:
        result = cpu_percent_from_samples(
            (total, idle),
            (total + total_delta, idle + idle_delta),
        )

        if idle_delta > total_delta:
            self.assertIsNone(result)
        else:
            assert result is not None
            self.assertGreaterEqual(result, 0.0)
            self.assertLessEqual(result, 100.0)

    @settings(max_examples=100, deadline=None, derandomize=True)
    @given(
        st.text(
            alphabet=st.characters(blacklist_categories=("Cs",)),
            max_size=512,
        )
    )
    def test_malformed_proc_text_fails_closed(self, payload: str) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "proc-file"
            path.write_text(payload, encoding="utf-8")

            cpu = read_proc_stat_cpu(path)
            memory = read_proc_meminfo(path)
            pressure = read_proc_pressure(path)

        if cpu is not None:
            self.assertGreaterEqual(cpu[0], 0)
            self.assertGreaterEqual(cpu[1], 0)
        self.assertTrue(all(value >= 0 for value in memory.values()))
        self.assertTrue(all(0.0 <= value <= 100.0 for value in pressure.values()))

    def test_calculates_cpu_percent_from_proc_stat_samples(self) -> None:
        self.assertEqual(
            cpu_percent_from_samples((1000, 800), (2000, 1600)),
            20.0,
        )
        self.assertIsNone(cpu_percent_from_samples((1000, 800), (1000, 800)))

    def test_reads_proc_stat_cpu_and_meminfo(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            (root / "stat").write_text("cpu  100 0 100 800 0 0 0 0 0 0\n", encoding="utf-8")
            (root / "meminfo").write_text(
                "MemTotal:       1048576 kB\n"
                "MemAvailable:    786432 kB\n"
                "SwapTotal:       524288 kB\n"
                "SwapFree:        393216 kB\n",
                encoding="utf-8",
            )
            (root / "pressure").mkdir()
            (root / "pressure" / "memory").write_text(
                "some avg10=1.25 avg60=0.50 avg300=0.10 total=123\n"
                "full avg10=0.75 avg60=0.20 avg300=0.05 total=45\n",
                encoding="utf-8",
            )

            self.assertEqual(read_proc_stat_cpu(root / "stat"), (1000, 800))
            self.assertEqual(read_proc_meminfo(root / "meminfo")["MemTotal"], 1048576)
            self.assertEqual(
                read_proc_pressure(root / "pressure" / "memory"),
                {"some": 1.25, "full": 0.75},
            )
            sampled = sample_node_runtime_metrics(proc_root=root, sample_seconds=0)

        self.assertEqual(sampled.memory_total_mb, 1024)
        self.assertEqual(sampled.memory_available_mb, 768)
        self.assertEqual(sampled.memory_used_mb, 256)
        self.assertEqual(sampled.memory_percent, 25.0)
        self.assertEqual(sampled.swap_total_mb, 512)
        self.assertEqual(sampled.swap_used_mb, 128)
        self.assertEqual(sampled.swap_free_mb, 384)
        self.assertEqual(sampled.memory_psi_some_avg10, 1.25)
        self.assertEqual(sampled.memory_psi_full_avg10, 0.75)


if __name__ == "__main__":
    unittest.main()
