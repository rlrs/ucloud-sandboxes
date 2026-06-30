from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from ucloud_sandboxes.runtime_metrics import (
    cpu_percent_from_samples,
    read_proc_meminfo,
    read_proc_stat_cpu,
    sample_node_runtime_metrics,
)


class RuntimeMetricsTests(unittest.TestCase):
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
                "MemAvailable:    786432 kB\n",
                encoding="utf-8",
            )

            self.assertEqual(read_proc_stat_cpu(root / "stat"), (1000, 800))
            self.assertEqual(read_proc_meminfo(root / "meminfo")["MemTotal"], 1048576)
            sampled = sample_node_runtime_metrics(proc_root=root, sample_seconds=0)

        self.assertEqual(sampled.memory_total_mb, 1024)
        self.assertEqual(sampled.memory_available_mb, 768)
        self.assertEqual(sampled.memory_used_mb, 256)
        self.assertEqual(sampled.memory_percent, 25.0)


if __name__ == "__main__":
    unittest.main()
