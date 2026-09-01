from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Barrier, Event, Lock, Thread
import unittest

from hypothesis import given, settings, strategies as st

from ucloud_sandboxes.models import NodeRuntimeMetrics, utc_now
from ucloud_sandboxes.runtime_metrics import (
    SingleFlightRuntimeMetricsSampler,
    cpu_percent_from_samples,
    read_proc_meminfo,
    read_proc_pressure,
    read_proc_stat_cpu,
    sample_node_runtime_metrics,
)


class RuntimeMetricsTests(unittest.TestCase):
    def test_single_flight_rejects_invalid_freshness(self) -> None:
        def provider() -> NodeRuntimeMetrics:
            return NodeRuntimeMetrics(collected_at=utc_now())

        for freshness in (-1.0, float("nan"), float("inf")):
            with self.subTest(freshness=freshness), self.assertRaises(ValueError):
                SingleFlightRuntimeMetricsSampler(
                    provider,
                    freshness_seconds=freshness,
                )

    def test_single_flight_coalesces_a_concurrent_sampling_burst(self) -> None:
        worker_count = 8
        start = Barrier(worker_count + 1)
        sample_started = Event()
        release_sample = Event()
        guard = Lock()
        calls = 0
        results: list[NodeRuntimeMetrics | None] = []
        errors: list[BaseException] = []
        expected = NodeRuntimeMetrics(
            collected_at=utc_now(),
            cpu_percent=12.5,
            cpu_count=32,
        )

        def provider() -> NodeRuntimeMetrics:
            nonlocal calls
            with guard:
                calls += 1
            sample_started.set()
            release_sample.wait(timeout=2.0)
            return expected

        sampler = SingleFlightRuntimeMetricsSampler(provider, clock=lambda: 0.0)

        def read() -> None:
            start.wait()
            try:
                observed = sampler()
            except BaseException as exc:
                with guard:
                    errors.append(exc)
            else:
                with guard:
                    results.append(observed)

        threads = [Thread(target=read) for _ in range(worker_count)]
        for thread in threads:
            thread.start()
        start.wait()
        self.assertTrue(sample_started.wait(timeout=1.0))
        release_sample.set()
        for thread in threads:
            thread.join(timeout=2.0)

        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(errors, [])
        self.assertEqual(calls, 1)
        self.assertEqual(len(results), worker_count)
        self.assertTrue(all(result is expected for result in results))

    def test_single_flight_refreshes_after_freshness_window(self) -> None:
        now = [10.0]
        calls = 0

        def provider() -> NodeRuntimeMetrics:
            nonlocal calls
            calls += 1
            return NodeRuntimeMetrics(
                collected_at=utc_now(),
                cpu_percent=float(calls),
                cpu_count=32,
            )

        sampler = SingleFlightRuntimeMetricsSampler(
            provider,
            freshness_seconds=0.2,
            clock=lambda: now[0],
        )

        first = sampler()
        self.assertIs(sampler(), first)
        now[0] += 0.199
        self.assertIs(sampler(), first)
        self.assertEqual(calls, 1)

        now[0] += 0.002
        refreshed = sampler()
        self.assertIsNot(refreshed, first)
        self.assertEqual(calls, 2)

    def test_single_flight_exception_wakes_waiter_and_is_retried(self) -> None:
        start = Barrier(3)
        first_started = Event()
        release_failure = Event()
        guard = Lock()
        calls = 0
        results: list[NodeRuntimeMetrics | None] = []
        errors: list[BaseException] = []
        recovered = NodeRuntimeMetrics(collected_at=utc_now(), cpu_percent=5.0)

        def provider() -> NodeRuntimeMetrics:
            nonlocal calls
            with guard:
                calls += 1
                attempt = calls
            if attempt == 1:
                first_started.set()
                release_failure.wait(timeout=2.0)
                raise RuntimeError("sample failed")
            return recovered

        sampler = SingleFlightRuntimeMetricsSampler(provider, clock=lambda: 0.0)

        def read() -> None:
            start.wait()
            try:
                observed = sampler()
            except BaseException as exc:
                with guard:
                    errors.append(exc)
            else:
                with guard:
                    results.append(observed)

        threads = [Thread(target=read) for _ in range(2)]
        for thread in threads:
            thread.start()
        start.wait()
        self.assertTrue(first_started.wait(timeout=1.0))
        release_failure.set()
        for thread in threads:
            thread.join(timeout=2.0)

        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(calls, 2)
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], RuntimeError)
        self.assertEqual(str(errors[0]), "sample failed")
        self.assertEqual(results, [recovered])
        self.assertIs(sampler(), recovered)
        self.assertEqual(calls, 2)

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
            (root / "stat").write_text(
                "cpu  100 0 100 800 0 0 0 0 0 0\n", encoding="utf-8"
            )
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
