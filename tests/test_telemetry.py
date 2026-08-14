import time
import unittest
from threading import Event, Lock, current_thread

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

from ucloud_sandboxes.telemetry import (
    NonBlockingBatchSpanProcessor,
    Telemetry,
    TelemetryHealth,
    TelemetrySettings,
)


class _BlockingExporter(SpanExporter):
    def __init__(self) -> None:
        self.started = Event()
        self.release = Event()
        self.lock = Lock()
        self.export_threads: list[str] = []
        self.exported = 0

    def export(self, spans):
        with self.lock:
            self.export_threads.append(current_thread().name)
        self.started.set()
        self.release.wait(5)
        with self.lock:
            self.exported += len(spans)
        return SpanExportResult.SUCCESS

    def shutdown(self) -> None:
        self.release.set()


class TelemetryTests(unittest.TestCase):
    def test_settings_require_an_origin_and_bounded_queue(self) -> None:
        normalized = TelemetrySettings(
            endpoint="https://collector.example:4318/",
            max_queue_size=64,
            max_export_batch_size=32,
        ).validated()
        self.assertEqual(normalized.endpoint, "https://collector.example:4318")

        with self.assertRaisesRegex(ValueError, r"HTTP\(S\) origin"):
            TelemetrySettings(endpoint="https://collector.example/path").validated()
        with self.assertRaisesRegex(ValueError, "cannot exceed"):
            TelemetrySettings(
                max_queue_size=1,
                max_export_batch_size=2,
            ).validated()

    def test_disabled_telemetry_is_a_fast_noop(self) -> None:
        telemetry = Telemetry.disabled()
        with telemetry.span("test.noop"):
            pass

        self.assertFalse(telemetry.enabled)
        self.assertEqual(telemetry.health()["accepted_spans"], 0)

    def test_w3c_context_survives_mixed_case_transport_headers(self) -> None:
        first = Telemetry.create(
            TelemetrySettings(endpoint="", trace_sample_ratio=1.0),
            service_name="first",
            service_version="test",
            deployment_id="test",
        )
        # Use SDK providers directly here so the test does not open an OTLP
        # connection while still exercising the platform's carrier handling.
        first_provider = TracerProvider()
        second_provider = TracerProvider()
        first.tracer = first_provider.get_tracer("first")
        second = Telemetry.disabled("second")
        second.tracer = second_provider.get_tracer("second")

        with first.span("gateway") as root:
            headers: dict[str, str] = {}
            first.inject(headers)
            mixed_case = {"Traceparent": headers["traceparent"]}
            with second.span(
                "worker",
                parent_context=second.extracted_context(mixed_case),
            ) as child:
                self.assertEqual(child.trace_id, root.trace_id)

        first_provider.shutdown()
        second_provider.shutdown()

    def test_exporter_backpressure_never_blocks_request_threads(self) -> None:
        exporter = _BlockingExporter()
        health = TelemetryHealth(queue_capacity=4)
        processor = NonBlockingBatchSpanProcessor(
            exporter,
            health,
            max_queue_size=4,
            max_export_batch_size=1,
            export_interval_ms=60_000,
        )
        provider = TracerProvider()
        provider.add_span_processor(processor)
        tracer = provider.get_tracer("test")

        with tracer.start_as_current_span("occupy-exporter"):
            pass
        self.assertTrue(exporter.started.wait(1))
        caller_thread = current_thread().name
        started = time.perf_counter()
        for index in range(1_000):
            with tracer.start_as_current_span(f"span-{index}"):
                pass
        elapsed = time.perf_counter() - started
        snapshot = health.snapshot(queue_size=processor.queue_size, enabled=True)

        self.assertLess(elapsed, 1.0)
        self.assertGreater(snapshot["dropped_spans"], 0)
        self.assertNotIn(caller_thread, exporter.export_threads)
        exporter.release.set()
        provider.shutdown()

    def test_sampled_spans_record_thread_cpu_without_an_extra_metric(self) -> None:
        exporter = InMemorySpanExporter()
        health = TelemetryHealth(queue_capacity=8)
        processor = NonBlockingBatchSpanProcessor(
            exporter,
            health,
            max_queue_size=8,
            max_export_batch_size=1,
            export_interval_ms=60_000,
        )
        provider = TracerProvider()
        provider.add_span_processor(processor)
        telemetry = Telemetry.disabled("cpu-test")
        telemetry.tracer = provider.get_tracer("cpu-test")
        telemetry._tracer_provider = provider
        telemetry._processor = processor

        cpu_started = time.thread_time()
        with telemetry.span("cpu.work"):
            while time.thread_time() - cpu_started < 0.002:
                pass
        self.assertTrue(processor.force_flush())
        spans = exporter.get_finished_spans()

        self.assertEqual(len(spans), 1)
        attributes = spans[0].attributes
        self.assertGreater(attributes["ucloud.span.thread_cpu.duration"], 0)
        provider.shutdown()


if __name__ == "__main__":
    unittest.main()
