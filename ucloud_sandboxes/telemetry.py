from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import math
from queue import Empty, Full, Queue
from threading import Event, Lock, Thread
import time
from typing import Any, Iterable, Iterator, Mapping, MutableMapping, Sequence
from urllib.parse import urlsplit, urlunsplit

from opentelemetry import metrics, trace
from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.metrics import Meter
from opentelemetry.propagate import extract, inject
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan, Span, TracerProvider
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult, SpanProcessor
from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased
from opentelemetry.trace import Link, SpanKind, Status, StatusCode


DEFAULT_EXPORT_INTERVAL_MS = 5_000
DEFAULT_EXPORT_TIMEOUT_MS = 3_000
DEFAULT_MAX_QUEUE_SIZE = 4_096
DEFAULT_MAX_EXPORT_BATCH_SIZE = 512
_SHUTDOWN_JOIN_SECONDS = 5.0
_MIN_THREAD_CPU_SPAN_DURATION_SECONDS = 0.001


@dataclass(frozen=True)
class TelemetrySettings:
    """Provider-neutral OTLP settings shared by every platform service."""

    endpoint: str = ""
    trace_sample_ratio: float = 0.1
    export_interval_ms: int = DEFAULT_EXPORT_INTERVAL_MS
    export_timeout_ms: int = DEFAULT_EXPORT_TIMEOUT_MS
    max_queue_size: int = DEFAULT_MAX_QUEUE_SIZE
    max_export_batch_size: int = DEFAULT_MAX_EXPORT_BATCH_SIZE

    def validated(self) -> "TelemetrySettings":
        endpoint = self.endpoint.strip().rstrip("/")
        if endpoint:
            parsed = urlsplit(endpoint)
            if (
                parsed.scheme not in {"http", "https"}
                or not parsed.hostname
                or parsed.username is not None
                or parsed.password is not None
                or parsed.path not in {"", "/"}
                or parsed.query
                or parsed.fragment
            ):
                raise ValueError("telemetry endpoint must be an HTTP(S) origin")
            netloc = parsed.hostname
            if parsed.port is not None:
                netloc += f":{parsed.port}"
            endpoint = urlunsplit((parsed.scheme, netloc, "", "", ""))
        if (
            isinstance(self.trace_sample_ratio, bool)
            or not isinstance(self.trace_sample_ratio, (int, float))
            or not math.isfinite(float(self.trace_sample_ratio))
            or not 0.0 <= float(self.trace_sample_ratio) <= 1.0
        ):
            raise ValueError("telemetry trace_sample_ratio must be between 0 and 1")
        for name in (
            "export_interval_ms",
            "export_timeout_ms",
            "max_queue_size",
            "max_export_batch_size",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"telemetry {name} must be a positive integer")
        if self.max_export_batch_size > self.max_queue_size:
            raise ValueError(
                "telemetry max_export_batch_size cannot exceed max_queue_size"
            )
        return TelemetrySettings(
            endpoint=endpoint,
            trace_sample_ratio=float(self.trace_sample_ratio),
            export_interval_ms=self.export_interval_ms,
            export_timeout_ms=self.export_timeout_ms,
            max_queue_size=self.max_queue_size,
            max_export_batch_size=self.max_export_batch_size,
        )


class TelemetryHealth:
    """Lock-bounded exporter health; never participates in product success."""

    def __init__(self, *, queue_capacity: int) -> None:
        self.queue_capacity = queue_capacity
        self._lock = Lock()
        self.accepted_spans = 0
        self.dropped_spans = 0
        self.exported_spans = 0
        self.failed_exports = 0
        self.last_success_at = ""
        self.last_error_at = ""
        self.last_error = ""

    def accepted(self) -> None:
        with self._lock:
            self.accepted_spans += 1

    def dropped(self) -> None:
        with self._lock:
            self.dropped_spans += 1

    def exported(self, count: int) -> None:
        with self._lock:
            self.exported_spans += max(0, count)
            self.last_success_at = _utc_now()
            self.last_error = ""

    def failed(self, error: BaseException | str) -> None:
        with self._lock:
            self.failed_exports += 1
            self.last_error_at = _utc_now()
            self.last_error = str(error)[:512]

    def snapshot(self, *, queue_size: int, enabled: bool) -> dict[str, Any]:
        with self._lock:
            return {
                "enabled": enabled,
                "queue_size": max(0, queue_size),
                "queue_capacity": self.queue_capacity,
                "accepted_spans": self.accepted_spans,
                "dropped_spans": self.dropped_spans,
                "exported_spans": self.exported_spans,
                "failed_exports": self.failed_exports,
                "last_success_at": self.last_success_at,
                "last_error_at": self.last_error_at,
                "last_error": self.last_error,
            }


class _HealthTrackingExporter(SpanExporter):
    def __init__(self, exporter: SpanExporter, health: TelemetryHealth) -> None:
        self._exporter = exporter
        self._health = health

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        try:
            result = self._exporter.export(spans)
        except BaseException as exc:
            self._health.failed(exc)
            return SpanExportResult.FAILURE
        if result is SpanExportResult.SUCCESS:
            self._health.exported(len(spans))
        else:
            self._health.failed(f"OTLP span export returned {result!s}")
        return result

    def force_flush(self, timeout_millis: int = 30_000) -> bool:
        flush = getattr(self._exporter, "force_flush", None)
        if flush is None:
            return True
        try:
            return bool(flush(timeout_millis=timeout_millis))
        except TypeError:
            return bool(flush(timeout_millis))

    def shutdown(self) -> None:
        self._exporter.shutdown()


class NonBlockingBatchSpanProcessor(SpanProcessor):
    """A bounded, drop-on-overload processor with no exporter work on callers.

    OpenTelemetry's processor contract allows ``on_end`` to run on the request
    thread. This implementation performs exactly one non-blocking queue append
    there. Serialization, compression, DNS, retries, and network I/O stay on a
    dedicated daemon thread. Saturation drops telemetry instead of adding tail
    latency to a sandbox operation.
    """

    def __init__(
        self,
        exporter: SpanExporter,
        health: TelemetryHealth,
        *,
        max_queue_size: int,
        max_export_batch_size: int,
        export_interval_ms: int,
    ) -> None:
        self._exporter = exporter
        self._health = health
        self._queue: Queue[ReadableSpan] = Queue(maxsize=max_queue_size)
        self._batch_size = max_export_batch_size
        self._interval_seconds = export_interval_ms / 1000.0
        self._shutdown = Event()
        self._flush = Event()
        self._worker = Thread(
            target=self._run,
            name="ucloud-telemetry-exporter",
            daemon=True,
        )
        self._worker.start()

    @property
    def queue_size(self) -> int:
        return self._queue.qsize()

    def on_start(self, span: Span, parent_context: Context | None = None) -> None:
        del span, parent_context

    def on_end(self, span: ReadableSpan) -> None:
        if self._shutdown.is_set() or not span.context.trace_flags.sampled:
            return
        try:
            self._queue.put_nowait(span)
            self._health.accepted()
        except Full:
            self._health.dropped()
            return
        if self._queue.qsize() >= self._batch_size:
            self._flush.set()

    def shutdown(self) -> None:
        self._shutdown.set()
        self._flush.set()
        self._worker.join(timeout=_SHUTDOWN_JOIN_SECONDS)
        try:
            self._exporter.shutdown()
        except BaseException as exc:
            self._health.failed(exc)

    def force_flush(self, timeout_millis: int = 30_000) -> bool:
        deadline = time.monotonic() + max(0, timeout_millis) / 1000.0
        self._flush.set()
        while not self._queue.empty() and time.monotonic() < deadline:
            time.sleep(0.001)
        remaining_ms = max(0, int((deadline - time.monotonic()) * 1000))
        try:
            return self._queue.empty() and self._exporter.force_flush(remaining_ms)
        except BaseException as exc:
            self._health.failed(exc)
            return False

    def _run(self) -> None:
        while not self._shutdown.is_set() or not self._queue.empty():
            self._flush.wait(self._interval_seconds)
            self._flush.clear()
            self._drain_once()
            while self._queue.qsize() >= self._batch_size:
                self._drain_once()

    def _drain_once(self) -> None:
        batch: list[ReadableSpan] = []
        while len(batch) < self._batch_size:
            try:
                batch.append(self._queue.get_nowait())
            except Empty:
                break
        if not batch:
            return
        try:
            self._exporter.export(batch)
        except BaseException as exc:
            self._health.failed(exc)


class Telemetry:
    def __init__(
        self,
        *,
        settings: TelemetrySettings,
        service_name: str,
        tracer: trace.Tracer,
        meter: Meter,
        tracer_provider: TracerProvider | None,
        meter_provider: MeterProvider | None,
        processor: NonBlockingBatchSpanProcessor | None,
        health: TelemetryHealth,
    ) -> None:
        self.settings = settings
        self.service_name = service_name
        self.tracer = tracer
        self.meter = meter
        self._tracer_provider = tracer_provider
        self._meter_provider = meter_provider
        self._processor = processor
        self._health = health
        self._duration = meter.create_histogram(
            "ucloud.platform.operation.duration",
            unit="s",
            description="End-to-end and phase duration for platform operations",
        )
        self._operations = meter.create_counter(
            "ucloud.platform.operation.count",
            description="Completed platform operations",
        )

    @classmethod
    def create(
        cls,
        settings: TelemetrySettings,
        *,
        service_name: str,
        service_version: str,
        deployment_id: str,
        attributes: Mapping[str, str | int | float | bool] | None = None,
    ) -> "Telemetry":
        settings = settings.validated()
        health = TelemetryHealth(queue_capacity=settings.max_queue_size)
        resource = Resource.create(
            {
                "service.name": service_name,
                "service.version": service_version,
                "deployment.id": deployment_id,
                **dict(attributes or {}),
            }
        )
        if not settings.endpoint:
            return cls(
                settings=settings,
                service_name=service_name,
                tracer=trace.NoOpTracerProvider().get_tracer(service_name),
                meter=metrics.NoOpMeterProvider().get_meter(service_name),
                tracer_provider=None,
                meter_provider=None,
                processor=None,
                health=health,
            )

        sampler = ParentBased(TraceIdRatioBased(settings.trace_sample_ratio))
        tracer_provider = TracerProvider(resource=resource, sampler=sampler)
        trace_exporter = _HealthTrackingExporter(
            OTLPSpanExporter(
                endpoint=f"{settings.endpoint}/v1/traces",
                timeout=settings.export_timeout_ms / 1000.0,
            ),
            health,
        )
        processor = NonBlockingBatchSpanProcessor(
            trace_exporter,
            health,
            max_queue_size=settings.max_queue_size,
            max_export_batch_size=settings.max_export_batch_size,
            export_interval_ms=settings.export_interval_ms,
        )
        tracer_provider.add_span_processor(processor)

        metric_reader = PeriodicExportingMetricReader(
            OTLPMetricExporter(
                endpoint=f"{settings.endpoint}/v1/metrics",
                timeout=settings.export_timeout_ms / 1000.0,
            ),
            export_interval_millis=settings.export_interval_ms,
            export_timeout_millis=settings.export_timeout_ms,
        )
        meter_provider = MeterProvider(
            resource=resource, metric_readers=[metric_reader]
        )
        return cls(
            settings=settings,
            service_name=service_name,
            tracer=tracer_provider.get_tracer(service_name, service_version),
            meter=meter_provider.get_meter(service_name, service_version),
            tracer_provider=tracer_provider,
            meter_provider=meter_provider,
            processor=processor,
            health=health,
        )

    @classmethod
    def disabled(cls, service_name: str = "ucloud-sandboxes-test") -> "Telemetry":
        return cls.create(
            TelemetrySettings(),
            service_name=service_name,
            service_version="test",
            deployment_id="test",
        )

    @property
    def enabled(self) -> bool:
        return self._processor is not None

    @contextmanager
    def span(
        self,
        name: str,
        *,
        kind: SpanKind = SpanKind.INTERNAL,
        attributes: Mapping[str, Any] | None = None,
        parent_context: Context | None = None,
        links: Iterable[Link] = (),
        metric_operation: str | None = None,
    ) -> Iterator["ObservedSpan"]:
        started = time.monotonic()
        status = "ok"
        with self.tracer.start_as_current_span(
            name,
            context=parent_context,
            kind=kind,
            attributes=_span_attributes(attributes),
            links=tuple(links),
            record_exception=False,
            set_status_on_exception=False,
        ) as raw_span:
            thread_cpu_started = (
                time.thread_time() if raw_span.is_recording() else None
            )
            span = ObservedSpan(raw_span)
            try:
                yield span
            except BaseException as exc:
                status = "error"
                span._error = True
                raw_span.record_exception(exc)
                raw_span.set_status(Status(StatusCode.ERROR, str(exc)[:256]))
                span.set_attribute("error.type", type(exc).__name__)
                raise
            finally:
                duration = max(0.0, time.monotonic() - started)
                if (
                    thread_cpu_started is not None
                    and duration >= _MIN_THREAD_CPU_SPAN_DURATION_SECONDS
                ):
                    thread_cpu_duration = max(
                        0.0,
                        time.thread_time() - thread_cpu_started,
                    )
                    raw_span.set_attribute(
                        "ucloud.span.thread_cpu.duration",
                        thread_cpu_duration,
                    )
                raw_status = getattr(
                    getattr(raw_span, "status", None), "status_code", None
                )
                if span._error or raw_status is StatusCode.ERROR:
                    status = "error"
                metric_attributes = {
                    "operation": metric_operation or name,
                    "status": status,
                }
                self._duration.record(duration, metric_attributes)
                self._operations.add(1, metric_attributes)

    def extracted_context(self, carrier: Mapping[str, str]) -> Context:
        return extract({str(key).lower(): str(value) for key, value in carrier.items()})

    def inject(self, carrier: MutableMapping[str, str]) -> None:
        inject(carrier)

    def current_trace_headers(self) -> dict[str, str]:
        carrier: dict[str, str] = {}
        inject(carrier)
        return {
            key: value
            for key, value in carrier.items()
            if key.lower() in {"traceparent", "tracestate"}
        }

    def add_event(
        self,
        name: str,
        attributes: Mapping[str, Any] | None = None,
    ) -> None:
        trace.get_current_span().add_event(
            name,
            _flatten_attributes(attributes or {}),
        )

    def set_current_attributes(self, attributes: Mapping[str, Any]) -> None:
        trace.get_current_span().set_attributes(_span_attributes(attributes))

    def link_from_headers(self, carrier: Mapping[str, str]) -> Link | None:
        context = trace.get_current_span(
            self.extracted_context(carrier)
        ).get_span_context()
        return Link(context) if context.is_valid else None

    def health(self) -> dict[str, Any]:
        return self._health.snapshot(
            queue_size=self._processor.queue_size if self._processor is not None else 0,
            enabled=self.enabled,
        )

    def shutdown(self) -> None:
        if self._tracer_provider is not None:
            self._tracer_provider.shutdown()
        if self._meter_provider is not None:
            self._meter_provider.shutdown()


class ObservedSpan:
    """Small sanitizing facade used by platform instrumentation."""

    def __init__(self, span: trace.Span) -> None:
        object.__setattr__(self, "_span", span)
        object.__setattr__(self, "_error", False)

    @property
    def span_id(self) -> str:
        return span_id_hex(self._span)

    @property
    def trace_id(self) -> str:
        return trace_id_hex(self._span)

    @property
    def status(self) -> str:
        return "ok"

    @status.setter
    def status(self, value: str) -> None:
        if value == "error":
            self._error = True
            self._span.set_status(Status(StatusCode.ERROR))

    def set_attribute(self, key: str, value: Any) -> None:
        encoded = _span_attributes({key: value})
        if encoded:
            self._span.set_attribute(key, encoded[key])

    def set_attributes(self, attributes: Mapping[str, Any]) -> None:
        self._span.set_attributes(_span_attributes(attributes))

    def add_event(
        self,
        name: str,
        attributes: Mapping[str, Any] | None = None,
    ) -> None:
        self._span.add_event(name, _flatten_attributes(attributes or {}))

    def set_error(self, error: BaseException | str) -> None:
        self._error = True
        set_span_error(self._span, error)


def set_span_error(span: trace.Span | ObservedSpan, error: BaseException | str) -> None:
    if isinstance(span, ObservedSpan):
        span = span._span
    description = str(error)[:256]
    span.set_status(Status(StatusCode.ERROR, description))
    if isinstance(error, BaseException):
        span.record_exception(error)
        span.set_attribute("error.type", type(error).__name__)


def trace_id_hex(span: trace.Span | None = None) -> str:
    candidate = span or trace.get_current_span()
    context = candidate.get_span_context()
    return f"{context.trace_id:032x}" if context.is_valid else ""


def span_id_hex(span: trace.Span | None = None) -> str:
    candidate = span or trace.get_current_span()
    context = candidate.get_span_context()
    return f"{context.span_id:016x}" if context.is_valid else ""


def _span_attributes(
    attributes: Mapping[str, Any] | None,
) -> dict[str, str | bool | int | float | Sequence[str | bool | int | float]]:
    result: dict[
        str, str | bool | int | float | Sequence[str | bool | int | float]
    ] = {}
    for key, value in (attributes or {}).items():
        if value is None:
            continue
        if isinstance(value, (str, bool, int, float)):
            result[str(key)] = value
            continue
        if isinstance(value, (tuple, list)) and all(
            isinstance(item, (str, bool, int, float)) for item in value
        ):
            result[str(key)] = value
            continue
        result[str(key)] = str(value)
    return result


def _flatten_attributes(
    attributes: Mapping[str, Any],
    *,
    prefix: str = "",
) -> dict[str, str | bool | int | float]:
    result: dict[str, str | bool | int | float] = {}
    for key, value in attributes.items():
        name = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, Mapping):
            result.update(_flatten_attributes(value, prefix=name))
        elif isinstance(value, (str, bool, int, float)):
            result[name] = value
        elif value is not None:
            result[name] = str(value)
    return result


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
