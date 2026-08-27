import unittest

from scripts.ucloud_observability_report import (
    _attribute_value,
    _attributes,
    _findings,
    _instant_series,
    _number,
    _span_id,
)


class UCloudObservabilityReportTests(unittest.TestCase):
    def test_normalizes_prometheus_instant_vectors(self) -> None:
        rows = _instant_series(
            [
                {
                    "metric": {"service_name": "gateway", "operation": "wake"},
                    "value": [123, "0.125"],
                },
                {"metric": {}, "value": [123, "NaN"]},
            ]
        )
        self.assertEqual(rows[0]["value"], 0.125)
        self.assertIsNone(rows[1]["value"])
        self.assertIsNone(_number("+Inf"))

    def test_decodes_otlp_attributes_and_span_ids(self) -> None:
        raw = [
            {"key": "service.name", "value": {"stringValue": "worker"}},
            {"key": "duration", "value": {"doubleValue": 1.5}},
        ]
        self.assertEqual(_attributes(raw), {"service.name": "worker", "duration": 1.5})
        self.assertEqual(_attribute_value({"boolValue": False}), False)
        self.assertEqual(_span_id("AQIDBA=="), "01020304")

    def test_findings_prioritize_health_pressure_and_errors(self) -> None:
        report = {
            "host": {
                "systemd_services": {"collector": "failed"},
                "memory": {"available_ratio": 0.1},
                "telemetry_disk": {"available_bytes": 10},
            },
            "backends": {"tempo": {"ok": False, "error": "down"}},
            "metrics": {
                "reporting_services": ["gateway"],
                "operation_counts": [
                    {
                        "labels": {
                            "service_name": "gateway",
                            "operation": "wake",
                            "status": "ok",
                        },
                        "value": 1,
                    }
                ],
                "operation_errors": [
                    {
                        "labels": {
                            "service_name": "gateway",
                            "operation": "wake",
                        },
                        "value": 2,
                    }
                ],
            },
        }
        kinds = {item["kind"] for item in _findings(report)}
        self.assertEqual(
            kinds,
            {
                "service_unhealthy",
                "backend_unhealthy",
                "memory_pressure",
                "telemetry_disk_pressure",
                "operation_errors_with_success",
            },
        )


if __name__ == "__main__":
    unittest.main()
