import subprocess
import unittest
from pathlib import Path


SCRIPT = (
    Path(__file__).parents[1] / "scripts" / "install_ucloud_observability_stack.sh"
)


class UCloudObservabilityStackTests(unittest.TestCase):
    def test_stack_is_pinned_bounded_private_and_locally_persistent(self) -> None:
        script = SCRIPT.read_text(encoding="utf-8")
        for required in (
            "grafana/tempo:2.10.7",
            "victoriametrics/victoria-metrics:v1.148.0",
            "otel/opentelemetry-collector-contrib:0.153.0",
            "grafana/grafana:12.3.1",
            "-p $private_bind_ip:4318:4318",
            "-p 127.0.0.1:3200:3200",
            "-p 127.0.0.1:8428:8428",
            "-p 127.0.0.1:3000:3000",
            "-storage.minFreeDiskSpaceBytes=20GB",
            "block_retention: 72h",
            "-retentionPeriod=14d",
            "--cpu-shares 256",
            "GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer",
            "/usr/local/bin/ucloud-observability-report",
        ):
            self.assertIn(required, script)
        self.assertNotIn("-p 0.0.0.0:", script)
        self.assertIn("/work/*|/mnt/ucloud/*", script)
        subprocess.run(["bash", "-n", str(SCRIPT)], check=True)


if __name__ == "__main__":
    unittest.main()
