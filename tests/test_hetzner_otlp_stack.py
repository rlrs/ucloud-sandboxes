import subprocess
import unittest
from pathlib import Path

SCRIPT = Path(__file__).parents[1] / "scripts" / "install_hetzner_otlp_stack.sh"


class HetznerOtlpStackTests(unittest.TestCase):
    def test_stack_is_pinned_bounded_and_private(self) -> None:
        script = SCRIPT.read_text(encoding="utf-8")
        for required in (
            "grafana/tempo:2.10.7",
            "victoriametrics/victoria-metrics:v1.148.0",
            "otel/opentelemetry-collector-contrib:0.153.0",
            "-p $private_bind_ip:4318:4318",
            "-p 127.0.0.1:3200:3200",
            "-p 127.0.0.1:8428:8428",
            "-storage.minFreeDiskSpaceBytes=5GB",
        ):
            self.assertIn(required, script)
        self.assertNotIn("-p 0.0.0.0:4318", script)
        subprocess.run(["bash", "-n", str(SCRIPT)], check=True)


if __name__ == "__main__":
    unittest.main()
