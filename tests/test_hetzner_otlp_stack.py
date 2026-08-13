import subprocess
import unittest
from pathlib import Path


class HetznerOtlpStackTests(unittest.TestCase):
    def test_stack_is_pinned_bounded_and_private(self) -> None:
        script = (
            Path(__file__).parents[1] / "scripts" / "install_hetzner_otlp_stack.sh"
        ).read_text(encoding="utf-8")

        self.assertIn("grafana/tempo:2.10.7", script)
        self.assertIn("victoriametrics/victoria-metrics:v1.148.0", script)
        self.assertIn("otel/opentelemetry-collector-contrib:0.153.0", script)
        self.assertIn("-p $private_bind_ip:4318:4318", script)
        self.assertIn("-p 127.0.0.1:3200:3200", script)
        self.assertIn("-p 127.0.0.1:8428:8428", script)
        self.assertIn("--memory 512m", script)
        self.assertIn("--memory 256m", script)
        self.assertIn("--memory 192m", script)
        self.assertIn("-storage.minFreeDiskSpaceBytes=5GB", script)
        self.assertIn("prometheusremotewrite/victoria", script)
        self.assertIn("otlp_grpc/tempo", script)
        self.assertIn("max_batch_request_parallelism: 1", script)
        self.assertIn("prefix: {quote(prefix)}", script)
        self.assertIn("max_block_duration: 5m", script)
        self.assertIn("complete_block_timeout: 10m", script)
        self.assertIn("flush_all_on_shutdown: true", script)
        self.assertIn("systemctl restart ucloud-telemetry-collector.service", script)
        self.assertNotIn("-p 0.0.0.0:4318", script)

        syntax = subprocess.run(
            [
                "bash",
                "-n",
                str(
                    Path(__file__).parents[1]
                    / "scripts"
                    / "install_hetzner_otlp_stack.sh"
                ),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(syntax.returncode, 0, syntax.stderr)


if __name__ == "__main__":
    unittest.main()
