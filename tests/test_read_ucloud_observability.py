import subprocess
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "scripts" / "read_ucloud_observability.sh"


class ReadUCloudObservabilityTests(unittest.TestCase):
    def test_wrapper_validates_inputs_and_resolves_ssh(self) -> None:
        script = SCRIPT.read_text(encoding="utf-8")
        for required in (
            'ucloud jobs ssh "$gateway_job_id" --print-only',
            "ucloud-observability-report",
            "StrictHostKeyChecking=accept-new",
            "--rate-window",
            "--trace-limit",
        ):
            self.assertIn(required, script)
        subprocess.run(["bash", "-n", str(SCRIPT)], check=True)


if __name__ == "__main__":
    unittest.main()
