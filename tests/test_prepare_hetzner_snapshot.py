from __future__ import annotations

import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "prepare_hetzner_snapshot.sh"


class PrepareHetznerSnapshotTests(unittest.TestCase):
    def test_script_is_role_aware_and_has_valid_bash_syntax(self) -> None:
        source = SCRIPT.read_text()

        self.assertIn('<sandbox|builder>', source)
        self.assertIn('runtime-ready-v*-"$expected_role"', source)
        self.assertIn('^(sandbox|builder)$', source)
        subprocess.run(["bash", "-n", str(SCRIPT)], check=True)


if __name__ == "__main__":
    unittest.main()
