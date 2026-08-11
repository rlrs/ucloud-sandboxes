import tempfile
import unittest
from pathlib import Path

from scripts.tmax_build_smoke import _materialize_tmax_context


class TMaxTests(unittest.TestCase):
    def _materialize(self, row: dict, *, row_idx: int = 0):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        return _materialize_tmax_context(
            row, row_idx=row_idx, output_root=Path(temporary.name)
        )

    def test_parse_container_definition_preserves_post_body(self) -> None:
        raw = """Bootstrap: docker
From: ubuntu:22.04

%post
    cat << 'EOF' > /home/user/example.py
print("hello")
EOF
    python3 /home/user/example.py
"""

        context = self._materialize({"task_id": "heredoc", "container_def": raw})
        post = (context.context_path / "post.sh").read_text(encoding="utf-8")
        dockerfile = (context.context_path / "Dockerfile").read_text(encoding="utf-8")
        self.assertIn("FROM ubuntu:22.04", dockerfile)
        self.assertIn("cat << 'EOF'", post)
        self.assertIn('print("hello")', post)
        self.assertIn("\nEOF\n", f"\n{post}\n")

    def test_parse_file_mappings(self) -> None:
        definition = """Bootstrap: docker
From: ubuntu:22.04

%files
            /source/a.txt /app/a.txt
            "/source/with space.txt" /app/b.txt
"""
        context = self._materialize({"task_id": "files", "container_def": definition})
        self.assertEqual(context.skipped_reason, "requires external %files fixtures")

    def test_materialize_rejects_unsupported_definitions(self) -> None:
        cases = (
            ({}, "missing container_def"),
            (
                {"container_def": "Bootstrap: singularity\nFrom: ubuntu:22.04"},
                "unsupported bootstrap: singularity",
            ),
            ({"container_def": "Bootstrap: docker"}, "missing base image"),
        )
        for row, expected in cases:
            with self.subTest(expected):
                self.assertEqual(self._materialize(row).skipped_reason, expected)

    def test_materialize_context_writes_dockerfile_and_tests(self) -> None:
        row = {
            "task_id": "task_000000_f8baca82",
            "test_initial_state": "def test_ok():\n    assert True\n",
            "container_def": """Bootstrap: docker
From: ubuntu:22.04

%post
    pip3 install pytest
    mkdir -p /home/user
    useradd -m -s /bin/bash user || true
""",
        }
        context = self._materialize(row, row_idx=1)
        self.assertFalse(context.skipped_reason)
        self.assertTrue((context.context_path / "Dockerfile").is_file())
        self.assertTrue((context.context_path / "post.sh").is_file())
        self.assertTrue((context.context_path / "test_initial_state.py").is_file())
        dockerfile = (context.context_path / "Dockerfile").read_text(encoding="utf-8")
        post = (context.context_path / "post.sh").read_text(encoding="utf-8")
        self.assertIn("FROM ubuntu:22.04", dockerfile)
        self.assertIn("PIP_DEFAULT_TIMEOUT=120", dockerfile)
        self.assertIn("COPY post.sh", dockerfile)
        self.assertIn("apt-get install -y python3-pytest", post)

    def test_image_id_for_task_is_short_and_stable(self) -> None:
        first = self._materialize({"task_id": "task_000000_f8baca82"})
        long = self._materialize({"task_id": "x" * 100}, row_idx=1)
        self.assertEqual(first.image_id, "tmax-task_000000_f8baca82")
        self.assertLessEqual(len(long.image_id), 64)

    def test_harden_post_script_keeps_non_pytest_pip_installs(self) -> None:
        definition = """Bootstrap: docker
From: ubuntu:22.04

%post
pip3 install pytest pandas
"""
        context = self._materialize({"task_id": "pip", "container_def": definition})
        post = (context.context_path / "post.sh").read_text(encoding="utf-8")
        self.assertIn("pip3 install pytest pandas", post)


if __name__ == "__main__":
    unittest.main()
