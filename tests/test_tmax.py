import tempfile
import unittest
from pathlib import Path

from ucloud_sandboxes.tmax import (
    harden_post_script,
    image_id_for_task,
    materialize_tmax_context,
    parse_container_definition,
    parse_file_mappings,
)


class TMaxTests(unittest.TestCase):
    def test_parse_container_definition_preserves_post_body(self) -> None:
        raw = """Bootstrap: docker
From: ubuntu:22.04

%post
    cat << 'EOF' > /home/user/example.py
print("hello")
EOF
    python3 /home/user/example.py
"""

        parsed = parse_container_definition(raw)

        self.assertEqual(parsed.bootstrap, "docker")
        self.assertEqual(parsed.base_image, "ubuntu:22.04")
        self.assertIn("cat << 'EOF'", parsed.post)
        self.assertIn('print("hello")', parsed.post)
        self.assertIn("\nEOF\n", f"\n{parsed.post}\n")

    def test_parse_file_mappings(self) -> None:
        mappings = parse_file_mappings(
            """
            /source/a.txt /app/a.txt
            "/source/with space.txt" /app/b.txt
            """
        )

        self.assertEqual(mappings[0].source, "/source/a.txt")
        self.assertEqual(mappings[0].destination, "/app/a.txt")
        self.assertEqual(mappings[1].source, "/source/with space.txt")

    def test_materialize_skips_external_files_by_default(self) -> None:
        row = {
            "task_id": "task_000000_c19dda5b",
            "container_def": """Bootstrap: docker
From: ubuntu:22.04

%files
    /gpfs/source.png /app/source.png

%post
true
""",
        }
        with tempfile.TemporaryDirectory() as raw_dir:
            context = materialize_tmax_context(
                row,
                row_idx=0,
                output_root=Path(raw_dir),
                registry_prefix="ucloud-sandbox-registry:5000/tmax",
                tag_suffix="test",
            )

        self.assertFalse(context.buildable)
        self.assertEqual(context.skipped_reason, "requires external %files fixtures")

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
        with tempfile.TemporaryDirectory() as raw_dir:
            context = materialize_tmax_context(
                row,
                row_idx=1,
                output_root=Path(raw_dir),
                registry_prefix="ucloud-sandbox-registry:5000/tmax",
                tag_suffix="test",
            )

            self.assertTrue(context.buildable)
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
        self.assertEqual(image_id_for_task("task_000000_f8baca82"), "tmax-task_000000_f8baca82")
        self.assertLessEqual(len(image_id_for_task("x" * 100)), 64)

    def test_harden_post_script_keeps_non_pytest_pip_installs(self) -> None:
        self.assertIn(
            "pip3 install pytest pandas",
            harden_post_script("pip3 install pytest pandas"),
        )


if __name__ == "__main__":
    unittest.main()
