from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from scripts.repack_node_bundle import validate_agent_runtime_dependencies


class RepackNodeBundleTests(unittest.TestCase):
    def test_rejects_missing_unconditional_runtime_dependency(self) -> None:
        with TemporaryDirectory() as raw_dir:
            runtime = Path(raw_dir)
            self._metadata(
                runtime,
                "ucloud_sandboxes-0.4.1.dist-info",
                "Name: ucloud-sandboxes\n"
                "Version: 0.4.1\n"
                "Requires-Dist: opentelemetry-sdk>=1.30\n",
            )

            with self.assertRaisesRegex(ValueError, "opentelemetry-sdk"):
                validate_agent_runtime_dependencies(runtime)

    def test_accepts_present_dependency_and_ignores_environment_markers(self) -> None:
        with TemporaryDirectory() as raw_dir:
            runtime = Path(raw_dir)
            self._metadata(
                runtime,
                "ucloud_sandboxes-0.4.1.dist-info",
                "Name: ucloud-sandboxes\n"
                "Version: 0.4.1\n"
                "Requires-Dist: opentelemetry-sdk>=1.30\n"
                'Requires-Dist: importlib-metadata; python_version < "3.10"\n',
            )
            self._metadata(
                runtime,
                "opentelemetry_sdk-1.44.0.dist-info",
                "Name: opentelemetry-sdk\nVersion: 1.44.0\n",
            )

            validate_agent_runtime_dependencies(runtime)

    @staticmethod
    def _metadata(runtime: Path, directory: str, contents: str) -> None:
        path = runtime / "site-packages" / directory / "METADATA"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents, encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
