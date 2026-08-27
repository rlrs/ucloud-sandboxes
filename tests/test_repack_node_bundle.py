from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from scripts.repack_node_bundle import (
    sha256_file,
    validate_agent_runtime_dependencies,
    validate_source_bundle,
)


class RepackNodeBundleTests(unittest.TestCase):
    def test_accepts_role_specific_builder_bundle(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            deb = root / "runtime/debs/package.deb"
            agent = root / "runtime/agent/node-agent-runtime.tar"
            module = root / "runtime/kernel/6.8.0/test.ko"
            for path, contents in (
                (deb, b"deb"),
                (agent, b"agent"),
                (module, b"module"),
            ):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(contents)
            manifest = {
                "version": 1,
                "runtime": {
                    "role": "builder",
                    "files": [{"name": deb.name, "sha256": sha256_file(deb)}],
                    "agent": {
                        "file": "runtime/agent/node-agent-runtime.tar",
                        "sha256": sha256_file(agent),
                    },
                    "kernel": {
                        "release": "6.8.0",
                        "files": [
                            {"name": module.name, "sha256": sha256_file(module)}
                        ],
                    },
                },
            }

            validate_source_bundle(root, manifest)

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
