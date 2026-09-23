import json
import os
from pathlib import Path
from ucloud_sandboxes.gvisor_distribution import GVISOR_COMMIT, GVISOR_SIDECARS
from tempfile import TemporaryDirectory
import unittest
import zipfile
from types import SimpleNamespace
from unittest.mock import patch

from scripts.repack_node_bundle import (
    add_runtime_debs,
    replace_direct_runtime,
    replace_agent_package,
    sha256_file,
    validate_agent_runtime_dependencies,
    validate_source_bundle,
)


class RepackNodeBundleTests(unittest.TestCase):
    def test_explicit_debian_extension_keeps_prior_closure_and_rejects_upgrade(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            destination = root / "bundle/runtime/debs"
            destination.mkdir(parents=True)
            old = destination / "libc_1_amd64.deb"
            old.write_bytes(b"qualified")
            extra = root / "erofs-utils_1_amd64.deb"
            extra.write_bytes(b"extra")
            newer = root / "libc_2_amd64.deb"
            newer.write_bytes(b"unqualified-upgrade")
            wrong = root / "wrong_1_arm64.deb"
            wrong.write_bytes(b"wrong-architecture")
            manifest = {"runtime": {"platform": {"architecture": "amd64"}, "packages": ["libc"]}}
            def inspect(argv, **kwargs):
                return SimpleNamespace(stdout="\n".join(Path(argv[-1]).stem.split("_")))
            with patch("scripts.repack_node_bundle.subprocess.run", side_effect=inspect):
                add_runtime_debs(root / "bundle", manifest, [extra, extra])
                self.assertEqual({item["name"] for item in manifest["runtime"]["files"]}, {old.name, extra.name})
                self.assertEqual(manifest["runtime"]["packages"], ["libc"])
                with self.assertRaisesRegex(ValueError, "replace qualified"):
                    add_runtime_debs(root / "bundle", manifest, [newer])
                with self.assertRaisesRegex(ValueError, "wrong-architecture"):
                    add_runtime_debs(root / "bundle", manifest, [wrong])
            self.assertEqual(old.read_bytes(), b"qualified")

    def test_repacked_agent_is_traversable_under_restrictive_umask(self):
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            runtime = root / "runtime"
            (runtime / "site-packages").mkdir(parents=True)
            wheel = root / "agent.whl"
            with zipfile.ZipFile(wheel, "w") as archive:
                archive.writestr("ucloud_sandboxes/relay/__init__.py", "")
                archive.writestr("ucloud_sandboxes/cli.py", "")
                archive.writestr(
                    "ucloud_sandboxes-0.5.35.dist-info/WHEEL",
                    "Root-Is-Purelib: true\nTag: py3-none-any\n",
                )
                archive.writestr(
                    "ucloud_sandboxes-0.5.35.dist-info/METADATA",
                    "Name: ucloud-sandboxes\nVersion: 0.5.35\n",
                )
            previous = os.umask(0o077)
            try:
                replace_agent_package(runtime, wheel)
            finally:
                os.umask(previous)
            for directory in (runtime / "site-packages").rglob("*"):
                if directory.is_dir():
                    self.assertEqual(directory.stat().st_mode & 0o777, 0o755)

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
                        "files": [{"name": module.name, "sha256": sha256_file(module)}],
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

    def test_runtime_replacement_validates_before_modifying_and_copies_companions(self):
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            source = root / "source"
            source.mkdir()
            files = {}
            for name in ["runsc", *("gvisor-bin/" + n for n in GVISOR_SIDECARS)]:
                path = source / name
                path.parent.mkdir(exist_ok=True)
                path.write_bytes(name.encode())
                path.chmod(0o755)
                files[name] = {"size": path.stat().st_size, "sha256": sha256_file(path)}
            (source / "build-manifest.json").write_text(
                json.dumps(
                    {
                        "schema": 2,
                        "gvisor_commit": GVISOR_COMMIT,
                        "files": files,
                    }
                )
            )
            helper = source / "init"
            helper.write_bytes(b"init")
            helper.chmod(0o755)
            bundle = root / "bundle"
            old = bundle / "runtime/direct/runsc"
            old.parent.mkdir(parents=True)
            old.write_bytes(b"old")
            manifest = {"runtime": {"role": "sandbox"}}
            sentry = source / "gvisor-bin/gvisor_sentry"
            original = sentry.read_bytes()
            sentry.write_bytes(b"tampered")
            with self.assertRaisesRegex(ValueError, "mismatch"):
                replace_direct_runtime(
                    bundle, manifest, source / "runsc", GVISOR_COMMIT, helper
                )
            self.assertEqual(old.read_bytes(), b"old")
            sentry.write_bytes(original)
            replace_direct_runtime(
                bundle, manifest, source / "runsc", GVISOR_COMMIT, helper
            )
            direct = manifest["runtime"]["direct_runsc"]
            self.assertEqual(direct["commit"], GVISOR_COMMIT)
            self.assertEqual(len(direct["sidecars"]), 4)
            for item in [
                direct,
                *direct["sidecars"],
                direct["build_manifest"],
                manifest["runtime"]["managed_init"],
            ]:
                self.assertEqual(sha256_file(bundle / item["file"]), item["sha256"])
                self.assertEqual((bundle / item["file"]).stat().st_size, item["size"])
            # Run the complete fresh-worker validator on the repacker output.
            from ucloud_sandboxes.vm_init import render_vm_init_script
            from tests.test_vm_init import VmInitTests, write_bundle
            complete = root / "complete"
            full_manifest = write_bundle(complete, "sandbox")
            replace_direct_runtime(complete, full_manifest, source / "runsc", GVISOR_COMMIT, helper)
            manifest_path = complete / "package-bundle.json"
            manifest_path.write_text(json.dumps(full_manifest))
            script = render_vm_init_script(VmInitTests._options(
                direct_runsc_commit=GVISOR_COMMIT, direct_split_memory_backing=True,
            ))
            start = script.index("import hashlib\nimport json\nimport os")
            validator = script[start:script.index('\nPY\n)"', start)]
            result = VmInitTests._run_bundle_validator(validator, complete)
            self.assertEqual(result.returncode, 0, result.stderr)
            validate_source_bundle(complete, full_manifest)
            full_manifest["runtime"]["direct_runsc"].pop("build_manifest")
            manifest_path.write_text(json.dumps(full_manifest))
            result = VmInitTests._run_bundle_validator(validator, complete)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("provenance metadata is absent", result.stderr)
            with self.assertRaisesRegex(ValueError, "provenance metadata is absent"):
                validate_source_bundle(complete, full_manifest)

    @staticmethod
    def _metadata(runtime: Path, directory: str, contents: str) -> None:
        path = runtime / "site-packages" / directory / "METADATA"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents, encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
