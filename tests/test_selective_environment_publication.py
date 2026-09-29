"""Public publication flow with a signed cached base and real OCI tail bytes.

The grouping threshold is reduced for small fixtures. Registry manifests,
compressed/uncompressed identities, component signatures and root bindings are
real; Docker mount/pull and EROFS construction are test adapters. Real filesystem
and EROFS equivalence is a separate qualification.
"""
from contextlib import ExitStack
import io
from pathlib import Path
import stat
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from tests.test_environment_layers import FORMAT, FakeDockerStore, Keys, LayerRegistry
from tests.test_oci_layer_materialize import directory, layer, member
from ucloud_sandboxes import environment_builder
from ucloud_sandboxes.environment_artifact import (
    EnvironmentArtifactRegistry, canonical_bytes, content_digest, layer_chain_id,
    load_image_environment,
)
from ucloud_sandboxes.environment_builder import FreshEnvironmentBuilder, publication_metrics


class StreamingRegistry(LayerRegistry):
    def __init__(self):
        super().__init__()
        self.opened = []
        self.streams = []

    def open_blob(self, repository, digest):
        self.opened.append((repository, digest))
        result = io.BytesIO(self.blobs[digest])
        self.streams.append(result)
        return result


class SelectiveEnvironmentPublicationTests(unittest.TestCase):
    def setUp(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.keys = Keys()
        self.client = StreamingRegistry()
        self.registry = EnvironmentArtifactRegistry(self.client, "environments", self.keys.trusted)
        self.store = FakeDockerStore(self.root / "docker")
        self.builder = FreshEnvironmentBuilder(self.store, self.registry, self.keys.key, self.root / "work")
        self.builder._layer_format = FORMAT
        self.mkfs_views = []
        self.stack.enter_context(patch.object(self.builder, "_mkfs", side_effect=self.mkfs))
        # Production uses a 64 MiB threshold. Keep the same planner with a small
        # threshold so a real uncompressed base closes its own fixture group.
        planner = environment_builder.plan_layer_groups
        self.stack.enter_context(patch.object(environment_builder, "plan_layer_groups",
            side_effect=lambda sizes, *, max_groups: planner(sizes, threshold=8192, max_groups=max_groups)))
        # Extraction is privileged in production. Test paths retain the caller's
        # ownership; signature/content/binding checks are not mocked.
        self.stack.enter_context(patch("ucloud_sandboxes.oci_layer_materialize.os.chown"))
        self.base = layer([directory("base"), member("base/data", b"base" * 2048)], compressed=False)
        self.add_image("base", [self.base])
        _, base_environment = self.publish("base")
        self.base_component = base_environment.components[0]
        self.mkfs_views.clear()
        self.client.opened.clear()
        self.client.puts.clear()

    def add_image(self, tag, layers):
        config = {"Cmd": ["/app/run"], "Env": ["FIXTURE=selective"], "WorkingDir": "/app", "User": "123:456"}
        raw = canonical_bytes({"rootfs": {"type": "layers", "diff_ids": [value[1] for value in layers]},
                               "config": config})
        image_id = content_digest(raw)
        self.client.blobs[image_id] = raw
        directories = []
        for index, (descriptor, diff_id, blob) in enumerate(layers):
            self.client.blobs[descriptor["digest"]] = blob
            # This is the already-materialized Docker adapter used only when a
            # test explicitly expects fallback. OCI semantics are tested below.
            diff = self.root / "docker-diffs" / tag / str(index)
            diff.mkdir(parents=True)
            (diff / "docker-materialized").write_bytes(blob)
            directories.append((diff_id, diff))
        self.store.add(tag, image_id, directories)
        self.store.configs[tag] = config
        self.client.layer_sizes[tag] = [value[0]["size"] for value in layers]
        self.client.manifests[tag] = canonical_bytes({
            "schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {"digest": image_id, "size": len(raw)},
            "layers": [value[0] for value in layers],
        })
        return image_id

    def mkfs(self, image, view, *, exclude_runtime_mounts):
        self.assertTrue(exclude_runtime_mounts)
        snapshot = {}
        for path in sorted(view.rglob("*")):
            info = path.lstat()
            snapshot[str(path.relative_to(view))] = {
                "mode": stat.S_IMODE(info.st_mode),
                "content": path.read_bytes().hex() if path.is_file() else None,
            }
        self.mkfs_views.append(snapshot)
        # The registry signs these bytes exactly; real mkfs byte equivalence is
        # intentionally not claimed by this integration fixture.
        image.write_bytes(bytes.fromhex(content_digest(canonical_bytes(snapshot)).removeprefix("sha256:")) * 128)

    def publish(self, tag):
        repository = "ucloud-managed/" + tag
        annotated = self.builder.publish_image("registry.example/" + repository + ":" + tag, allowlist=("*",))
        _, environment = load_image_environment(self.registry, repository, annotated)
        return annotated, environment

    def tail(self, content=b"#!/bin/sh\necho selective\n"):
        return layer([directory("app"), member("app/run", content, mode=0o751)])

    def test_cached_signed_base_and_real_tail_skip_docker_and_preserve_root_binding(self):
        tail = self.tail()
        image_id = self.add_image("derived", [self.base, tail])
        with publication_metrics() as metrics, \
             patch.object(self.store, "_checked", side_effect=AssertionError("must not pull")), \
             patch.object(self.store, "operation_lease", side_effect=AssertionError("must not mount")):
            _, environment = self.publish("derived")
        self.assertEqual(environment.source_image, image_id)
        self.assertEqual(tuple(diff_id for digest in environment.components
                               for diff_id in self.registry.load(digest).source_layers),
                         (self.base[1], tail[1]))
        self.assertEqual(environment.image_config["Env"], ["FIXTURE=selective"])
        self.assertEqual(environment.image_config["User"], "123:456")
        self.assertEqual(environment.components[0], self.base_component)
        self.assertEqual(len(environment.components), 2)
        component = self.registry.load(environment.components[1])
        self.assertEqual(component.source_layers, (tail[1],))
        self.assertEqual(component.parent, layer_chain_id([self.base[1]]))
        self.assertEqual(self.client.opened, [("ucloud-managed/derived", tail[0]["digest"])])
        self.assertTrue(all(stream.closed for stream in self.client.streams))
        self.assertEqual(self.mkfs_views, [{"app": {"mode": 0o755, "content": None},
                                          "app/run": {"mode": 0o751, "content": b"#!/bin/sh\necho selective\n".hex()}}])
        self.assertEqual(metrics["groups_reused"], 1)
        self.assertEqual(metrics["groups_built"], 1)
        self.assertEqual(metrics["selective_materializations"], 1)
        self.assertEqual(metrics["oci_layers_materialized"], 1)
        self.assertEqual(metrics["oci_download_bytes"], tail[0]["size"])
        self.assertEqual(metrics["docker_pull_skipped"], 1)
        self.assertNotIn("docker_pull_ms", metrics)
        self.assertGreaterEqual(metrics["selective_materialization_ms"], 0)
        self.assertFalse(any(path.name.endswith((".blob", ".tar", ".diff")) for path in self.builder.work_root.iterdir()))

    def test_whiteout_and_lower_dependent_parent_fall_back_before_selective_signing(self):
        unsupported = [layer([member(".wh.deleted")]), layer([member("app/run", b"missing parent")])]
        for index, tail in enumerate(unsupported):
            with self.subTest(index=index):
                tag = "fallback" + str(index)
                image_id = self.add_image(tag, [self.base, tail])
                self.mkfs_views.clear()
                with publication_metrics() as metrics, \
                     patch.object(self.store, "_checked", wraps=self.store._checked) as pull, \
                     patch.object(self.store, "operation_lease", wraps=self.store.operation_lease) as lease:
                    _, environment = self.publish(tag)
                pull.assert_called_once_with("docker", "pull", "registry.example/ucloud-managed/" + tag + ":" + tag, timeout=600)
                lease.assert_called_once()
                self.assertEqual(environment.source_image, image_id)
                self.assertEqual(environment.components[0], self.base_component)
                self.assertEqual(len(self.mkfs_views), 1)
                self.assertIn("docker-materialized", self.mkfs_views[0])
                self.assertEqual(metrics["selective_fallbacks"], 1)
                self.assertNotIn("selective_materializations", metrics)
                self.assertNotIn("docker_pull_skipped", metrics)
                self.assertEqual(metrics["groups_built"], 1)
                self.assertEqual(metrics["groups_reused"], 1)

    def test_corrupt_source_bindings_never_pull_sign_or_publish(self):
        for corruption in ("config", "compressed_blob", "diff_id"):
            with self.subTest(corruption=corruption):
                tail = self.tail(corruption.encode())
                if corruption == "diff_id":
                    tail = tail[0], content_digest(b"wrong uncompressed identity"), tail[2]
                tag = "corrupt-" + corruption
                image_id = self.add_image(tag, [self.base, tail])
                if corruption == "config":
                    self.client.blobs[image_id] = self.client.blobs[image_id].replace(b"selective", b"incorrect")
                elif corruption == "compressed_blob":
                    blob = bytearray(tail[2])
                    blob[-1] ^= 1
                    self.client.blobs[tail[0]["digest"]] = bytes(blob)
                original_manifests = dict(self.client.manifests)
                original_tags = dict(self.client.tags)
                self.client.puts.clear()
                with patch.object(self.store, "_checked", side_effect=AssertionError("must fail closed")), \
                     patch("ucloud_sandboxes.environment_builder.sign_layer_component", side_effect=AssertionError("must not sign")), \
                     patch("ucloud_sandboxes.environment_artifact.publish_environment", side_effect=AssertionError("must not sign root")), \
                     self.assertRaisesRegex(ValueError, "identity mismatch"):
                    self.publish(tag)
                self.assertEqual(self.client.manifests, original_manifests)
                self.assertEqual(self.client.tags, original_tags)
                self.assertEqual(self.client.puts, [])
                self.assertEqual(self.mkfs_views, [])
                self.assertTrue(all(stream.closed for stream in self.client.streams))

    def test_all_selected_layers_authenticate_before_any_new_component_is_signed(self):
        first = self.tail()
        second = self.tail(b"second tail with wrong uncompressed identity")
        second = second[0], content_digest(b"not the second tail"), second[2]
        self.add_image("two-tails", [self.base, first, second])
        original_tags = dict(self.client.tags)
        with patch.object(self.store, "_checked", side_effect=AssertionError("must fail closed")), \
             patch("ucloud_sandboxes.environment_builder.sign_layer_component", side_effect=AssertionError("must not sign")), \
             patch("ucloud_sandboxes.environment_artifact.publish_environment", side_effect=AssertionError("must not sign root")), \
             self.assertRaisesRegex(ValueError, "uncompressed layer identity mismatch"):
            self.publish("two-tails")
        self.assertEqual(self.client.tags, original_tags)
        self.assertEqual(self.mkfs_views, [])
        self.assertEqual([digest for _, digest in self.client.opened], [first[0]["digest"], second[0]["digest"]])
        self.assertTrue(all(stream.closed for stream in self.client.streams))


if __name__ == "__main__":
    unittest.main()
