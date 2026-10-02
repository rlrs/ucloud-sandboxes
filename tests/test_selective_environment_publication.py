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

    def mkfs(self, image, view, *, exclude_runtime_mounts, preserve_mtimes):
        self.assertTrue(exclude_runtime_mounts)
        self.assertEqual(preserve_mtimes, self.builder.layer_format()["layout"] == 2)
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

    def test_multiple_private_tail_diffs_move_into_group_without_recopied_payload(self):
        first = self.tail()
        second = layer([directory("app"), member("app/extra", b"additional file", mode=0o640)])
        image_id = self.add_image("two-private-tails", [self.base, first, second])
        with patch.object(environment_builder, "_copy_entry", side_effect=AssertionError("private payload copied")), \
             patch.object(self.store, "_checked", side_effect=AssertionError("must not pull")), \
             publication_metrics() as metrics:
            _, environment = self.publish("two-private-tails")
        self.assertEqual(environment.source_image, image_id)
        self.assertEqual(self.registry.load(environment.components[-1]).source_layers, (first[1], second[1]))
        self.assertEqual(self.mkfs_views, [{"app": {"mode": 0o755, "content": None},
            "app/extra": {"mode": 0o640, "content": b"additional file".hex()},
            "app/run": {"mode": 0o751, "content": b"#!/bin/sh\necho selective\n".hex()}}])
        self.assertEqual(metrics["selective_materializations"], 1)
        self.assertEqual(metrics["groups_built"], 1)

    def test_isolated_prepared_view_preserves_original_group_binding_and_phase_metrics(self):
        from ucloud_sandboxes.environment_prepare import PreparationResult
        from ucloud_sandboxes.oci_layer_materialize import materialize_layers
        first = self.tail()
        second = layer([directory("app"), member("app/extra", b"additional file")])
        self.add_image("isolated", [self.base, first, second])
        self.builder.preparation_subprocess = True
        scratch = []
        def prepare(client, repository, layers, diff_ids, group_counts, root, *, timeout_seconds):
            self.assertEqual(timeout_seconds, 600)
            scratch.append(root)
            self.assertEqual(group_counts, [2])
            self.assertEqual(diff_ids, [first[1], second[1]])
            directories = materialize_layers(client, repository, layers, diff_ids, root / "diffs")
            view = root / "view-0"
            environment_builder.squash_layer_diffs(directories, view, consume_private_diffs=True)
            return PreparationResult((view,), {"selective_materialization_ms": 12.5,
                "squash_ms": 3.25, "selective_subprocess_ms": 40.0})
        with patch("ucloud_sandboxes.environment_prepare.prepare_in_subprocess", side_effect=prepare) as child, \
             patch.object(self.store, "_checked", side_effect=AssertionError("must not pull")), \
             publication_metrics() as metrics:
            _, environment = self.publish("isolated")
        child.assert_called_once()
        self.assertEqual(self.registry.load(environment.components[-1]).source_layers, (first[1], second[1]))
        self.assertEqual(self.mkfs_views[0]["app/run"]["content"], b"#!/bin/sh\necho selective\n".hex())
        self.assertEqual(self.mkfs_views[0]["app/extra"]["content"], b"additional file".hex())
        self.assertEqual(metrics["selective_materialization_ms"], 12.5)
        self.assertEqual(metrics["squash_ms"], 3.25)
        self.assertEqual(metrics["selective_subprocess_ms"], 40.0)
        self.assertTrue(all(not path.exists() for path in scratch))

    def test_complete_cache_hit_does_not_start_preparation_child(self):
        self.builder.preparation_subprocess = True
        with patch("ucloud_sandboxes.environment_prepare.prepare_in_subprocess",
                   side_effect=AssertionError("cache hit must not spawn")), publication_metrics() as metrics:
            _, environment = self.publish("base")
        self.assertEqual(environment.components, (self.base_component,))
        self.assertEqual(self.mkfs_views, [])
        self.assertNotIn("selective_subprocess_ms", metrics)

    def test_isolated_fallback_uses_docker_but_unexpected_failure_does_not_publish(self):
        from ucloud_sandboxes.environment_prepare import PreparationError, PreparationResult
        self.builder.preparation_subprocess = True
        self.add_image("isolated-fallback", [self.base, self.tail()])
        with patch("ucloud_sandboxes.environment_prepare.prepare_in_subprocess",
                   return_value=PreparationResult((), {"selective_materialization_ms": 2.0,
                       "squash_ms": 0.0, "selective_subprocess_ms": 30.0}, fallback=True)), \
             patch.object(self.store, "_checked", wraps=self.store._checked) as pull:
            self.publish("isolated-fallback")
        pull.assert_called_once()
        self.assertIn("docker-materialized", self.mkfs_views[0])
        self.add_image("isolated-failed", [self.base, self.tail(b"distinct missing tail")])
        scratch = []
        def fail(*args, timeout_seconds):
            self.assertEqual(timeout_seconds, 600)
            scratch.append(args[-1])
            (args[-1] / "partial").write_bytes(b"discarded")
            raise PreparationError("child failed")
        self.client.puts.clear()
        with patch("ucloud_sandboxes.environment_prepare.prepare_in_subprocess", side_effect=fail), \
             patch.object(self.store, "_checked", side_effect=AssertionError("must not pull")), \
             patch.object(environment_builder, "sign_layer_component", side_effect=AssertionError("must not sign")), \
             self.assertRaises(PreparationError):
            self.publish("isolated-failed")
        self.assertEqual(self.client.puts, [])
        self.assertTrue(all(not path.exists() for path in scratch))

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
