"""Per-layer EROFS publication: schema, planning, squash, reuse and mounting."""
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import hashlib
import os
from pathlib import Path
import stat
import subprocess
import sys
from threading import Event
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from tests.test_environment_artifact import MemoryRegistry
from ucloud_sandboxes import environment_builder
from ucloud_sandboxes.direct_registry import DirectRegistryCapacityUnavailable
from ucloud_sandboxes.environment_artifact import (
    COMPONENT_SCHEMA, COMPONENT_SCHEMA_V2, EMPTY_LAYER_DIFF_ID, OCI_IMAGE, EnvironmentArtifactRegistry,
    EnvironmentComponent, LayerEnvironmentComponent, bind_source_layers, canonical_bytes, content_digest,
    layer_chain_id, layer_group_key, load_image_environment, publish_environment, sign_component,
    sign_layer_component,
)
from ucloud_sandboxes.environment_backend import NO_BLOCK_DEVICE, mount_has_dependents
from ucloud_sandboxes.environment_builder import (
    LAYER_GROUP_BYTES, FreshEnvironmentBuilder, plan_layer_groups, publication_metrics, squash_layer_diffs,
)
from ucloud_sandboxes.environment_manifest import EnvironmentManifest, HOST_EROFS_ABI
from ucloud_sandboxes.environment_rootfs import EnvironmentDeviceCapacityError, EnvironmentRootfsStore
from ucloud_sandboxes.image_rootfs import DockerImageConfig, DockerOverlay2RootfsStore
from ucloud_sandboxes.managed_registry import RegistryRequestError


MIB = 1024 ** 2
FORMAT = {"layout": 1, "mkfs": "mkfs.erofs (erofs-utils) 1.8.1", "compression": "lz4",
          "excludes": ["dev", "proc", "run", "sys"]}


def digest(character):
    return "sha256:" + character * 64


def diff_id(name):
    return content_digest(name.encode())


class Keys:
    def __init__(self):
        self.key = Ed25519PrivateKey.generate()
        public = self.key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
        self.trusted = {content_digest(public): public}


def image_file(path, fill):
    path.write_bytes(fill * 4096 * 3)
    return path


class LayerComponentSchemaTests(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.keys = Keys()
        self.client = MemoryRegistry()
        self.registry = EnvironmentArtifactRegistry(self.client, "environments", self.keys.trusted)

    def layer(self, layers, parent, fill=b"l"):
        image = image_file(self.root / (hashlib.sha256(repr((layers, fill)).encode()).hexdigest() + ".erofs"), fill)
        component = sign_layer_component(image, source_layers=layers, parent=parent,
                                         layer_format=FORMAT, signing_key=self.keys.key)
        return component, self.registry.publish(image, component, tag="layer-" + component.group_key)

    def test_v1_and_v2_round_trip_with_schema_bound_signatures(self):
        whole = sign_component(image_file(self.root / "whole.erofs", b"w"), source_image=digest("1"),
                               signing_key=self.keys.key)
        whole_digest = self.registry.publish(self.root / "whole.erofs", whole, tag="whole")
        layer, layer_digest = self.layer((diff_id("a"), diff_id("b")), None)
        self.assertEqual(whole.schema, COMPONENT_SCHEMA)
        self.assertEqual(layer.schema, COMPONENT_SCHEMA_V2)
        self.assertEqual(self.registry.load(whole_digest), whole)
        self.assertEqual(self.registry.load(layer_digest), layer)
        self.assertIsInstance(EnvironmentComponent.from_dict(layer.to_dict()), LayerEnvironmentComponent)
        self.assertEqual(EnvironmentComponent.from_dict(whole.to_dict()), whole)
        self.assertEqual(layer.group_key, layer_group_key(FORMAT, None, [diff_id("a"), diff_id("b")]))
        for changed in (replace(layer, source_layers=(diff_id("a"),)),
                        replace(layer, parent=diff_id("z")),
                        replace(layer, format=FORMAT | {"compression": ""})):
            with self.subTest(changed=changed.unsigned()), self.assertRaisesRegex(ValueError, "signature"):
                changed.authenticate(self.keys.trusted)
        # A v2 index relabelled as v1 (or the reverse) is not a valid document.
        with self.assertRaises(ValueError):
            EnvironmentComponent.from_dict(layer.to_dict() | {"schema": COMPONENT_SCHEMA})
        with self.assertRaises(ValueError):
            EnvironmentComponent.from_dict(whole.to_dict() | {"schema": COMPONENT_SCHEMA_V2})
        for invalid in ({"source_layers": [EMPTY_LAYER_DIFF_ID]}, {"source_layers": []},
                        {"format": FORMAT | {"layout": 2}}, {"format": FORMAT | {"excludes": ["sys", "dev"]}},
                        {"parent": "not-a-digest"}):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                LayerEnvironmentComponent.from_dict(layer.to_dict() | invalid)

    def test_group_key_is_content_addressed_by_format_parent_and_layers(self):
        layers = [diff_id("a"), diff_id("b")]
        key = layer_group_key(FORMAT, None, layers)
        self.assertEqual(key, layer_group_key(dict(reversed(FORMAT.items())), None, list(layers)))
        self.assertNotEqual(key, layer_group_key(FORMAT, diff_id("base"), layers))
        self.assertNotEqual(key, layer_group_key(FORMAT, None, list(reversed(layers))))
        self.assertNotEqual(key, layer_group_key(FORMAT | {"mkfs": "mkfs.erofs 1.9"}, None, layers))
        self.assertEqual(layer_chain_id([]), None)
        self.assertEqual(layer_chain_id([diff_id("a")]), diff_id("a"))
        self.assertEqual(layer_chain_id(layers), content_digest((diff_id("a") + " " + diff_id("b")).encode()))

    def test_root_binding_requires_exactly_the_image_layers(self):
        a, b, c = diff_id("a"), diff_id("b"), diff_id("c")
        lower, lower_digest = self.layer((a, b), None)
        upper, upper_digest = self.layer((c,), layer_chain_id([a, b]))
        foreign, foreign_digest = self.layer((c,), None, fill=b"f")
        toolkit = sign_component(image_file(self.root / "toolkit.erofs", b"t"), source_image=digest("9"),
                                 signing_key=self.keys.key)
        toolkit_digest = self.registry.publish(self.root / "toolkit.erofs", toolkit, tag="toolkit")

        def publish(components, diff_ids):
            return publish_environment(self.registry, source_image=digest("1"),
                environment=EnvironmentManifest(components[0], toolkits=tuple(components[1:])),
                image_config={"Cmd": ["/bin/sh"]}, signing_key=self.keys.key, tag="root",
                source_diff_ids=diff_ids)

        # Empty-tar layers change no file and are ignored; toolkits may follow.
        publish([lower_digest, upper_digest, toolkit_digest], [a, EMPTY_LAYER_DIFF_ID, b, c])
        for components, diff_ids, message in (
            ([lower_digest], [a, b, c], "differ"),
            ([lower_digest, upper_digest], [a, b], "differ"),
            ([lower_digest, upper_digest], [a, c, b], "differ"),
            ([lower_digest, foreign_digest], [a, b, c], "parent chain"),
            ([lower_digest, upper_digest], None, "diff_ids"),
        ):
            with self.subTest(diff_ids=diff_ids), self.assertRaisesRegex(ValueError, message):
                publish(components, diff_ids)
        with self.assertRaisesRegex(ValueError, "differ"):
            bind_source_layers([lower, upper], [])

    def test_rootfs_fingerprint_is_stable_over_ordered_components(self):
        manifest = EnvironmentManifest(digest("1"), toolkits=(digest("2"), digest("3")))
        again = EnvironmentManifest.from_dict(manifest.to_dict())
        self.assertEqual(manifest.rootfs_fingerprint(HOST_EROFS_ABI), again.rootfs_fingerprint(HOST_EROFS_ABI))
        # Pinned: checkpoints on every worker bind this exact value.
        self.assertEqual(manifest.rootfs_fingerprint(HOST_EROFS_ABI),
                         "99502df2ac8406c222ecc8ff1e4dc3e19a658fe7904482335200ddb800889631")
        self.assertEqual(EnvironmentManifest(digest("1")).rootfs_fingerprint(HOST_EROFS_ABI),
                         "52a222069c1bd02192e15d0d003c133ec5e2b7ea95ae5c1dc917c1ef49ee5fcf")
        self.assertNotEqual(
            manifest.rootfs_fingerprint(HOST_EROFS_ABI),
            EnvironmentManifest(digest("1"), toolkits=(digest("3"), digest("2"))).rootfs_fingerprint(HOST_EROFS_ABI))


class PlanLayerGroupsTests(unittest.TestCase):
    def test_small_layers_group_until_the_threshold_and_large_layers_stand_alone(self):
        self.assertEqual(plan_layer_groups([MIB, 2 * MIB, 80 * MIB, MIB]), [(0, 2), (2, 3), (3, 4)])
        self.assertEqual(plan_layer_groups([40 * MIB, 30 * MIB, MIB]), [(0, 2), (2, 3)])
        self.assertEqual(plan_layer_groups([MIB] * 3), [(0, 3)])
        self.assertEqual(plan_layer_groups([]), [])

    def test_shared_base_prefix_plans_identical_lower_groups(self):
        base = [5 * MIB, 90 * MIB, 20 * MIB, 50 * MIB, MIB, 3 * MIB]
        base_groups = plan_layer_groups(base)
        for task in ([MIB], [2 * MIB, 70 * MIB], [100 * MIB]):
            with self.subTest(task=task):
                groups = plan_layer_groups(base + task)
                closed = [group for group in base_groups if group[1] < len(base)]
                self.assertEqual(groups[:len(closed)], closed)
                self.assertEqual(groups[-1][1], len(base) + len(task))

    def test_cap_merges_only_the_top_groups(self):
        sizes = [LAYER_GROUP_BYTES] * 30
        groups = plan_layer_groups(sizes, max_groups=24)
        self.assertEqual(len(groups), 24)
        self.assertEqual(groups[:23], [(index, index + 1) for index in range(23)])
        self.assertEqual(groups[23], (23, 30))
        with self.assertRaises(ValueError):
            plan_layer_groups([MIB], max_groups=0)


class _Tree:
    """Write overlay2-encoded layers and inspect a squash result."""

    def __init__(self, whiteout, opaque, is_whiteout, is_opaque):
        self.whiteout, self.opaque = whiteout, opaque
        self.is_whiteout, self.is_opaque = is_whiteout, is_opaque

    @staticmethod
    def file(path, text):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    def scenario(self, root):
        lower, first, second = root / "lower", root / "first", root / "second"
        for directory in (lower, first, second):
            directory.mkdir()
        self.file(lower / "etc/keep", "base")
        self.file(lower / "etc/gone", "base")
        self.file(lower / "opt/old/x", "base")
        self.file(lower / "var/lib/a", "base")
        # First layer of the group.
        self.file(first / "etc/new1", "1")
        self.file(first / "tmp2/build/x", "scratch")
        (first / "opt").mkdir()
        self.whiteout(first / "opt/old")
        self.file(first / "data", "file first")
        self.file(first / "hl1", "h")
        os.link(first / "hl1", first / "hl2")
        self.file(first / "proc/ignored", "runtime mount")
        # Second layer of the group.
        (second / "etc").mkdir()
        self.whiteout(second / "etc/gone")
        (second / "etc/new1").symlink_to("keep")
        (second / "tmp2/build").mkdir(parents=True)
        self.whiteout(second / "tmp2/build/x")
        self.file(second / "opt/old/y", "2")
        self.file(second / "data/z", "dir second")
        self.file(second / "var/new", "2")
        self.opaque(second / "var")
        self.file(second / "hl1", "changed")
        return lower, first, second

    def check(self, test, result):
        def kind(path):
            info = path.lstat()
            if self.is_whiteout(info):
                return "whiteout"
            if stat.S_ISDIR(info.st_mode):
                return "opaque" if self.is_opaque(path) else "dir"
            if stat.S_ISLNK(info.st_mode):
                return "->" + os.readlink(path)
            return path.read_text()
        tree = {str(path.relative_to(result)): kind(path) for path in sorted(result.rglob("*"))}
        test.assertEqual(tree, {
            "etc": "dir", "etc/gone": "whiteout", "etc/new1": "->keep",
            # The deletion of a file this group created hides nothing below.
            "tmp2": "dir", "tmp2/build": "dir",
            "opt": "dir", "opt/old": "opaque", "opt/old/y": "2",
            "data": "opaque", "data/z": "dir second",
            "var": "opaque", "var/new": "2",
            "hl1": "changed", "hl2": "h",
        })
        test.assertEqual((result / "hl2").stat().st_nlink, 1)


@unittest.skipUnless(hasattr(os, "chflags") and hasattr(stat, "UF_NODUMP"),
                     "stand-in overlay encoding uses BSD file flags")
class SquashSemanticsTests(unittest.TestCase):
    """The squash rules over a stand-in encoding: FIFO whiteouts, flagged opaque dirs.

    Real 0:0 devices and trusted xattrs need Linux root (LinuxSquashTests).
    """

    def test_squash_matches_the_separate_layers(self):
        def is_opaque(path):
            return bool(path.lstat().st_flags & stat.UF_NODUMP)

        def set_opaque(path):
            os.chflags(path, path.lstat().st_flags | stat.UF_NODUMP, follow_symlinks=False)

        tree = _Tree(os.mkfifo, set_opaque, lambda info: stat.S_ISFIFO(info.st_mode), is_opaque)
        with TemporaryDirectory() as raw, \
             patch.object(environment_builder, "_is_whiteout", tree.is_whiteout), \
             patch.object(environment_builder, "_make_whiteout", os.mkfifo), \
             patch.object(environment_builder, "_is_opaque", is_opaque), \
             patch.object(environment_builder, "_set_opaque", set_opaque):
            root = Path(raw)
            lower, first, second = tree.scenario(root)
            squash_layer_diffs([first, second], root / "view", lower_dirs=[lower])
            tree.check(self, root / "view")
            with self.assertRaises(ValueError):
                squash_layer_diffs([first], root / "view")


@unittest.skipUnless(sys.platform == "linux" and os.geteuid() == 0, "overlay whiteouts need Linux root")
class LinuxSquashTests(unittest.TestCase):
    def test_squash_matches_the_separate_layers_with_overlay_encoding(self):
        tree = _Tree(
            lambda path: os.mknod(path, stat.S_IFCHR | 0o600, os.makedev(0, 0)),
            lambda path: os.setxattr(path, "trusted.overlay.opaque", b"y"),
            environment_builder._is_whiteout, environment_builder._is_opaque,
        )
        with TemporaryDirectory() as raw:
            root = Path(raw)
            lower, first, second = tree.scenario(root)
            squash_layer_diffs([first, second], root / "view", lower_dirs=[lower])
            tree.check(self, root / "view")


class LayerRegistry(MemoryRegistry):
    def __init__(self):
        super().__init__()
        self.base_url = "http://registry.example"
        self.layer_sizes = {}
        self.puts = []
        self.refuse_puts = False

    def manifest_layers(self, repository, reference):
        return SimpleNamespace(layers=[SimpleNamespace(size=size) for size in self.layer_sizes[reference]])

    def put_manifest(self, repository, tag, payload, *, media_type):
        if self.refuse_puts and tag.startswith("layer-"):
            raise RegistryRequestError(400, "PUT", tag, "BLOB_UNKNOWN")
        self.puts.append(tag)
        return super().put_manifest(repository, tag, payload, media_type=media_type)


class FakeDockerStore(DockerOverlay2RootfsStore):
    def __init__(self, root):
        self.docker_binary = "docker"
        self.root = root
        self.images = {}
        self.configs = {}

    def _checked(self, *argv, timeout=60):
        return ""

    def add(self, name, image_id, layers):
        rootfs = self.root / name / "rootfs"
        rootfs.mkdir(parents=True)
        (rootfs / "merged").write_text(name)
        self.images[name] = (image_id, rootfs, layers)

    def _record(self, image_ref):
        return self.images[image_ref.rsplit(":", 1)[1]]

    @contextmanager
    def operation_lease(self, image_ref):
        image_id, rootfs, _layers = self._record(image_ref)
        yield SimpleNamespace(image_id=image_id, rootfs=rootfs,
                              image_config=DockerImageConfig.from_inspection(
                                  self.configs.get(image_ref.rsplit(":", 1)[1], {"Cmd": ["/bin/sh"]})))

    def layer_diffs(self, image_ref):
        image_id, _rootfs, layers = self._record(image_ref)
        return image_id, tuple(item[0] for item in layers), tuple(item[1] for item in layers)

    def collect_image(self, image_id, *, is_referenced):
        return True


class LayerPublicationTests(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.keys = Keys()
        self.client = LayerRegistry()
        self.registry = EnvironmentArtifactRegistry(self.client, "environments", self.keys.trusted)
        self.store = FakeDockerStore(self.root / "docker")
        self.builder = FreshEnvironmentBuilder(self.store, self.registry, self.keys.key, self.root / "scratch")
        self.views, self.listings = [], []
        self.base_layers = [self.layer("b0", MIB), self.layer("b1", 2 * MIB),
                            self.layer("b2", 80 * MIB), self.layer("b3", MIB)]
        self.add_image("base", "a", self.base_layers)

    def layer(self, name, size):
        directory = self.root / "diffs" / name
        (directory / name).mkdir(parents=True)
        (directory / name / "content").write_text(name)
        return diff_id(name), directory, size

    def add_image(self, tag, character, layers):
        image_id = digest(character)
        self.store.add(tag, image_id, [(item[0], item[1]) for item in layers])
        self.client.layer_sizes[tag] = [item[2] for item in layers]
        self.client.manifests[tag] = canonical_bytes({
            "schemaVersion": 2, "mediaType": OCI_IMAGE, "config": {"digest": image_id}, "layers": []})

    def mkfs(self, argv, **_kwargs):
        if argv[1] == "-V":
            return subprocess.CompletedProcess(argv, 0, stdout=FORMAT["mkfs"] + "\n", stderr="")
        view = Path(argv[-1])
        self.views.append(view)
        listing = sorted(str(path.relative_to(view)) for path in view.rglob("*"))
        self.listings.append(listing)
        Path(argv[-2]).write_bytes(hashlib.sha256(repr(listing).encode()).digest() * 128)
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    def publish(self, tag):
        with patch("ucloud_sandboxes.environment_builder.subprocess.run", side_effect=self.mkfs):
            annotated = self.builder.publish_image("registry.example/ucloud-managed/" + tag + ":" + tag,
                                                   allowlist=("*",))
        _root, environment = load_image_environment(self.registry, "ucloud-managed/" + tag, annotated)
        return annotated, [self.registry.load(component) for component in environment.components], environment

    def registry_config(self, tag="base"):
        layers = self.base_layers
        raw = canonical_bytes({"rootfs": {"type": "layers", "diff_ids": [v[0] for v in layers]},
                               "config": {"Cmd": ["/bin/sh"], "Env": ["FIXTURE=1"]}})
        image_id = content_digest(raw)
        self.client.blobs[image_id] = raw
        self.client.manifests[tag] = canonical_bytes({
            "schemaVersion": 2, "mediaType": OCI_IMAGE,
            "config": {"digest": image_id, "size": len(raw)},
            "layers": [{"digest": v[0], "size": v[2]} for v in layers]})
        _, rootfs, diffs = self.store.images[tag]
        self.store.images[tag] = image_id, rootfs, diffs
        self.store.configs[tag] = {"Cmd": ["/bin/sh"], "Env": ["FIXTURE=1"]}
        return image_id

    def test_complete_layer_hit_skips_docker_and_preserves_signed_configuration(self):
        image_id = self.registry_config()
        _, _, cold = self.publish("base")
        self.views.clear()
        with publication_metrics() as metrics, \
             patch.object(self.store, "_checked", side_effect=AssertionError("must not pull")), \
             patch.object(self.store, "operation_lease", side_effect=AssertionError("must not mount")):
            _, _, warm = self.publish("base")
        self.assertEqual(warm.source_image, image_id)
        self.assertEqual(warm, cold)
        self.assertEqual(warm.image_config["Env"], ["FIXTURE=1"])
        self.assertEqual(self.views, [])
        self.assertEqual(metrics["docker_pull_skipped"], 1)
        self.assertEqual(metrics["groups_reused"], 3)
        self.assertNotIn("mkfs_ms", metrics)

    def test_partial_cache_miss_pulls_and_builds_only_the_missing_group(self):
        self.registry_config()
        _, base, _ = self.publish("base")
        self.client.tags.pop("layer-" + base[-1].group_key)
        self.views.clear()
        with patch.object(self.store, "_checked", wraps=self.store._checked) as pull, \
             publication_metrics() as metrics:
            _, components, _ = self.publish("base")
        self.assertEqual(pull.call_count, 1)
        self.assertEqual(len(self.views), 1)
        self.assertEqual(components, base)
        self.assertEqual(metrics["groups_built"], 1)
        self.assertEqual(metrics["groups_reused"], 2)
        self.assertIn("docker_pull_ms", metrics)

    def test_reuse_avoids_duplicate_manifest_reads_but_keeps_refresh_and_root_validation(self):
        self.registry_config()
        _, components, environment = self.publish("base")
        self.client.puts.clear()
        with patch.object(self.client, "manifest_document", wraps=self.client.manifest_document) as reads:
            annotated = self.builder.publish_image("registry.example/ucloud-managed/base:base", allowlist=("*",))
        requested = [call.args for call in reads.call_args_list]
        for component, digest in zip(components, environment.components):
            tag = "layer-" + component.group_key
            # Preflight and refresh each read the tag once. Their already-read
            # document is authenticated without another immutable-manifest GET.
            self.assertEqual(requested.count(("environments", tag)), 2)
            # Final publication still reloads each immutable component and
            # verifies exact source binding before signing the environment.
            self.assertEqual(requested.count(("environments", digest)), 1)
        self.assertEqual([tag for tag in self.client.puts if tag.startswith("layer-")],
                         ["layer-" + component.group_key for component in components])
        _, result = load_image_environment(self.registry, "ucloud-managed/base", annotated)
        self.assertEqual(result, environment)

    def test_reuse_checks_source_parent_and_format_from_supplied_document(self):
        self.registry_config()
        _, components, _ = self.publish("base")
        component = components[0]
        tag = "layer-" + component.group_key
        self.client.puts.clear()
        for layers, parent, layer_format in (
                ([diff_id("foreign")], component.parent, component.format),
                (component.source_layers, diff_id("foreign"), component.format),
                (component.source_layers, component.parent, component.format | {"compression": ""})):
            with self.subTest(layers=layers, parent=parent, layer_format=layer_format), \
                 self.assertLogs("ucloud_sandboxes.environment_builder", level="WARNING"):
                self.assertIsNone(self.builder._reuse_layer_component(tag, layers, parent, layer_format))
        self.assertEqual(self.client.puts, [])

    def test_corrupt_oci_config_cannot_be_signed_from_a_cache_hit(self):
        image_id = self.registry_config()
        self.publish("base")
        self.client.blobs[image_id] = self.client.blobs[image_id].replace(b"FIXTURE=1", b"FIXTURE=2")
        with patch.object(self.store, "_checked", side_effect=AssertionError("must fail closed")), \
             self.assertRaisesRegex(ValueError, "config content identity"):
            self.publish("base")

    def test_unsupported_mkfs_version_query_retains_whole_image_fallback(self):
        self.registry_config()
        with patch.object(self.builder, "layer_format", side_effect=OSError("no version option")):
            _, components, _ = self.publish("base")
        self.assertEqual(len(components), 1)
        self.assertNotIsInstance(components[0], LayerEnvironmentComponent)

    def test_concurrent_builders_share_one_conversion_and_release_failed_lock(self):
        first_entered, release, duplicate_entered, second_started = (Event() for _ in range(4))
        other = FreshEnvironmentBuilder(self.store, self.registry, self.keys.key, self.builder.work_root)
        calls = []

        def mkfs(image, view, **kwargs):
            calls.append(view)
            if len(calls) > 1:
                duplicate_entered.set()
            first_entered.set()
            if not release.wait(3):
                raise TimeoutError("test did not release conversion")
            image.write_bytes(b"x" * 4096)

        def publish(builder):
            if builder is other:
                second_started.set()
            return builder._publish_layer_group([self.base_layers[2][1]], [diff_id("b2")],
                lower_dirs=[], parent=None, layer_format=FORMAT)

        with patch.object(FreshEnvironmentBuilder, "_mkfs", side_effect=mkfs), \
             ThreadPoolExecutor(2) as pool:
            first = pool.submit(publish, self.builder)
            self.assertTrue(first_entered.wait(2))
            second = pool.submit(publish, other)
            try:
                self.assertTrue(second_started.wait(2))
                self.assertFalse(duplicate_entered.wait(0.1))
            finally:
                release.set()
            first_result, second_result = first.result(timeout=3), second.result(timeout=3)
        self.assertEqual(first_result[0], second_result[0])
        self.assertEqual((first_result[1], second_result[1]), (False, True))
        self.assertEqual(len(calls), 1)
        # A failed owner leaves neither a held lock nor an apparently ready tag.
        self.client.tags.clear()
        with patch.object(self.builder, "_mkfs", side_effect=OSError("conversion failed")), \
             self.assertRaises(OSError):
            publish(self.builder)
        with patch.object(other, "_mkfs", side_effect=lambda image, *a, **kw: image.write_bytes(b"x" * 4096)):
            self.assertFalse(publish(other)[1])

    def test_images_sharing_a_base_share_its_layer_components(self):
        _annotated, base, base_environment = self.publish("base")
        self.assertEqual([component.source_layers for component in base],
                         [(diff_id("b0"), diff_id("b1")), (diff_id("b2"),), (diff_id("b3"),)])
        self.assertTrue(all(isinstance(component, LayerEnvironmentComponent) for component in base))
        # A multi-layer group is squashed; a single layer is its diff directory.
        self.assertEqual(self.views[1:], [self.base_layers[2][1], self.base_layers[3][1]])
        self.assertEqual(self.listings[0], ["b0", "b0/content", "b1", "b1/content"])

        self.add_image("task", "b", [*self.base_layers, (EMPTY_LAYER_DIFF_ID, self.root / "empty", 32),
                                     self.layer("t0", 2 * MIB)])
        (self.root / "empty").mkdir()
        self.views.clear()
        self.client.puts.clear()
        _annotated, task, task_environment = self.publish("task")
        # The base's two closed groups are reused; its open top group joins the task layer.
        self.assertEqual(task_environment.components[:2], base_environment.components[:2])
        self.assertEqual(task[2].source_layers, (diff_id("b3"), diff_id("t0")))
        self.assertEqual(task[2].parent, layer_chain_id([diff_id(name) for name in ("b0", "b1", "b2")]))
        self.assertEqual(len(self.views), 1)
        # Reuse re-puts each index tag, restarting its retention grace.
        self.assertEqual([tag for tag in self.client.puts if tag.startswith("layer-")],
                         ["layer-" + component.group_key for component in task])
        self.assertEqual(task_environment.environment.rootfs_fingerprint(HOST_EROFS_ABI),
                         EnvironmentManifest.from_dict(task_environment.environment.to_dict())
                         .rootfs_fingerprint(HOST_EROFS_ABI))

    def test_stale_or_unwritable_index_tags_are_rebuilt(self):
        whole = sign_component(image_file(self.root / "whole.erofs", b"w"), source_image=digest("1"),
                               signing_key=self.keys.key)
        tag = "layer-" + layer_group_key(FORMAT, None, [diff_id("b0"), diff_id("b1")])
        self.registry.publish(self.root / "whole.erofs", whole, tag=tag)
        _annotated, base, _environment = self.publish("base")
        self.assertEqual(len(self.views), 3)
        self.assertEqual(self.registry.load(self.client.tags[tag]), base[0])
        # A registry that refuses the re-put (its blobs were swept) rebuilds.
        self.views.clear()
        self.client.refuse_puts = True
        with patch("ucloud_sandboxes.environment_builder.subprocess.run", side_effect=self.mkfs), \
             self.assertRaises(RegistryRequestError):
            self.builder.publish_image("registry.example/ucloud-managed/base:base", allowlist=("*",))
        self.assertEqual(len(self.views), 1)

    def test_unsplittable_images_fall_back_to_one_whole_image_component(self):
        self.client.layer_sizes["base"] = [MIB]
        _annotated, components, _environment = self.publish("base")
        self.assertEqual(len(components), 1)
        self.assertIsInstance(components[0], EnvironmentComponent)
        self.assertEqual(components[0].source_image, digest("a"))
        self.assertEqual(self.views, [self.store.images["base"][1]])

    def test_failed_squash_falls_back_to_the_whole_image(self):
        with patch.object(environment_builder, "squash_layer_diffs", side_effect=ValueError("special file")):
            _annotated, components, _environment = self.publish("base")
        self.assertEqual(len(components), 1)
        self.assertEqual(components[0].source_image, digest("a"))

    def test_composed_image_mounts_relative_lowers_and_counts_shared_devices(self):
        annotated, _base, environment = self.publish("base")
        components = self.root / "backend" / "components"
        mounts, commands = set(), []

        class Runner:
            def run(self, command, **_kwargs):
                commands.append(command)
                if command[0] in {"mount", "env"}:
                    mounts.add(Path(command[-1]))
                code = int(Path(command[-1]) not in mounts) if command[0] == "mountpoint" else 0
                return SimpleNamespace(returncode=code, stdout="", stderr="")

        ensured = []

        def ensure(component):
            ensured.append(component)
            return components / component[7:]

        backend = SimpleNamespace(ensure=ensure, drop=lambda _digest: True)
        store = EnvironmentRootfsStore(self.root / "store", self.registry, backend, runner=Runner(),
                                       referenced=lambda _: False, block_devices=8)
        with store.operation_lease("registry.example/ucloud-managed/base@" + annotated) as image:
            self.assertEqual(image.backend_abi, HOST_EROFS_ABI)
            self.assertEqual(len(image.environment.toolkits), 2)
        mount = next(command for command in commands if command[0] == "env")
        self.assertEqual(mount[:5], ("env", "-C", str(components), "LIBMOUNT_FORCE_MOUNT2=always", "mount"))
        lowers = mount[mount.index("-o") + 1].removeprefix("ro,lowerdir=").split(":")
        self.assertEqual(lowers, [component[7:] for component in reversed(environment.components)])
        self.assertEqual(ensured[:3], list(environment.components))
        self.assertEqual(store.operation_snapshot()["environment_devices_in_use"], 3)
        self.assertEqual(store.operation_snapshot()["environment_devices_free"], 5)
        self.assertTrue(store.collect_image(image.image_id, is_referenced=lambda _: False))
        self.assertEqual(store.operation_snapshot()["environment_devices_in_use"], 0)

        def exhausted(_component):
            raise RuntimeError(NO_BLOCK_DEVICE)

        store.backend = SimpleNamespace(ensure=exhausted, drop=lambda _digest: True)
        with self.assertRaises(EnvironmentDeviceCapacityError) as raised:
            with store.operation_lease("registry.example/ucloud-managed/base@" + annotated):
                pass
        self.assertIsInstance(raised.exception, DirectRegistryCapacityUnavailable)


    def test_partial_device_exhaustion_releases_unused_exports(self):
        _annotated, _base, environment = self.publish("base")
        active = set()

        def ensure(component):
            if component not in active and len(active) == 2:
                raise RuntimeError(NO_BLOCK_DEVICE)
            active.add(component)
            return self.root / "components" / component[7:]

        def drop(component):
            active.discard(component)
            return True

        store = EnvironmentRootfsStore(self.root / "store", self.registry,
            SimpleNamespace(ensure=ensure, drop=drop), block_devices=2,
            runner=SimpleNamespace(run=lambda *args, **kwargs: SimpleNamespace(returncode=1)))
        (store.images / ("a" * 64)).mkdir()
        with self.assertRaises(EnvironmentDeviceCapacityError):
            store._mount(digest("a"), environment)
        self.assertEqual(active, set())


class RelativeLowerDependencyTests(unittest.TestCase):
    def test_relative_lowers_resolve_against_the_components_directory(self):
        with TemporaryDirectory() as raw:
            components = Path(raw) / "components"
            first, second, third = (components / (character * 64) for character in "123")
            for path in (first, second, third):
                path.mkdir(parents=True)
            overlay = (f"101 1 0:123 / /image ro - overlay overlay ro,lowerdir={'2' * 64}:{'1' * 64}\n"
                       "102 1 0:124 / /docker ro - overlay overlay ro,lowerdir=l/ABC:l/DEF\n")
            with patch("ucloud_sandboxes.environment_backend.Path.read_text", return_value=overlay):
                self.assertTrue(mount_has_dependents(first, include_bind_mounts=False))
                self.assertTrue(mount_has_dependents(second, include_bind_mounts=False))
                self.assertFalse(mount_has_dependents(third, include_bind_mounts=False))
            plus = f"103 1 0:125 / /image ro - overlay overlay ro,lowerdir+={first},lowerdir+={'2' * 64}\n"
            with patch("ucloud_sandboxes.environment_backend.Path.read_text", return_value=plus):
                self.assertTrue(mount_has_dependents(first, include_bind_mounts=False))
                self.assertTrue(mount_has_dependents(second, include_bind_mounts=False))


if __name__ == "__main__":
    unittest.main()
