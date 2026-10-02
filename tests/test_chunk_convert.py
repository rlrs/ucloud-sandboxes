"""The converter and packer: dedupe, idempotency, crash injection at every
write-path step (design §3), granularity, and the tree verifier."""
from pathlib import Path
import tarfile
from tempfile import TemporaryDirectory
import unittest

from tests.chunk_store_support import REPOSITORY, ChunkStoreFixture, layer, sample_images, signing
from ucloud_sandboxes.chunk_convert import (STEPS, compare_trees, expected_tree, overlay_whiteouts,
                                            strip_environment_annotation)
from ucloud_sandboxes.environment_artifact import (ENVIRONMENT_ANNOTATION, RafsEnvironmentComponent, load_image_environment,
                                                   load_environment)

TEST_TIER = "contract"


class Crash(Exception):
    pass


class ConverterTests(unittest.TestCase):
    def setUp(self):
        self.signer = signing()

    def store(self, **kwargs):
        store = ChunkStoreFixture(self, signer=self.signer, **kwargs)
        self.images = sample_images(store.client)
        return store

    def test_conversion_publishes_a_signed_root_and_dedupes_across_images(self):
        store = self.store()
        first = store.converter.convert(REPOSITORY, "a")
        environment = load_environment(store.registry, first["root"])
        self.assertEqual(environment.source_image, self.images[0][1])
        base = store.registry.load(environment.environment.base)
        self.assertIsInstance(base, RafsEnvironmentComponent)
        self.assertEqual((base.format["layout"], environment.environment.toolkits), ("image", ()))
        self.assertEqual(len(store.roots()), 1)
        packs_after_a, new_a = store.packs(), first["metrics"]["chunks_new"]
        store.converter.metrics.clear()
        second = store.converter.convert(REPOSITORY, "b")
        # The base layer is reused through the layer cache; of B's top layer
        # only the chunks of its own file are new (the shared file is known).
        self.assertEqual(second["metrics"]["layers_reused"], 1)
        self.assertEqual(second["metrics"]["chunks_new"], 1)
        self.assertEqual(len(store.packs()), len(packs_after_a) + 1)
        self.assertGreater(new_a, 5)
        self.assertNotEqual(first["root"], second["root"])

    def test_a_rerun_converges_on_the_same_root_without_new_packs(self):
        store = self.store()
        first = store.converter.convert(REPOSITORY, "a")
        objects = dict(store.objects.objects)
        store.converter.metrics.clear()
        again = store.converter.convert(REPOSITORY, "a")
        self.assertEqual(again["root"], first["root"])
        self.assertEqual(store.objects.objects, objects)
        self.assertNotIn("chunks_new", again["metrics"])

    def test_a_crash_at_every_step_leaves_no_visible_image_and_a_rerun_converges(self):
        reference = self.store().converter.convert(REPOSITORY, "a")["root"]
        for step in STEPS:
            store = self.store()
            seen = []

            def crash(name, step=step, **_):
                seen.append(name)
                if name == step and seen.count(name) == 1:
                    raise Crash(step)
            store.converter.step = crash
            with self.subTest(step=step):
                with self.assertRaises(Crash):
                    store.converter.convert(REPOSITORY, "a")
                if step != "root_published":
                    self.assertEqual(store.roots(), [])
                store.converter.step = None
                self.assertEqual(store.converter.convert(REPOSITORY, "a")["root"], reference)
                # Every chunk the root needs is committed and served.
                base = load_environment(store.registry, reference).environment.base
                self.assertEqual(store.index.reader.locator(base).epoch, 1)

    def test_an_attached_copy_lets_workers_resolve_the_new_root(self):
        store = self.store()
        original = dict(store.client.manifests)
        result = store.converter.convert(REPOSITORY, "a", attach_tag="a-rafs")
        root, environment = load_image_environment(store.registry, REPOSITORY, result["image_manifest"])
        self.assertEqual((root, environment.source_image), (result["root"], self.images[0][1]))
        self.assertEqual(store.client.tags["a"], self.images[0][0])  # The source tag is untouched.
        self.assertTrue(set(original) <= set(store.client.manifests))

    def test_layer_granularity_publishes_one_shared_component_per_layer(self):
        store = self.store(layout="layer")
        first = store.converter.convert(REPOSITORY, "a")
        second = store.converter.convert(REPOSITORY, "b")
        self.assertEqual(len(first["components"]), 2)
        self.assertEqual(first["components"][0], second["components"][0])  # The shared base layer.
        for digest in first["components"]:
            component = store.registry.load(digest)
            self.assertEqual((component.format["layout"], component.source_image), ("layer", None))

    def test_images_that_differ_from_their_config_are_refused(self):
        store = self.store()
        store.client.blobs[next(iter(store.client.blobs))] = b"tampered"
        with self.assertRaises(ValueError):
            for tag in ("a", "b"):
                store.converter.convert(REPOSITORY, tag)


def read(path):
    with tarfile.open(path) as reader:
        return {member.name: member for member in reader}


class TreeTests(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)

    def tars(self, *layers):
        paths = []
        for index, (blob, _) in enumerate(layers):
            paths.append(self.root / f"{index}.tar.gz")
            paths[-1].write_bytes(blob)
        return paths

    def test_expected_tree_applies_whiteouts_opaques_and_hardlinks(self):
        tars = self.tars(layer([("d/x", b"1"), ("d/y", b"2"), ("e/z", b"3"), ("f", b"4")]),
                         layer([("d/.wh.x", b""), ("e/.wh..wh..opq", b""), ("e/new", b"5"), ("g", ("link", "f"))]))
        tree = expected_tree(tars)
        self.assertEqual(sorted(tree), ["d/y", "e/new", "f", "g"])
        self.assertIsNotNone(tree["g"][-1])
        self.assertEqual(tree["g"][-1], tree["f"][-1])
        actual = {name: value[:-1] + (None,) for name, value in tree.items()}
        actual["g"] = actual["g"][:-1] + ("f",)
        actual["f"] = actual["f"][:-1] + ("f",)
        self.assertEqual(compare_trees(tree, actual), [])
        unlinked = {**actual, "g": actual["g"][:-1] + (None,)}
        self.assertEqual(compare_trees(tree, unlinked), ["hardlink groups differ: f,g"])
        broken = dict(actual)
        broken["d/y"] = ("file", 0o600) + broken["d/y"][2:]
        del broken["e/new"]
        self.assertEqual(len(compare_trees(tree, broken)), 2)

    def test_a_replaced_hardlink_member_leaves_its_group(self):
        # conda: lib/x.pyc is a link into pkgs/; a later layer rewrites lib/x.pyc.
        tars = self.tars(layer([("pkgs/x.pyc", b"old"), ("lib/x.pyc", ("link", "pkgs/x.pyc")),
                                ("pkgs/y", b"y"), ("lib/y", ("link", "pkgs/y")), ("lib/y2", ("link", "pkgs/y"))]),
                         layer([("lib/x.pyc", b"new"), ("pkgs/.wh.y", b"")]),
                         layer([("pkgs/z", b"z"), ("lib/z", ("link", "pkgs/z"))]))
        tree = expected_tree(tars)
        self.assertIsNone(tree["lib/x.pyc"][-1])
        self.assertEqual(tree["lib/y"][-1], tree["lib/y2"][-1])
        self.assertNotEqual(tree["lib/y"][-1], tree["lib/z"][-1])
        actual = {name: value[:-1] + (None,) for name, value in tree.items()}
        for name, label in (("lib/y", "lib/y"), ("lib/y2", "lib/y"), ("pkgs/z", "lib/z"), ("lib/z", "lib/z")):
            actual[name] = actual[name][:-1] + (label,)
        self.assertEqual(compare_trees(tree, actual), [])

    def test_overlay_whiteouts_for_per_layer_stacking(self):
        source = self.tars(layer([("d/.wh.x", b""), ("e/.wh..wh..opq", b""), ("e/new", b"5")], compress=False))[0]
        overlay_whiteouts(source, self.root / "overlay.tar")
        members = read(self.root / "overlay.tar")
        self.assertTrue(members["d/x"].ischr() and (members["d/x"].devmajor, members["d/x"].devminor) == (0, 0))
        self.assertEqual(members["e"].pax_headers.get("SCHILY.xattr.trusted.overlay.opaque"), "y")
        self.assertNotIn("e/.wh..wh..opq", members)

    def test_derived_manifests_drop_the_old_root_annotation(self):
        document = {"schemaVersion": 2, "annotations": {ENVIRONMENT_ANNOTATION: "sha256:" + "1" * 64, "keep": "1"}}
        self.assertEqual(strip_environment_annotation(document), {"schemaVersion": 2, "annotations": {"keep": "1"}})
        self.assertEqual(strip_environment_annotation({"annotations": {ENVIRONMENT_ANNOTATION: "x"}}), {})


if __name__ == "__main__":
    unittest.main()
