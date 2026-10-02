"""Layout-2 layer components keep file mtimes, so Python's .pyc caches stay valid.

The view tests run everywhere: they record the tree the builder hands mkfs.
The real tests need erofs-utils 1.9+ (mkfs.erofs --mkfs-time --MZ and fsck.erofs
--extract=DIR). They publish through the builder's layer-group path with its
exact mkfs flags, then read the signed image back through a host extraction.
"""
from contextlib import ExitStack
import hashlib
import importlib.util
import os
from pathlib import Path
import py_compile
import shutil
import stat
import subprocess
import sys
import tarfile
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from tests.test_environment_artifact import MemoryRegistry
from tests.test_environment_layers import FORMAT, Keys
from tests.test_oci_layer_materialize import REPOSITORY, MemoryRegistry as BlobRegistry, directory, layer, member
from ucloud_sandboxes.environment_artifact import EnvironmentArtifactRegistry
from ucloud_sandboxes.environment_builder import FreshEnvironmentBuilder, normalize_directory_times
from ucloud_sandboxes.oci_layer_materialize import materialize_layers

SOURCE_TIME = 1_700_000_000
LINK_TIME = 1_650_000_000
# Equal sizes: the cached bytecode is valid for SOURCE only while the image
# keeps SOURCE's mtime, and importing then returns the cached value.
SOURCE = b"VALUE = 'source'\n"
CACHED = b"VALUE = 'cached'\n"
CACHE = "app/__pycache__/mod." + sys.implementation.cache_tag + ".pyc"
# Header times of every regular file and symlink in the two layers.
FILE_TIMES = {"app/mod.py": SOURCE_TIME, CACHE: SOURCE_TIME + 5, "app/link": LINK_TIME,
              "tool/run.py": SOURCE_TIME + 9}


def _real_erofs():
    tools = [shutil.which("mkfs.erofs"), shutil.which("fsck.erofs")]
    if not all(tools):
        return False
    mkfs, fsck = (subprocess.run([tool, "--help"], capture_output=True, text=True) for tool in tools)
    return (all(option in mkfs.stdout + mkfs.stderr for option in ("--mkfs-time", "--MZ", "lz4"))
            and "--extract[=" in fsck.stdout + fsck.stderr)


def timed(entry, mtime):
    entry[0].mtime = mtime
    return entry


def compiled(root):
    """Timestamp bytecode of CACHED, stamped with SOURCE's mtime and size."""
    source = root / "compile" / "mod.py"
    source.parent.mkdir()
    source.write_bytes(CACHED)
    os.utime(source, (SOURCE_TIME, SOURCE_TIME))
    target = root / "compile" / "mod.pyc"
    py_compile.compile(str(source), cfile=str(target), doraise=True,
                       invalidation_mode=py_compile.PycInvalidationMode.TIMESTAMP)
    return target.read_bytes()


class LayoutTwoFixture:
    def setUp(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.keys = Keys()
        stack = ExitStack()
        self.addCleanup(stack.close)
        # Extraction is privileged in production; tests keep their own owner.
        stack.enter_context(patch("ucloud_sandboxes.oci_layer_materialize.os.chown"))
        package = layer([timed(directory("app"), 1_600_000_000), timed(member("app/mod.py", SOURCE), SOURCE_TIME),
                         timed(directory("app/__pycache__"), 1_600_000_001),
                         timed(member(CACHE, compiled(self.root)), FILE_TIMES[CACHE]),
                         timed(member("app/link", kind=tarfile.SYMTYPE, linkname="mod.py"), LINK_TIME)])
        tool = layer([timed(directory("tool"), 1_600_000_002),
                      timed(member("tool/run.py", b"print('run')\n"), FILE_TIMES["tool/run.py"])])
        self.layers = [package, tool]
        self.blobs = BlobRegistry(self.layers)

    def layer_format(self, builder):
        return builder.layer_format()

    def publish(self, name, *, preserve, consume=True, count=2, directory_time):
        """Publish one group with a fresh builder and registry; nothing is reused."""
        layers = self.layers[:count]
        registry = EnvironmentArtifactRegistry(MemoryRegistry(), "environments", self.keys.trusted)
        builder = FreshEnvironmentBuilder(None, registry, self.keys.key, self.root / name / "work",
                                          preserve_mtimes=preserve)
        directories = materialize_layers(self.blobs, REPOSITORY, [item[0] for item in layers],
                                         [item[1] for item in layers], self.root / name / "diffs")
        # Another extraction, squash or Docker layer creation makes directories
        # at another time. Only file and symlink times come from the tar headers.
        for top in directories:
            for path in (top, *top.rglob("*")):
                if path.is_dir() and not path.is_symlink():
                    os.utime(path, ns=(directory_time, directory_time))
        digest, reused = builder._publish_layer_group(
            directories, [item[1] for item in layers], lower_dirs=(), parent=None,
            layer_format=self.layer_format(builder), consume_private_diffs=consume)
        self.assertFalse(reused)
        return registry, registry.load(digest)


class OwnedViewTimesTests(LayoutTwoFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.views = []
        stack = ExitStack()
        self.addCleanup(stack.close)
        stack.enter_context(patch.object(FreshEnvironmentBuilder, "_mkfs", autospec=True, side_effect=self.mkfs))

    def layer_format(self, builder):
        builder._layer_format = FORMAT | {"layout": 2 if builder.preserve_mtimes else 1}
        return builder._layer_format

    def mkfs(self, builder, image, view, *, exclude_runtime_mounts, preserve_mtimes):
        self.assertTrue(exclude_runtime_mounts)
        self.assertEqual(preserve_mtimes, builder.preserve_mtimes)
        tree = {}
        for path in (view, *view.rglob("*")):
            info = path.lstat()
            tree[str(path.relative_to(view))] = (
                stat.S_IFMT(info.st_mode), info.st_mtime_ns,
                path.read_bytes() if stat.S_ISREG(info.st_mode) else
                os.readlink(path) if stat.S_ISLNK(info.st_mode) else None)
        self.views.append(dict(sorted(tree.items())))
        image.write_bytes(hashlib.sha256(repr(self.views[-1]).encode()).digest() * 128)

    def test_owned_views_feed_mkfs_the_same_tree_whatever_their_directory_times(self):
        for consume, count in ((True, 2), (True, 1), (False, 2)):
            with self.subTest(consume_private_diffs=consume, layers=count):
                self.views.clear()
                _, first = self.publish("a", preserve=True, consume=consume, count=count, directory_time=10**18)
                _, second = self.publish("b", preserve=True, consume=consume, count=count, directory_time=2 * 10**18)
                self.assertEqual(first.image_digest, second.image_digest)
                self.assertEqual(self.views[0], self.views[1])
                times = {name: mtime for name, (kind, mtime, _) in self.views[0].items() if kind != stat.S_IFDIR}
                self.assertEqual(times, {name: value * 10**9 for name, value in FILE_TIMES.items()
                                         if count == 2 or name.startswith("app/")})
                self.assertEqual({mtime for kind, mtime, _ in self.views[0].values() if kind == stat.S_IFDIR}, {0})
                shutil.rmtree(self.root / "a")
                shutil.rmtree(self.root / "b")

    def test_layout_one_and_a_borrowed_docker_diff_feed_mkfs_their_views_unchanged(self):
        # Layout 1 zeroes every time in mkfs. A lone Docker diff is borrowed and
        # keeps Docker's directory times; only its file times are deterministic.
        for preserve, consume, count in ((False, True, 2), (True, False, 1)):
            with self.subTest(preserve=preserve, consume_private_diffs=consume, layers=count):
                self.views.clear()
                self.publish("a", preserve=preserve, consume=consume, count=count, directory_time=10**18)
                directories = {mtime for kind, mtime, _ in self.views[0].values() if kind == stat.S_IFDIR}
                self.assertEqual(directories, {10**18})
                self.assertEqual(self.views[0]["app/mod.py"][1], SOURCE_TIME * 10**9)
                shutil.rmtree(self.root / "a")


class NormalizeDirectoryTimesTests(unittest.TestCase):
    def test_directory_and_whiteout_times_go_to_zero_and_file_and_symlink_times_stay(self):
        with TemporaryDirectory() as temporary:
            view = Path(temporary) / "view"
            (view / "app" / "lib").mkdir(parents=True)
            (view / "app" / "mod.py").write_bytes(SOURCE)
            (view / "app" / "link").symlink_to("mod.py")
            # A FIFO stands in for a 0:0 whiteout device, which needs root.
            os.mkfifo(view / "app" / "lib" / "gone")
            for path in (view, *view.rglob("*")):
                os.utime(path, ns=(7, SOURCE_TIME * 10**9), follow_symlinks=False)
            normalize_directory_times(view)
            self.assertEqual({str(path.relative_to(view)): path.lstat().st_mtime_ns for path in (view, *view.rglob("*"))},
                             {".": 0, "app": 0, "app/lib": 0, "app/lib/gone": 0,
                              "app/mod.py": SOURCE_TIME * 10**9, "app/link": SOURCE_TIME * 10**9})


@unittest.skipUnless(_real_erofs(), "needs erofs-utils 1.9+ (mkfs.erofs --mkfs-time --MZ, fsck.erofs --extract=DIR)")
class RealErofsLayoutTwoTests(LayoutTwoFixture, unittest.TestCase):
    def extract(self, registry, component, name):
        image = self.root / (name + ".erofs")
        image.write_bytes(registry.client.blobs[component.image_digest])
        view = self.root / (name + "-extracted")
        subprocess.run(["fsck.erofs", "--extract=" + str(view), str(image)], check=True, capture_output=True)
        return view

    def import_value(self, view):
        spec = importlib.util.spec_from_file_location("layout_mod_" + view.name.replace("-", "_"),
                                                      view / "app" / "mod.py")
        module = importlib.util.module_from_spec(spec)
        cache = sorted(path.name for path in (view / "app" / "__pycache__").iterdir())
        with patch.object(sys, "dont_write_bytecode", True), patch.object(sys, "pycache_prefix", None):
            spec.loader.exec_module(module)
        self.assertEqual(sorted(path.name for path in (view / "app" / "__pycache__").iterdir()), cache)
        return module.VALUE

    def test_equal_layer_groups_build_identical_layout_two_images(self):
        for consume, count in ((True, 2), (True, 1), (False, 2)):
            with self.subTest(consume_private_diffs=consume, layers=count):
                _, first = self.publish(f"a-{consume}-{count}", preserve=True, consume=consume, count=count,
                                        directory_time=10**18)
                _, second = self.publish(f"b-{consume}-{count}", preserve=True, consume=consume, count=count,
                                         directory_time=2 * 10**18)
                self.assertEqual(first.format["layout"], 2)
                self.assertEqual(first.image_digest, second.image_digest)

    def test_layout_two_keeps_source_mtimes_and_its_timestamp_bytecode_stays_valid(self):
        registry, component = self.publish("layout-2", preserve=True, directory_time=10**18)
        view = self.extract(registry, component, "layout-2")
        for name, mtime in FILE_TIMES.items():
            with self.subTest(path=name):
                self.assertEqual((view / name).lstat().st_mtime_ns, mtime * 10**9)
        source, data = (view / "app" / "mod.py").stat(), (view / CACHE).read_bytes()
        # Python's own timestamp check: flags 0, then source mtime and size.
        self.assertEqual(int.from_bytes(data[4:8], "little"), 0)
        self.assertEqual(int.from_bytes(data[8:12], "little"), int(source.st_mtime) & 0xFFFFFFFF)
        self.assertEqual(int.from_bytes(data[12:16], "little"), source.st_size & 0xFFFFFFFF)
        self.assertEqual(self.import_value(view), "cached")

    def test_layout_one_zeroes_mtimes_so_python_ignores_the_bytecode(self):
        registry, component = self.publish("layout-1", preserve=False, directory_time=10**18)
        self.assertEqual(component.format["layout"], 1)
        view = self.extract(registry, component, "layout-1")
        self.assertEqual((view / "app" / "mod.py").lstat().st_mtime_ns, 0)
        self.assertEqual(self.import_value(view), "source")


if __name__ == "__main__":
    unittest.main()
