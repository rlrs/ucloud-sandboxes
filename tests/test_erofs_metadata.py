"""EROFS metadata walker against real mkfs.erofs images (plan C2.2).

Completeness is proven three independent ways:

* every byte that ``fsck.erofs --extract`` and ``dump.erofs --nid -e`` read
  (traced with strace), minus the regular-file data extents dump.erofs
  reports, lies in a block the walker returns;
* with every byte outside the walker's ranges and those data extents
  overwritten, fsck still passes and dump.erofs prints identical inodes;
* on that overwritten image a separate test-only reader resolves every
  path and returns the source tree's lstat fields, names, symlink targets
  and xattrs (fsck/dump 1.4 read neither shared xattrs nor symlink targets).
"""
import bisect
import os
from pathlib import Path
import random
import re
import shutil
import socket
import stat
import struct
import subprocess
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from ucloud_sandboxes.environment_builder import FreshEnvironmentBuilder, WHOLE_IMAGE_EXCLUDED
from ucloud_sandboxes.erofs_metadata import UnsupportedErofs, metadata_ranges, symlink_targets, walk

MKFS = shutil.which("mkfs.erofs")
DUMP = shutil.which("dump.erofs")
FSCK = shutil.which("fsck.erofs")
STRACE = shutil.which("strace")
_USAGE = subprocess.run([MKFS, "--help"], capture_output=True, text=True) if MKFS else None
# The builder's layout 2 (-T 0 --mkfs-time --MZ) needs erofs-utils 1.9.
LAYOUT_TWO = _USAGE is not None and all(option in _USAGE.stdout + _USAGE.stderr for option in ("--mkfs-time", "--MZ"))
NEEDS_LAYOUT_TWO = "needs erofs-utils 1.9+ (mkfs.erofs --mkfs-time --MZ) for the builder's layout 2"
BLOCK = 4096
_EXTENT = re.compile(r"^\s*\d+:\s+\d+\.\.\s*\d+\s*\|\s*\d+\s*:\s*(\d+)\.\.\s*(\d+)\s*\|", re.M)
_OPEN = re.compile(r'^(\d+)\s+openat\(AT_FDCWD, "([^"]*)", [^)]*\)\s+=\s+(\d+)')
_CLOSE = re.compile(r"^(\d+)\s+close\((\d+)\)")
_PREAD = re.compile(r"^(\d+)\s+pread64\((\d+), [^,]*, (\d+), (\d+)\)\s+=\s+(\d+)")
_UNTRACED = re.compile(r"^(\d+)\s+(?:read|mmap|preadv2?)\((\d+),")
_PREFIXES = {1: "user.", 2: "system.posix_acl_access", 3: "system.posix_acl_default", 4: "trusted.", 6: "security."}


def build_tree(root: Path, *, small_files=60, big_entries=160):
    """Deep trees, many small files, hardlinks, symlinks, xattrs, whiteouts."""
    rng = random.Random(7)
    root.mkdir()
    deep = root
    for depth in range(36):
        deep = deep / f"d{depth:02d}"
        deep.mkdir()
        (deep / "leaf").write_text(f"depth {depth}\n")
    small = root / "small"
    small.mkdir()
    for index in range(small_files):
        (small / f"file-{index:05d}-{'x' * (index % 60)}").write_bytes(rng.randbytes(index % 300))
    big = root / "bigdir"  # Several directory blocks.
    big.mkdir()
    for index in range(big_entries):
        (big / ("n" * (index % 200 + 1) + str(index))).touch()
    links = root / "links"
    links.mkdir()
    (links / "target").write_bytes(b"hard link target\n" * 100)
    for index in range(4):
        os.link(links / "target", links / f"hard-{index}")
    os.link(small / "file-00010-xxxxxxxxxx", links / "cross-dir-hard")
    os.symlink("target", links / "short")
    os.symlink("t" * 1500, links / "medium")
    os.symlink("/" + "long/" * 600 + "end", links / "long")
    xattrs = root / "xattrs"
    xattrs.mkdir()
    for index in range(30):
        path = xattrs / f"inline-{index}"
        path.write_text("x")
        os.setxattr(path, "user.unique", f"value-{index}".encode() * (index + 1))
    # Values on three or more inodes become shared; enough of them push the
    # shared xattr area beyond block 0.
    for index in range(60):
        for copy in range(3):
            path = xattrs / f"shared-{index}-{copy}"
            path.write_text("s")
            os.setxattr(path, f"user.pair{index % 7}", f"shared value {index} ".encode() * 6)
    big_value = xattrs / "big-value"
    big_value.write_text("y")
    os.setxattr(big_value, "user.big", rng.randbytes(3000))
    layer = root / "layer"
    layer.mkdir()
    opaque = layer / "opaque"
    opaque.mkdir()
    # trusted.overlay.opaque needs root; the user namespace has the same layout.
    name = "trusted.overlay.opaque" if os.geteuid() == 0 else "user.overlay.opaque"
    os.setxattr(opaque, name, b"y")
    (opaque / "kept").write_text("kept\n")
    for index in range(12):
        os.mknod(layer / f"whiteout-{index}", 0o600 | stat.S_IFCHR, os.makedev(0, 0))
    os.mkfifo(layer / "fifo")
    with socket.socket(socket.AF_UNIX) as listener:
        listener.bind(str(layer / "socket"))
    data = root / "data"
    data.mkdir()
    # Compressible files of many logical-cluster counts, some with xattrs so
    # their compact indexes start at different 32-byte offsets.
    line = b"".join(b"line %d of compressible text\n" % index for index in range(70000))
    for index, size in enumerate((5000, 9000, 13000, 30000, 34000, 36000, 64000, 66000, 70000, 140000,
                                  400000, len(line))):
        path = data / f"text-{size}"
        path.write_bytes(line[:size])
        if index % 3:
            os.setxattr(path, "user.pad", b"p" * (4 * index + 1))
    (data / "random").write_bytes(rng.randbytes(300_000))
    (data / "random-tail").write_bytes(rng.randbytes(4096 * 3 + 1234))
    (data / "exact-block").write_bytes(rng.randbytes(8192))
    (data / "empty").touch()
    (root / "emptydir").mkdir()
    for excluded in WHOLE_IMAGE_EXCLUDED:
        (root / excluded).mkdir()
        (root / excluded / "excluded").write_text("must be excluded\n")


def scattered_tree(root: Path):
    """A 39 MiB image whose metadata layout 1 spreads over every chunk.

    200 directories of eight half-compressible files, each with a unique
    inline xattr. Without --MZ, mkfs.erofs 1.9 writes each directory's
    inodes into a new block beside the data written so far.
    """
    rng = random.Random(12)
    for index in range(1600):
        path = root / f"d{index // 8:03d}" / f"f{index % 8}"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"".join(rng.randbytes(64) + bytes(64) for _ in range(312)))
        os.setxattr(path, "user.unique", rng.randbytes(600))


def builder_image(view: Path, image: Path, *, compression="lz4", preserve_mtimes=False):
    """mkfs.erofs exactly as FreshEnvironmentBuilder runs it (layout 1 by default)."""
    FreshEnvironmentBuilder(None, None, None, image.parent, compression=compression)._mkfs(
        image, view, exclude_runtime_mounts=True, preserve_mtimes=preserve_mtimes)
    return image


def _ranges_blocks(ranges):
    return {block for start, end in ranges for block in range(start // BLOCK, (end - 1) // BLOCK + 1)}


def _subtract(ranges, holes):
    """Byte ranges minus sorted disjoint holes, by bisection."""
    merged = []
    for start, end in sorted(holes):
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    starts = [start for start, _ in merged]
    result = []
    for start, end in ranges:
        position = max(0, bisect.bisect_right(starts, start) - 1)
        cursor = start
        while cursor < end and position < len(merged):
            hole_start, hole_end = merged[position]
            if hole_end <= cursor:
                position += 1
                continue
            if hole_start >= end:
                break
            if hole_start > cursor:
                result.append((cursor, hole_start))
            cursor = max(cursor, hole_end)
            position += 1
        if cursor < end:
            result.append((cursor, end))
    return result


def _walk_inodes(image):
    inodes = []
    metadata = metadata_ranges(image, on_inode=lambda *item: inodes.append(item))
    return metadata, inodes


def _traced(command, image, log, environment=None):
    """(image byte ranges read by ``command`` and its children, stdout), via strace."""
    stdout = subprocess.run([STRACE, "-f", "-qq", "-s", "0", "-e",
                             "trace=openat,close,pread64,read,mmap,preadv,preadv2", "-o", str(log), *command],
                            check=True, capture_output=True, text=True, env=environment).stdout
    open_fds, reads, opened = set(), [], False
    for line in log.read_text().splitlines():
        if match := _OPEN.match(line):
            key = (match.group(1), match.group(3))
            open_fds.discard(key)
            if match.group(2) == str(image):
                open_fds.add(key)
                opened = True
        elif match := _CLOSE.match(line):
            open_fds.discard((match.group(1), match.group(2)))
        elif (match := _PREAD.match(line)) and (match.group(1), match.group(2)) in open_fds:
            if int(match.group(5)):
                reads.append((int(match.group(4)), int(match.group(4)) + int(match.group(5))))
        elif (match := _UNTRACED.match(line)) and (match.group(1), match.group(2)) in open_fds:
            raise AssertionError("image access the oracle cannot attribute: " + line)
    if not opened:
        raise AssertionError("traced tool never opened the image")
    return reads, stdout


def _dump_all(image, nids, *, trace_log=None):
    """dump.erofs --nid -e for each nid in one shell; optionally traced."""
    script = 'for nid in "$@"; do echo "@@nid $nid"; "$DUMP" --nid=$nid -e "$IMAGE" || echo "@@failed"; done'
    command = ["sh", "-c", script, "dump", *map(str, nids)]
    environment = os.environ | {"DUMP": DUMP, "IMAGE": str(image)}
    if trace_log is not None:
        reads, output = _traced(command, image, trace_log, environment)
    else:
        reads = []
        output = subprocess.run(command, check=True, capture_output=True, text=True, env=environment).stdout
    sections = {}
    for section in output.split("@@nid ")[1:]:
        nid, _, body = section.partition("\n")
        sections[int(nid)] = body
    return sections, reads


def _data_extents(sections, inodes):
    extents = []
    for nid, kind, *_ in inodes:
        if kind == stat.S_IFREG:
            extents += [(int(a), int(b)) for a, b in _EXTENT.findall(sections[nid])]
    return extents


class ReferenceReader:
    """Test-only EROFS reader for path walks, lstat, readlink and xattrs.

    Deliberately written apart from the walker; it is checked against the
    source tree before it is trusted on an overwritten image.
    """

    def __init__(self, data):
        self.data = data
        (magic,) = struct.unpack_from("<I", data, 1024)
        assert magic == 0xE0F5E1E2
        self.block = 1 << data[1024 + 12]
        self.root = struct.unpack_from("<H", data, 1024 + 14)[0]
        self.meta, self.xattr = (value * self.block for value in struct.unpack_from("<II", data, 1024 + 40))

    def inode(self, nid):
        at = self.meta + 32 * nid
        i_format, xattr_count, mode = struct.unpack_from("<HHH", self.data, at)
        if i_format & 1:
            size, i_u = struct.unpack_from("<QI", self.data, at + 8)
            uid, gid = struct.unpack_from("<II", self.data, at + 24)
            nlink = struct.unpack_from("<I", self.data, at + 44)[0]
            isize = 64
        else:
            nlink, size, _mtime, i_u = struct.unpack_from("<HIII", self.data, at + 6)
            uid, gid = struct.unpack_from("<HH", self.data, at + 24)
            isize = 32
        xsize = 12 + 4 * (xattr_count - 1) if xattr_count else 0
        return {"mode": mode, "size": size, "nlink": nlink, "uid": uid, "gid": gid, "i_u": i_u,
                "layout": (i_format >> 1) & 7, "xattrs_at": at + isize, "xattr_size": xsize,
                "inline_at": at + isize + xsize}

    def contents(self, inode):
        size, block = inode["size"], self.block
        if inode["layout"] == 0:
            return self.data[inode["i_u"] * block:inode["i_u"] * block + size]
        full = (size - 1) // block if size else 0
        head = self.data[inode["i_u"] * block:(inode["i_u"] + full) * block] if full else b""
        return head + self.data[inode["inline_at"]:inode["inline_at"] + size - full * block]

    def entries(self, inode):
        raw, result = self.contents(inode), {}
        for offset in range(0, len(raw), self.block):
            piece = raw[offset:offset + self.block]
            count = struct.unpack_from("<H", piece, 8)[0] // 12
            items = [struct.unpack_from("<QH", piece, 12 * index) for index in range(count)]
            for index, (nid, name_at) in enumerate(items):
                end = items[index + 1][1] if index + 1 < count else len(piece)
                result[piece[name_at:end].split(b"\0", 1)[0].decode()] = nid
        return result

    def _entry(self, at):
        name_len, index, value_size = struct.unpack_from("<BBH", self.data, at)
        name = _PREFIXES[index] + self.data[at + 4:at + 4 + name_len].decode()
        value = self.data[at + 4 + name_len:at + 4 + name_len + value_size]
        return name, value, (4 + name_len + value_size + 3) // 4 * 4

    def xattrs(self, inode):
        if not inode["xattr_size"]:
            return {}
        at, result = inode["xattrs_at"], {}
        shared = self.data[at + 4]
        for index in range(shared):
            (xattr_id,) = struct.unpack_from("<I", self.data, at + 12 + 4 * index)
            name, value, _ = self._entry(self.xattr + 4 * xattr_id)
            result[name] = value
        cursor, end = at + 12 + 4 * shared, at + inode["xattr_size"]
        while cursor < end:
            name, value, length = self._entry(cursor)
            result[name] = value
            cursor += length
        return result

    def tree(self):
        """{relative path: (nid, inode, symlink target, xattrs, child names)}."""
        result, pending = {}, [("", self.root)]
        while pending:
            path, nid = pending.pop()
            inode = self.inode(nid)
            kind = stat.S_IFMT(inode["mode"])
            names = target = None
            if kind == stat.S_IFDIR:
                children = {name: child for name, child in self.entries(inode).items() if name not in (".", "..")}
                names = sorted(children)
                pending += [(f"{path}/{name}".lstrip("/"), child) for name, child in children.items()]
            elif kind == stat.S_IFLNK:
                target = self.contents(inode).decode()
            result[path] = (nid, inode, target, self.xattrs(inode), names)
        return result


def _source_paths(view):
    """Every path of the published view, without following symlinks."""
    paths = [view]
    for directory, names, files in os.walk(view):
        if Path(directory) == view:
            names[:] = [name for name in names if name not in WHOLE_IMAGE_EXCLUDED]
            files = [name for name in files if name not in WHOLE_IMAGE_EXCLUDED]
        paths += [Path(directory) / name for name in names + files]
    return paths


def _xattrs(path):
    return {name: os.getxattr(path, name, follow_symlinks=False)
            for name in os.listxattr(path, follow_symlinks=False)}


def _without_mkfs_xattrs(source, xattrs):
    """Image xattrs minus mkfs.erofs's own. erofs-utils 1.9 (not 1.4) gives
    each directory directly holding a whiteout an empty trusted.overlay.origin,
    so overlayfs filters those whiteouts even where the directory is unmerged."""
    holds_whiteout = stat.S_ISDIR(source.lstat().st_mode) and any(
        stat.S_ISCHR(info.st_mode) and info.st_rdev == 0 for info in map(os.lstat, source.iterdir()))
    name = "trusted.overlay.origin"
    if holds_whiteout and xattrs.get(name) == b"" and name not in _xattrs(source):
        return {key: value for key, value in xattrs.items() if key != name}
    return xattrs


@unittest.skipUnless(MKFS, "mkfs.erofs (erofs-utils) is not installed")
class ErofsMetadataTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = TemporaryDirectory()
        cls.root = Path(cls.directory.name)
        cls.view = cls.root / "view"
        try:
            build_tree(cls.view)
        except PermissionError as exc:  # Unprivileged whiteout mknod needs Linux >= 5.8.
            cls.directory.cleanup()
            raise unittest.SkipTest(f"fixture needs whiteout devices: {exc}")
        cls.image = builder_image(cls.view, cls.root / "builder.erofs")
        cls.metadata, cls.inodes = _walk_inodes(cls.image)

    @classmethod
    def tearDownClass(cls):
        cls.directory.cleanup()

    def variant(self, name, *options):
        image = self.root / f"{name}.erofs"
        if not image.exists():
            subprocess.run([MKFS, "-T", "0", "-U", "00000000-0000-0000-0000-000000000000", *options,
                            "--exclude-regex=^(" + "|".join(sorted(WHOLE_IMAGE_EXCLUDED)) + ")$",
                            str(image), str(self.view)], check=True, capture_output=True)
        return image

    def newer_variants(self):
        """The builder's layout 2: extended inodes for kept mtimes, and inodes
        and directories in a metadata zone. Only erofs-utils 1.9+ builds it; a
        skipped subtest would report the whole proof skipped, so older mkfs
        leaves it out and the layout-2 prefetch and mtime tests report the skip."""
        if not LAYOUT_TWO:
            return ()
        image = self.root / "layout-2.erofs"
        if not image.exists():
            # mkfs stages the zone in $TMPDIR; the builder points it at the image's directory.
            with patch.dict(os.environ, TMPDIR=str(self.root / "missing")):
                builder_image(self.view, image, preserve_mtimes=True)
        return (("layout-2", image),)

    def test_symlink_targets_are_byte_exact_by_path(self):
        # Rollback's source of truth: nydus-image unpack's tar drops '.' components.
        tree = self.root / "links"
        if not tree.exists():
            (tree / "share/terminfo/31").mkdir(parents=True)
            for index in range(300):  # A directory over one block.
                (tree / "share/terminfo/31" / f"entry-{index:04d}-{'x' * 20}").symlink_to(f".././61/adm{index}")
            (tree / "dot").symlink_to("./a/./b/../c")
            (tree / "long").symlink_to("y" * 4000)
            os.link(tree / "dot", tree / "dot-again", follow_symlinks=False)
        for view, image in ((tree, self.root / "links.erofs"), (self.view, self.image)):
            if not image.exists():
                subprocess.run([MKFS, "-T", "0", str(image), str(view)], check=True, capture_output=True)
            expected = {os.path.relpath(os.path.join(directory, name), view): os.readlink(os.path.join(directory, name))
                        for directory, names, files in os.walk(view) for name in names + files
                        if os.path.islink(os.path.join(directory, name))}
            with self.subTest(image=image.name):
                self.assertEqual(symlink_targets(image.read_bytes()), expected)

    def test_walk_reaches_every_inode_and_records_never_overlap(self):
        expected = {(info.st_dev, info.st_ino) for info in map(os.lstat, _source_paths(self.view))}
        self.assertEqual(self.metadata.inodes, len(expected))
        self.assertEqual(self.metadata.symlinks, 3)
        kinds = {(kind, layout) for _, kind, layout, *_ in self.inodes}
        for wanted in ((stat.S_IFDIR, 2), (stat.S_IFREG, 3), (stat.S_IFLNK, 2), (stat.S_IFCHR, 0),
                       (stat.S_IFIFO, 0), (stat.S_IFSOCK, 0), (stat.S_IFREG, 0), (stat.S_IFREG, 2)):
            self.assertIn(wanted, kinds)
        # Over-estimating an index array would run into the next record.
        records = sorted((start, end) for *_, start, end in self.inodes)
        for (_, end), (start, _) in zip(records, records[1:]):
            self.assertLessEqual(end, start)
        self.assertEqual(self.metadata.ranges[0][0], 0)
        self.assertTrue(all(a[1] < b[0] for a, b in zip(self.metadata.ranges, self.metadata.ranges[1:])))
        per_chunk = self.metadata.chunk_bytes(256 * 1024)
        self.assertEqual(sum(size for _, size in per_chunk), self.metadata.metadata_bytes)
        self.assertEqual(per_chunk[0][0], 0)

    @unittest.skipUnless(DUMP and FSCK and STRACE, "dump.erofs, fsck.erofs and strace are required")
    def test_every_traced_non_data_block_is_in_the_metadata_ranges(self):
        for name, image in (("builder", self.image), ("uncompressed", self.variant("plain")),
                            ("extended-inodes", self.variant("extended", "-zlz4", "--force-uid=70000")),
                            ("legacy-indexes", self.variant("legacy", "-zlz4", "-Elegacy-compress")),
                            ("big-pcluster", self.variant("big", "-zlz4", "-C16384")),
                            ("chunked", self.variant("chunked", "--chunksize=65536")), *self.newer_variants()):
            with self.subTest(image=name):
                metadata, inodes = (self.metadata, self.inodes) if image == self.image else _walk_inodes(image)
                # Trace dump.erofs for every inode of the builder image; for
                # variants, every directory, symlink and indexed file plus a
                # sample of the rest keeps the test fast.
                traced = [nid for index, (nid, kind, layout, *_) in enumerate(inodes)
                          if image == self.image or kind in (stat.S_IFDIR, stat.S_IFLNK)
                          or layout not in (0, 2) or not index % 8]
                _, dump_reads = _dump_all(image, traced, trace_log=self.root / f"{name}.dump.trace")
                sections, _ = _dump_all(image, [nid for nid, *_ in inodes])
                self.assertFalse([nid for nid, body in sections.items() if "@@failed" in body])
                reads, _ = _traced([FSCK, "--extract", str(image)], image, self.root / f"{name}.fsck.trace")
                self.assertGreater(len(reads), metadata.inodes)
                residual = _subtract(reads + dump_reads, _data_extents(sections, inodes))
                self.assertEqual(sorted(_ranges_blocks(residual) - _ranges_blocks(metadata.ranges)), [])

    @unittest.skipUnless(DUMP and FSCK, "dump.erofs and fsck.erofs are required")
    def test_overwriting_everything_outside_metadata_and_data_changes_nothing_visible(self):
        for name, image in (("builder", self.image),
                            ("extended-inodes", self.variant("extended", "-zlz4", "--force-uid=70000")),
                            ("legacy-indexes", self.variant("legacy", "-zlz4", "-Elegacy-compress")),
                            ("big-pcluster", self.variant("big", "-zlz4", "-C16384")),
                            ("chunked", self.variant("chunked", "--chunksize=65536")), *self.newer_variants()):
            with self.subTest(image=name):
                metadata, inodes = (self.metadata, self.inodes) if image == self.image else _walk_inodes(image)
                sections, _ = _dump_all(image, [nid for nid, *_ in inodes])
                original = image.read_bytes()
                overwritten = bytearray(b"\x5a" * len(original))
                for start, end in [*metadata.ranges, *_data_extents(sections, inodes)]:
                    overwritten[start:end] = original[start:end]
                self.assertNotEqual(bytes(overwritten), original)
                garbage = self.root / f"{name}.overwritten.erofs"
                garbage.write_bytes(overwritten)
                subprocess.run([FSCK, "--extract", str(garbage)], check=True, capture_output=True)
                # Variants compare every directory, symlink and indexed file.
                nids = [nid for nid, kind, layout, *_ in inodes if image == self.image
                        or kind in (stat.S_IFDIR, stat.S_IFLNK) or layout not in (0, 2)]
                after, _ = _dump_all(garbage, nids)
                self.assertEqual(after, {nid: sections[nid] for nid in nids})
                # The walker sees the same metadata on the overwritten bytes.
                self.assertEqual(walk(bytes(overwritten)).ranges, metadata.ranges)

    def test_paths_lstat_readlink_and_xattrs_survive_overwriting_non_metadata(self):
        for name, image in (("builder", self.image), *self.newer_variants()):
            with self.subTest(image=name):
                self.check_reference_tree(image, self.metadata if image == self.image else walk(image.read_bytes()))

    def check_reference_tree(self, image, metadata):
        original = image.read_bytes()
        reference = ReferenceReader(original).tree()
        regular = {path for path, (_, inode, *_) in reference.items() if stat.S_ISREG(inode["mode"])}
        overwritten = bytearray(b"\x5a" * len(original))
        for start, end in metadata.ranges:
            overwritten[start:end] = original[start:end]
        tree = ReferenceReader(bytes(overwritten)).tree()
        expected = {}
        for source in _source_paths(self.view):
            expected[source.relative_to(self.view).as_posix() if source != self.view else ""] = source
        self.assertEqual(set(tree), set(expected))
        hardlinks = {}
        for relative, source in expected.items():
            with self.subTest(path=relative):
                nid, inode, target, xattrs, names = tree[relative]
                self.assertEqual(reference[relative][1:], tree[relative][1:])
                info = source.lstat()
                self.assertEqual(inode["mode"], info.st_mode)
                self.assertEqual((inode["uid"], inode["gid"]), (info.st_uid, info.st_gid))
                self.assertEqual(_without_mkfs_xattrs(source, xattrs), _xattrs(source))
                if stat.S_ISDIR(info.st_mode):
                    self.assertEqual(names, sorted(name for name in os.listdir(source)
                                                   if relative or name not in WHOLE_IMAGE_EXCLUDED))
                else:
                    self.assertEqual(inode["nlink"], info.st_nlink)
                if stat.S_ISLNK(info.st_mode):
                    self.assertEqual(target, os.readlink(source))
                if stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode):
                    self.assertEqual(inode["size"], info.st_size)
                if stat.S_ISCHR(info.st_mode):
                    self.assertEqual(inode["i_u"], 0)  # Whiteout rdev 0:0.
                hardlinks.setdefault(info.st_ino, set()).add(nid)
        self.assertTrue(regular)
        self.assertTrue(all(len(nids) == 1 for nids in hardlinks.values()))
        self.assertIn(5, [inode["nlink"] for _, inode, *_ in tree.values()])

    def test_unqualified_features_and_layouts_are_refused_not_guessed(self):
        original = self.image.read_bytes()
        superblock = 1024

        def changed(offset, layout, value):
            data = bytearray(original)
            struct.pack_into(layout, data, offset, value)
            return bytes(data)

        incompat = struct.unpack_from("<I", original, superblock + 80)[0]
        refused = {
            "not erofs": changed(superblock, "<I", 0x12345678),
            "ztailpacking": changed(superblock + 80, "<I", incompat | 0x10),
            "fragments": changed(superblock + 80, "<I", incompat | 0x20),
            "xattr prefixes": changed(superblock + 80, "<I", incompat | 0x40),
            "48bit": changed(superblock + 80, "<I", incompat | 0x80),
            "metabox": changed(superblock + 80, "<I", incompat | 0x100),
            "device table": changed(superblock + 80, "<I", incompat | 0x8),
            "unknown compat": changed(superblock + 8, "<I", 0x80),
            "block size": changed(superblock + 12, "<B", 13),
            "extra devices": changed(superblock + 86, "<H", 1),
            "packed inode": changed(superblock + 96, "<Q", 5),
        }
        root_nid = struct.unpack_from("<H", original, superblock + 14)[0]
        root = 32 * root_nid
        i_format = struct.unpack_from("<H", original, root)[0]
        refused["unknown inode format bit"] = changed(root, "<H", i_format | 0x20)
        chunked = bytearray(changed(root, "<H", (i_format & 1) | 4 << 1))
        struct.pack_into("<I", chunked, superblock + 80, incompat | 0x4)
        refused["chunk-based directory"] = bytes(chunked)
        refused["compressed directory"] = changed(root, "<H", (i_format & 1) | 3 << 1)
        for name, data in refused.items():
            with self.subTest(name=name), self.assertRaises(UnsupportedErofs):
                walk(data)
        inode = ReferenceReader(original).inode(root_nid)
        self.assertEqual((inode["layout"], inode["size"] < BLOCK), (2, True))
        # Entries sort "." and ".." first; the third names a real child.
        for name, data in {"truncated": original[:8192], "empty": b"\0" * 4096,
                           "inode count": changed(superblock + 16, "<Q", self.metadata.inodes + 1),
                           "dirent outside image": changed(inode["inline_at"] + 24, "<Q", 1 << 40)}.items():
            with self.subTest(name=name), self.assertRaises(ValueError):
                walk(data)

    def test_deadline_check_runs_during_large_walks(self):
        calls = []
        metadata_ranges(self.image, check=lambda: calls.append(1))
        self.assertTrue(calls)
        with self.assertRaisesRegex(RuntimeError, "deadline"):
            metadata_ranges(self.image, check=lambda: (_ for _ in ()).throw(RuntimeError("deadline")))


if __name__ == "__main__":
    unittest.main()
