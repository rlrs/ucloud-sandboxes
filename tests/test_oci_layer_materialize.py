"""Integrity and filesystem semantics for the bounded OCI layer fast path."""
from contextlib import ExitStack
import errno
import gzip
import hashlib
import io
import os
from pathlib import Path, PurePosixPath
import stat
import sys
import tarfile
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from ucloud_sandboxes import oci_layer_materialize as materialize


OCI_TAR = "application/vnd.oci.image.layer.v1.tar"
OCI_GZIP = OCI_TAR + "+gzip"
REPOSITORY = "qualification/image"


def sha256(data):
    return "sha256:" + hashlib.sha256(data).hexdigest()


def member(name, content=b"", *, kind=tarfile.REGTYPE, mode=0o644,
           uid=None, gid=None, linkname="", pax=None):
    info = tarfile.TarInfo(name)
    info.type = kind
    info.mode = mode
    info.uid = os.getuid() if uid is None else uid
    info.gid = os.getgid() if gid is None else gid
    info.mtime = 123456789
    info.linkname = linkname
    info.pax_headers = {} if pax is None else dict(pax)
    info.size = len(content) if kind == tarfile.REGTYPE else 0
    return info, content


def directory(name, mode=0o755, **kwargs):
    return member(name, kind=tarfile.DIRTYPE, mode=mode, **kwargs)


def layer(entries, *, compressed=True, tar_format=tarfile.PAX_FORMAT):
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w", format=tar_format) as archive:
        for info, content in entries:
            archive.addfile(info, io.BytesIO(content) if info.isreg() else None)
    raw = buffer.getvalue()
    blob = gzip.compress(raw, mtime=0) if compressed else raw
    return {"mediaType": OCI_GZIP if compressed else OCI_TAR,
            "digest": sha256(blob), "size": len(blob)}, sha256(raw), blob


class MemoryRegistry:
    def __init__(self, layers):
        self.blobs = {descriptor["digest"]: blob for descriptor, _, blob in layers}
        self.calls = []
        self.streams = []

    def open_blob(self, repository, digest):
        self.calls.append((repository, digest))
        stream = io.BytesIO(self.blobs[digest])
        self.streams.append(stream)
        return stream


class MaterializeLayersTests(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.extract_count = 0
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        # Production extraction runs as root; do not require privileged tests.
        self.chown = self.stack.enter_context(patch.object(materialize.os, "chown"))

    def extract(self, layers, *, destination=None, client=None, **kwargs):
        client = MemoryRegistry(layers) if client is None else client
        self.extract_count += 1
        destination = self.root / f"materialized-{self.extract_count}" if destination is None else destination
        paths = materialize.materialize_layers(
            client, REPOSITORY, [value[0] for value in layers],
            [value[1] for value in layers], destination, **kwargs,
        )
        self.assertTrue(all(stream.closed for stream in client.streams))
        return paths, client

    def reject(self, entries, *, exception=None, **kwargs):
        value = layer(entries, **kwargs)
        client = MemoryRegistry([value])
        with self.assertRaises(exception or materialize.UnsupportedLayer):
            self.extract([value], client=client)
        self.assertTrue(all(stream.closed for stream in client.streams))

    def test_gzip_preserves_contents_modes_symlinks_and_hardlinks(self):
        value = layer([
            directory("app", mode=0o750),
            member("app/main", b"#!/bin/sh\necho ready\n", mode=0o751),
            member("app/hard", kind=tarfile.LNKTYPE, mode=0o751, linkname="app/main"),
            member("app/relative", kind=tarfile.SYMTYPE, mode=0o777, linkname="main"),
        ])
        (output,), client = self.extract([value])
        self.assertEqual(client.calls, [(REPOSITORY, value[0]["digest"])])
        self.assertEqual((output / "app/main").read_bytes(), b"#!/bin/sh\necho ready\n")
        self.assertEqual(stat.S_IMODE((output / "app").stat().st_mode), 0o750)
        self.assertEqual(stat.S_IMODE((output / "app/main").stat().st_mode), 0o751)
        self.assertEqual((output / "app/main").stat().st_ino, (output / "app/hard").stat().st_ino)
        self.assertEqual(os.readlink(output / "app/relative"), "main")
        self.assertEqual(stat.S_IMODE(output.stat().st_mode), 0o755)
        self.assertTrue(any(call.args[1:3] == (0, 0) for call in self.chown.call_args_list))

    def test_uncompressed_tar_and_optional_explicit_root(self):
        value = layer([directory(".", uid=0, gid=0), member("answer", b"42")], compressed=False)
        (output,), _ = self.extract([value])
        self.assertEqual((output / "answer").read_bytes(), b"42")
        self.assertEqual(stat.S_IMODE(output.stat().st_mode), 0o755)

    def test_explicit_parents_and_hardlink_targets_can_follow_their_users(self):
        value = layer([
            member("app/hard", kind=tarfile.LNKTYPE, linkname="app/main"),
            member("app/main", b"body"),
            directory("app"),
        ])
        (output,), _ = self.extract([value])
        self.assertEqual((output / "app/hard").read_bytes(), b"body")
        self.assertEqual((output / "app/hard").stat().st_ino, (output / "app/main").stat().st_ino)

    def test_absolute_and_dangling_symlink_targets_are_preserved_as_inert_text(self):
        value = layer([
            member("absolute", kind=tarfile.SYMTYPE, mode=0o777, linkname="/missing/container/path"),
            member("relative", kind=tarfile.SYMTYPE, mode=0o777, linkname="../missing"),
        ])
        (output,), _ = self.extract([value])
        self.assertEqual(os.readlink(output / "absolute"), "/missing/container/path")
        self.assertEqual(os.readlink(output / "relative"), "../missing")

    def test_recognized_pax_path_and_timestamp_are_applied(self):
        value = layer([member("header-name", b"body", pax={"path": "effective-name", "mtime": "123456789.25"})])
        (output,), _ = self.extract([value])
        self.assertFalse((output / "header-name").exists())
        self.assertEqual((output / "effective-name").read_bytes(), b"body")
        self.assertAlmostEqual((output / "effective-name").stat().st_mtime, 123456789.25, places=5)

    def test_empty_tar_produces_empty_diff_directory(self):
        (output,), _ = self.extract([layer([])])
        self.assertEqual(list(output.iterdir()), [])
        self.assertEqual(stat.S_IMODE(output.stat().st_mode), 0o755)

    def test_no_selected_layers_does_not_read_registry(self):
        client = MemoryRegistry([])
        with self.assertRaises(materialize.UnsupportedLayer):
            self.extract([], client=client)
        self.assertEqual(client.calls, [])

    def test_each_layer_has_an_independent_diff_directory(self):
        values = [layer([member("same", b"lower")]), layer([member("same", b"upper")])]
        outputs, _ = self.extract(values)
        self.assertEqual(len(set(outputs)), 2)
        self.assertEqual([(path / "same").read_bytes() for path in outputs], [b"lower", b"upper"])

    def test_directory_metadata_is_applied_after_creating_children(self):
        value = layer([directory("readonly", mode=0o500), member("readonly/child", b"ok", mode=0o400)])
        (output,), _ = self.extract([value])
        parent = output / "readonly"
        self.addCleanup(parent.chmod, 0o700)
        self.assertEqual((parent / "child").read_bytes(), b"ok")
        self.assertEqual(stat.S_IMODE(parent.stat().st_mode), 0o500)

    def test_member_ownership_is_requested_without_following_symlinks(self):
        value = layer([
            member("owned", b"body", uid=123, gid=456),
            member("link", kind=tarfile.SYMTYPE, mode=0o777, uid=321, gid=654, linkname="owned"),
        ])
        self.extract([value])
        calls = self.chown.call_args_list
        self.assertTrue(any(call.args[1:3] == (123, 456) for call in calls))
        self.assertTrue(any(call.args[1:3] == (321, 654)
                            and call.kwargs.get("follow_symlinks") is False for call in calls))

    def test_compressed_digest_mismatch_is_corruption_before_payload_extraction(self):
        descriptor, diff_id, blob = layer([member("payload", b"do not extract")])
        changed = dict(descriptor, digest=sha256(b"wrong blob"))
        client = MemoryRegistry([(changed, diff_id, blob)])
        with self.assertRaises(ValueError) as raised:
            self.extract([(changed, diff_id, blob)], client=client)
        self.assertNotIsInstance(raised.exception, materialize.UnsupportedLayer)
        self.assertEqual(list(self.root.rglob("payload")), [])
        self.assertTrue(all(stream.closed for stream in client.streams))

    def test_uncompressed_digest_mismatch_is_corruption_before_payload_extraction(self):
        descriptor, _, blob = layer([member("payload", b"do not extract")])
        value = descriptor, sha256(b"wrong diff"), blob
        client = MemoryRegistry([value])
        with self.assertRaises(ValueError) as raised:
            self.extract([value], client=client)
        self.assertNotIsInstance(raised.exception, materialize.UnsupportedLayer)
        self.assertEqual(list(self.root.rglob("payload")), [])
        self.assertTrue(all(stream.closed for stream in client.streams))

    def test_actual_blob_size_must_equal_descriptor_size(self):
        descriptor, diff_id, blob = layer([member("payload", b"body")])
        for delta in (-1, 1):
            with self.subTest(delta=delta):
                value = dict(descriptor, size=len(blob) + delta), diff_id, blob
                client = MemoryRegistry([value])
                with self.assertRaises(ValueError) as raised:
                    self.extract([value], destination=self.root / str(delta), client=client)
                self.assertNotIsInstance(raised.exception, materialize.UnsupportedLayer)
                self.assertTrue(all(stream.closed for stream in client.streams))
        self.assertEqual(list(self.root.rglob("payload")), [])

    def test_truncated_compressed_stream_is_rejected_and_closed(self):
        descriptor, diff_id, blob = layer([member("payload", b"body")])
        value = descriptor, diff_id, blob[:-8]
        client = MemoryRegistry([value])
        with self.assertRaises(ValueError):
            self.extract([value], client=client)
        self.assertEqual(list(self.root.rglob("payload")), [])
        self.assertTrue(all(stream.closed for stream in client.streams))

    def test_invalid_gzip_with_matching_blob_identity_still_cannot_extract(self):
        descriptor, diff_id, original = layer([member("payload", b"body")])
        blob = original[:-8]
        value = dict(descriptor, digest=sha256(blob), size=len(blob)), diff_id, blob
        client = MemoryRegistry([value])
        with self.assertRaises((ValueError, OSError, EOFError, tarfile.TarError)):
            self.extract([value], client=client)
        self.assertEqual(list(self.root.rglob("payload")), [])
        self.assertTrue(all(stream.closed for stream in client.streams))

    def test_invalid_tar_with_matching_digests_still_cannot_extract(self):
        raw = b"this is not a tar archive"
        blob = gzip.compress(raw, mtime=0)
        value = {"mediaType": OCI_GZIP, "digest": sha256(blob), "size": len(blob)}, sha256(raw), blob
        client = MemoryRegistry([value])
        with self.assertRaises((ValueError, OSError, tarfile.TarError)):
            self.extract([value], client=client)
        self.assertTrue(all(stream.closed for stream in client.streams))

    def test_fragmented_registry_reads_produce_the_same_verified_payload(self):
        class ShortReads(io.BytesIO):
            def read(self, size=-1):
                return super().read(min(size, 7) if size >= 0 else 7)

        value = layer([member("payload", b"body")])
        client = MemoryRegistry([value])
        response = ShortReads(value[2])
        client.streams.append(response)
        with patch.object(client, "open_blob", return_value=response):
            (output,), _ = self.extract([value], client=client)
        self.assertEqual((output / "payload").read_bytes(), b"body")

    def test_declared_compressed_budget_is_aggregate_and_checked_before_download(self):
        values = [layer([member("one", b"a")]), layer([member("two", b"b")])]
        client = MemoryRegistry(values)
        total = sum(value[0]["size"] for value in values)
        with self.assertRaises(materialize.UnsupportedLayer):
            self.extract(values, client=client, max_compressed_bytes=total - 1)
        self.assertEqual(client.calls, [])

    def test_unpacked_tar_budget_prevents_high_compression_expansion(self):
        value = layer([member("payload", b"z" * 100_000)])
        self.assertLess(value[0]["size"], 4096)
        client = MemoryRegistry([value])
        with self.assertRaises(materialize.UnsupportedLayer):
            self.extract([value], client=client, max_unpacked_bytes=4096)
        self.assertEqual(list(self.root.rglob("payload")), [])
        self.assertTrue(all(stream.closed for stream in client.streams))

    def test_unpacked_budget_is_aggregate_across_layers(self):
        values = [layer([member("one", b"a")]), layer([member("two", b"b")])]
        # Each small tar is exactly one 10-KiB record; their sum exceeds this.
        with self.assertRaises(materialize.UnsupportedLayer):
            self.extract(values, max_unpacked_bytes=15 * 1024)

    def test_unsupported_media_type_falls_back_before_download(self):
        descriptor, diff_id, blob = layer([])
        value = dict(descriptor, mediaType="application/vnd.oci.image.layer.v1.tar+zstd"), diff_id, blob
        client = MemoryRegistry([value])
        with self.assertRaises(materialize.UnsupportedLayer):
            self.extract([value], client=client)
        self.assertEqual(client.calls, [])

    def test_descriptor_and_diff_id_cardinality_must_match(self):
        value = layer([])
        client = MemoryRegistry([value])
        with self.assertRaises(ValueError):
            materialize.materialize_layers(client, REPOSITORY, [value[0]], [], self.root / "diffs")
        self.assertEqual(client.calls, [])

    def test_archive_member_paths_cannot_escape_the_diff_root(self):
        for name in ("../escape", "/escape", "safe/../../escape"):
            with self.subTest(name=name):
                self.reject([member(name, b"bad")])
        self.assertFalse((self.root / "escape").exists())

    def test_parent_directory_must_be_explicit_in_the_same_tar(self):
        self.reject([member("missing/child", b"body")])
        lower = layer([directory("present")])
        upper = layer([member("present/child", b"body")])
        with self.assertRaises(materialize.UnsupportedLayer):
            self.extract([lower, upper], destination=self.root / "cross-layer-parent")

    def test_regular_file_or_symlink_cannot_be_a_parent(self):
        for first in (member("parent", b"body"),
                      member("parent", kind=tarfile.SYMTYPE, mode=0o777, linkname="elsewhere")):
            with self.subTest(kind=first[0].type):
                self.reject([first, member("parent/child", b"bad")])

    def test_symlink_traversal_cannot_write_outside_destination(self):
        outside = self.root / "outside"
        outside.mkdir()
        sentinel = outside / "sentinel"
        sentinel.write_bytes(b"original")
        self.reject([
            member("redirect", kind=tarfile.SYMTYPE, mode=0o777, linkname=str(outside)),
            member("redirect/sentinel", b"changed"),
        ])
        self.assertEqual(sentinel.read_bytes(), b"original")

    def test_duplicate_canonical_paths_and_root_entries_are_rejected(self):
        for entries in (
            [member("same", b"first"), member("same", b"second")],
            [member("same", b"first"), member("./same", b"second")],
            [directory("."), directory("./")],
            [directory("same"), member("same", b"second")],
        ):
            with self.subTest(names=[entry[0].name for entry in entries]):
                self.reject(entries)

    def test_root_entry_must_be_a_directory(self):
        for info in (member(".", b"bad"), member(".", kind=tarfile.SYMTYPE, linkname="elsewhere")):
            with self.subTest(kind=info[0].type):
                self.reject([info])

    def test_whiteouts_and_opaque_markers_require_full_materialization(self):
        for name in (".wh.removed", ".wh..wh..opq", "nested/.wh.removed"):
            with self.subTest(name=name):
                self.reject([directory("nested"), member(name)])

    def test_device_fifo_and_sparse_members_require_full_materialization(self):
        for kind in (tarfile.CHRTYPE, tarfile.BLKTYPE, tarfile.FIFOTYPE, tarfile.GNUTYPE_SPARSE):
            with self.subTest(kind=kind):
                self.reject([member("special", kind=kind)], tar_format=tarfile.GNU_FORMAT)

    def test_xattrs_and_unrecognized_pax_metadata_are_not_silently_discarded(self):
        for key in ("SCHILY.xattr.user.note", "LIBARCHIVE.xattr.user.note", "vendor.unknown"):
            with self.subTest(key=key):
                self.reject([member("file", b"body", pax={key: "value"})])

    def test_pax_cannot_override_a_safe_header_with_an_unsafe_path(self):
        self.reject([member("safe-name", b"body", pax={"path": "../escape"})])

    def test_hardlink_target_must_be_a_regular_member_in_this_tar(self):
        hard = member("alias", kind=tarfile.LNKTYPE, linkname="target")
        for prefix in ([], [directory("target", mode=0o644)],
                       [member("target", kind=tarfile.SYMTYPE, mode=0o644, linkname="elsewhere")],
                       [member("real", b"body"), member("target", kind=tarfile.LNKTYPE, linkname="real")]):
            with self.subTest(target=prefix):
                self.reject([*prefix, hard])
        with self.assertRaises(materialize.UnsupportedLayer):
            self.extract([layer([member("target", b"lower")]), layer([hard])],
                         destination=self.root / "cross-layer-link")

    def test_hardlink_cannot_disagree_with_target_inode_metadata(self):
        for changed in ({"mode": 0o600}, {"uid": os.getuid() + 1}, {"gid": os.getgid() + 1}):
            with self.subTest(changed=changed):
                self.reject([member("target", b"body"),
                             member("alias", kind=tarfile.LNKTYPE, linkname="target", **changed)])

    def test_hardlink_target_path_must_stay_inside_tar(self):
        for target in ("../outside", "/outside", "directory/../../outside"):
            with self.subTest(target=target):
                self.reject([member("alias", kind=tarfile.LNKTYPE, linkname=target)])

    def test_fast_path_normalization_matches_posix_paths_and_existing_rejections(self):
        names = ("", ".", "./", "/", "//", "///a", "a//./b/", "a/../b",
                 "../", "a/..", ".wh.deleted", "a/.wh..wh..opq", "é/☃",
                 "a\\..\\b", "a b/file", "a/././b", "x" * 4097, "a\0b")
        for name in names:
            for root in (True, False):
                with self.subTest(name=name[:50], root=root):
                    path = PurePosixPath(name)
                    rejected = (len(name) > 4096 or "\0" in name or path.is_absolute()
                                or ".." in path.parts or (not path.parts and not root)
                                or any(part.startswith(".wh.") for part in path.parts))
                    if rejected:
                        with self.assertRaises(materialize.UnsupportedLayer):
                            materialize._path(name, root=root)
                    else:
                        self.assertEqual(materialize._path(name, root=root), path.parts)

    @unittest.skipUnless(sys.platform.startswith("linux") and hasattr(os, "sendfile"),
                         "regular-file sendfile fast path requires Linux")
    def test_kernel_copy_is_bounded_to_each_regular_payload_and_preserves_metadata(self):
        large = bytes(range(256)) * (materialize.BLOCK // 128 + 1)
        value = layer([
            directory("app", mode=0o750), member("app/empty", b""),
            member("app/large", large, mode=0o4510), member("app/last", b"last-payload"),
            member("app/hard", kind=tarfile.LNKTYPE, mode=0o4510, linkname="app/large"),
        ])
        native = os.sendfile
        with patch.object(materialize.os, "sendfile", wraps=native) as copies, \
                patch.object(tarfile.TarFile, "extractfile", side_effect=AssertionError("unexpected Python copy")):
            (output,), _ = self.extract([value])
        self.assertEqual((output / "app/large").read_bytes(), large)
        self.assertEqual((output / "app/empty").read_bytes(), b"")
        self.assertEqual((output / "app/last").read_bytes(), b"last-payload")
        self.assertEqual((output / "app/large").stat().st_ino, (output / "app/hard").stat().st_ino)
        self.assertEqual(stat.S_IMODE((output / "app/large").stat().st_mode), 0o4510)
        self.assertEqual((output / "app/large").stat().st_mtime, 123456789)
        self.assertEqual(sum(call.args[3] for call in copies.call_args_list), len(large) + len(b"last-payload"))
        self.assertTrue(all(0 < call.args[3] <= materialize.BLOCK for call in copies.call_args_list))

    def test_kernel_copy_unavailable_or_declined_uses_portable_tar_reader(self):
        value = layer([member("first", b"first-payload"), member("second", b"second-payload")])
        for error in (None, errno.ENOSYS, errno.EXDEV, errno.EINVAL, errno.EOPNOTSUPP, errno.ENOTSOCK, 0):
            with self.subTest(error=error):
                if error is None:
                    replacement = None
                elif error == 0:
                    def replacement(*args):
                        return 0
                else:
                    def replacement(*args, _error=error):
                        raise OSError(_error, "unsupported native copy")
                with patch.object(materialize.os, "sendfile", replacement, create=True):
                    (output,), _ = self.extract([value])
                self.assertEqual((output / "first").read_bytes(), b"first-payload")
                self.assertEqual((output / "second").read_bytes(), b"second-payload")

    @unittest.skipUnless(sys.platform.startswith("linux") and hasattr(os, "sendfile"),
                         "regular-file sendfile fast path requires Linux")
    def test_short_kernel_copies_and_partial_copy_fallback_do_not_repeat_or_omit_bytes(self):
        value = layer([member("first", b"0123456789abcdef"), member("second", b"next")])
        native = os.sendfile
        for fallback in (False, True):
            calls = [0]
            def copy(output, source, offset, count):
                calls[0] += 1
                if fallback and calls[0] == 2:
                    raise OSError(errno.EINVAL, "declined after a partial copy")
                return native(output, source, offset, min(count, 3))
            with self.subTest(fallback=fallback), patch.object(materialize.os, "sendfile", side_effect=copy):
                (output,), _ = self.extract([value])
            self.assertEqual((output / "first").read_bytes(), b"0123456789abcdef")
            self.assertEqual((output / "second").read_bytes(), b"next")

    def test_real_kernel_copy_errors_are_not_silently_retried_as_portable_copy(self):
        value = layer([member("file", b"body")])
        for code in (errno.ENOSPC, errno.EIO):
            with self.subTest(code=code), patch.object(materialize.os, "sendfile", side_effect=OSError(code, "failed"), create=True):
                with self.assertRaises(OSError) as raised:
                    self.extract([value])
                self.assertEqual(raised.exception.errno, code)

    def test_truncated_regular_payload_is_rejected_when_native_copy_cannot_complete(self):
        # Both identities can authenticate a malformed/truncated tar; extraction
        # must still reject it instead of silently accepting a short output file.
        header, _ = member("truncated", b"1234567890")
        raw = header.tobuf(format=tarfile.USTAR_FORMAT) + b"123"
        value = {"mediaType": OCI_TAR, "digest": sha256(raw), "size": len(raw)}, sha256(raw), raw
        with patch.object(materialize.os, "sendfile", return_value=0, create=True):
            with self.assertRaises(tarfile.ReadError):
                self.extract([value])


if __name__ == "__main__":
    unittest.main()
