"""C3.1 residue policy and commit schemas (docs/rl-state-primitives.md §3.3, §10.1)."""
from dataclasses import replace
import hashlib
import io
import json
import random
import tarfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from ucloud_sandboxes import commit_policy
from ucloud_sandboxes.commit_policy import (
    CommitBuild, CommitExport, CommitExportRequest, CommitPolicy, CommitRefused, FilterResult, filter_upper,
    staging_repository,
)
from ucloud_sandboxes.commit_steps import commit_build, commit_policy as policy_for, export_step, secret_digests
from ucloud_sandboxes.direct_oci import DirectOciConfigBuilder
from ucloud_sandboxes.environment_prepare import MAX_REQUEST_BYTES

TOKEN = "0123456789abcdef0123456789abcdef"
DIGEST = "sha256:" + "a" * 64


def entry(name, data=None, *, kind=tarfile.REGTYPE, mode=0o644, uid=0, mtime=1_700_000_000, linkname="",
          xattrs=None, dev=(0, 0), pax=None):
    info = tarfile.TarInfo(name)
    info.type, info.mode, info.uid, info.gid, info.mtime, info.linkname = kind, mode, uid, uid, mtime, linkname
    info.devmajor, info.devminor = dev
    info.pax_headers = {**{f"SCHILY.xattr.{key}": value for key, value in (xattrs or {}).items()}, **(pax or {})}
    if data is not None:
        info.size = len(data)
    return info, data


def directory(name, **options):
    return entry(name, kind=tarfile.DIRTYPE, **{"mode": 0o755, **options})


def whiteout(name):
    return entry(name, kind=tarfile.CHRTYPE)


def upper(members, *, encoding="utf-8", format=tarfile.PAX_FORMAT):
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w", format=format, encoding=encoding) as archive:
        for info, data in members:
            archive.addfile(info, None if data is None else io.BytesIO(data))
    buffer.seek(0)
    return buffer


def run(members, policy=CommitPolicy(), secrets=(), **options):
    output = io.BytesIO()
    result = filter_upper(upper(members, **options), output, policy, secrets)
    output.seek(0)
    with tarfile.open(fileobj=output) as archive:
        listing = {member.name: member for member in archive}
        bodies = {name: archive.extractfile(member).read() for name, member in listing.items() if member.isreg()}
    return result, listing, bodies, output.getvalue()


def refused(test, code, members, policy=CommitPolicy(), secrets=(), **options):
    with test.assertRaises(CommitRefused) as caught:
        run(members, policy, secrets, **options)
    test.assertEqual(caught.exception.code, code)


ROOT = directory(".")


class ResidueRuleTests(unittest.TestCase):
    def test_mount_escapes_fail_but_bare_mount_points_drop(self):
        for name in ("./run/ucloud", "./run/ucloud/fork.json", "./proc/1/environ", "./sys/kernel/x",
                     "./dev/shm/x", "./dev/null"):
            with self.subTest(name=name):
                member = directory(name) if name == "./run/ucloud" else entry(name, b"x")
                refused(self, "commit_residue_forbidden", [ROOT, member])
        result, listing, _, _ = run([ROOT, directory("./dev", xattrs={"trusted.overlay.opaque": "y"}),
                                     directory("./proc"), directory("./sys"), directory("./run"), directory("./tmp")])
        self.assertEqual(list(listing), ["."])
        self.assertEqual(result.drops, {"volatile": 6})

    def test_malformed_members_fail(self):
        cases = {
            "absolute": [entry("/etc/passwd", b"x")],
            "dotdot": [entry("./etc/../passwd", b"x")],
            "nul": [entry("./a", b"x", pax={"path": "a\0b"})],
            "too long": [entry("./" + "a/" * 2100 + "b", b"x")],
            "symlink target too long": [entry("./s", kind=tarfile.SYMTYPE, linkname="a/" * 2048)],
            "device": [entry("./dev-node", kind=tarfile.CHRTYPE, dev=(1, 3))],
            "block device": [entry("./disk", kind=tarfile.BLKTYPE)],
            "link to dropped": [entry("./tmp/secret", b"x"), entry("./kept", kind=tarfile.LNKTYPE,
                                                                     linkname="./tmp/secret")],
            "link to absent": [entry("./kept", kind=tarfile.LNKTYPE, linkname="./missing")],
            "link to directory": [directory("./d"), entry("./kept", kind=tarfile.LNKTYPE, linkname="./d")],
            "trusted xattr": [entry("./a", b"x", xattrs={"trusted.overlay.redirect": "/b"})],
            "opaque on a file": [entry("./a", b"x", xattrs={"trusted.overlay.opaque": "y"})],
            "libarchive xattr": [entry("./a", b"x", pax={"LIBARCHIVE.xattr.user.a": "eA=="})],
            "duplicate": [entry("./a", b"x"), entry("./a", b"y")],
            "below a file": [entry("./a", b"x"), entry("./a/b", b"y")],
            "file over a directory": [entry("./a/b", b"y"), entry("./a", b"x")],
            "whiteout and file": [whiteout("./a"), entry("./a", b"x")],
            "opaque below a file": [entry("./a", b"x"), entry("./a/.wh..wh..opq", b"")],
            "file below an opaque marker": [entry("./a/.wh..wh..opq", b""), entry("./a", b"x")],
            "opaque below a whiteout": [whiteout("./a"), entry("./a/.wh..wh..opq", b"")],
            "malformed whiteout": [directory("./.wh.x")],
            "nested whiteout": [entry("./.wh..wh.x", b"")],
            "root file": [entry(".", b"x")],
        }
        for label, members in cases.items():
            with self.subTest(label):
                refused(self, "commit_residue_forbidden", [ROOT, *members])
        refused(self, "commit_residue_forbidden", [ROOT, entry("./caf\xe9", b"x")], encoding="latin-1",
                format=tarfile.USTAR_FORMAT)
        with self.assertRaises(CommitRefused):
            filter_upper(io.BytesIO(b"not a tar" * 100), io.BytesIO(), CommitPolicy())

    def test_bounds(self):
        refused(self, "commit_too_large", [ROOT, entry("./a", b"x" * 11)], CommitPolicy.of(max_bytes=10))
        with patch.object(commit_policy, "MAX_MEMBERS", 3):
            refused(self, "commit_too_large", [ROOT, entry("./a", b""), entry("./b", b""), entry("./c", b"")])

    def test_live_credentials_fail_in_names_bodies_and_across_read_chunks(self):
        live = (hashlib.sha256(TOKEN.encode()).hexdigest(),)
        refused(self, "commit_secret_residue", [ROOT, entry(f"./token-{TOKEN}", b"")], secrets=live)
        refused(self, "commit_secret_residue", [ROOT, entry("./env", f"TOKEN={TOKEN}\n".encode())], secrets=live)
        refused(self, "commit_secret_residue",
                [ROOT, entry("./link", kind=tarfile.SYMTYPE, linkname=f"/x/{TOKEN}")], secrets=live)
        refused(self, "commit_secret_residue", [ROOT, entry("./x", b"", xattrs={f"user.{TOKEN}": "v"})], secrets=live)
        # tarfile copies bodies in 1 MiB reads: the token straddles the first boundary.
        body = b"x" * ((1 << 20) - 10) + TOKEN.encode() + b"\n"
        refused(self, "commit_secret_residue", [ROOT, entry("./big", body)], secrets=live)
        refused(self, "commit_secret_residue", [ROOT, entry("./tail", b"y" + TOKEN.encode())], secrets=live)
        # Maximal runs only: a longer hex run, another token or no live digest is user data.
        for members, secrets in (([entry("./a", (TOKEN + "0").encode())], live),
                                 ([entry("./a", ("f" * 32).encode())], live),
                                 ([entry("./a", TOKEN.encode())], ())):
            result, listing, _, _ = run([ROOT, *members], secrets=secrets)
            self.assertIn("a", listing)

    def test_drop_rules_are_counted(self):
        spec = SimpleNamespace(ssh=SimpleNamespace(enabled=True, user="agent"),
                               linux_host=SimpleNamespace(enable_sshd=False))
        policy = policy_for(spec, exclude=["/workspace/build"])
        self.assertEqual(policy.identity, ("/etc/ssh/ssh_host_*", "/home/agent/.ssh/authorized_keys*"))
        members = [
            ROOT, directory("./etc"), directory("./etc/ssh"),
            entry("./.ucloud-init", b"x"), entry("./.ucloud-job-init", b"x"), directory("./.ucloud-managed"),
            entry("./.ucloud-managed/state.json", b"{}"), entry("./etc/resolv.conf", b"x"),
            entry("./etc/hosts", b"x"), entry("./etc/hostname", b"x"),
            entry("./etc/ssh/ssh_host_ed25519_key", b"k"), entry("./etc/ssh/sshd_config", b"kept"),
            entry("./home/agent/.ssh/authorized_keys", b"k"), entry("./home/agent/.ssh/known_hosts", b"kept"),
            entry("./run/lock", b"x"), entry("./tmp/scratch", b"x"),
            entry("./var/cache/apt/archives/a.deb", b"x"), entry("./var/cache/apt/archives/partial/b.deb", b"kept"),
            entry("./root/.cache/pip/wheel", b"x"), entry("./root/.cache/uv/x", b"x"),
            entry("./root/.npm/_cacache/x", b"x"), entry("./root/.cache/other", b"kept"),
            entry("./workspace/build/out.o", b"x"), entry("./workspace/build2", b"kept"),
        ]
        result, listing, _, _ = run(members, policy)
        self.assertEqual(result.drops, {"build_residue": 4, "caller": 1, "host_written": 7, "identity": 2,
                                        "volatile": 2})
        self.assertEqual(sorted(name for name, member in listing.items() if member.isreg()), [
            "etc/ssh/sshd_config", "home/agent/.ssh/known_hosts", "root/.cache/other", "var/cache/apt/archives/partial/b.deb",
            "workspace/build2"])
        self.assertEqual(DirectOciConfigBuilder.platform_written_paths(replace_spec(spec, enabled=False)), ())

    def test_include_paths_keep_ancestor_metadata_but_not_outside_deletions(self):
        policy = CommitPolicy.of(include_paths=["/workspace/project"])
        members = [ROOT, directory("./workspace", mode=0o700, xattrs={"trusted.overlay.opaque": "y"}),
                   directory("./workspace/project"), entry("./workspace/project/main.py", b"print(1)\n"),
                   entry("./workspace/other", b"x"), whiteout("./workspace/gone"), whiteout("./etc/passwd"),
                   whiteout("./workspace/project/old.py"), entry("./usr/bin/tool", b"x")]
        result, listing, _, _ = run(members, policy)
        self.assertEqual(sorted(listing), [".", "workspace", "workspace/project", "workspace/project/.wh.old.py",
                                           "workspace/project/main.py"])
        self.assertEqual(listing["workspace"].mode, 0o700)
        self.assertEqual(result.drops, {"caller": 5})


def replace_spec(spec, **ssh):
    return SimpleNamespace(ssh=SimpleNamespace(**{**vars(spec.ssh), **ssh}), linux_host=spec.linux_host)


class NormalizationTests(unittest.TestCase):
    def test_both_whiteout_and_opaque_encodings_give_one_layer(self):
        runsc = [ROOT, directory("./data", xattrs={"trusted.overlay.opaque": "y"}), whiteout("./data/original"),
                 directory("./d"), whiteout("./d/x")]
        oci = [ROOT, directory("./data"), entry("./data/.wh..wh..opq", b""), entry("./data/.wh.original", b""),
               directory("./d"), entry("./d/.wh.x", b"")]
        both = [ROOT, directory("./data", xattrs={"trusted.overlay.opaque": "y"}), entry("./data/.wh..wh..opq", b""),
                whiteout("./data/original"), directory("./d"), whiteout("./d/x")]
        results = [run(members) for members in (runsc, oci, both)]
        self.assertEqual(len({result.diff_id for result, *_ in results}), 1)
        _, listing, _, _ = results[0]
        self.assertEqual(sorted(listing), [".", "d", "d/.wh.x", "data", "data/.wh..wh..opq", "data/.wh.original"])
        self.assertNotIn("SCHILY.xattr.trusted.overlay.opaque", listing["data"].pax_headers)

    def test_metadata_kept_and_symlinks_never_followed(self):
        members = [ROOT, directory("./bin", mode=0o751, uid=1000),
                   entry("./bin/tool", b"#!/bin/sh\n", mode=0o4755, uid=1000, mtime=1_700_000_123,
                         xattrs={"security.capability": "\x01\x00\x00\x02", "user.note": "kept",
                                 "security.selinux": "dropped"}),
                   entry("./escape", kind=tarfile.SYMTYPE, linkname="/proc/self/environ"),
                   entry("./fifo", kind=tarfile.FIFOTYPE)]
        result, listing, bodies, _ = run(members)
        tool = listing["bin/tool"]
        self.assertEqual((tool.mode, tool.uid, tool.gid, tool.mtime), (0o4755, 1000, 1000, 1_700_000_123))
        self.assertEqual(tool.pax_headers, {"SCHILY.xattr.security.capability": "\x01\x00\x00\x02",
                                            "SCHILY.xattr.user.note": "kept"})
        self.assertEqual((listing["bin"].mode, listing["bin"].uid), (0o751, 1000))
        self.assertEqual(listing["escape"].linkname, "/proc/self/environ")
        self.assertTrue(listing["fifo"].isfifo())
        self.assertEqual(bodies["bin/tool"], b"#!/bin/sh\n")
        self.assertEqual((result.members, result.drops), (5, {}))

    def test_hardlinks_carry_data_on_the_first_sorted_name(self):
        members = [ROOT, entry("./z-target", b"shared", mode=0o640, uid=7),
                   entry("./a-link", kind=tarfile.LNKTYPE, linkname="./z-target"),
                   entry("./m-link", kind=tarfile.LNKTYPE, linkname="z-target")]
        _, listing, bodies, _ = run(members)
        self.assertEqual(bodies, {"a-link": b"shared"})
        for name in ("m-link", "z-target"):
            self.assertTrue(listing[name].islnk())
            self.assertEqual(listing[name].linkname, "a-link")
        self.assertEqual((listing["a-link"].mode, listing["a-link"].uid), (0o640, 7))

    def test_diff_id_is_stable_across_member_orders(self):
        members = [ROOT, directory("./a"), entry("./a/x", b"1"), entry("./a/y", b"2"), whiteout("./b"),
                   directory("./c"), entry("./c/l", kind=tarfile.LNKTYPE, linkname="./a/x"),
                   entry("./s", kind=tarfile.SYMTYPE, linkname="a/x"), entry("./t", b"t", mtime=1.75)]
        expected = run(members)[3]
        shuffled = list(members)
        for seed in range(5):
            random.Random(seed).shuffle(shuffled)
            self.assertEqual(run(shuffled)[3], expected)


class SchemaTests(unittest.TestCase):
    def test_policy_round_trip_hash_and_strictness(self):
        policy = CommitPolicy.of(include_paths=["/w", "/a", "/w"], exclude=["/w/tmp"], identity=["/etc/ssh/x_*"])
        self.assertEqual(policy.include_paths, ("/a", "/w"))
        self.assertEqual(CommitPolicy.from_dict(policy.to_dict()), policy)
        self.assertEqual(policy.sha256, CommitPolicy.from_dict(policy.to_dict()).sha256)
        self.assertNotEqual(policy.sha256, CommitPolicy.of(include_paths=["/a", "/w"]).sha256)
        raw = policy.to_dict()
        for bad in (raw | {"extra": 1}, raw | {"schema": "v0"}, raw | {"include_paths": ["/w", "/a"]},
                    raw | {"exclude": ["relative"]}, raw | {"exclude": ["/a/../b"]}, raw | {"include_paths": "/a"},
                    raw | {"max_bytes": True}, raw | {"max_bytes": 0}):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                CommitPolicy.from_dict(bad)
        with self.assertRaises(ValueError):
            CommitPolicy.of(exclude=[f"/{index}" for index in range(257)])
        # The largest policy and secret set still fit the keyless child's request.
        with self.assertRaisesRegex(ValueError, "byte bound"):
            CommitPolicy.of(exclude=[f"/{index:04d}" + "x" * 300 for index in range(256)])
        largest = CommitPolicy.of(include_paths=[f"/{index:03d}" + "x" * 236 for index in range(256)])
        self.assertGreater(len(commit_policy.canonical(largest.to_dict())), commit_policy.MAX_POLICY_BYTES - 4096)
        secrets = sorted(hashlib.sha256(str(index).encode()).hexdigest() for index in range(4096))
        request = {"policy": largest.to_dict(), "secret_digests": secrets, "root": "/" + "r" * 4096,
                   "registry_url": "https://" + "h" * 2040, "repository": "commits/" + "0" * 32}
        self.assertLess(len(json.dumps(request)), MAX_REQUEST_BYTES - 1024)

    def test_export_request_and_record_are_strict(self):
        request = CommitExportRequest("c-1", 3, "swe-1234-setup")
        self.assertEqual(CommitExportRequest.from_dict(request.to_dict()), request)
        for bad in ({"generation": 0}, {"generation": True}, {"operation_id": "-x"}, {"image_id": "a/b"},
                    {"resume": 1}, {"max_bytes": 0}):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                CommitExportRequest.from_dict(request.to_dict() | bad)
        with self.assertRaises(ValueError):
            CommitExportRequest.from_dict(request.to_dict() | {"extra": 1})
        repository = staging_repository("swe-1234-setup")
        self.assertRegex(repository, r"^commits/[0-9a-f]{32}$")
        exporting = CommitExport("s1", request, "exporting", False, ("/etc/ssh/ssh_host_*",), repository)
        staged = replace(exporting, state="staged", blob_digest=DIGEST, size=10)
        failed = replace(exporting, state="failed", error_code="commit_export_failed")
        for record in (exporting, staged, failed):
            self.assertEqual(CommitExport.from_dict(record.to_dict()), record)
        for bad in ({"state": "staged"}, {"blob_digest": DIGEST}, {"repository": "commits/other"},
                    {"state": "failed"}, {"was_paused": 0}, {"state": "published"}, {"identity": ("relative",)}):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                replace(exporting, **bad)
        with self.assertRaises(ValueError):
            CommitExport.from_dict(staged.to_dict() | {"schema": "v0"})

    def test_build_object_round_trip_and_bindings(self):
        policy = CommitPolicy.of(identity=["/etc/ssh/ssh_host_*"])
        request = CommitExportRequest("c-1", 3, "swe-1234-setup")
        export = CommitExport("s1", request, "staged", False, policy.identity, staging_repository("swe-1234-setup"),
                              DIGEST, 10)
        live = secret_digests([TOKEN, TOKEN])
        build = commit_build(export, policy, parent_image="registry:5000/managed/base@" + DIGEST,
                             parent_root="sha256:" + "b" * 64, secrets=live)
        self.assertEqual(CommitBuild.from_dict(build.to_dict()), build)
        self.assertEqual(build.repository, export.repository)
        self.assertEqual(build.secret_digests, (hashlib.sha256(TOKEN.encode()).hexdigest(),))
        for bad in ({"parent_image": "registry:5000/managed/base:1"}, {"secret_digests": ("A" * 64,)},
                    {"secret_digests": ("b" * 64, "a" * 64)}, {"blob_size": 0}, {"parent_root": "sha256:x"}):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                replace(build, **bad)
        for other in (replace(export, state="exporting", blob_digest="", size=0), replace(export, identity=())):
            with self.assertRaises(ValueError):
                commit_build(other, policy, parent_image=build.parent_image, parent_root=build.parent_root)
        result = FilterResult(DIGEST, 10, 2, {"caller": 1})
        self.assertEqual(FilterResult.from_dict(result.to_dict()), result)
        with self.assertRaises(ValueError):
            FilterResult.from_dict(result.to_dict() | {"drops": {"secret": 1}})

    def test_export_step_maps_worker_answers(self):
        policy = CommitPolicy()
        request = CommitExportRequest("c-1", 3, "img")
        exporting = CommitExport("s1", request, "exporting", False, (), staging_repository("img"))
        answers = []

        def call(method, path, payload):
            self.assertEqual((method, path, payload), ("POST", "/v1/sandboxes/s1/commit-export", request.to_dict()))
            return answers.pop(0)

        answers[:] = [(202, {"export": None}), (202, {"export": exporting.to_dict()}),
                      (200, {"export": replace(exporting, state="staged", blob_digest=DIGEST, size=1).to_dict()})]
        self.assertIsNone(export_step(call, "s1", request, policy))
        self.assertIsNone(export_step(call, "s1", request, policy))
        self.assertEqual(export_step(call, "s1", request, policy).blob_digest, DIGEST)
        answers[:] = [(409, {"error": "busy", "error_code": "commit_source_busy", "retryable": True}),
                      (200, {"export": replace(exporting, state="failed", error_code="commit_export_failed").to_dict()})]
        for code, retryable in (("commit_source_busy", True), ("commit_export_failed", False)):
            with self.assertRaises(CommitRefused) as caught:
                export_step(call, "s1", request, policy)
            self.assertEqual((caught.exception.code, caught.exception.retryable), (code, retryable))
        answers[:] = [(202, {"export": replace(exporting, identity=("/etc/x",)).to_dict()})]
        with self.assertRaisesRegex(ValueError, "does not match"):
            export_step(call, "s1", request, policy)


if __name__ == "__main__":
    unittest.main()
