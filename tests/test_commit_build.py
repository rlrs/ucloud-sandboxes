"""C3.1 builder half: commit components, roots and the filtered-tar conversion.

Publication runs against an in-memory registry with real signatures, OCI
documents and root bindings. ``RealConversionTests`` runs real ``mkfs.erofs
--tar`` (erofs-utils 1.8+) and skips with the reason otherwise.
"""
import base64
from dataclasses import replace
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import tarfile
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from tests.test_commit_policy import ROOT, TOKEN, directory, entry, upper, whiteout
from tests.test_environment_artifact import MemoryRegistry
from tests.test_registry_client_contract import _RegistryHTTPServer
from ucloud_sandboxes import environment_artifact
from ucloud_sandboxes.commit_policy import COMMIT_ANNOTATION, CommitBuild, CommitPolicy, CommitRefused, staging_repository
from ucloud_sandboxes.environment_artifact import (
    ENVIRONMENT_ANNOTATION, OCI_IMAGE, CommitEnvironmentComponent, EnvironmentArtifactRegistry, EnvironmentComponent,
    bind_components, canonical_bytes, content_digest, load_environment, load_image_environment,
    publish_environment, sign_commit_component, sign_component,
)
from ucloud_sandboxes.environment_builder import FreshEnvironmentBuilder
from ucloud_sandboxes.environment_manifest import EnvironmentManifest
from ucloud_sandboxes.environment_prepare import PreparationError, prepare_commit_in_subprocess
from ucloud_sandboxes.managed_registry import RegistryClient

TEST_TIER = "contract"
FORMAT = {"layout": 2, "mkfs": "mkfs.erofs (erofs-utils) 1.8.6", "compression": "lz4",
          "excludes": ["dev", "proc", "run", "sys"]}
MKFS, FSCK = shutil.which("mkfs.erofs"), shutil.which("fsck.erofs")
OCI_CONFIG = "application/vnd.oci.image.config.v1+json"
OCI_GZIP = "application/vnd.oci.image.layer.v1.tar+gzip"


class CommitRegistry(MemoryRegistry):
    """Blobs are shared across repositories, so cross-repository mounts are no-ops."""
    base_url, timeout_seconds = "http://registry.local", 5

    def open_blob(self, repository, digest):
        return io.BytesIO(self.blobs[digest])

    def mount_blob(self, repository, source_repository, digest):
        return digest in self.blobs


def upper_tar(members):
    return upper(members).getvalue()


UPPER = [ROOT, directory("./workspace"), entry("./workspace/main.py", b"print('kept')\n", mtime=1_700_000_001),
         entry("./workspace/alias", kind=tarfile.LNKTYPE, linkname="./workspace/main.py"),
         entry("./workspace/link", kind=tarfile.SYMTYPE, linkname="main.py"),
         directory("./data", xattrs={"trusted.overlay.opaque": "y"}), entry("./data/new", b"new"),
         directory("./etc"), whiteout("./etc/motd"), entry("./etc/hosts", b"dropped"),
         entry("./.ucloud-init", b"dropped"), directory("./tmp"), entry("./workspace/build.log", b"excluded")]
POLICY = CommitPolicy.of(exclude=["/workspace/build.log"])


class CommitFixture(unittest.TestCase):
    """A signed parent image (one whole-image component) and a staged upper."""

    def setUp(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.key = Ed25519PrivateKey.generate()
        public = self.key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
        self.client = CommitRegistry()
        self.registry = EnvironmentArtifactRegistry(self.client, "environments", {content_digest(public): public})
        layer = gzip.compress(b"parent layer", mtime=0)
        self.parent_diff_id = content_digest(b"parent layer")
        self.client.blobs[content_digest(layer)] = layer
        self.parent_ref, self.parent_root = self.publish_parent("managed/base", [
            {"mediaType": OCI_GZIP, "digest": content_digest(layer), "size": len(layer)}])
        self.builder = FreshEnvironmentBuilder(None, self.registry, self.key, self.root / "work")

    def publish_parent(self, repository, layers):
        config = canonical_bytes({"architecture": "amd64", "os": "linux", "config": {"Cmd": ["/bin/sh"]},
                                  "rootfs": {"type": "layers", "diff_ids": [self.parent_diff_id]}})
        self.client.blobs[content_digest(config)] = config
        image = self.root / f"{content_digest(config)[7:]}.erofs"
        image.write_bytes(b"base" * 4096)
        base = self.registry.publish(image, sign_component(image, source_image=content_digest(config),
                                                           signing_key=self.key), tag="base-" + repository[-4:])
        root = publish_environment(self.registry, source_image=content_digest(config),
                                   environment=EnvironmentManifest(base), image_config={"Cmd": ["/bin/sh"]},
                                   signing_key=self.key, tag="root-" + repository[-4:])
        manifest = canonical_bytes({"schemaVersion": 2, "mediaType": OCI_IMAGE, "layers": layers,
                                    "config": {"mediaType": OCI_CONFIG, "digest": content_digest(config),
                                               "size": len(config)},
                                    "annotations": {ENVIRONMENT_ANNOTATION: root}})
        self.client.put_manifest(repository, "1", manifest, media_type=OCI_IMAGE)
        return f"registry.local/{repository}:1@{content_digest(manifest)}", root

    def stage(self, members, *, parent_ref=None, parent_root=None, policy=POLICY, secrets=(), image_id="img-1"):
        blob = upper_tar(members)
        self.client.blobs[content_digest(blob)] = blob
        return CommitBuild("s1", 3, "c-1", image_id, parent_ref or self.parent_ref, parent_root or self.parent_root,
                           content_digest(blob), len(blob), policy, tuple(secrets))


def fake_mkfs(image, view, **_options):
    Path(image).write_bytes(hashlib.sha256(Path(view).read_bytes()).digest() * 1024)


class CommitComponentTests(CommitFixture):
    def component(self, parent_root=None, fill=b"c"):
        image = self.root / "commit.erofs"
        image.write_bytes(fill * 8192)
        return sign_commit_component(image, diff_id="sha256:" + "d" * 64, parent_root=parent_root or self.parent_root,
                                     policy_sha256=POLICY.sha256, layer_format=FORMAT, signing_key=self.key)

    def test_sign_verify_and_schema_dispatch(self):
        component = self.component()
        self.assertIsInstance(component, CommitEnvironmentComponent)
        self.assertEqual(EnvironmentComponent.from_dict(json.loads(canonical_bytes(component.to_dict()))), component)
        self.assertIs(component.authenticate(self.registry.trusted_keys), component)
        for changed in (replace(component, parent_root="sha256:" + "e" * 64),
                        replace(component, policy_sha256="f" * 64), replace(component, diff_id="sha256:" + "0" * 64)):
            with self.assertRaisesRegex(ValueError, "signature"):
                changed.authenticate(self.registry.trusted_keys)
        for bad in ({"source_kind": "fresh-build-v1"}, {"extra": 1}, {"policy_sha256": "x"}):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                EnvironmentComponent.from_dict(component.to_dict() | bad)

    def test_other_signing_domains_never_verify_a_commit(self):
        component = self.component()
        unsigned = canonical_bytes(component.unsigned())
        for domain in (environment_artifact._SIGNING_DOMAIN, environment_artifact._ENVIRONMENT_DOMAIN, b""):
            forged = replace(component, signature=base64.b64encode(self.key.sign(domain + unsigned)).decode("ascii"))
            with self.subTest(domain=domain), self.assertRaisesRegex(ValueError, "signature"):
                forged.authenticate(self.registry.trusted_keys)

    def test_bind_components_refuses_splices_and_misordering(self):
        base, commit = self.registry.load(self.parent_components()[0]), self.component()
        other = self.component(parent_root="sha256:" + "9" * 64, fill=b"o")
        roots = {self.parent_root: self.parent_components(), "sha256:" + "9" * 64: ("sha256:" + "8" * 64,)}
        digests = (*self.parent_components(), "sha256:" + "7" * 64)
        bind_components(digests, [base, commit], roots.__getitem__)
        for components in ([base, other], [commit, base], [commit], [base, commit, base]):
            with self.subTest(components=components), self.assertRaises(ValueError):
                bind_components(digests[:len(components)], components, roots.__getitem__)
        with self.assertRaisesRegex(ValueError, "8 deep"):
            bind_components(("x",) * 10, [base, *[commit] * 9], roots.__getitem__)

    def test_only_commit_publication_composes_commits_and_only_on_its_parent(self):
        component = self.component()
        digest = self.registry.publish(self.root / "commit.erofs", component, tag="commit-fixture")
        parent = load_environment(self.registry, self.parent_root)
        environment = EnvironmentManifest(parent.environment.base, toolkits=(digest,))
        document, _ = self.client.manifest_document("managed/base", self.parent_ref.rpartition("@")[2])
        parent_config = self.client.blobs[document["config"]["digest"]]
        config = json.loads(parent_config)
        config["rootfs"]["diff_ids"].append(component.diff_id)
        source = content_digest(canonical_bytes(config))

        def publish(**changes):
            return publish_environment(self.registry, **{
                "source_image": source, "environment": environment, "image_config": parent.image_config,
                "signing_key": self.key, "tag": "commit-root", "parent_root": self.parent_root,
                "parent_config": parent_config, "source_diff_ids": config["rootfs"]["diff_ids"], **changes})

        root = publish()
        self.assertEqual(load_environment(self.registry, root).components, (*parent.components, digest))
        for changes in ({"source_diff_ids": [component.diff_id]}, {"image_config": {"Cmd": ["/other"]}},
                        {"parent_config": parent_config + b" "}, {"parent_root": None},
                        {"environment": EnvironmentManifest(digest)}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                publish(**changes)

    def parent_components(self):
        return load_environment(self.registry, self.parent_root).components


class PublishCommitTests(CommitFixture):
    """The publication order and bindings, with a stand-in mkfs."""

    def setUp(self):
        super().setUp()
        self.builder._layer_format = FORMAT
        patcher = patch.object(self.builder, "_mkfs", side_effect=fake_mkfs)
        self.mkfs = patcher.start()
        self.addCleanup(patcher.stop)

    def test_publishes_component_layer_root_and_annotated_manifest(self):
        commit = self.stage(UPPER)
        result = self.builder.publish_commit(commit, image_ref="registry.local/managed/commits:img-1")
        self.assertEqual(self.mkfs.call_args.kwargs, {"exclude_runtime_mounts": False, "preserve_mtimes": True,
                                                      "tar": True})
        document, _ = self.client.manifest_document("managed/commits", "img-1")
        self.assertEqual(content_digest(canonical_bytes(document)), result["manifest_digest"])
        parent, _ = self.client.manifest_document("managed/base", self.parent_ref.rpartition("@")[2])
        self.assertEqual(document["layers"][:-1], parent["layers"])
        layer = document["layers"][-1]
        self.assertEqual((layer["mediaType"], layer["digest"]), (OCI_GZIP, result["layer_digest"]))
        filtered = gzip.decompress(self.client.blobs[layer["digest"]])
        self.assertEqual(content_digest(filtered), result["diff_id"])
        with tarfile.open(fileobj=io.BytesIO(filtered)) as archive:
            self.assertEqual(archive.getnames(), [".", "data", "data/.wh..wh..opq", "data/new", "etc",
                                                  "etc/.wh.motd", "workspace", "workspace/alias",
                                                  "workspace/link", "workspace/main.py"])
        config = json.loads(self.client.blobs[document["config"]["digest"]])
        self.assertEqual(config["rootfs"]["diff_ids"], [self.parent_diff_id, result["diff_id"]])
        provenance = json.loads(document["annotations"][COMMIT_ANNOTATION])
        self.assertEqual({key: provenance[key] for key in ("parent_root", "diff_id", "policy_sha256", "image_id")},
                         {"parent_root": self.parent_root, "diff_id": result["diff_id"],
                          "policy_sha256": POLICY.sha256, "image_id": "img-1"})
        root, environment = load_image_environment(self.registry, "managed/commits", result["manifest_digest"])
        self.assertEqual(root, result["environment_root"])
        parent_environment = load_environment(self.registry, self.parent_root)
        self.assertEqual(environment.components, (*parent_environment.components, result["component_digest"]))
        self.assertEqual(environment.image_config, parent_environment.image_config)
        component = self.registry.load(result["component_digest"])
        self.assertEqual((component.parent_root, component.diff_id, component.policy_sha256, component.format),
                         (self.parent_root, result["diff_id"], POLICY.sha256, FORMAT))
        self.assertEqual(result["drops"], {"caller": 1, "host_written": 2, "volatile": 1})
        self.assertEqual(result["bytes"]["filtered"], len(filtered))
        # Content addressed end to end: a re-run publishes the same image.
        again = self.builder.publish_commit(commit, image_ref="registry.local/managed/commits:img-1")
        self.assertEqual(again, result)

    def test_commits_chain_on_commits_and_never_onto_another_root(self):
        first = self.builder.publish_commit(self.stage(UPPER), image_ref="registry.local/managed/commits:img-1")
        first_ref = "registry.local/managed/commits:img-1@" + first["manifest_digest"]
        second = self.builder.publish_commit(
            self.stage([ROOT, entry("./second", b"2")], parent_ref=first_ref, parent_root=first["environment_root"],
                       image_id="img-2"), image_ref="registry.local/managed/commits:img-2")
        _, environment = load_image_environment(self.registry, "managed/commits", second["manifest_digest"])
        self.assertEqual(environment.components[-2:], (first["component_digest"], second["component_digest"]))
        with self.assertRaisesRegex(ValueError, "parent root"):
            self.builder.publish_commit(self.stage(UPPER, parent_ref=first_ref, parent_root=self.parent_root),
                                        image_ref="registry.local/managed/commits:img-3")
        with patch.object(environment_artifact, "MAX_COMMIT_DEPTH", 2), self.assertRaises(CommitRefused) as caught:
            self.builder.publish_commit(self.stage(UPPER, parent_ref="registry.local/managed/commits:img-2@"
                                                   + second["manifest_digest"], parent_root=second["environment_root"]),
                                        image_ref="registry.local/managed/commits:img-4")
        self.assertEqual(caught.exception.code, "commit_chain_too_deep")

    def test_refusals_publish_nothing(self):
        live = (hashlib.sha256(TOKEN.encode()).hexdigest(),)
        for members, secrets, code in (([ROOT, directory("./run/ucloud")], (), "commit_residue_forbidden"),
                                       ([ROOT, entry("./key", TOKEN.encode())], live, "commit_secret_residue")):
            with self.subTest(code=code), self.assertRaises(CommitRefused) as caught:
                self.builder.publish_commit(self.stage(members, secrets=secrets),
                                            image_ref="registry.local/managed/commits:refused")
            self.assertEqual(caught.exception.code, code)
        self.assertNotIn("refused", self.client.tags)
        self.mkfs.assert_not_called()
        unrooted = "registry.local/managed/bare:1@" + self.bare_parent()
        with self.assertRaises(CommitRefused) as caught:
            self.builder.publish_commit(self.stage(UPPER, parent_ref=unrooted),
                                        image_ref="registry.local/managed/commits:bare")
        self.assertEqual(caught.exception.code, "commit_requires_environment_root")
        with self.assertRaises(CommitRefused) as caught:  # Chunk store M2 deleted the parent's OCI.
            self.builder.publish_commit(self.stage(UPPER, parent_ref="registry.local/managed/gone:1@sha256:" + "e" * 64),
                                        image_ref="registry.local/managed/commits:gone")
        self.assertEqual(caught.exception.code, "commit_parent_released")
        # A signed parent whose manifest omits mediaType (optional in OCI) has no layout to extend.
        document, _ = self.client.manifest_document("managed/base", self.parent_ref.rpartition("@")[2])
        bare = canonical_bytes({key: value for key, value in document.items() if key != "mediaType"})
        self.client.put_manifest("managed/untyped", "1", bare, media_type=OCI_IMAGE)
        with self.assertRaisesRegex(ValueError, "one OCI or Docker image manifest"):
            self.builder.publish_commit(self.stage(UPPER, parent_ref="registry.local/managed/untyped:1@"
                                                   + content_digest(bare)), image_ref="registry.local/managed/c:u")
        with self.assertRaisesRegex(ValueError, "owned tag"):
            self.builder.publish_commit(self.stage(UPPER), image_ref="registry.local/managed/c@sha256:" + "0" * 64)

    def test_the_parent_rehashes_the_filtered_layer_before_converting(self):
        prepare = self.builder._prepare_commit

        def tampered(commit, root):
            result = prepare(commit, root)
            with (root / "filtered.tar").open("ab") as stream:
                stream.write(b"\0" * 512)
            return result

        with patch.object(self.builder, "_prepare_commit", side_effect=tampered), \
                self.assertRaisesRegex(ValueError, "changed after preparation"):
            self.builder.publish_commit(self.stage(UPPER), image_ref="registry.local/managed/commits:img-1")
        self.mkfs.assert_not_called()

    def bare_parent(self):
        manifest = canonical_bytes({"schemaVersion": 2, "mediaType": OCI_IMAGE, "layers": [],
                                    "config": {"mediaType": OCI_CONFIG, "digest": "sha256:" + "1" * 64, "size": 2}})
        self.client.put_manifest("managed/bare", "1", manifest, media_type=OCI_IMAGE)
        return content_digest(manifest)


class KeylessPreparationTests(unittest.TestCase):
    """The ``commit-upper`` request in a real child against a local blob server."""

    def invoke(self, members, *, secrets=(), body=None):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        scratch = Path(temporary.name) / "prepare"
        scratch.mkdir(mode=0o700)
        blob = upper_tar(members)
        commit = CommitBuild("s1", 3, "c-1", "img-1", "registry.local/managed/base@sha256:" + "1" * 64,
                             "sha256:" + "2" * 64, content_digest(blob), len(blob), POLICY, tuple(secrets))
        path = f"/v2/{staging_repository('img-1')}/blobs/{commit.blob_digest}"

        def responder(method, request_path, headers, payload):
            self.assertEqual((method, request_path), ("GET", path))
            return 200, {"Content-Type": "application/octet-stream"}, blob if body is None else body

        with _RegistryHTTPServer(responder) as server:
            return prepare_commit_in_subprocess(RegistryClient(server.base_url, timeout_seconds=5), commit, scratch,
                                                timeout_seconds=30), scratch

    def test_child_filters_and_refuses_without_the_signing_key(self):
        result, scratch = self.invoke(UPPER)
        filtered = (scratch / "filtered.tar").read_bytes()
        self.assertEqual((content_digest(filtered), result.size), (result.diff_id, len(filtered)))
        self.assertEqual(result.drops, {"caller": 1, "host_written": 2, "volatile": 1})
        self.assertFalse((scratch / "upper.tar").exists())
        with self.assertRaises(CommitRefused) as caught:
            self.invoke([ROOT, entry("./key", TOKEN.encode())], secrets=(hashlib.sha256(TOKEN.encode()).hexdigest(),))
        self.assertEqual(caught.exception.code, "commit_secret_residue")
        with self.assertRaises(PreparationError):
            self.invoke(UPPER, body=upper_tar([ROOT]))


def _mkfs_supports_tar():
    if MKFS is None or FSCK is None:
        return False
    usage = subprocess.run([MKFS, "--help"], capture_output=True, text=True)
    return "--tar=" in usage.stdout + usage.stderr and "--aufs" in usage.stdout + usage.stderr


@unittest.skipUnless(_mkfs_supports_tar(), "needs mkfs.erofs and fsck.erofs with --tar and --aufs (erofs-utils 1.8+)")
class RealConversionTests(CommitFixture):
    def test_filtered_tar_becomes_an_overlay_lower_in_both_layouts(self):
        for layout in (1, 2):
            with self.subTest(layout=layout):
                builder = FreshEnvironmentBuilder(None, self.registry, self.key, self.root / f"work-{layout}",
                                                  mkfs_erofs=MKFS, preserve_mtimes=layout == 2)
                result = builder.publish_commit(self.stage(UPPER), image_ref=f"registry.local/managed/c:{layout}")
                component = self.registry.load(result["component_digest"])
                self.assertEqual(component.format["layout"], layout)
                image = self.root / f"layout-{layout}.erofs"
                image.write_bytes(self.client.blobs[component.image_digest])
                tree = self.root / f"tree-{layout}"
                privileged = os.geteuid() == 0
                subprocess.run([FSCK, f"--extract={tree}", "--xattrs" if privileged else "--no-xattrs", str(image)],
                               check=True, capture_output=True)
                self.assertEqual((tree / "workspace/main.py").read_bytes(), b"print('kept')\n")
                self.assertEqual((tree / "workspace/main.py").stat().st_ino, (tree / "workspace/alias").stat().st_ino)
                self.assertEqual(os.readlink(tree / "workspace/link"), "main.py")
                motd = (tree / "etc/motd").lstat()
                self.assertTrue(stat.S_ISCHR(motd.st_mode) and motd.st_rdev == 0)
                if privileged:
                    self.assertEqual(os.getxattr(tree / "data", "trusted.overlay.opaque"), b"y")
                self.assertFalse(any((tree / name).exists() for name in (
                    "etc/hosts", ".ucloud-init", "tmp", "workspace/build.log", "data/.wh..wh..opq")))
                mtime = (tree / "workspace/main.py").stat().st_mtime
                self.assertEqual(mtime, 1_700_000_001 if layout == 2 else 0)
                again = builder.publish_commit(self.stage(UPPER), image_ref=f"registry.local/managed/c:{layout}")
                self.assertEqual(again["component_digest"], result["component_digest"])


if __name__ == "__main__":
    unittest.main()
