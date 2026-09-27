import hashlib
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import time
import unittest

from ucloud_sandboxes.registry_sweep import (
    JOURNAL_NAME,
    SWEEP_SUFFIX,
    DistributionTree,
    RegistrySweepAborted,
    SweepJournal,
    sweep_registry_blobs,
)


HOUR = 3600.0


class FakeDistribution:
    """A Docker Distribution filesystem tree with controllable timestamps."""

    def __init__(self, root: Path, *, now: float) -> None:
        self.root = root
        self.tree = DistributionTree(root)
        self.now = now

    def _age(self, path: Path, age: float) -> None:
        stamp = self.now - age
        os.utime(path, (stamp, stamp))

    def blob(self, content: bytes, *, age: float = 10 * HOUR) -> str:
        digest = "sha256:" + hashlib.sha256(content).hexdigest()
        data = self.tree.blob_dir(digest) / "data"
        data.parent.mkdir(parents=True, exist_ok=True)
        data.write_bytes(content)
        self._age(data, age)
        return digest

    def link(self, directory: Path, digest: str, *, age: float = 10 * HOUR) -> Path:
        link = directory / "sha256" / digest.removeprefix("sha256:") / "link"
        link.parent.mkdir(parents=True, exist_ok=True)
        link.write_text(digest)
        self._age(link, age)
        self._age(link.parent.parent, age)
        return link

    def layer_link(self, repository: str, digest: str, *, age: float = 10 * HOUR) -> Path:
        return self.link(self.tree.repositories / repository / "_layers", digest, age=age)

    def manifest(
        self,
        repository: str,
        *,
        layers: list[str] = (),
        config: str | None = None,
        children: list[str] = (),
        tag: str | None = "latest",
        age: float = 10 * HOUR,
        link_layers: bool = True,
    ) -> str:
        document = {"schemaVersion": 2}
        if children:
            document["manifests"] = [{"digest": child} for child in children]
        else:
            document["config"] = {"digest": config or self.blob(b"config-" + repository.encode())}
            document["layers"] = [{"digest": layer} for layer in layers]
            if link_layers:
                for blob in (document["config"]["digest"], *layers):
                    self.layer_link(repository, blob, age=age)
        digest = self.blob(json.dumps(document, sort_keys=True).encode(), age=age)
        repo = self.tree.repositories / repository / "_manifests"
        self.link(repo / "revisions", digest, age=age)
        if tag is not None:
            current = repo / "tags" / tag / "current" / "link"
            current.parent.mkdir(parents=True, exist_ok=True)
            current.write_text(digest)
        return digest

    def delete_manifest(self, repository: str, digest: str) -> None:
        """What the registry's DELETE API leaves behind: layer links and blobs."""

        revision = (self.tree.repositories / repository / "_manifests" / "revisions"
                    / "sha256" / digest.removeprefix("sha256:"))
        (revision / "link").unlink()
        revision.rmdir()

    def exists(self, digest: str) -> bool:
        return (self.tree.blob_dir(digest) / "data").exists()


class RegistrySweepTests(unittest.TestCase):
    def setUp(self) -> None:
        self._directory = TemporaryDirectory()
        self.root = Path(self._directory.name)
        self.now = time.time()
        self.registry = FakeDistribution(self.root, now=self.now)

    def tearDown(self) -> None:
        self._directory.cleanup()

    def sweep(self, **kwargs):
        kwargs.setdefault("grace_seconds", 2 * HOUR)
        kwargs.setdefault("sleep", lambda _seconds: None)
        return sweep_registry_blobs(self.root, **kwargs)

    def test_reachable_blobs_survive_and_unreferenced_blobs_go(self) -> None:
        shared = self.registry.blob(b"shared-layer")
        kept_layer = self.registry.blob(b"kept-layer")
        dead_layer = self.registry.blob(b"dead-layer")
        kept = self.registry.manifest("ucloud-managed/kept", layers=[shared, kept_layer])
        dead = self.registry.manifest("ucloud-managed/dead", layers=[shared, dead_layer])
        self.registry.delete_manifest("ucloud-managed/dead", dead)

        result = self.sweep()

        for digest in (shared, kept_layer, kept):
            self.assertTrue(self.registry.exists(digest))
        self.assertFalse(self.registry.exists(dead_layer))
        self.assertFalse(self.registry.exists(dead))
        # Its links went with the blob; the shared layer's stale link too.
        dead_layers = self.registry.tree.repositories / "ucloud-managed/dead/_layers/sha256"
        self.assertEqual(list(dead_layers.iterdir()), [])
        self.assertEqual(result.deleted_blobs, 3)  # layer, config, manifest
        self.assertEqual(result.removed_stale_links, 1)
        self.assertGreater(result.deleted_bytes, 0)

    def test_index_children_and_their_blobs_are_reachable(self) -> None:
        layer = self.registry.blob(b"platform-layer")
        child = self.registry.manifest("ucloud-managed/multi", layers=[layer], tag=None)
        attestation = self.registry.manifest("ucloud-managed/multi", tag=None)
        index = self.registry.manifest("ucloud-managed/multi", children=[child, attestation])

        result = self.sweep()

        for digest in (layer, child, attestation, index):
            self.assertTrue(self.registry.exists(digest))
        self.assertEqual(result.deleted_blobs, 0)

    def test_recent_blobs_and_recently_linked_blobs_stay(self) -> None:
        fresh = self.registry.blob(b"uploading", age=0.5 * HOUR)
        old_but_linked = self.registry.blob(b"deduplicated")
        self.registry.layer_link("ucloud-managed/pushing", old_but_linked, age=0.2 * HOUR)
        orphan = self.registry.blob(b"orphan")

        result = self.sweep()

        self.assertTrue(self.registry.exists(fresh))
        self.assertTrue(self.registry.exists(old_but_linked))
        self.assertFalse(self.registry.exists(orphan))
        self.assertEqual(result.skipped_recent, 2)

    def test_a_manifest_written_during_the_sweep_restores_its_blob(self) -> None:
        orphan = self.registry.blob(b"reused-base-layer")
        link = self.registry.layer_link("ucloud-managed/old", orphan)

        def concurrent_push(_seconds: float) -> None:
            # A push verified the layer through its link before the sweep
            # removed it, then committed its manifest during the settle wait.
            self.registry.now = time.time()
            self.registry.manifest(
                "ucloud-managed/new", layers=[orphan], age=0, link_layers=False,
            )

        result = self.sweep(sleep=concurrent_push)

        self.assertTrue(self.registry.exists(orphan))
        self.assertTrue(link.exists())
        self.assertEqual(result.restored_blobs, 1)
        self.assertEqual(list(self.registry.tree.renamed_entries()), [])

    def test_a_link_created_during_the_sweep_restores_its_blob(self) -> None:
        orphan = self.registry.blob(b"reuploaded")

        def concurrent_upload(_seconds: float) -> None:
            self.registry.now = time.time()
            self.registry.layer_link("ucloud-managed/new", orphan, age=0)

        result = self.sweep(sleep=concurrent_upload)

        self.assertTrue(self.registry.exists(orphan))
        self.assertEqual(result.restored_blobs, 1)

    def test_candidates_referenced_since_the_scan_are_skipped_before_unlinking(self) -> None:
        orphan = self.registry.blob(b"late-reference")
        link = self.registry.layer_link("ucloud-managed/old", orphan)

        def before_batch(_batch: list[str]) -> None:
            self.registry.now = time.time()
            self.registry.manifest(
                "ucloud-managed/new", layers=[orphan], age=0, link_layers=False,
            )

        result = self.sweep(before_batch=before_batch)

        self.assertTrue(self.registry.exists(orphan))
        self.assertTrue(link.exists())
        self.assertEqual((result.deleted_blobs, result.restored_blobs), (0, 0))

    def test_an_interrupted_batch_is_undone_from_its_journal(self) -> None:
        orphan = self.registry.blob(b"interrupted")
        link = self.registry.layer_link("ucloud-managed/old", orphan)
        blob_dir = self.registry.tree.blob_dir(orphan)
        SweepJournal(self.root / JOURNAL_NAME).write(
            blobs={orphan: [str(link.parent)]},
        )
        link.unlink()
        link.parent.rmdir()
        blob_dir.rename(blob_dir.with_name(blob_dir.name + SWEEP_SUFFIX))

        result = self.sweep(grace_seconds=100 * HOUR)

        self.assertEqual(result.recovered_renames, 1)
        self.assertTrue(self.registry.exists(orphan))
        self.assertTrue(link.exists())
        self.assertFalse((self.root / JOURNAL_NAME).exists())

    def test_unparseable_manifest_aborts_before_deleting(self) -> None:
        broken = self.registry.blob(b"not json")
        self.registry.link(
            self.registry.tree.repositories / "ucloud-managed/broken/_manifests/revisions",
            broken,
        )
        orphan = self.registry.blob(b"orphan")

        with self.assertRaises(RegistrySweepAborted):
            self.sweep()
        self.assertTrue(self.registry.exists(orphan))


if __name__ == "__main__":
    unittest.main()
