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
        kwargs.setdefault("writers_stopped", True)
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

    def test_collection_refuses_to_run_with_writers(self) -> None:
        orphan = self.registry.blob(b"not-safe-to-delete")
        with self.assertRaises(RegistrySweepAborted):
            self.sweep(writers_stopped=False)
        self.assertTrue(self.registry.exists(orphan))

    def test_rewritten_link_is_found_even_with_unchanged_parent_mtime(self) -> None:
        orphan = self.registry.blob(b"reused-layer")
        link = self.registry.layer_link("repo", orphan)
        parent_mtime = link.parent.parent.stat().st_mtime
        link.write_text(orphan)
        os.utime(link, (self.now, self.now))
        self.assertEqual(link.parent.parent.stat().st_mtime, parent_mtime)
        current = self.registry.tree.scan_repository("repo", self.registry.tree.repositories / "repo", since=self.now)
        self.assertIn(orphan, current.layers)
        self.sweep()
        self.assertTrue(self.registry.exists(orphan))

    def test_missing_referenced_manifest_aborts_before_deleting(self) -> None:
        manifest = self.registry.manifest("repo")
        (self.registry.tree.blob_dir(manifest) / "data").unlink()
        orphan = self.registry.blob(b"orphan")
        with self.assertRaises(RegistrySweepAborted):
            self.sweep()
        self.assertTrue(self.registry.exists(orphan))

    def test_unknown_or_malformed_references_abort_before_deleting(self) -> None:
        for document in ({"schemaVersion": 2}, {"layers": "not-a-list"},
                         {"layers": [{"digest": "sha512:unsupported"}]}):
            with self.subTest(document=document):
                broken = self.registry.blob(json.dumps(document).encode())
                link = self.registry.link(self.registry.tree.repositories / "repo/_manifests/revisions", broken)
                orphan = self.registry.blob(b"must-survive")
                with self.assertRaises(RegistrySweepAborted):
                    self.sweep()
                self.assertTrue(self.registry.exists(orphan))
                link.unlink()

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
