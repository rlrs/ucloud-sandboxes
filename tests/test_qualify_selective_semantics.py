import gzip
import io
import json
import tarfile
import unittest

from scripts.qualify_selective_semantics import append_documents, fixture_layers
from ucloud_sandboxes.oci_layer_materialize import UnsupportedLayer, _members


class SemanticFixtureTests(unittest.TestCase):
    def test_tar_proof_encodes_accepted_links_and_independent_fallback_reasons(self):
        lower, cases = fixture_layers("ucloud-qual-local-test")
        for name, value in {"lower": lower, **cases}.items():
            with self.subTest(case=name), tarfile.open(fileobj=io.BytesIO(gzip.decompress(value["payload"]))) as archive:
                if name in {"lower", "links"}:
                    members = _members(archive)
                    self.assertTrue(members)
                else:
                    with self.assertRaises(UnsupportedLayer):
                        _members(archive)
        with tarfile.open(fileobj=io.BytesIO(gzip.decompress(cases["cross-layer-hardlink"]["payload"]))) as archive:
            entries = archive.getmembers()
            links = [entry for entry in entries if entry.islnk()]
            self.assertEqual(len(links), 1)
            self.assertNotIn(links[0].linkname, {entry.name for entry in entries})
        self.assertLess(sum(value["descriptor"]["size"] for value in [lower, *cases.values()]), 16 * 1024)

    def test_append_does_not_change_base_or_inherit_environment_annotation(self):
        lower, _ = fixture_layers("ucloud-qual-local-test")
        base = {"schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "config": {"mediaType": "application/vnd.oci.image.config.v1+json"}, "layers": [],
                "annotations": {"org.ucloud.immutable-environment.v1": "old-environment"}}
        config = {"rootfs": {"type": "layers", "diff_ids": []}, "config": {"Cmd": ["python"]}}
        raw_config, raw_manifest = append_documents(base, config, [lower])
        self.assertEqual(config["rootfs"]["diff_ids"], [])
        self.assertIn("annotations", base)
        self.assertNotIn("annotations", json.loads(raw_manifest))
        self.assertEqual(json.loads(raw_config)["rootfs"]["diff_ids"], [lower["diff_id"]])
        self.assertEqual(json.loads(raw_manifest)["layers"], [lower["descriptor"]])


if __name__ == "__main__":
    unittest.main()
