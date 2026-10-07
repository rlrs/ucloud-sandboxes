"""Toolkit layers (docs/toolkit-layers.md): the spec field, and the gateway's
composition of an image root with toolkit components."""

import unittest

from ucloud_sandboxes.sandbox import SandboxSpec, sandbox_spec_fingerprint

TEST_TIER = "contract"
PINNED = "vf-harness@sha256:" + "d" * 64


def spec(**values):
    return SandboxSpec(id="box", image="registry/image@sha256:" + "a" * 64, memory_mb=512, cpus=1, disk_mb=1024,
                       **values)


class ToolkitSpecTests(unittest.TestCase):
    def test_absent_toolkits_keep_every_spec_and_fingerprint_unchanged(self):
        plain = spec()
        self.assertNotIn("toolkits", plain.to_dict())
        self.assertEqual(SandboxSpec.from_dict({**plain.to_dict(), "toolkits": []}).to_dict(), plain.to_dict())
        self.assertNotEqual(sandbox_spec_fingerprint(spec(toolkits=(PINNED,))), sandbox_spec_fingerprint(plain))

    def test_requests_name_tags_and_the_gateway_pins_digests(self):
        for refs in (("vf-harness:latest",), (PINNED,), ("a:1", "b:2", "c:3", "d:4")):
            requested = spec(toolkits=refs)
            requested.validate()
            self.assertEqual(SandboxSpec.from_dict(requested.to_dict()).toolkits, refs)

    def test_malformed_repeated_or_too_many_toolkits_are_refused(self):
        for refs, message in (
            (("vf-harness",), "name:tag or name@sha256"),
            (("Vf:1",), "name:tag or name@sha256"),
            (("x@sha256:abc",), "name:tag or name@sha256"),
            (("a:1", "a:2"), "once"),
            (("a:1", "b:1", "c:1", "d:1", "e:1"), "at most 4"),
        ):
            with self.subTest(refs=refs), self.assertRaisesRegex(ValueError, message):
                spec(toolkits=refs).validate()


if __name__ == "__main__":
    unittest.main()
