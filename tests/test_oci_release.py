"""Chunk store M2 OCI release (plan §5.4): a released image keeps resolving,
by tag and by digest, after ``chunk-migrate release-oci`` deletes its OCI
manifest, and the release deletes nothing a lease, route or build still needs."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import re
import sqlite3
import threading
from types import SimpleNamespace
import unittest
from urllib.parse import unquote, urlparse

# Import the module, not the TestCase: discovery would rerun it here.
from tests import test_environment_artifact as artifact_fixtures
from tests.harness import LocalFleet
from ucloud_sandboxes.capabilities import ENVIRONMENT_RAFS_CAPABILITY, ENVIRONMENT_ROOT_CAPABILITY
from ucloud_sandboxes.chunk_migrate import release_oci
from ucloud_sandboxes.environment_artifact import OCI_IMAGE, canonical_bytes, content_digest, publish_environment
from ucloud_sandboxes.environment_dependencies import EnvironmentDependencyResolver
from ucloud_sandboxes.environment_manifest import EnvironmentManifest
from ucloud_sandboxes.gateway.image_resolution import ImageResolution
from ucloud_sandboxes.gateway.image_roots import ImageRootsStore, released_lookup
from ucloud_sandboxes.managed_registry import (RegistryClient, RegistryRequestError, RegistryUsageStore,
                                               digest_protection_tag)
from ucloud_sandboxes.routing import open_routing_store

TEST_TIER = "contract"
D = {name: "sha256:" + name * 64 for name in "123456789"}


class FakeOciRegistry(ThreadingHTTPServer):
    """Distribution's manifest API: by tag or digest, tag lists, protection-tag
    PUTs, and DELETE of a digest with every tag on it."""

    def __init__(self):
        self.manifests, self.tags = {}, {}  # (repository, digest) -> bytes; (repository, tag) -> digest
        super().__init__(("127.0.0.1", 0), FakeOciHandler)
        self.url = "http://%s:%d" % self.server_address
        self.host = urlparse(self.url).netloc
        threading.Thread(target=self.serve_forever, daemon=True).start()

    def push(self, repository, tag, config_digest, layers=((D["9"], 100),)):
        body = canonical_bytes({"schemaVersion": 2, "mediaType": OCI_IMAGE, "config": {"digest": config_digest,
                                "size": 2}, "layers": [{"digest": d, "size": s} for d, s in layers]})
        self.manifests[(repository, content_digest(body))] = body
        self.tags[(repository, tag)] = content_digest(body)
        return content_digest(body)

    def delete(self, repository, digest):
        self.manifests.pop((repository, digest))
        for key in [key for key, value in self.tags.items() if key[0] == repository and value == digest]:
            del self.tags[key]


class FakeOciHandler(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def reply(self, status, body=b"", headers=()):
        self.send_response(status)
        for name, value in (("Content-Length", str(len(body))), *headers):
            self.send_header(name, value)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def manifest(self):
        match = re.fullmatch(r"/v2/(.+)/manifests/([^?]+)", self.path)
        repository, reference = unquote(match[1]), unquote(match[2])
        digest = reference if reference.startswith("sha256:") else self.server.tags.get((repository, reference))
        return repository, reference, digest, self.server.manifests.get((repository, digest))

    def do_GET(self):
        listing = re.fullmatch(r"/v2/(.+)/tags/list(\?.*)?", self.path)
        if listing:
            name = unquote(listing[1])
            tags = sorted(tag for repository, tag in self.server.tags if repository == name)
            return self.reply(200, json.dumps({"name": name, "tags": tags}).encode())
        _repository, _reference, digest, body = self.manifest()
        if body is None:
            return self.reply(404, b'{"errors":[{"code":"MANIFEST_UNKNOWN"}]}')
        self.reply(200, body, (("Docker-Content-Digest", digest), ("Content-Type", OCI_IMAGE)))

    do_HEAD = do_GET

    def do_PUT(self):
        repository, tag, _digest, _body = self.manifest()
        body = self.rfile.read(int(self.headers["Content-Length"]))
        self.server.manifests[(repository, content_digest(body))] = body
        self.server.tags[(repository, tag)] = content_digest(body)
        self.reply(201, headers=(("Docker-Content-Digest", content_digest(body)),))

    def do_DELETE(self):
        repository, _reference, digest, body = self.manifest()
        if body is None:
            return self.reply(404)
        self.server.delete(repository, digest)
        self.reply(202)


class OciReleaseTests(unittest.TestCase):
    def setUp(self):
        # The artifact fixtures (a signing key, an environments registry), not their tests.
        artifact_fixtures.EnvironmentArtifactTests.setUp(self)
        self.oci = FakeOciRegistry()
        self.addCleanup(self.oci.server_close)
        self.addCleanup(self.oci.shutdown)
        manifest, self.source = EnvironmentManifest(self.digest), self.component.source_image
        self.old, self.new = (publish_environment(self.registry, source_image=self.source, environment=manifest,
                                                  image_config={"Cmd": [f"/bin/{name}"]}, signing_key=self.key,
                                                  tag=name) for name in ("old", "new"))

    def released(self, roots, repository, digest, *, state="released", build_input=False):
        roots.record_converted(repository, digest, config_digest=self.source, old_root=self.old, new_root=self.new,
                               wave="3", build_input=build_input)
        for step in ("switched", "released")[:1 + (state == "released")]:
            roots.transition(repository, digest, step)

    def test_readers_answer_from_image_roots_once_a_released_manifest_is_gone(self):
        roots = ImageRootsStore(self.root / "image-roots.sqlite3")
        digest, other = self.oci.push("ucloud-managed/a", "v1", self.source), self.oci.push("ucloud-managed/b", "v1", D["8"])
        switched = self.oci.push("ucloud-managed/c", "v1", self.source)
        self.released(roots, "ucloud-managed/a", digest)
        self.released(roots, "ucloud-managed/c", switched, state="switched")
        deleted = []
        resolution = ImageResolution(image_manager=SimpleNamespace(store=SimpleNamespace(delete_by_tags=deleted.extend)),
                                     registry_url=self.oci.url, registry_worker_url=None, disk_monitor=None,
                                     fleet=None, image_roots=roots)
        ref = f"{self.oci.host}/ucloud-managed/a:v1"
        self.assertEqual(resolution.resolve_and_protect_manifest(ref), digest)  # The registry answers first.
        self.assertIn(("ucloud-managed/a", digest_protection_tag(digest)), self.oci.tags)
        self.assertFalse(roots.released_digest("ucloud-managed/a", "v1"))  # No tags remembered yet.
        roots.record_tags("ucloud-managed/a", digest, ["v1"])
        for repository, image in (("ucloud-managed/a", digest), ("ucloud-managed/b", other),
                                  ("ucloud-managed/c", switched)):
            self.oci.delete(repository, image)
        resolution.manifest_cache.clear()
        self.assertEqual(resolution.resolve_and_protect_manifest(ref), digest)
        self.assertEqual(resolution.resolve_and_protect_manifest(f"{ref}@{digest}"), digest)
        self.assertEqual(resolution.managed_manifest_digest(f"{self.oci.host}/ucloud-managed/a@{digest}"), digest)
        self.assertNotIn(("ucloud-managed/a", digest), self.oci.manifests)  # No protection tag restores it.
        with self.assertRaises(RegistryRequestError):  # Another digest under the tag is still unknown.
            resolution.resolve_and_protect_manifest(f"{ref}@{other}")
        for image in (f"{self.oci.host}/ucloud-managed/b:v1@{other}", f"{self.oci.host}/ucloud-managed/c:v1@{switched}"):
            with self.subTest(image=image), self.assertRaises(RegistryRequestError):  # Unmapped, or not released.
                resolution.resolve_and_protect_manifest(image)
        record = {"id": "a", "tag": ref, "source": "build:a", "pushed": True, "location": "control-plane"}
        missing = {**record, "id": "b", "tag": f"{self.oci.host}/ucloud-managed/b:v1", "manifest_digest": other}
        kept = resolution.enrich_records(({**record, "manifest_digest": digest}, record, missing))
        self.assertEqual([(item["id"], item["manifest_digest"]) for item in kept], [("a", digest)] * 2)
        self.assertEqual(deleted, [missing["tag"]])  # An unmapped missing image is still forgotten.
        released = released_lookup(self.root / "images.sqlite")  # The hourly prune's stale-record check.
        self.assertEqual([released("ucloud-managed/a", "v1"), released("ucloud-managed/a", "v2", digest),
                          released("ucloud-managed/b", "v1", other)], [True, True, False])
        self.assertFalse(released_lookup(self.root / "none" / "images.sqlite")("ucloud-managed/a", "v1"))

        # The gateway's dependency resolver: a digest dispatches its row, a tag its remembered digest.
        resolver = EnvironmentDependencyResolver(self.registry, image_roots=roots)  # No OCI manifests in it.
        for image in (f"{ref}@{digest}", ref):
            self.assertEqual(resolver.root(image), self.new)
        with self.assertRaises(RegistryRequestError):
            resolver.root(f"{self.oci.host}/ucloud-managed/b:v1")
        with self.assertRaisesRegex(ValueError, "not released"):
            roots.mark_oci_released("ucloud-managed/c", switched, layer_bytes=1)

    def test_release_oci_remembers_tags_then_deletes_only_what_nothing_needs(self):
        roots, usage = ImageRootsStore(self.root / "image-roots.sqlite3"), RegistryUsageStore(self.root / "usage.sqlite")
        client, images = RegistryClient(self.oci.url), {}
        for name in "abcdefg":
            images[name] = self.oci.push(f"ucloud-managed/{name}", "v1", self.source, ((D["9"], 100), (D[str(ord(name) % 7 + 1)], 10)))
            self.released(roots, f"ucloud-managed/{name}", images[name], build_input=name == "b",
                          state="switched" if name == "g" else "released")
        self.oci.tags[("ucloud-managed/a", "latest")] = images["a"]
        client.ensure_digest_protection_tag("ucloud-managed/a", images["a"])
        catalog = self.root / "prepared-images.sqlite3"
        db = sqlite3.connect(catalog)
        for table in ("prepared_sources", "prepared_foundations", "prepared_decisions"):
            db.execute(f"CREATE TABLE {table} (payload TEXT, family TEXT)")
        db.execute("INSERT INTO prepared_decisions VALUES (?, '')", (f"r:5000/ucloud-managed/c:v1@{images['c']}",))
        db.commit()
        db.close()
        for owner, name in (("image-pool:a", "a"), ("sandbox-route:v1:x", "a"), ("prepared-build:v1:y", "d")):
            usage.acquire_reference(f"ucloud-managed/{name}", "v1", owner, digest=images[name])
        state = SimpleNamespace(prepared={}, image_warmups={"w": SimpleNamespace(image="r:5000/ucloud-managed/f:v1")},
                                sandboxes={"old": SimpleNamespace(spec={"image": f"r:5000/ucloud-managed/e:v1@{images['e']}"}),
                                           "new": SimpleNamespace(spec={"image": f"r:5000/ucloud-managed/a@{images['a']}",
                                                                        "environment_root": self.new})})
        routes = SimpleNamespace(load=lambda: state)

        def release(execute):
            return release_oci(roots, client, usage, "3", catalog_file=catalog, routing_store=routes, execute=execute)
        dry = release(False)
        self.assertEqual({key: dry[key] for key in ("images", "build_inputs", "read_by_routes", "leased", "released",
                                                    "layer_bytes", "unique_layer_bytes", "gone", "errors")},
                         {"images": 6, "build_inputs": 2, "read_by_routes": 2, "leased": {"prepared-build": 1},
                          "released": 1, "layer_bytes": 110, "unique_layer_bytes": 110, "gone": 0, "errors": {}})
        self.assertEqual(len(self.oci.manifests), 7)  # A dry run deletes nothing.
        done = release(True)
        self.assertEqual((done["released"], done["leased"]), (1, {"prepared-build": 1}))
        self.assertEqual({key[0] for key in self.oci.manifests}, {f"ucloud-managed/{name}" for name in "bcdefg"})
        self.assertEqual(roots.released_digest("ucloud-managed/a", "latest"), images["a"])
        self.assertFalse(roots.released_digest("ucloud-managed/a", digest_protection_tag(images["a"])))
        self.assertEqual(roots.oci_released(), {("ucloud-managed/a", images["a"]): 110})
        self.assertEqual(roots.journal("ucloud-managed/a")[-1][4:6], ("released", "released"))
        self.oci.delete("ucloud-managed/e", images["e"])  # Gone some other way: counted, and marked.
        state.sandboxes.clear()
        again = release(True)
        self.assertEqual((again["released"], again["gone"]), (0, 2))
        self.assertIn(("ucloud-managed/e", images["e"]), roots.oci_released())

    def test_creates_by_tag_and_digest_dispatch_the_root_after_the_manifest_is_deleted(self):
        self.client.ensure_digest_protection_tag = lambda *_args: None  # The environments repository's.
        digest, repository = self.oci.push("ucloud-managed/task", "v1", self.source), "ucloud-managed/task"
        tag_ref, digest_ref = f"{self.oci.host}/{repository}:v1", f"{self.oci.host}/{repository}@{digest}"
        with LocalFleet(nodes=1, gateway_options={
                "registry_url": self.oci.url, "registry_usage_file": self.root / "usage.sqlite",
                "environment_registry": self.registry, "dispatch_environment_roots": True}) as fleet:
            for ref in (f"{tag_ref}@{digest}", digest_ref):
                fleet.catalog.add(ref, {"etc/hostname": "task\n"})
            node, pulled = fleet.nodes[0], []
            handler = node.server.RequestHandlerClass
            handler.__bases__[0].capabilities += (ENVIRONMENT_ROOT_CAPABILITY, ENVIRONMENT_RAFS_CAPABILITY)
            runtime = handler.image_manager.runtime
            runtime.pulls_environment_roots, pull = True, runtime.pull
            runtime.pull = lambda image, environment_root=None: pulled.append(environment_root) or pull(image)
            fleet.heartbeat()
            roots = ImageRootsStore(fleet.root / "gateway" / "image-roots.sqlite3")
            self.released(roots, repository, digest)
            summary = release_oci(roots, RegistryClient(self.oci.url), RegistryUsageStore(self.root / "usage.sqlite"),
                                  "3", catalog_file=self.root / "none.sqlite3",
                                  routing_store=open_routing_store(fleet.routing_file), execute=True)
            self.assertEqual((summary["released"], self.oci.manifests), (1, {}), summary)
            for sandbox_id, image in (("by-tag", tag_ref), ("by-digest", digest_ref)):
                created = fleet.request("POST", "/v1/sandboxes", token="sandbox", payload={
                    "id": sandbox_id, "image": image, "cpus": 1, "memory_mb": 256, "disk_mb": 1024, "network": "none"})
                self.assertEqual(created.status, 201, created.body)
                spec = fleet.route(sandbox_id).spec
                self.assertEqual((spec["image"].rpartition("@")[2], spec["environment_root"]), (digest, self.new))
                self.assertEqual(node.registration(sandbox_id).spec.environment_root, self.new)
            self.assertEqual(pulled, [])  # A create pinned to its dispatched root attaches it itself (0.9.20).
            self.assertEqual(self.oci.manifests, {})  # No protection tag restored the manifest.
