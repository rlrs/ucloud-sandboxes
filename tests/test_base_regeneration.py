"""Volume-free builds (M2 plan §5.4): a verified regeneration receipt per
image, release-oci of build inputs that have one, and builds that take an
OCI-released base's regenerated copy as a BuildKit named context."""
import base64
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest import mock

# Import the modules, not their TestCases: discovery would rerun them here.
from tests import test_oci_release as oci_fixtures
from tests.chunk_store_support import REPOSITORY, Registry, layer
from ucloud_sandboxes import chunk_convert
from ucloud_sandboxes.chunk_convert import unpack_environment, verify_regeneration
from ucloud_sandboxes.chunk_migrate import read_jsonl, regenerate_image, release_oci, verify_regenerations
from ucloud_sandboxes.environment_artifact import OCI_IMAGE, canonical_bytes, content_digest
from ucloud_sandboxes.environment_config import EnvironmentDeploymentConfig
from ucloud_sandboxes.gateway.base_regeneration import (
    REGENERATED_REPOSITORY, BaseRegenerating, BaseRegeneration, BaseReleased, regenerated_tag, regeneration_claim)
from ucloud_sandboxes.gateway.image_roots import ImageRootsStore
from ucloud_sandboxes.images import DockerImageRuntime, ImageBuildSpec, image_build_fingerprint
from ucloud_sandboxes.managed_registry import RegistryClient, RegistryUsageStore

TEST_TIER = "contract"
D = {name: "sha256:" + name * 64 for name in "123456789"}
CONFIG = canonical_bytes({"architecture": "amd64", "os": "linux", "rootfs": {"type": "layers", "diff_ids": [D["1"]]},
                          "config": {"Env": ["PATH=/bin"], "OnBuild": ["RUN echo inherited"], "Shell": ["/bin/bash", "-c"],
                                     "Labels": {"team": "rl"}},
                          "history": [{"created_by": "FROM base"}, {"created_by": "ENV PATH=/bin", "empty_layer": True}]})
FILES = [("etc", "dir"), ("etc/hosts", b"127.0.0.1 localhost\n"), ("opt", "dir"), ("opt/tool", b"x" * 5000),
         ("opt/link", ("symlink", "tool"))]


class RegenerationTests(unittest.TestCase):
    """The receipt and the regenerated image, with ``nydus-image unpack``
    replaced by the tree it must produce (test_chunk_store_nydus runs it)."""

    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root, self.client = Path(directory.name), Registry()
        self.client.blobs[content_digest(CONFIG)] = CONFIG
        layers = [layer(FILES[:2] + [("etc/gone", b"old")]), layer([("etc/.wh.gone", b"")] + FILES[2:])]
        for blob, _ in layers:
            self.client.blobs[content_digest(blob)] = blob
        manifest = canonical_bytes({"schemaVersion": 2, "mediaType": OCI_IMAGE, "config": {
            "digest": content_digest(CONFIG), "size": len(CONFIG)}, "layers": [
            {"digest": content_digest(blob), "size": len(blob)} for blob, _ in layers]})
        self.client.put_manifest(REPOSITORY, "a", manifest, media_type=OCI_IMAGE)
        self.digest, self.environment = content_digest(manifest), SimpleNamespace(
            source_image=content_digest(CONFIG), image_config={"Env": ["PATH=/bin"]}, environment=SimpleNamespace(toolkits=()))
        self.unpacked = layer(FILES, compress=False)[0]  # The squashed tree of both layers.

    @contextmanager
    def regenerated(self, *_args, **_kwargs):
        with TemporaryDirectory(dir=self.root) as scratch:
            (Path(scratch) / "layer.tar").write_bytes(self.unpacked)
            yield self.environment, Path(scratch) / "layer.tar"

    def test_a_receipt_binds_the_tree_and_the_regenerated_image_keeps_the_original_config(self):
        with mock.patch.object(chunk_convert, "regenerated_layer", self.regenerated):
            receipt = verify_regeneration(None, None, D["7"], self.client, REPOSITORY, self.digest, work_root=self.root)
            self.assertEqual((receipt["verified"], receipt["differences"], base64.b64decode(receipt["config"])),
                             (True, [], CONFIG))
            self.assertEqual(receipt["diff_id"], "sha256:" + hashlib.sha256(self.unpacked).hexdigest())
            registry = SimpleNamespace(client=self.client)
            result = unpack_environment(registry, None, D["7"], repository=REGENERATED_REPOSITORY, tag="copy",
                                        work_root=self.root, image_config=CONFIG, diff_id=receipt["diff_id"])
            manifest, _ = self.client.manifest_document(REGENERATED_REPOSITORY, "copy")
            config = json.loads(self.client.blobs[manifest["config"]["digest"]])
            self.assertEqual((result["manifest_digest"], config["config"], config["rootfs"]["diff_ids"]),
                             (content_digest(self.client.manifests[result["manifest_digest"]]),
                              json.loads(CONFIG)["config"], [receipt["diff_id"]]))  # ONBUILD, SHELL, labels kept.
            self.assertEqual([item.get("empty_layer") for item in config["history"]], [True, True, None])
            with self.assertRaisesRegex(ValueError, "differs from the verified"):
                unpack_environment(registry, None, D["7"], repository=REGENERATED_REPOSITORY, tag="other",
                                   work_root=self.root, image_config=CONFIG, diff_id=D["8"])
            with self.assertRaisesRegex(ValueError, "not the one the root names"):
                unpack_environment(registry, None, D["7"], repository=REGENERATED_REPOSITORY, tag="other",
                                   work_root=self.root, image_config=CONFIG + b" ")
            self.assertNotIn("other", self.client.tags)
            self.unpacked = layer(FILES[:3] + [("opt/tool", b"y" * 5000)] + FILES[4:], compress=False)[0]
            differ = verify_regeneration(None, None, D["7"], self.client, REPOSITORY, self.digest, work_root=self.root)
            self.assertEqual((differ["verified"], len(differ["differences"])), (False, 1))
            self.environment.source_image = D["9"]
            with self.assertRaisesRegex(ValueError, "names another image config"):
                verify_regeneration(None, None, D["7"], self.client, REPOSITORY, self.digest, work_root=self.root)

    def test_verification_resumes_and_reports_differences_and_failures(self):
        receipts, calls = self.root / "receipts.jsonl", []
        rows = [{"repository": f"r/{name}", "manifest_digest": D["1"], "new_root": D["2"]} for name in "abc"]

        def verify(repository, digest, root):
            calls.append(repository)
            if repository == "r/c":
                raise RuntimeError("store node timeout")
            return {"repository": repository, "manifest_digest": digest, "root": root, "verified": repository == "r/a"}
        summary = verify_regenerations(rows + rows[:1], verify=verify, receipts=receipts, parallel=2)
        self.assertEqual((summary["pending"], summary["verified"], summary["differ"], summary["failed"]),
                         (3, 1, [f"r/b@{D['1']}"], [f"r/c@{D['1']}"]))
        verify_regenerations(rows, verify=verify, receipts=receipts, parallel=2)
        self.assertEqual(sorted(calls), ["r/a", "r/b", "r/c", "r/c"])  # Only the failure reruns.
        self.assertEqual(len(read_jsonl(receipts)), 4)


class ReleaseAndBuildTests(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.oci = oci_fixtures.FakeOciRegistry()
        self.addCleanup(self.oci.server_close)
        self.addCleanup(self.oci.shutdown)
        self.roots = ImageRootsStore(self.root / "image-roots.sqlite3")

    def released(self, name, *, build_input=True):
        repository = f"ucloud-managed/{name}"
        digest = self.oci.push(repository, "v1", content_digest(CONFIG))
        self.roots.record_converted(repository, digest, config_digest=content_digest(CONFIG), old_root=D["1"],
                                    new_root=D["2"], wave="3", build_input=build_input)
        self.roots.transition(repository, digest, "switched")
        self.roots.transition(repository, digest, "released")
        return repository, digest

    def receipt(self, key, root=D["2"]):
        return {"repository": key[0], "manifest_digest": key[1], "root": root, "diff_id": D["5"], "verified": True,
                "config": base64.b64encode(CONFIG).decode()}

    def test_release_oci_takes_build_inputs_only_with_a_verified_receipt(self):
        keys = [self.released(name, build_input=name != "d") for name in "abcd"]
        receipts = {keys[0]: self.receipt(keys[0]), keys[1]: self.receipt(keys[1], root=D["3"]),
                    keys[3]: self.receipt(keys[3])}
        routes = SimpleNamespace(load=lambda: SimpleNamespace(sandboxes={}, prepared={}, image_warmups={}))

        def release(execute, include=True):
            return release_oci(self.roots, RegistryClient(self.oci.url), RegistryUsageStore(self.root / "usage.sqlite"),
                               "3", catalog_file=self.root / "none.sqlite3", routing_store=routes, execute=execute,
                               include_build_inputs=include, receipts=receipts)
        self.assertEqual((release(False, include=False)["released"], release(False)["no_receipt"]), (1, 2))
        done = release(True)  # b's receipt names another root; c has none.
        self.assertEqual((done["released"], done["build_inputs"], done["no_receipt"]), (2, 3, 2))
        self.assertEqual({key[0] for key in self.oci.manifests}, {"ucloud-managed/b", "ucloud-managed/c"})
        self.assertEqual(self.roots.regenerable(), {keys[0], keys[3]})
        self.assertEqual(self.roots.regeneration(*keys[0])["config"], CONFIG)
        with self.assertRaisesRegex(ValueError, "another root or config"):
            self.roots.record_regeneration(*keys[1], root=D["3"], diff_id=D["5"], config=CONFIG)

    def test_regenerate_records_the_copy_or_the_error(self):
        key = self.released("a")
        self.roots.record_regeneration(*key, root=D["2"], diff_id=D["5"], config=CONFIG)
        with mock.patch.object(chunk_convert, "unpack_environment", return_value={"manifest_digest": D["6"]}) as unpack:
            regenerate_image(self.roots, "environments", "index", *key, work_root=self.root, nydus_image="nydus")
        self.assertEqual(unpack.call_args.kwargs | {"work_root": None}, {
            "repository": REGENERATED_REPOSITORY, "tag": regenerated_tag(*key), "work_root": None,
            "nydus_image": "nydus", "access": None, "image_config": CONFIG, "diff_id": D["5"], "reserve_bytes": 0})
        self.assertEqual(self.roots.regeneration(*key)["regenerated_digest"], D["6"])
        with mock.patch.object(chunk_convert, "unpack_environment", side_effect=OSError(28, "no space")), \
                self.assertRaises(OSError):
            regenerate_image(self.roots, "environments", "index", *key, work_root=self.root, nydus_image="nydus")
        row = self.roots.regeneration(*key)
        self.assertEqual((row["regenerated_digest"], row["error"]), ("", "OSError: [Errno 28] no space"))

    def regeneration(self, spawned, worker_host="workers:5000"):
        def spawn(repository, digest):  # What chunk-migrate regenerate leaves behind.
            spawned.append((repository, digest))
            copy = self.oci.push(REGENERATED_REPOSITORY, regenerated_tag(repository, digest), D["4"])
            self.roots.mark_regenerated(repository, digest, digest=copy)
        return BaseRegeneration(self.roots, RegistryClient(self.oci.url), registry_hosts=(self.oci.host,),
                                worker_host=worker_host, work_root=self.root / "regeneration", spawn=spawn)

    def test_a_build_naming_a_released_base_waits_for_its_copy_then_takes_it(self):
        key, kept = self.released("a"), self.released("kept")
        self.roots.record_tags(*key, ["v1"])
        regeneration, spawned = self.regeneration([]), []
        regeneration.spawn = lambda *image: spawned.append(image)
        self.assertEqual(regeneration.contexts([f"FROM {self.oci.host}/{key[0]}:v1"]), ({}, []))  # No receipts yet.
        self.roots.record_regeneration(*key, root=D["2"], diff_id=D["5"], config=CONFIG)
        self.roots.record_regeneration(*kept, root=D["2"], diff_id=D["5"], config=CONFIG)
        self.oci.delete(*key)
        with regeneration_claim(self.root / "regeneration", *key) as claimed:  # chunk-migrate regenerate runs.
            self.assertTrue(claimed)
            with self.assertRaises(BaseRegenerating):
                regeneration.contexts([f"FROM {self.oci.host}/{key[0]}:v1"])
            with regeneration_claim(self.root / "regeneration", *key) as again:
                self.assertFalse(again)
        self.assertEqual(spawned, [])  # Another process holds the image.
        texts = [f"FROM {self.oci.host}/{key[0]}:v1 AS base\nCOPY --from={self.oci.host}/{kept[0]}@{kept[1]} / /\n",
                 f"{self.oci.host}/{key[0]}@{key[1]}", "ubuntu:22.04"]
        with self.assertRaisesRegex(BaseRegenerating, f"{key[0]}:v1"):
            regeneration.contexts(texts)
        self.assertEqual(spawned, [key, key])  # The tag and the digest name it; kept's manifest is still there.
        regeneration = self.regeneration([])
        with self.assertRaises(BaseRegenerating):
            regeneration.contexts(texts)
        contexts, copies = regeneration.contexts(texts)
        copy = f"workers:5000/{REGENERATED_REPOSITORY}:{regenerated_tag(*key)}@{self.roots.regeneration(*key)['regenerated_digest']}"
        self.assertEqual((contexts, copies), ({f"{self.oci.host}/{key[0]}:v1": "docker-image://" + copy,
                                               f"{self.oci.host}/{key[0]}@{key[1]}": "docker-image://" + copy},
                                              [copy, copy]))
        self.oci.push(key[0], "v1", D["8"])  # A tag pushed again is the registry's answer.
        self.assertEqual(len(regeneration.contexts([f"FROM {self.oci.host}/{key[0]}:v1"])[0]), 0)
        self.roots.mark_regenerated(*kept, error="ValueError: the regenerated layer differs")
        self.oci.delete(*kept)
        with self.assertRaisesRegex(BaseReleased, "regenerated layer differs"):
            regeneration.contexts([f"FROM {self.oci.host}/{kept[0]}@{kept[1]}"])
        unverified = self.released("unverified")
        self.oci.delete(*unverified)
        with self.assertRaisesRegex(BaseReleased, "without a verified regeneration"):
            regeneration.contexts([f"FROM {self.oci.host}/{unverified[0]}@{unverified[1]}"])

    def test_the_gateway_answers_retryable_then_builds_with_the_copy_as_named_context(self):
        from tests.test_control_plane import (
            ContextRecordingRuntime, ControlPlaneTests, ResourceQuantity, _gateway_server, _running_server,
            _store_build_context, build_builder_node_agent_server, build_heartbeat, post_heartbeat_with_headers)
        from tests.test_prepared_images import PIN, archive
        key = self.released("source")
        self.roots.record_regeneration(*key, root=D["2"], diff_id=D["5"], config=CONFIG)
        self.oci.delete(*key)
        spawned, runtime, root = [], ContextRecordingRuntime(), self.root
        builder = build_builder_node_agent_server(
            '127.0.0.1', 0, state_file=root / 'builder-state.json', image_file=root / 'builder-images.json',
            job_id='job-builder', node_id='builder-1', image_runtime=runtime, node_control_bearer_token='node-secret',
            build_context_store_dir=root / 'builder-contexts')
        gateway = _gateway_server(root, routing_file=root / 'routes.json', gateway_bearer_token='gateway-secret',
                                  node_control_bearer_token='node-secret', build_context_store_dir=root / 'contexts',
                                  registry_url=self.oci.url, registry_usage_file=root / 'usage.sqlite',
                                  base_regeneration=self.regeneration(spawned, worker_host=self.oci.host))
        reference = f"{self.oci.host}/{key[0]}:v1@{key[1]}"  # A prepared source the catalog names.
        gateway.RequestHandlerClass.prepared_image_catalog.register_source({
            'status': 'ready', 'source': 'ubuntu:22.04', 'source_reference': PIN, 'reference': reference})
        with _running_server(builder) as node, _running_server(gateway) as base:
            context = _store_build_context(gateway, archive({'Dockerfile': b'FROM ubuntu:22.04\nRUN echo task\n'}))
            post_heartbeat_with_headers(base + '/v1/nodes/heartbeat', build_heartbeat(
                job_id='job-builder', node_id='builder-1', node_epoch=builder.RequestHandlerClass.node_epoch,
                node_url=node, capabilities=('image-cache', 'image-build', 'snapshot'),
                total_resources=ResourceQuantity(vcpu=16, memory_mb=49152, disk_mb=200000)),
                {'Authorization': 'Bearer test-heartbeat-secret'})
            raw = {**context, 'id': 'task', 'tag': 'private:5000/task:latest', 'push': True,
                   'base_contexts': {'ubuntu:22.04': 'docker-image://elsewhere/x'}}  # Clients cannot set one.

            def build():
                return ControlPlaneTests._json_request(self, base + '/v1/images/build', method='POST', payload=raw,
                                                       headers={'Authorization': 'Bearer gateway-secret'},
                                                       allow_error=True)
            waiting = build()
            self.assertEqual((waiting['status'], waiting['body']['error_code'], waiting['headers']['Retry-After']),
                             (503, 'base_regenerating', '30'))
            result = build()
        copy = f"{self.oci.host}/{REGENERATED_REPOSITORY}:{regenerated_tag(*key)}@{self.roots.regeneration(*key)['regenerated_digest']}"
        self.assertEqual((spawned, result['prepared']['reference']), ([key], reference))
        self.assertEqual([item for item in result['command'] if 'docker-image://' in item],
                         [f"{reference}=docker-image://{copy}"])
        self.assertEqual(runtime.dockerfiles, [f'FROM {reference}\nRUN echo task\n'.encode()])  # Bytes unchanged.
        leases = RegistryUsageStore(root / 'usage.sqlite').snapshot().leases.values()
        self.assertIn((REGENERATED_REPOSITORY, "regenerated-base"),
                      {(lease.repository, lease.owner.split(":")[0]) for lease in leases})

    def test_named_contexts_reach_the_build_command_and_only_then_its_identity(self):
        raw = {"id": "task", "tag": "r/task:latest", "context_path": "."}
        plain, based = ImageBuildSpec.from_dict(raw), ImageBuildSpec.from_dict(
            {**raw, "base_contexts": {"h/a@" + D["1"]: "docker-image://h/c@" + D["2"]}})
        self.assertNotIn("--build-context", DockerImageRuntime(dry_run=True).build_command(plain))
        self.assertIn(f"h/a@{D['1']}=docker-image://h/c@{D['2']}", DockerImageRuntime(dry_run=True).build_command(based))
        identity = "archive:" + D["3"]
        self.assertNotEqual(*(image_build_fingerprint(spec, context_identity=identity) for spec in (plain, based)))
        legacy = {"build_args": {}, "context_identity": identity, "dockerfile": "Dockerfile", "image_id": "task",
                  "labels": {}, "push": False, "tag": "r/task:latest", "version": 1}  # Identities stay put.
        self.assertEqual(image_build_fingerprint(plain, context_identity=identity), hashlib.sha256(json.dumps(
            legacy, separators=(",", ":"), sort_keys=True).encode()).hexdigest())
        with self.assertRaisesRegex(ValueError, "base_contexts"):
            ImageBuildSpec.from_dict({**raw, "base_contexts": {"h/a": "h/c"}}).validate()
        store = {"endpoint": "https://s3", "bucket": "b", "region": "r", "prefix": "p", "access_key_id_env": "K",
                 "secret_access_key_env": "S", "force_path_style": True, "index_url": "http://i:1", "index_listen": "i:1",
                 "index_database": "/i", "read_token_file": "/r", "write_token_file": "/w", "url_ttl_seconds": 3600,
                 "mount_granularity": "image", "nydus_image": "/n", "concurrent_misses": 1}
        with self.assertRaisesRegex(ValueError, "regenerate_bases"):
            EnvironmentDeploymentConfig.from_dict({"trusted_keys_file": "/t", "regenerate_bases": True,
                                                   "chunk_store": store})
        self.assertNotIn("regenerate_bases", EnvironmentDeploymentConfig.from_dict({"trusted_keys_file": "/t"}).to_dict())


if __name__ == "__main__":
    unittest.main()


class PinnedTarNamesTests(unittest.TestCase):
    def test_names_this_host_looked_up_become_the_converters(self):
        # 2026-10-05: uid 100 is "postgres" on the gateway and nobody on a
        # converter, so the gateway's regeneration missed the receipt's diff ID.
        import io
        import tarfile

        def tar(path, uname):
            with tarfile.open(path, "w", format=tarfile.GNU_FORMAT) as archive:
                for name, uid in (("var/cache/apt/archives/partial", 100), ("x" * 120, 0)):
                    member = tarfile.TarInfo(name)
                    member.uid, member.uname, member.gname, member.size, member.mtime = (
                        uid, uname if uid == 100 else "root", "root", 3, 1685412244)
                    archive.addfile(member, io.BytesIO(b"abc"))
        with TemporaryDirectory() as directory:
            gateway, converter = Path(directory, "gateway.tar"), Path(directory, "converter.tar")
            tar(gateway, "postgres")
            tar(converter, "")
            hosts = {"host_user": {0: "root", 100: "postgres"}.get, "host_group": {0: "root"}.get}
            chunk_convert.pin_tar_names(gateway, users={0: "root"}, groups={0: "root"}, **hosts)
            self.assertEqual(gateway.read_bytes(), converter.read_bytes())
            chunk_convert.pin_tar_names(converter, users={0: "root"}, groups={0: "root"}, **hosts)
            self.assertEqual(gateway.read_bytes(), converter.read_bytes())  # Already the converters': unchanged.
            self.assertEqual([member.uname for member in tarfile.open(gateway).getmembers()], ["", "root"])


class StoreReadRetryTests(unittest.TestCase):
    """Regeneration waits out a store node's transient errors, not real ones."""

    def flaky(self, failures):
        calls = []

        def read(url, start, length):
            calls.append(url)
            if len(calls) <= len(failures):
                raise failures[len(calls) - 1]
            return b"bytes"
        return read, calls

    def test_transient_store_errors_are_retried(self):
        from ucloud_sandboxes.managed_registry import RegistryRequestError
        read, calls = self.flaky([RegistryRequestError(503, "GET", "/v1/objects/p", "fill failed"),
                                  ConnectionResetError()])
        slept = []
        self.assertEqual(chunk_convert._retrying(read, sleep=slept.append)("u", 0, 5), b"bytes")
        self.assertEqual((len(calls), slept), (3, [2, 4]))

    def test_a_client_error_or_an_exhausted_budget_is_final(self):
        from ucloud_sandboxes.managed_registry import RegistryRequestError
        read, calls = self.flaky([RegistryRequestError(404, "GET", "/v1/objects/p", "missing")])
        with self.assertRaises(RegistryRequestError):
            chunk_convert._retrying(read, sleep=lambda _: None)("u", 0, 5)
        self.assertEqual(len(calls), 1)
        read, calls = self.flaky([RegistryRequestError(503, "GET", "/p", "x")] * 9)
        with self.assertRaises(RegistryRequestError):
            chunk_convert._retrying(read, delays=(1, 1), sleep=lambda _: None)("u", 0, 5)
        self.assertEqual(len(calls), 3)
