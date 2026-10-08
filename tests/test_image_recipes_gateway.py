"""Image recipes through a real gateway and builder agent (dry-run builds)."""
import hashlib
import json
import time
import unittest
from urllib import error, request

from tests.test_control_plane import (ContextRecordingRuntime, _gateway_server, _running_server, _tar_gz_context,
                                      _temporary_root, build_builder_node_agent_server, build_heartbeat)
from ucloud_sandboxes.agent import post_heartbeat_with_headers
from ucloud_sandboxes.models import ResourceQuantity

TOKEN = {"Authorization": "Bearer gateway-secret"}


def call(url, payload=None, method="POST"):
    data = None if payload is None else json.dumps(payload).encode()
    req = request.Request(url, data=data, method=method, headers={**TOKEN, "Content-Type": "application/json"})
    try:
        with request.urlopen(req, timeout=10) as response:
            return response.status, json.loads(response.read() or b"{}"), dict(response.headers)
    except error.HTTPError as exc:
        return exc.code, json.loads(exc.read() or b"{}"), dict(exc.headers)


class ImageRecipeGatewayTests(unittest.TestCase):
    def test_register_ensure_build_and_create_by_name(self):
        archive = _tar_gz_context({"Dockerfile": b"FROM scratch\nCOPY task /task\n", "task": b"task one\n"})
        digest = f"sha256:{hashlib.sha256(archive).hexdigest()}"
        name = "prime/primeintellect/tmax:task_000001_abc"
        with _temporary_root() as root:
            runtime = ContextRecordingRuntime()
            builder = build_builder_node_agent_server(
                "127.0.0.1", 0, state_file=root / "builder.json", image_file=root / "builder-images.json",
                job_id="job-builder", node_id="builder-1", image_runtime=runtime,
                node_control_bearer_token="node-secret", build_context_store_dir=root / "builder-contexts")
            gateway = _gateway_server(root, routing_file=root / "routes.json", gateway_bearer_token="gateway-secret",
                                      node_control_bearer_token="node-secret",
                                      build_context_store_dir=root / "gateway-contexts",
                                      registry_worker_url="http://registry.example:5000")
            with _running_server(builder) as builder_url, _running_server(gateway) as base:
                recipe = {"name": name, "context_archive_digest": digest, "context_archive_size": len(archive),
                          "retention": "pinned"}
                missing = call(f"{base}/v1/image-recipes", {"recipes": [recipe]})
                uploaded = request.Request(f"{base}/v1/image-contexts/{digest}", data=archive, method="PUT",
                                           headers={**TOKEN, "Content-Type": "application/gzip"})
                request.urlopen(uploaded, timeout=10).close()
                registered = call(f"{base}/v1/image-recipes", {"recipes": [recipe]})
                unknown = call(f"{base}/v1/images/ensure", {"names": ["nobody/knows:1"]})
                queued = call(f"{base}/v1/images/ensure", {"names": [name]})  # No builder yet.
                create = call(f"{base}/v1/sandboxes", {"id": "by-name", "image": name, "cpus": 1, "memory_mb": 256,
                                                        "disk_mb": 1024, "network": "none"})
                # The upload ages out of the gateway's context store; the recipe keeps its copy.
                gateway.RequestHandlerClass.build_context_store.path(digest).unlink()
                post_heartbeat_with_headers(
                    f"{base}/v1/nodes/heartbeat",
                    build_heartbeat(job_id="job-builder", node_id="builder-1",
                                    node_epoch=builder.RequestHandlerClass.node_epoch, node_url=builder_url,
                                    capabilities=("image-cache", "image-build", "snapshot"),
                                    total_resources=ResourceQuantity(vcpu=16, memory_mb=49152, disk_mb=200000)),
                    {"Authorization": "Bearer test-heartbeat-secret"})
                states = []
                for _ in range(100):
                    status = call(f"{base}/v1/images/ensure", {"names": [name]})[1]["images"][name]
                    states.append(status["state"])
                    if status["state"] in ("ready", "failed"):
                        break
                    time.sleep(0.05)
                after = call(f"{base}/v1/sandboxes", {"id": "by-name-2", "image": name, "cpus": 1, "memory_mb": 256,
                                                       "disk_mb": 1024, "network": "none"})

        self.assertEqual(missing[0], 400)
        self.assertEqual(missing[1]["error_code"], "build_context_missing")
        self.assertEqual((registered[0], registered[1]["registered"]), (200, 1))
        image_id = registered[1]["recipes"][0]["image_id"]
        self.assertTrue(image_id.startswith("recipe-"))
        self.assertEqual(unknown[1]["images"]["nobody/knows:1"], {"state": "unknown"})
        self.assertEqual(queued[1]["images"][name]["state"], "queued")
        self.assertEqual((create[0], create[1].get("error_code")), (503, "image_building"), create)
        self.assertTrue(create[1]["retryable"])
        self.assertEqual(states[-1], "ready", states)
        self.assertIn("building", states)
        self.assertIn("registry.example:5000/ucloud-managed/", status["reference"])
        # Ready: the create gets past the recipe (and fails later: this gateway has no sandbox node).
        self.assertNotEqual(after[1].get("error_code"), "image_building", after)
        self.assertEqual(len(runtime.dockerfiles), 1)  # One build, from the recipe's kept context.
        self.assertIn(b"COPY task /task", runtime.dockerfiles[0])


    def test_prepared_names_and_the_index_views(self):
        from types import SimpleNamespace
        from urllib.parse import quote
        from ucloud_sandboxes.gateway.image_roots import ImageRootsStore
        digest = "sha256:" + "d" * 64
        repository = "ucloud-managed/precomputed-abc"
        name = "aweaiteam/scaleswe:beetbox_beets_pr3661"
        with _temporary_root() as root:
            gateway = _gateway_server(root, routing_file=root / "routes.json", gateway_bearer_token="gateway-secret",
                                      node_control_bearer_token="node-secret",
                                      build_context_store_dir=root / "gateway-contexts",
                                      registry_url="http://registry.example:5000",
                                      registry_worker_url="http://registry.example:5000")
            roots = ImageRootsStore(root / "image-roots.sqlite3")
            roots.record_converted(repository, digest, config_digest="sha256:" + "c" * 64,
                                   old_root="sha256:" + "e" * 64, new_root="sha256:" + "f" * 64, wave="m2",
                                   build_input=False)
            roots.transition(repository, digest, "switched")
            gateway.RequestHandlerClass.services.registry_refs.dependency_resolver = SimpleNamespace(image_roots=roots)
            row = {"name": name, "prepared_reference": f"{repository}@{digest}", "environment": "scaleswe",
                   "tasks": ["beetbox__beets-3661"], "source": {"dataset": "PrimeIntellect/Scale-SWE-Verified"}}
            with _running_server(gateway) as base:
                missing = call(f"{base}/v1/image-recipes",
                               {"recipes": [{**row, "name": "x:1", "prepared_reference": f"{repository}@sha256:{'0' * 64}"}]})
                registered = call(f"{base}/v1/image-recipes", {"recipes": [row]})
                archive = _tar_gz_context({"Dockerfile": b"FROM debian:12\nRUN true\n"})
                context = f"sha256:{hashlib.sha256(archive).hexdigest()}"
                request.urlopen(request.Request(f"{base}/v1/image-contexts/{context}", data=archive, method="PUT",
                                                headers={**TOKEN, "Content-Type": "application/gzip"}),
                                timeout=10).close()
                baseless = call(f"{base}/v1/image-recipes", {"recipes": [{
                    "name": "tmax:task_9", "context_archive_digest": context, "context_archive_size": len(archive),
                    "environment": "tmax", "tasks": ["task_9"]}]})
                mixed = call(f"{base}/v1/image-recipes", {"partial": True, "recipes": [
                    row, {**row, "name": "x:2", "prepared_reference": f"{repository}@sha256:{'0' * 64}"}]})
                ensured = call(f"{base}/v1/images/ensure", {"names": [name]})[1]["images"][name]
                summary = call(f"{base}/v1/image-index", method="GET")
                detail = call(f"{base}/v1/image-index/name?name={quote(name)}", method="GET")
                listed = call(f"{base}/v1/image-index/names?environment=scaleswe&state=ready", method="GET")
                tasks = call(f"{base}/v1/image-index/task-ids?environment=scaleswe", method="GET")
                unknown = call(f"{base}/v1/image-index/name?name=nobody", method="GET")
                create = call(f"{base}/v1/sandboxes", {"id": "by-name", "image": name, "cpus": 1, "memory_mb": 256,
                                                        "disk_mb": 1024, "network": "none"})
                from argparse import Namespace
                from contextlib import redirect_stdout
                import io
                from ucloud_sandboxes.image_index_cli import cmd_export_task_ids, cmd_summary
                (root / "token").write_text("gateway-secret\n")
                cli = dict(gateway_url=base, api_token_file=root / "token", json=False)
                printed = io.StringIO()
                with redirect_stdout(printed):
                    cmd_summary(Namespace(**cli))
                    cmd_export_task_ids(Namespace(**cli, directory=root / "allow"))
                exported = json.loads((root / "allow" / "scaleswe.task-ids.json").read_text())

        self.assertEqual(exported, ["beetbox__beets-3661"])
        self.assertIn("scaleswe", printed.getvalue())
        self.assertEqual((missing[0], missing[1]["error_code"], missing[1]["names"]), (400, "image_not_prepared", ["x:1"]))
        self.assertEqual(registered[0], 200)
        self.assertEqual((mixed[0], mixed[1]["registered"], [r["name"] for r in mixed[1]["refused"]]),
                         (200, 1, ["x:2"]))
        # A recipe in the index must build on something prepared; this one builds on debian:12.
        self.assertEqual((baseless[0], baseless[1]["error_code"], baseless[1]["names"]),
                         (400, "image_not_prepared", ["tmax:task_9"]))
        self.assertEqual(ensured, {"image_id": registered[1]["recipes"][0]["image_id"], "state": "ready",
                                   "reference": f"registry.example:5000/{repository}@{digest}"})
        self.assertEqual(summary[1]["environments"]["scaleswe"]["states"]["ready"], 1)
        self.assertEqual(summary[1]["totals"]["tasks"], 1)
        self.assertEqual((detail[1]["kind"], detail[1]["task_ids"]), ("prepared", ["beetbox__beets-3661"]))
        self.assertEqual([item["name"] for item in listed[1]["names"]], [name])
        self.assertEqual(tasks[1], {"environment": "scaleswe", "task_ids": ["beetbox__beets-3661"], "excluded": {}})
        self.assertEqual(unknown[0], 404)
        # The create gets past the name (and fails later: this gateway has no sandbox node).
        self.assertNotIn(create[1].get("error_code"), ("image_building", "image_build_failed"), create)


if __name__ == "__main__":
    unittest.main()
