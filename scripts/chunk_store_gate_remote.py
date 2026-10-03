#!/usr/bin/env python3
"""Host side of scripts/chunk_store_gate.py, the chunk store's M1 gate run.

The driver stages this one file to the gateway and to the gate's hosts and
runs one subcommand per step. ``derive-config``, ``merge-trust``,
``index-tally``, ``bench`` and ``drain`` use only the standard library (the
gateway's and workers' system Python); the rest import the release's
``ucloud_sandboxes`` from the node bundle's agent runtime (``/opt/m1-gate/py``).

Long steps write their JSON result to ``--out`` and an exit record to
``--out`` + ``.done``, and skip work an earlier run already recorded, so a
step killed with its host or its SSH session resumes where it stopped.
Nothing here prints a secret: token, key and credential files are opened by
path, and errors are cut to their last lines with signatures redacted.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

CLI = "import sys; from ucloud_sandboxes.cli import main; sys.exit(main(sys.argv[1:]))"
RUN_PREFIX = "spike/m1/"
ENVIRONMENTS = "environments"
_SECRET = re.compile(r"(X-Amz-Signature=|X-Amz-Credential=|Bearer |SECRET[A-Z_]*=|ACCESS_KEY[A-Z_]*=)\S+")


def redact(text):
    return _SECRET.sub(lambda match: match.group(1) + "<redacted>", text or "")


def tail(text, lines=8):
    return redact("\n".join((text or "").strip().splitlines()[-lines:]))


def write_json(path, value, mode=0o644):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, indent=1, sort_keys=True) + "\n")
    os.chmod(temporary, mode)
    os.replace(temporary, path)


def finish(args, status, result=None):
    if result is not None and getattr(args, "out", None):
        write_json(args.out, result)
    if getattr(args, "out", None):
        write_json(str(args.out) + ".done", {"status": status, "finished": time.time()})
    return status


def load_env_file(path):
    """``NAME=value`` lines (an EnvironmentFile); values never leave the process."""
    values = {}
    for line in Path(path).read_text().splitlines():
        name, sep, value = line.partition("=")
        if sep and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name.strip()):
            values[name.strip()] = value.strip().strip("'\"")
    return values


def results_by_index(path):
    found = {}
    if Path(path).exists():
        for line in Path(path).read_text().splitlines():
            if line.strip():
                record = json.loads(line)
                found[record["index"]] = record
    return found


def append_jsonl(path, record, lock):
    with lock, open(path, "a") as stream:
        stream.write(json.dumps(record, sort_keys=True) + "\n")


def sample_refs(sample_path, registry_host):
    """Sample index -> (repository, manifest digest) in our registry."""
    refs = {}
    for index, item in enumerate(json.loads(Path(sample_path).read_text())):
        reference = item["prepared_reference"]
        name, _, digest = reference.partition("@")
        repository = name.split("/", 1)[1].rsplit(":", 1)[0]
        refs[index] = (repository, digest, f"{registry_host}/{repository}@{digest}")
    return refs


# --- Gateway side, standard library only ---

def set_dotted(document, dotted, value):
    node = document
    *parents, last = dotted.split(".")
    for name in parents:
        node = node.setdefault(name, {})
    node[last] = value


def cmd_derive_config(args):
    """A copy of the live deployment.json with this run's chunk_store block.

    Only the copy is written; the source is opened read-only and never changed.
    """
    raw = json.loads(Path(args.source).read_text())
    if raw.get("immutable_environments") is None:
        raise SystemExit("the source deployment has no immutable_environments block")
    if args.block:
        block = json.loads(Path(args.block).read_text())
        if not str(block.get("prefix", "")).startswith(RUN_PREFIX):
            raise SystemExit(f"chunk_store.prefix must stay under {RUN_PREFIX}")
        raw["immutable_environments"]["chunk_store"] = block
    elif raw["immutable_environments"].get("chunk_store") is not None:
        raise SystemExit("the baseline copy must not carry a chunk_store block")
    for item in args.set:
        dotted, _, value = item.partition("=")
        set_dotted(raw, dotted, json.loads(value))
    write_json(args.out, raw, 0o600)
    if args.owner:
        subprocess.run(["chown", f"{args.owner}:{args.owner}", args.out], check=True)
    print(json.dumps({"out": args.out, "set": sorted(item.partition("=")[0] for item in args.set)}))
    return 0


def cmd_merge_trust(args):
    merged = {}
    for path in args.trust:
        merged.update(json.loads(Path(path).read_text()))
    write_json(args.out, merged, 0o644)
    if args.owner:
        subprocess.run(["chown", f"{args.owner}:{args.owner}", args.out], check=True)
    print(json.dumps({"keys": sorted(merged)}))
    return 0


def cmd_index_tally(args):
    """Read-only totals of the index (design §1.3), on the store node."""
    import sqlite3
    connection = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    packs, pack_bytes = connection.execute("SELECT COUNT(*), COALESCE(SUM(bytes), 0) FROM packs").fetchone()
    result = {"packs": packs, "pack_bytes": pack_bytes,
              "chunks": connection.execute("SELECT COUNT(*) FROM chunks").fetchone()[0],
              "roots": connection.execute("SELECT COUNT(*) FROM roots").fetchone()[0],
              "layers_complete": connection.execute(
                  "SELECT COUNT(*) FROM layers WHERE bootstrap IS NOT NULL").fetchone()[0]}
    print(json.dumps(result, sort_keys=True))
    return finish(args, 0, result)


# --- Converter side: the release's runtime ---

def registry_client(url):
    from ucloud_sandboxes.managed_registry import RegistryClient
    return RegistryClient(url, timeout_seconds=600)


def cmd_validate_config(args):
    from ucloud_sandboxes.config import DeploymentConfig
    store = DeploymentConfig.from_file(Path(args.config)).immutable_environments.chunk_store
    if store is None or not store.prefix.startswith(RUN_PREFIX):
        raise SystemExit("config has no chunk_store under the run prefix")
    print(json.dumps({"ok": True, "prefix": store.prefix, "index_url": store.index_url}))
    return 0


def cmd_mirror(args):
    """Copy the sample read-only from production into the gate registry,
    byte for byte (manifest digests unchanged), at most ``--rate-mb`` MB/s."""
    from ucloud_sandboxes.managed_registry import MANIFEST_ACCEPT
    target = registry_client(args.target_url)
    refs = sample_refs(args.sample, "")
    done = json.loads(Path(args.out).read_text()) if Path(args.out).exists() else {}
    budget = {"start": time.monotonic(), "bytes": 0}

    def throttle(count):
        budget["bytes"] += count
        ahead = budget["bytes"] / (args.rate_mb * 1e6) - (time.monotonic() - budget["start"])
        if ahead > 0:
            time.sleep(ahead)

    def get(path, accept=None):  # GET only: production is never written.
        request = urllib.request.Request(args.source_url.rstrip("/") + path, method="GET",
                                         headers={"Accept": accept} if accept else {})
        return urllib.request.urlopen(request, timeout=600)

    for index, (repository, digest, _) in sorted(refs.items()):
        if done.get(str(index), {}).get("ok"):
            continue
        with get(f"/v2/{repository}/manifests/{digest}", MANIFEST_ACCEPT) as response:
            payload, media = response.read(), response.headers.get("Content-Type", "").split(";")[0]
        if "sha256:" + hashlib.sha256(payload).hexdigest() != digest:
            raise SystemExit(f"image {index}: manifest digest mismatch")
        document = json.loads(payload)
        copied = 0
        for descriptor in [document["config"], *document["layers"]]:
            if target.blob_exists(repository, descriptor["digest"]):
                continue
            with tempfile.NamedTemporaryFile(dir=args.work_root) as spool, \
                    get(f"/v2/{repository}/blobs/{descriptor['digest']}") as response:
                hasher = hashlib.sha256()
                while chunk := response.read(1 << 20):
                    hasher.update(chunk)
                    spool.write(chunk)
                    throttle(len(chunk))
                spool.flush()
                if "sha256:" + hasher.hexdigest() != descriptor["digest"]:
                    raise SystemExit(f"image {index}: blob digest mismatch")
                target.upload_blob_file(repository, spool.name, descriptor["digest"], descriptor["size"])
                copied += descriptor["size"]
        stored = target.put_manifest(repository, args.tag, payload, media_type=media)
        done[str(index)] = {"ok": stored == digest, "repository": repository, "digest": digest, "copied": copied}
        write_json(args.out, done)
    bad = [index for index, record in done.items() if not record["ok"]]
    return finish(args, 1 if bad or len(done) != len(refs) else 0, done)


def cmd_holdout(args):
    """Images whose top layer no other sample image has, for crash injection:
    their layer steps must run, so the crash phase converts them first."""
    client = registry_client(args.registry_url)
    refs = sample_refs(args.sample, "")
    protected = {int(item) for item in args.protect.split(",") if item}
    tops, counts = {}, {}
    for index, (repository, digest, _) in sorted(refs.items()):
        document, _ = client.manifest_document(repository, digest)
        config = json.loads(client.blob_bytes(repository, document["config"]["digest"],
                                              max_bytes=document["config"]["size"]))
        diff_ids = config["rootfs"]["diff_ids"]
        tops[index] = diff_ids[-1]
        for diff_id in diff_ids:
            counts[diff_id] = counts.get(diff_id, 0) + 1
    chosen = [index for index in sorted(tops) if index not in protected and counts[tops[index]] == 1][:args.count]
    if len(chosen) < args.count:
        raise SystemExit("not enough images with a unique top layer for every crash step")
    print(json.dumps(chosen))
    return finish(args, 0, chosen)


def converter_argv(args, ref, owner, devices, attach_tag=True):
    argv = [sys.executable, "-c", CLI, "convert-environment", "--config", args.config,
            "--chunk-index-token-file", args.token_file, "--work-root", args.work_root,
            "--environment-registry-url", args.registry_url, "--environment-registry-repository", ENVIRONMENTS,
            "--environment-trusted-keys", args.trust, "--image-ref", ref,
            "--environment-signing-key", args.signing_key, "--owner", owner]
    for device in devices:
        argv += ["--verify-device", device]
    if getattr(args, "nydusd_blobs", False):  # The nydusd spike's conversions.
        argv.append("--nydusd-blobs")
    return argv + (["--attach-tag", args.attach_tag] if attach_tag else [])


def run_converter(argv, env, timeout=3 * 3600):
    started = time.monotonic()
    completed = subprocess.run(argv, env=env, capture_output=True, text=True, timeout=timeout)
    record = {"returncode": completed.returncode, "seconds": round(time.monotonic() - started, 2)}
    if completed.returncode == 0:
        record.update(json.loads(completed.stdout.strip().splitlines()[-1]))
    else:
        record["error"] = tail(completed.stderr)
    return record


def slot_devices(slot, per_slot):
    return [f"/dev/nbd{slot * per_slot + offset}" for offset in range(per_slot)]


def cmd_convert(args):
    """Convert the sample (less the crash holdout) with bounded parallelism."""
    env = {**os.environ, **load_env_file(args.s3_env)}
    refs = sample_refs(args.sample, args.registry_url.split("://", 1)[1])
    skip = set(json.loads(Path(args.exclude).read_text())) if args.exclude else set()
    done = results_by_index(args.results)
    pending = [index for index in sorted(refs) if index not in skip and not done.get(index, {}).get("ok")]
    lock, slots = threading.Lock(), list(range(args.parallel))

    def one(index):
        with lock:
            slot = slots.pop()
        try:
            # One owner per slot: a shared owner made layer claims and chunk
            # reservations treat every parallel converter as one builder.
            record = run_converter(converter_argv(args, refs[index][2], f"{args.owner}:{slot}",
                                                  slot_devices(slot, args.devices_per_slot)), env)
        finally:
            with lock:
                slots.append(slot)
        record.update(index=index, ok=record["returncode"] == 0, verified=record["returncode"] == 0)
        append_jsonl(args.results, record, lock)

    with ThreadPoolExecutor(args.parallel) as pool:
        list(pool.map(one, pending))
    done = results_by_index(args.results)
    wanted = [index for index in refs if index not in skip]
    summary = {"converted": sum(1 for index in wanted if done.get(index, {}).get("ok")), "wanted": len(wanted),
               "failed": sorted(index for index in wanted if not done.get(index, {}).get("ok"))}
    return finish(args, 0 if not summary["failed"] else 1, summary)


def cmd_crash_run(args):
    """kill -9 this converter process at the ``--nth`` arrival at ``--step``."""
    from ucloud_sandboxes import chunk_convert
    original, seen = chunk_convert.RafsConverter._step, {"count": 0}

    def step(self, name, **details):
        if name == args.step:
            seen["count"] += 1
            if seen["count"] == args.nth:
                sys.stdout.flush()
                os.kill(os.getpid(), signal.SIGKILL)
        return original(self, name, **details)

    chunk_convert.RafsConverter._step = step
    from ucloud_sandboxes.cli import main
    return main(args.argv)


def partial_roots(registry, index_client, tags):
    """New root tags that do not load in full; a complete root is not partial."""
    from ucloud_sandboxes.environment_artifact import load_environment
    broken = []
    for tag in sorted(tag for tag in tags if tag.startswith("rafs-root-")):
        try:
            digest = registry.client.manifest_digest(ENVIRONMENTS, tag)
            environment = load_environment(registry, digest)
            for component in environment.components:
                index_client.locator(component)
        except Exception as exc:  # noqa: BLE001 - every failure is a visible partial image
            broken.append({"tag": tag, "error": tail(str(exc), 2)})
    return broken


def cmd_crash(args):
    """Crash injection (design §3): per write-path step, a fresh image is
    killed there, inspected for visible partial state, then converted twice.

    An image whose conversion never reaches the step (a layer with no new
    chunks has no pack_put) completes, verified, and the next holdout image
    is tried; holdout images left over are converted plainly at the end, so
    the whole sample is converted once."""
    from ucloud_sandboxes.chunk_index import ChunkIndexClient
    from ucloud_sandboxes.environment_config import configured_environment_registry, read_token
    env = {**os.environ, **load_env_file(args.s3_env)}
    host = args.registry_url.split("://", 1)[1]
    refs = sample_refs(args.sample, host)
    holdout = json.loads(Path(args.holdout).read_text())
    steps = args.steps.split(",")
    if len(holdout) < len(steps):
        raise SystemExit("the holdout has fewer images than write-path steps")
    from ucloud_sandboxes.config import DeploymentConfig
    store = DeploymentConfig.from_file(Path(args.config)).immutable_environments.chunk_store
    registry = configured_environment_registry(args.registry_url, ENVIRONMENTS, args.trust)
    index_client = ChunkIndexClient(store.index_url, read_token(args.token_file).decode())
    done = results_by_index(args.results)
    lock = threading.Lock()
    devices = slot_devices(0, args.devices_per_slot)
    queue = [index for index in holdout if index not in done]
    for step in steps:
        if any(record.get("step") == step and record.get("killed") for record in done.values()):
            continue
        while queue:
            index = queue.pop(0)
            record = crash_one(args, registry, index_client, refs[index], index, step, devices, env)
            append_jsonl(args.results, record, lock)
            if record["killed"]:
                break
    for index in queue:  # Not needed for a step: convert it plainly.
        record = run_converter(converter_argv(args, refs[index][2], args.owner, devices), env)
        record.update(index=index, step=None, killed=False, ok=record["returncode"] == 0,
                      verified=record["returncode"] == 0)
        append_jsonl(args.results, record, lock)
    done = results_by_index(args.results)
    reached = {record["step"] for record in done.values() if record.get("killed") and record["ok"]}
    summary = {"steps": len(steps), "ok": len(reached & set(steps)),
               "not_reached": sorted(set(steps) - {record["step"] for record in done.values()
                                                   if record.get("killed")})}
    return finish(args, 0 if summary["ok"] == len(steps) else 1, summary)


def crash_one(args, registry, index_client, ref_entry, index, step, devices, env):
    repository, _, ref = ref_entry
    before = set(registry.client.tags(ENVIRONMENTS))
    crash = subprocess.run([sys.executable, os.path.abspath(__file__), "crash-run", "--step", step, "--",
                            *converter_argv(args, ref, args.owner, devices)[3:]],
                           env=env, capture_output=True, text=True, timeout=3 * 3600)
    killed = crash.returncode == -signal.SIGKILL
    if not killed:  # The step never ran: the conversion completed (and verified) or failed.
        return {"index": index, "step": step, "killed": False, "crash_returncode": crash.returncode,
                "crash_error": tail(crash.stderr), "ok": False, "verified": crash.returncode == 0}
    # Visible means a worker could run it: the attach tag, or a root that does not load in full.
    new_tags = set(registry.client.tags(ENVIRONMENTS)) - before
    attach_visible = registry.client.tag_exists(repository, args.attach_tag)
    broken = partial_roots(registry, index_client, new_tags)
    first = run_converter(converter_argv(args, ref, args.owner, devices), env)
    second = run_converter(converter_argv(args, ref, args.owner, devices), env)
    metrics = second.get("metrics", {})
    converged = (first.get("returncode") == 0 and second.get("returncode") == 0
                 and first.get("root") == second.get("root")
                 and not metrics.get("layers_converted") and not metrics.get("pack_bytes"))
    return {"index": index, "step": step, "killed": True, "crash_returncode": crash.returncode,
            "new_tags": sorted(new_tags), "attach_tag_visible": attach_visible, "partial_roots": broken,
            "no_visible_partial": not attach_visible and not broken, "rerun": first,
            "rerun_again": {key: second.get(key) for key in ("returncode", "root", "metrics", "error")},
            "converged": converged, "verified": first.get("returncode") == 0,  # The rerun ran the tree check.
            "ok": not attach_visible and not broken and converged}


def chunk_store_config(path):
    from ucloud_sandboxes.config import DeploymentConfig
    store = DeploymentConfig.from_file(Path(path)).immutable_environments.chunk_store
    if store is None or not store.prefix.startswith(RUN_PREFIX):
        raise SystemExit(f"refusing a chunk_store prefix outside {RUN_PREFIX}")
    return store


def classify(key):
    if "/packs/" in key and key.endswith(".pack"):
        return "packs"
    if key.endswith(".boot.zst"):
        return "bootstraps"
    if key.endswith(".map"):
        return "maps"
    return "other"


def cmd_s3_tally(args):
    """Stored bytes under the run prefix: packs plus bootstraps plus maps."""
    os.environ.update(load_env_file(args.s3_env))
    store = chunk_store_config(args.config)
    totals = {name: {"objects": 0, "bytes": 0} for name in ("packs", "bootstraps", "maps", "other")}
    for key, info in store.object_store().client.list_objects(store.prefix + "/"):
        totals[classify(key)]["objects"] += 1
        totals[classify(key)]["bytes"] += info.size
    totals["stored_bytes"] = sum(totals[name]["bytes"] for name in ("packs", "bootstraps", "maps"))
    totals["prefix"] = store.prefix
    print(json.dumps(totals, sort_keys=True))
    return finish(args, 0, totals)


def cmd_s3_delete(args):
    """Delete every object under the run prefix, and only there."""
    os.environ.update(load_env_file(args.s3_env))
    store = chunk_store_config(args.config)
    if store.prefix.rstrip("/") != args.prefix.rstrip("/"):
        raise SystemExit("the configured prefix is not this run's prefix")
    client = store.object_store().client
    deleted = 0
    for key, _ in client.list_objects(store.prefix + "/"):
        if not key.startswith(store.prefix + "/"):
            raise SystemExit("listing returned a key outside the run prefix")
        client.delete(key)
        deleted += 1
    remaining = len(client.list_objects(store.prefix + "/"))
    result = {"deleted": deleted, "remaining": remaining, "prefix": store.prefix}
    print(json.dumps(result))
    return finish(args, 0 if remaining == 0 else 1, result)


def layer_tars(client, repository, digest, directory):
    document, _ = client.manifest_document(repository, digest)
    paths = []
    for position, layer in enumerate(document["layers"]):
        path = Path(directory) / f"layer-{position}"
        with client.open_blob(repository, layer["digest"]) as response, open(path, "wb") as stream:
            while chunk := response.read(1 << 20):
                stream.write(chunk)
        paths.append(path)
    return paths


def cmd_rollback(args):
    """unpack-environment, then today's builder (publish-environment) on the
    regenerated image; the unpacked tree must equal the source OCI layers."""
    from ucloud_sandboxes.chunk_convert import compare_trees, expected_tree
    env = {**os.environ, **load_env_file(args.s3_env)}
    host = args.registry_url.split("://", 1)[1]
    refs = sample_refs(args.sample, host)
    roots = {index: record["root"] for index, record in results_by_index(args.converted).items() if record.get("ok")}
    client = registry_client(args.registry_url)
    done, lock = results_by_index(args.results), threading.Lock()
    for index in [int(item) for item in args.indices.split(",")]:
        if done.get(index, {}).get("ok"):
            continue
        repository, digest, _ = refs[index]
        output = f"{host}/{repository}:{args.output_tag}"
        record = {"index": index, "root": roots.get(index)}
        with tempfile.TemporaryDirectory(dir=args.work_root) as scratch:
            unpack = subprocess.run([sys.executable, "-c", CLI, "unpack-environment", "--config", args.config,
                                     "--chunk-index-token-file", args.token_file, "--work-root", scratch,
                                     "--environment-registry-url", args.registry_url,
                                     "--environment-registry-repository", ENVIRONMENTS,
                                     "--environment-trusted-keys", args.trust, "--root", str(record["root"]),
                                     "--output-ref", output], env=env, capture_output=True, text=True, timeout=7200)
            record["unpacked"] = unpack.returncode == 0
            if record["unpacked"]:
                record.update(unpack=json.loads(unpack.stdout.strip().splitlines()[-1]))
                for name in ("source", "unpacked"):
                    (Path(scratch) / name).mkdir()
                source = expected_tree(layer_tars(client, repository, digest, Path(scratch) / "source"))
                unpacked = expected_tree(layer_tars(client, repository, record["unpack"]["manifest_digest"],
                                                    Path(scratch) / "unpacked"))
                record["differences"] = compare_trees(source, unpacked)
                build = subprocess.run([sys.executable, "-c", CLI, "publish-environment", "--image-ref", output,
                                        "--state-root", str(Path(args.work_root) / f"builder-{index}"),
                                        "--environment-signing-key", args.signing_key,
                                        "--environment-allow-path", "*",
                                        "--environment-registry-url", args.registry_url,
                                        "--environment-registry-repository", ENVIRONMENTS,
                                        "--environment-trusted-keys", args.trust],
                                       env=env, capture_output=True, text=True, timeout=7200)
                record["rebuilt"] = build.returncode == 0
                record["build"] = json.loads(build.stdout.strip().splitlines()[-1]) if record["rebuilt"] else {
                    "error": tail(build.stderr)}
            else:
                record["error"] = tail(unpack.stderr)
        record["ok"] = bool(record.get("unpacked") and record.get("rebuilt") and not record.get("differences"))
        append_jsonl(args.results, record, lock)
    done = results_by_index(args.results)
    summary = {"ok": sum(1 for record in done.values() if record["ok"]), "images": len(args.indices.split(","))}
    return finish(args, 0 if summary["ok"] == summary["images"] else 1, summary)


# --- Canary workers: the node agent's own API, standard library only ---

IMAGE_MOUNTS = ("/environment-io/components/", "/ucloud-rootfs-cache/images/")


def unmount_images(runner=subprocess.run):
    """With the node's services stopped and no sandbox left: unmount the image
    mounts the backend keeps for reuse (overlays, then their EROFS lowers), the
    drain its restart fence asks for. A sandbox bundle still mounted refuses."""
    listing = runner(["findmnt", "-rn", "-o", "TARGET,FSTYPE"], check=True, capture_output=True, text=True).stdout
    mounts = [line.rsplit(" ", 1) for line in listing.splitlines() if " " in line]
    if any("/direct-runtime/bundles/" in target for target, _ in mounts):
        raise SystemExit("refusing to unmount images under a sandbox bundle mount")
    for kind in ("overlay", "erofs"):
        for target in sorted((target for target, fstype in mounts if fstype == kind
                              and any(part in target for part in IMAGE_MOUNTS)), key=len, reverse=True):
            runner(["umount", target], check=True)


AGENT_CLI = "/work/ucloud-sandboxes/bin/ucloud-sandboxes"


def create_operation(spec, generation, cli=AGENT_CLI):
    """The ``_ucloud_operation`` a node agent requires on create, as the
    gateway sends it; the spec hash comes from the node's own package, whose
    site-packages its CLI wrapper names (the bench runs on system Python).
    A deleted id is fenced by a tombstone, so each attempt takes a new,
    larger generation (a resumed bench reuses its sandbox names)."""
    match = re.search(r"PYTHONPATH=(\S+)", Path(cli).read_text())
    if match and match.group(1) not in sys.path:
        sys.path.insert(0, match.group(1))
    from ucloud_sandboxes.sandbox import SandboxSpec, sandbox_spec_fingerprint
    return {"operation_id": f"m1-gate-{generation}", "generation": generation, "kind": "create",
            "spec_hash": sandbox_spec_fingerprint(SandboxSpec.from_dict(spec))}


class Node:
    """The worker's node agent on its local address, with its control token."""

    def __init__(self, env_file="/etc/ucloud-sandboxes/node.env"):
        values = {name: value for name, value in (
            (line.partition("=")[0], line.partition("=")[2]) for line in Path(env_file).read_text().splitlines())
            if name.startswith("UCLOUD_")}
        unquote = lambda value: (shlex.split(value) or [""])[0]  # noqa: E731
        token_file = unquote(values.get("UCLOUD_NODE_CONTROL_BEARER_TOKEN_FILE", ""))
        self.token = Path(token_file).read_text().strip() if token_file else unquote(
            values.get("UCLOUD_NODE_CONTROL_BEARER_TOKEN", ""))
        host = unquote(values.get("UCLOUD_NODE_AGENT_HOST", "127.0.0.1"))
        self.base = f"http://{'127.0.0.1' if host in ('', '0.0.0.0') else host}:{unquote(values['UCLOUD_NODE_AGENT_PORT'])}"
        self.state = unquote(values["UCLOUD_STATE_DIR"])
        self.fences = {}  # sandbox id -> (generation, operation id) of its create

    def call(self, method, path, body=None, timeout=600, headers=None, retry_seconds=120.0):
        """A 503 (an agent still starting after a reset, deferred admission) is
        retried; creates carry a fixed operation id, so a retry is the same create."""
        request = urllib.request.Request(self.base + path, method=method,
                                         data=None if body is None else json.dumps(body).encode(),
                                         headers={"Authorization": "Bearer " + self.token,
                                                  "Content-Type": "application/json", **(headers or {})})
        deadline = time.monotonic() + retry_seconds
        while True:
            try:
                with urllib.request.urlopen(request, timeout=timeout) as response:
                    payload = response.read()
                return json.loads(payload) if payload else {}
            except urllib.error.HTTPError as error:
                if error.code != 503 or time.monotonic() >= deadline:
                    raise
            time.sleep(2)

    def environment_io(self):
        try:
            heartbeat = self.call("GET", "/v1/heartbeat", timeout=30)
        except OSError:
            return {}
        found = ((heartbeat.get("heartbeat") or heartbeat).get("runtime_metrics") or {}).get("environment_io") or {}
        return {key: value for key, value in found.items() if isinstance(value, (int, float))}

    def create(self, sandbox_id, image):
        # The gateway always sizes a create; an unsized one is refused as a zero request.
        spec = {"id": sandbox_id, "image": image, "network": "bridge", "command": ["sleep", "infinity"],
                "cpus": 2, "memory_mb": 4096, "disk_mb": 8192}
        operation = create_operation(spec, time.time_ns() // 1_000_000)
        self.fences[sandbox_id] = (operation["generation"], operation["operation_id"])
        started = time.monotonic()
        self.call("POST", "/v1/sandboxes", {**spec, "_ucloud_operation": operation})
        return time.monotonic() - started

    def run(self, sandbox_id, command, timeout=300):
        started = time.monotonic()
        reply = self.call("POST", f"/v1/sandboxes/{sandbox_id}/exec?initial_wait_seconds=0.05",
                          {"command": command, "env": {}, "working_dir": None, "stdin": False, "tty": False})
        session, after = reply["session"], 0
        while session.get("exit_code") is None and time.monotonic() - started < timeout:
            events = self.call("GET", f"/v1/exec/{session['id']}/events?after={after}&limit=1000&wait_seconds=5")
            session = events.get("session") or session
            after = max([after, *(event.get("sequence", 0) for event in events.get("events", []))])
        return {"wall": round(time.monotonic() - started, 3), "rc": session.get("exit_code")}

    def delete(self, sandbox_id, fence=None):
        """Delete with the create's operation fence; a sandbox left behind keeps
        its mounts, and the next reset's backend then refuses to start."""
        generation, operation_id = fence or self.fences[sandbox_id]
        try:
            self.call("DELETE", f"/v1/sandboxes/{sandbox_id}", headers={
                "X-UCloud-Sandbox-Generation": str(generation), "X-UCloud-Sandbox-Operation-Id": operation_id})
        except urllib.error.HTTPError as error:
            if error.code != 404:
                raise

    def reset(self, *, clear_traces):
        """Cold node: fresh backend, empty chunk cache, dropped page cache.

        Only on a node holding nothing but this bench's sandboxes (canaries can
        take real placements); those are deleted first, as the restarted
        backend refuses any mount the old one left (a fence, by design)."""
        found = {item.get("id") or (item.get("spec") or {}).get("id"):
                 (item.get("generation"), item.get("operation_id"))
                 for item in self.call("GET", "/v1/sandboxes")["sandboxes"]}
        if any(not str(sandbox_id).startswith("m1-") for sandbox_id in found):
            raise SystemExit(f"refusing to reset a node with other sandboxes: {sorted(map(str, found))}")
        for sandbox_id, fence in found.items():
            self.delete(sandbox_id, fence)
        root = Path(self.state) / "environment-io"
        subprocess.run(["systemctl", "stop", "ucloud-sandbox-node.service", "ucloud-environment-io.service"],
                       check=True)
        unmount_images()
        for name in ("cache", *(("traces",) if clear_traces else ())):
            subprocess.run(["rm", "-rf", "--one-file-system", str(root / name)], check=True)
        os.sync()
        Path("/proc/sys/vm/drop_caches").write_text("3\n")
        subprocess.run(["systemctl", "start", "ucloud-environment-io.service", "ucloud-sandbox-node.service"],
                       check=True)
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            try:
                urllib.request.urlopen(self.base + "/healthz", timeout=5).read()
                return
            except OSError:
                time.sleep(1)
        raise SystemExit("node agent did not come back after the reset")


COMMANDS = {
    "import_sys": ["sh", "-c", "python3 -c 'import sys' || python -c 'import sys'"],
    "git_status": ["sh", "-c", "cd /testbed 2>/dev/null && git status --porcelain | wc -l || echo no-testbed"],
    "pip_version": ["sh", "-c", "python3 -m pip --version || pip --version"],
}


def delta(after, before):
    return {key: round(value - before.get(key, 0), 3) for key, value in after.items()
            if value != before.get(key, 0)}


def cycle(node, name, image, command):
    before = node.environment_io()
    record = {"create": round(node.create(name, image), 3)}
    try:
        record.update(node.run(name, COMMANDS[command]))
    finally:
        node.delete(name)  # A trace is saved when the sandbox ends.
    record["environment_io"] = delta(node.environment_io(), before)
    return record


def cmd_bench(args):
    """Cold first commands per image (demand cycle records the trace, traced
    cycle replays it), or one N-way burst of distinct images per mode."""
    node, images = Node(), json.loads(Path(args.images).read_text())
    result = json.loads(Path(args.out).read_text()) if Path(args.out).exists() else {}
    if args.kind == "seq":
        for index, image in images.items():
            for command in COMMANDS:
                key = f"{index}:{command}"
                if key in result:
                    continue
                node.reset(clear_traces=True)
                demand = cycle(node, f"m1-{args.run}-{index}-{command[:3]}-d".replace("_", "-"), image, command)
                node.reset(clear_traces=False)
                traced = cycle(node, f"m1-{args.run}-{index}-{command[:3]}-t".replace("_", "-"), image, command)
                result[key] = {"demand": demand, "traced": traced}
                write_json(args.out, result)
        return finish(args, 0, result)
    for mode in ("demand", "traced"):
        if mode in result:
            continue
        node.reset(clear_traces=mode == "demand")
        before, started = node.environment_io(), time.monotonic()

        def sandbox(item, mode=mode, started=started):
            index, image = item
            name = f"m1-{args.run}-b{index}-{mode[0]}"
            record = {"index": index, "create": round(node.create(name, image), 3)}
            record["import_sys"] = node.run(name, COMMANDS["import_sys"])
            record["pip_version"] = node.run(name, COMMANDS["pip_version"])
            record["done"] = round(time.monotonic() - started, 3)
            return record

        with ThreadPoolExecutor(len(images)) as pool:
            rows = list(pool.map(sandbox, list(images.items())[:args.n]))
        wall = max(row["done"] for row in rows)
        for row in rows:
            node.delete(f"m1-{args.run}-b{row['index']}-{mode[0]}")
        result[mode] = {"wall": wall, "n": len(rows), "rows": rows, "environment_io": delta(node.environment_io(), before)}
        write_json(args.out, result)
    return finish(args, 0, result)


def cmd_drain(args):
    """Close admission so placement stops choosing this node; a node whose
    agent never started (init failed early) has nothing to drain."""
    if not Path("/etc/ucloud-sandboxes/node.env").exists():
        print(json.dumps({"drained": False, "reason": "no node environment"}))
        return 0
    node = Node()
    try:
        reply = node.call("POST", "/v1/drain", {"token": args.token, "draining": True}, timeout=30)
    except OSError:
        if subprocess.run(["systemctl", "is-active", "--quiet", "ucloud-sandbox-node.service"]).returncode == 0:
            raise
        print(json.dumps({"drained": False, "reason": "node agent not running"}))
        return 0
    print(json.dumps({"drained": True, "draining": (reply.get("drain") or reply).get("draining")}, sort_keys=True))
    return 0


def parser():
    root = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = root.add_subparsers(dest="command", required=True)

    def add(name, function, *options):
        command = commands.add_parser(name)
        for option in options:
            optional = option in ("owner", "exclude", "protect", "block")
            command.add_argument(f"--{option}", required=not optional, default="" if optional else None)
        command.set_defaults(func=function)
        return command

    add("derive-config", cmd_derive_config, "source", "block", "out", "owner").add_argument(
        "--set", action="append", default=[], help="dotted.path=<json value>")
    add("merge-trust", cmd_merge_trust, "out", "owner").add_argument("--trust", action="append", required=True)
    add("index-tally", cmd_index_tally, "db", "out")
    add("validate-config", cmd_validate_config, "config")
    add("mirror", cmd_mirror, "source-url", "target-url", "sample", "tag", "work-root", "out").add_argument(
        "--rate-mb", type=float, default=100.0)
    add("holdout", cmd_holdout, "registry-url", "sample", "protect", "out").add_argument("--count", type=int)
    common = ("config", "token-file", "work-root", "registry-url", "trust", "signing-key", "owner", "attach-tag",
              "sample", "s3-env", "out")
    convert = add("convert", cmd_convert, *common, "results", "exclude")
    convert.add_argument("--parallel", type=int, default=8)
    convert.add_argument("--devices-per-slot", type=int, default=4)
    convert.add_argument("--nydusd-blobs", action="store_true")
    crash = add("crash", cmd_crash, *common, "results", "holdout", "steps")
    crash.add_argument("--devices-per-slot", type=int, default=4)
    crash_run = commands.add_parser("crash-run")
    crash_run.add_argument("--step", required=True)
    crash_run.add_argument("--nth", type=int, default=1)
    crash_run.add_argument("argv", nargs=argparse.REMAINDER)
    crash_run.set_defaults(func=lambda args: cmd_crash_run(
        argparse.Namespace(**{**vars(args), "argv": args.argv[1:] if args.argv[:1] == ["--"] else args.argv})))
    add("s3-tally", cmd_s3_tally, "config", "s3-env", "out")
    add("s3-delete", cmd_s3_delete, "config", "s3-env", "prefix", "out")
    add("rollback", cmd_rollback, "config", "token-file", "work-root", "registry-url", "trust", "signing-key",
        "sample", "s3-env", "converted", "indices", "output-tag", "results", "out")
    bench = add("bench", cmd_bench, "images", "run", "out")
    bench.add_argument("--kind", choices=("seq", "burst"), required=True)
    bench.add_argument("--n", type=int, default=20)
    add("drain", cmd_drain, "token")
    return root


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        return args.func(args)
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001 - record the failure for the driver, then fail
        finish(args, 1)
        print(f"{args.command} failed: {tail(f'{type(exc).__name__}: {exc}', 4)}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
