#!/usr/bin/env python3
"""Register a training dataset's image recipes with the gateway (docs/image-recipes.md).

Two steps, so the dataset checkouts and the gateway's token never meet:

  export  FAMILY --dataset REPO [--environments REPO] --out BUNDLE [--limit N]
      Where the pinned dataset checkouts are. Writes BUNDLE/manifest.jsonl and one
      build context per task (BUNDLE/contexts/<task>/), plus BUNDLE/excluded.jsonl.
  register BUNDLE [--retention pinned|cached] [--prebuild N]
      On the gateway, with the SDK. Registers every manifest row; with --prebuild,
      builds them, N names in flight at a time, and writes BUNDLE/built.jsonl.

FAMILY is tmax or terminal-lego. A task's name is its task.toml
[environment].docker_image: exactly what verifiers asks the gateway for.

- tmax: --dataset is a prime-tasks checkout; contexts are datasets/tmax/<task>/environment/.
- terminal-lego: --dataset is a Terminal-Lego-15k checkout; --environments a
  research-environments checkout at the selection's environment_revision, whose
  verifier split and fingerprint make the pinned verifier recipe (as the 2026-10-01
  resolver did, build/terminal-resolver-20261001/inventory.py).

Both checkouts may be partial or sparse clones: every file is read from git
objects, and missing blobs are fetched from origin. Git LFS pointers are
replaced by their objects (the origin's LFS batch API, checked against the
pointer), as the Harbor loader training uses would see them. Contexts are the whole
environment/ tree (the build pilot found sparse checkouts missing _fixtures/).
"""
from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass, field
import hashlib
import importlib.util
import json
import re
import subprocess
import sys
import threading
import time
import tomllib
import urllib.request
from pathlib import Path

ENVIRONMENT_REVISION = "c7ea0d7fe3379f0e6ffb0a68c82c80c184601f92"  # The selection's research-environments pin.
MAX_CONTEXT_BYTES = 8 * 1024 ** 2  # As the selection: larger contexts were not qualified.
MAX_CONTEXT_MEMBERS = 1000
LFS_POINTER = b"version https://git-lfs.github.com/spec/v1"
FETCH_BATCH = 100
TERMINAL_TOOLS = ("\nUSER root\nRUN if ! command -v git >/dev/null || ! command -v patch >/dev/null || "
                  "! command -v bash >/dev/null; then apt-get update && apt-get install -y --no-install-recommends "
                  "git patch bash ca-certificates; fi\n")
TERMINAL_VERIFIER = ("\nCOPY --from=ghcr.io/astral-sh/uv:0.9.5 /uv /uvx /usr/local/bin/\n"
                     "ENV UV_PYTHON_INSTALL_DIR=/opt/terminal-lego-python UV_CACHE_DIR=/opt/terminal-lego-cache\n"
                     "COPY verifier-bootstrap.sh /opt/verifier-bootstrap.sh\nRUN bash -e /opt/verifier-bootstrap.sh\n")


@dataclass
class Entry:
    mode: str
    oid: str
    path: str


@dataclass
class TaskRecipe:
    task: str
    name: str | None = None
    files: dict = field(default_factory=dict)  # relative path -> (bytes, git mode)
    directories: set = field(default_factory=set)
    reason: str | None = None


class GitObjects:
    """Read a checkout's tree and blobs from git, fetching blobs a partial clone lacks."""

    def __init__(self, repo):
        self.repo = Path(repo)

    def git(self, *args, **kwargs):
        return subprocess.run(["git", "-C", str(self.repo), *args], check=True, capture_output=True, **kwargs).stdout

    def revision(self):
        return self.git("rev-parse", "HEAD").decode().strip()

    def tree(self, prefix=""):
        out = self.git("ls-tree", "-r", "-z", "HEAD", *([prefix] if prefix else []))
        entries = []
        for item in filter(None, out.decode().split("\0")):
            meta, path = item.split("\t", 1)
            mode, kind, oid = meta.split()
            if kind == "blob":
                entries.append(Entry(mode, oid, path))
        return entries

    def fetch_missing(self, oids):
        # Asking about a missing object in a partial clone fetches it, one at a
        # time: list what is local instead, then fetch the rest in batches.
        local = {line.split()[0] for line in self.git(
            "cat-file", "--batch-all-objects", "--batch-check=%(objectname) %(objecttype)").decode().splitlines()
            if line.endswith(" blob")}
        missing = sorted(set(oids) - local)
        for start in range(0, len(missing), FETCH_BATCH):
            group = missing[start:start + FETCH_BATCH]
            subprocess.run(["git", "-c", "gc.auto=0", "-C", str(self.repo), "fetch", "--no-tags",
                            "--no-write-fetch-head", "--recurse-submodules=no", "--filter=blob:none", "origin",
                            "--stdin"], input="\n".join(group).encode() + b"\n", check=True, capture_output=True,
                           timeout=600)
            print(f"  fetched {min(start + FETCH_BATCH, len(missing))}/{len(missing)} blobs", file=sys.stderr,
                  flush=True)
        return len(missing)

    def origin(self):
        return self.git("config", "--get", "remote.origin.url").decode().strip()

    def resolve_lfs(self, blobs, cache):
        """Replace LFS pointers in {oid: bytes} with their objects, through the
        origin's LFS batch API (GitHub and Hugging Face both serve it); each
        download is checked against the pointer's sha256 and size. ``cache`` is
        a directory of objects by sha256."""
        pointers = {}
        for oid, data in blobs.items():
            if data.startswith(LFS_POINTER) and len(data) < 1024:
                fields = dict(line.split(" ", 1) for line in data.decode().splitlines() if " " in line)
                sha, size = fields.get("oid", "").removeprefix("sha256:"), int(fields.get("size", "-1"))
                if re.fullmatch("[0-9a-f]{64}", sha) and size >= 0:
                    pointers[oid] = (sha, size)
        cache = Path(cache)
        cache.mkdir(parents=True, exist_ok=True)
        wanted = sorted({pointer for pointer in pointers.values() if not (cache / pointer[0]).is_file()})
        origin = self.origin().removesuffix("/").removesuffix(".git") + ".git"
        for start in range(0, len(wanted), FETCH_BATCH):
            group = wanted[start:start + FETCH_BATCH]
            body = json.dumps({"operation": "download", "transfers": ["basic"],
                               "objects": [{"oid": sha, "size": size} for sha, size in group]}).encode()
            req = urllib.request.Request(origin + "/info/lfs/objects/batch", data=body, method="POST", headers={
                "Accept": "application/vnd.git-lfs+json", "Content-Type": "application/vnd.git-lfs+json"})
            with urllib.request.urlopen(req, timeout=120) as response:
                objects = json.load(response)["objects"]
            for item in objects:
                action = (item.get("actions") or {}).get("download")
                if action is None:
                    continue  # Left as a pointer; export excludes the task.
                get = urllib.request.Request(action["href"], headers=action.get("header") or {})
                with urllib.request.urlopen(get, timeout=300) as response:
                    data = response.read()
                if hashlib.sha256(data).hexdigest() != item["oid"] or len(data) != item["size"]:
                    raise ValueError(f"LFS object {item['oid']} failed its pointer's check")
                (cache / item["oid"]).write_bytes(data)
            print(f"  LFS {min(start + FETCH_BATCH, len(wanted))}/{len(wanted)} objects", file=sys.stderr, flush=True)
        for oid, (sha, _size) in pointers.items():
            if (cache / sha).is_file():
                blobs[oid] = (cache / sha).read_bytes()
        return len(pointers)

    def blobs(self, oids):
        """{oid: bytes}, through one `git cat-file --batch`."""
        oids = list(dict.fromkeys(oids))
        found = {}
        with subprocess.Popen(["git", "-C", str(self.repo), "cat-file", "--batch"], stdin=subprocess.PIPE,
                              stdout=subprocess.PIPE) as process:
            def feed():  # Concurrently: git blocks writing once we stop reading.
                process.stdin.write("\n".join(oids).encode() + b"\n")
                process.stdin.close()
            writer = threading.Thread(target=feed, daemon=True)
            writer.start()
            for oid in oids:
                header = process.stdout.readline().split()
                if len(header) != 3:
                    raise ValueError(f"blob {oid} is missing from {self.repo}")
                found[oid] = process.stdout.read(int(header[2]))
                process.stdout.read(1)
            writer.join()
        return found


def docker_image(toml_bytes):
    environment = tomllib.loads(toml_bytes.decode()).get("environment") or {}
    image = environment.get("docker_image")
    return image if isinstance(image, str) and image else None


def context_reason(files, directories):
    """Why a context cannot be a recipe, or None; the selection's rules."""
    if any(mode == "120000" for _, mode in files.values()):
        return "context symlink"
    if any(name in {".dockerignore", "Dockerfile.dockerignore"} and data for name, (data, _) in files.items()):
        return "context ignore file"
    if any(data.startswith(LFS_POINTER) for data, _ in files.values()):
        return "unresolved LFS pointer"
    if sum(len(data) for data, _ in files.values()) > MAX_CONTEXT_BYTES:
        return "context too large"
    if len(files) + len(directories) >= MAX_CONTEXT_MEMBERS:
        return "context has too many members"
    return None


def group_tasks(entries, depth):
    """{task: [entries]} keyed by the path component at ``depth``."""
    tasks = {}
    for entry in entries:
        parts = entry.path.split("/")
        if len(parts) > depth + 1:  # Inside a directory at ``depth``, not a file there (README.md).
            tasks.setdefault(parts[depth], []).append(entry)
    return tasks


def environment_files(entries, blobs, marker="/environment/"):
    files, directories = {}, set()
    for entry in entries:
        if marker not in entry.path:
            continue
        relative = entry.path.split(marker, 1)[1]
        files[relative] = (blobs[entry.oid], entry.mode)
        directories.update(str(parent) for parent in Path(relative).parents if str(parent) != ".")
    return files, directories


def tmax_recipes(dataset, limit, lfs_cache):
    git = GitObjects(dataset)
    tasks = group_tasks(git.tree("datasets/tmax/"), 2)
    names = sorted(tasks)[:limit] if limit else sorted(tasks)
    wanted = [e.oid for task in names for e in tasks[task] if "/environment/" in e.path or e.path.endswith("/task.toml")]
    git.fetch_missing(wanted)
    blobs = git.blobs(wanted)
    git.resolve_lfs(blobs, lfs_cache)
    for task in names:
        recipe = TaskRecipe(task)
        toml = next((e for e in tasks[task] if e.path == f"datasets/tmax/{task}/task.toml"), None)
        recipe.name = docker_image(blobs[toml.oid]) if toml else None
        recipe.files, recipe.directories = environment_files(tasks[task], blobs)
        if recipe.name is None:
            recipe.reason = "no [environment].docker_image"
        elif "Dockerfile" not in recipe.files:
            recipe.reason = "no environment/Dockerfile"
        else:
            recipe.reason = context_reason(recipe.files, recipe.directories)
        yield recipe


def terminal_generator(environments):
    """The pinned verifier split and fingerprint, and the CA isolation repair."""
    git = GitObjects(environments)
    if git.revision() != ENVIRONMENT_REVISION:
        raise SystemExit(f"--environments must be at {ENVIRONMENT_REVISION}, the selection's environment_revision")
    root = Path(environments)
    spec = importlib.util.spec_from_file_location(
        "terminal_verifier", root / "environments/terminal/terminal_lego/terminal_lego/verifier.py")
    verifier = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(verifier)
    sys.path.insert(0, str(root / "tools"))
    from ucloud_recipe_repairs import isolate_verifier_ca
    return verifier.split_verifier_script, verifier.verifier_fingerprint, isolate_verifier_ca


def terminal_recipes(dataset, environments, limit, lfs_cache):
    split, fingerprint, isolate = terminal_generator(environments)
    git = GitObjects(dataset)
    tasks = {task: entries for task, entries in group_tasks(git.tree(), 0).items() if task.startswith("task_")}
    names = sorted(tasks)[:limit] if limit else sorted(tasks)
    wanted = [e.oid for task in names for e in tasks[task]
              if "/environment/" in e.path or e.path in (f"{task}/task.toml", f"{task}/tests/test.sh")]
    git.fetch_missing(wanted)
    blobs = git.blobs(wanted)
    git.resolve_lfs(blobs, lfs_cache)
    for task in names:
        recipe = TaskRecipe(task)
        by_path = {e.path: e for e in tasks[task]}
        toml, test = by_path.get(f"{task}/task.toml"), by_path.get(f"{task}/tests/test.sh")
        recipe.name = docker_image(blobs[toml.oid]) if toml else None
        files, directories = environment_files(tasks[task], blobs)
        dockerfile = files.pop("Dockerfile", (None, None))[0]
        if recipe.name is None or dockerfile is None or test is None:
            recipe.reason = "no docker_image, Dockerfile or tests/test.sh"
            yield recipe
            continue
        original = blobs[test.oid].decode()
        before, _after, warm = split(original)
        text = dockerfile.decode()
        built = {"dockerfile": text + TERMINAL_TOOLS + TERMINAL_VERIFIER
                 + f"RUN {warm}\nRUN echo {fingerprint(original)} > /opt/terminal-lego-verifier.sha256\n",
                 "files": {"verifier-bootstrap.sh": before}}
        if not any(path.startswith("task_file/") for path in files) and \
                re.search(r"^COPY\s+\./task_file/?\s+", text, re.M):
            built["directories"] = ["task_file"]
        built = isolate(built)
        recipe.files = {**files, "Dockerfile": (built["dockerfile"].encode(), "100644"),
                        **{path: (data.encode(), "100644") for path, data in built["files"].items()}}
        recipe.directories = directories | set(built.get("directories", ()))
        if re.search(r"^FROM (?:alpine:|fedora:|centos:|docker:)", built["dockerfile"], re.M):
            recipe.reason = "verifier requires apt on non-Debian base"
        else:
            recipe.reason = context_reason(recipe.files, recipe.directories)
        yield recipe


def write_context(root, recipe):
    """The context directory; a new export never reuses an old one."""
    context = root / "contexts" / recipe.task
    context.mkdir(parents=True)
    for directory in sorted(recipe.directories):
        (context / directory).mkdir(parents=True, exist_ok=True)
    for relative, (data, mode) in sorted(recipe.files.items()):
        target = context / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        target.chmod(0o755 if mode == "100755" else 0o644)
    return context


def export(args):
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=False)  # Never write into an earlier bundle.
    if args.family == "tmax":
        recipes = tmax_recipes(args.dataset, args.limit, args.lfs_cache)
    else:
        if not args.environments:
            raise SystemExit("terminal-lego needs --environments")
        recipes = terminal_recipes(args.dataset, args.environments, args.limit, args.lfs_cache)
    counts, seen = Counter(), {}
    with (out / "manifest.jsonl").open("w") as manifest, (out / "excluded.jsonl").open("w") as excluded:
        for recipe in recipes:
            if recipe.reason is None and recipe.name in seen:
                recipe.reason = f"name also used by {seen[recipe.name]}"
            if recipe.reason:
                counts[recipe.reason] += 1
                excluded.write(json.dumps({"task": recipe.task, "name": recipe.name, "reason": recipe.reason}) + "\n")
                continue
            seen[recipe.name] = recipe.task
            write_context(out, recipe)
            digest = hashlib.sha256(json.dumps({k: [hashlib.sha256(v[0]).hexdigest(), v[1]]
                                                for k, v in sorted(recipe.files.items())}).encode()).hexdigest()
            manifest.write(json.dumps({"name": recipe.name, "task": recipe.task, "family": args.family,
                                       "context": f"contexts/{recipe.task}", "files_sha256": digest}) + "\n")
            counts["exported"] += 1
    meta = {"family": args.family, "dataset_revision": GitObjects(args.dataset).revision(),
            "environment_revision": GitObjects(args.environments).revision() if args.environments else None,
            "exported_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "counts": dict(counts)}
    (out / "bundle.json").write_text(json.dumps(meta, indent=1) + "\n")
    print(json.dumps(meta), flush=True)


def register(args):
    import ucloud_sandboxes_sdk as sdk
    root = Path(args.bundle)
    rows = [json.loads(line) for line in (root / "manifest.jsonl").read_text().splitlines()]
    if args.limit:
        rows = rows[:args.limit]
    token = Path(args.token_file).read_text().strip()
    client = sdk.SandboxClient(args.gateway, api_token=token, timeout_seconds=120)
    registered = changed = 0
    for start in range(0, len(rows), args.batch):
        batch = rows[start:start + args.batch]
        result = client.register_image_recipes(
            [sdk.ImageRecipe(row["name"], root / row["context"], retention=args.retention) for row in batch])
        registered += result["registered"]
        changed += result["changed"]
        print(json.dumps({"registered": registered, "changed": changed, "of": len(rows)}), flush=True)
    if not args.prebuild:
        return
    names, statuses = [row["name"] for row in rows], {}
    with (root / "built.jsonl").open("a") as built:
        for start in range(0, len(names), args.prebuild):
            chunk = names[start:start + args.prebuild]
            final = client.wait_for_images(chunk, timeout_seconds=args.chunk_timeout, poll_interval_seconds=15)
            for name, status in final.items():
                statuses[name] = status["state"]
                built.write(json.dumps({"name": name, **status}) + "\n")
            print(json.dumps({"built": start + len(chunk), "of": len(names),
                              "states": dict(Counter(statuses.values()))}), flush=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    out = commands.add_parser("export")
    out.add_argument("family", choices=("tmax", "terminal-lego"))
    out.add_argument("--dataset", required=True)
    out.add_argument("--environments")
    out.add_argument("--out", required=True)
    out.add_argument("--limit", type=int, default=0)
    out.add_argument("--lfs-cache", required=True, help="directory of downloaded LFS objects, reused across exports")
    reg = commands.add_parser("register")
    reg.add_argument("bundle")
    reg.add_argument("--gateway", default="http://127.0.0.1:8090")
    reg.add_argument("--token-file", default="/var/lib/ucloud-sandboxes/state/sandbox-api-token")
    reg.add_argument("--retention", choices=("pinned", "cached"), default="pinned")
    reg.add_argument("--batch", type=int, default=500)
    reg.add_argument("--limit", type=int, default=0)
    reg.add_argument("--prebuild", type=int, default=0, help="build N names at a time after registering")
    reg.add_argument("--chunk-timeout", type=float, default=6 * 3600)
    args = parser.parse_args(argv)
    (export if args.command == "export" else register)(args)


if __name__ == "__main__":
    main()
