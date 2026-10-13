#!/usr/bin/env bash
# Build the agent harness toolkit image (docs/toolkit-layers.md) from offline harness
# bundles: the trees verifiers' ACP harnesses install under /var/tmp (Node, Pi,
# OpenCode, Prime Agent), already installed, at /opt/ucloud/toolkits/vf-agents/<tree>.
#   build.sh IMAGE_REF BUNDLE.tar.gz...
# A bundle holds var/tmp/vf-<tree>/ directories (verifiers-ucloud offline_harness_bundle).
# Trees in several bundles must be identical. verifiers-ucloud's harness_toolkit links
# /var/tmp/vf-<tree> to the toolkit, so each harness's install finds itself done:
# Node and OpenCode check their binary's version, Pi its mcp/.versions stamp. The
# install lock each harness takes inside its directory ships empty; a sandbox's
# writes land in its own copy-on-write upper.
# Prime Agent's bundle also holds its Python kernel under /root: the venv
# (.prime/agent/kernel-venv) and the uv Python it was made from (.local/share/uv/python).
# Both become the vf-prime-kernel tree, their absolute paths rewritten to it, and the
# toolkit's `links` file asks for /root/.prime/agent/kernel-venv, where the harness and
# Prime Agent look for the kernel. The bundle's uv (.local/bin) is left out: Prime Agent
# only runs it to rebuild a kernel, which a relay-only sandbox cannot do.
# Publish it with publish-environment --environment-allow-path opt/ucloud/toolkits/vf-agents,
# then register it with toolkit-register.
set -euo pipefail
[[ $# -ge 2 ]] || { echo "usage: $0 IMAGE_REF BUNDLE.tar.gz..." >&2; exit 2; }
readonly IMAGE_REF="$1" TOOLKIT=vf-agents
shift
context=$(mktemp -d -t vf-agents-toolkit.XXXXXXXX)
cleanup() { [[ "$context" == "${TMPDIR:-/tmp}"/vf-agents-toolkit.* && -d "$context" ]] && rm -r -- "$context"; }
trap cleanup EXIT
python3 - "$context" "$TOOLKIT" "$@" <<'PY'
import hashlib, json, posixpath, re, sys, tarfile
from pathlib import Path, PurePosixPath

context, toolkit, bundles = Path(sys.argv[1]), sys.argv[2], sys.argv[3:]
target = context / "opt/ucloud/toolkits" / toolkit
trees, sources = {}, []
# Trees a bundle holds outside /var/tmp: where each goes in the toolkit (as
# var/tmp/vf-<tree>/...), and the link that puts it back where it is looked for.
RELOCATED = {
    "root/.prime/agent/kernel-venv": "var/tmp/vf-prime-kernel/kernel-venv",
    "root/.local/share/uv/python": "var/tmp/vf-prime-kernel/python",
}
LINKS = {"/root/.prime/agent/kernel-venv": "/var/tmp/vf-prime-kernel/kernel-venv"}
DROPPED = ("root/.local/bin",)


def relocated(name):
    for old, new in RELOCATED.items():
        if name == old or name.startswith(old + "/"):
            return new + name[len(old):]
    return None


def relocated_path(path):
    """An absolute path into a relocated tree, at its place in the toolkit's /var/tmp."""
    moved = relocated(path.lstrip("/")) if path.startswith("/") else None
    return "/" + moved if moved else None

def digest(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()

for bundle in bundles:
    with tarfile.open(bundle, "r:gz") as tar:
        members, listing = [], {}
        for m in tar.getmembers():
            name = m.name.rstrip("/")
            if any(name == d or name.startswith(d + "/") or d.startswith(name + "/") for d in DROPPED):
                continue
            if m.islnk():  # Hard links name their target by its bundle path.
                m.linkname = relocated(m.linkname) or m.linkname
            if relocated(name):
                m.name = relocated(name)
            elif any(old.startswith(name + "/") for old in RELOCATED):
                continue  # A parent of a relocated tree (root/, root/.prime/, ...).
            parts = PurePosixPath(m.name).parts
            if m.name.startswith("/") or ".." in parts or len(parts) < 3 or parts[:2] != ("var", "tmp") \
                    or not parts[2].startswith("vf-"):
                raise SystemExit(f"{bundle}: unexpected member {m.name}")
            if m.mode & 0o6000 or not (m.isfile() or m.isdir() or m.issym() or m.islnk()):
                raise SystemExit(f"{bundle}: unsupported member {m.name}")
            if m.issym() and m.linkname.startswith("/"):
                moved = relocated_path(m.linkname)
                if moved is None:
                    raise SystemExit(f"{bundle}: absolute symlink {m.name}")
                # Relative, so it resolves wherever the toolkit is mounted.
                m.linkname = posixpath.relpath(moved, posixpath.dirname("/" + m.name))
            body = hashlib.sha256(tar.extractfile(m).read()).hexdigest() if m.isfile() else m.linkname
            listing.setdefault(parts[2], {})[m.name] = (m.type, m.mode & 0o7777, body)
            members.append(m)
        for tree, files in listing.items():
            if tree in trees and trees[tree] != files:
                raise SystemExit(f"{bundle}: {tree} differs from an earlier bundle's")
            if tree not in trees:
                trees[tree] = files
                tar.extractall(context / "stage", members=[m for m in members if m.name in files],
                               filter="fully_trusted")
    sources.append({"bundle": Path(bundle).name, "sha256": digest(bundle)})

target.parent.mkdir(parents=True)
(context / "stage/var/tmp").rename(target)
(context / "stage/var").rmdir(); (context / "stage").rmdir()
for tree in trees:  # Every harness's lock, so flock never needs to create one.
    (target / tree / "install.lock").touch(exist_ok=True)
links = {}
for cfg in target.glob("vf-*/**/pyvenv.cfg"):  # A venv names its base Python by absolute path.
    text = cfg.read_text()
    home = re.search(r"^home = (.*)$", text, re.M)
    moved = relocated_path(home.group(1)) if home else None
    if moved:
        cfg.write_text(text.replace(home.group(0), f"home = {moved}"))
for link, tree in LINKS.items():
    if (target / tree.removeprefix("/var/tmp/")).is_dir():
        links[link] = tree
if links:  # Read by verifiers-ucloud's harness_toolkit: one "LINK TARGET" per line.
    (target / "links").write_text("".join(f"{link} {tree}\n" for link, tree in sorted(links.items())))
manifest = {"name": toolkit, "trees": sorted(trees), "links": links, "sources": sources}
pi = target / "vf-pi/mcp/.versions"
if pi.exists():
    manifest["pi_versions"] = pi.read_text().strip()
(target / "toolkit.json").write_text(json.dumps(manifest, indent=2) + "\n")
print(json.dumps(manifest))
PY
printf 'FROM scratch\nCOPY opt /opt\n' > "$context/Dockerfile"
docker build -t "$IMAGE_REF" "$context"
echo "built $IMAGE_REF"
