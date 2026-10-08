"""Sandbox builds: Dockerfile planning, the generated script (run for real as
namespace root where unshare allows), COPY --from pulls, the spool, and one
build through a fake gateway."""
import base64
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
from tempfile import TemporaryDirectory
import threading
import unittest
from unittest.mock import patch
from urllib import error

from ucloud_sandboxes import sandbox_build
from ucloud_sandboxes.sandbox_build import (BuildFailed, BuildRunner, ContextTree, ExternalImages, Spool, Unsupported,
                                            bundle, context_tar, parse_dockerfile, plan_build, words)

TEST_TIER = "contract"
BASE = {"Env": ["PATH=/usr/local/bin:/usr/bin:/bin", "LANG=C.UTF-8"], "Cmd": ["bash"], "Entrypoint": [],
        "WorkingDir": "", "User": ""}
FROM = "FROM 10.0.0.1:5000/ucloud-managed/foundation@sha256:" + "a" * 64 + "\n"


def context(files, *, owner=1000):
    """A build context tar.gz: name -> bytes, or None for a directory."""
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w", format=tarfile.PAX_FORMAT) as archive:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.uid = info.gid = owner
            info.mtime = 1_600_000_000
            if data is None:
                info.type, info.mode = tarfile.DIRTYPE, 0o755
                archive.addfile(info)
            else:
                info.size, info.mode = len(data), 0o755 if name.endswith(".sh") else 0o644
                archive.addfile(info, io.BytesIO(data))
    return gzip.compress(raw.getvalue(), mtime=0)


def tree(files):
    return ContextTree.of(tarfile.open(fileobj=io.BytesIO(context_tar(context(files), {}))).getmembers())


class DockerfileTests(unittest.TestCase):
    def test_continuations_join_as_buildkit_does(self):
        text = ("FROM x\n# comment\nRUN apt-get install -y \\\n    a \\  \n# inside\n\n    b\n"
                "copy --from=img /uv /uvx /bin/\n  ENV A=1\n")
        items = parse_dockerfile(text)
        self.assertEqual([item.keyword for item in items], ["FROM", "RUN", "COPY", "ENV"])
        self.assertEqual(items[1].args, "apt-get install -y     a     b")
        self.assertEqual((items[2].flags, items[2].args), (("--from=img",), "/uv /uvx /bin/"))

    def test_words_quote_and_expand_like_docker(self):
        env = {"A": "1", "E": "", "P": "/bin"}
        self.assertEqual(words('x="a b" y=\'$A\' z=$A${A}', env), ["x=a b", "y=$A", "z=11"])
        self.assertEqual(words("${E:-d}|${E-d}|${A:+set}|${N:+set}|$N.|\\$A", env, split=False), "d||set||.|$A")
        self.assertEqual(words('"$P:/x" a\\ b', env), ["/bin:/x", "a b"])
        with self.assertRaises(Unsupported):
            words("${A/x/y}", env)


class PlanTests(unittest.TestCase):
    TL = FROM + ("WORKDIR /app\nCOPY ./task_file /app/task_file\nUSER root\n"
                 "COPY --from=ghcr.io/astral-sh/uv:0.9.5 /uv /uvx /usr/local/bin/\n"
                 "ENV UV_PYTHON_INSTALL_DIR=/opt/py UV_CACHE_DIR=/opt/cache\nENV PATH=\"/opt/bin:${PATH}\"\n"
                 "COPY verifier-bootstrap.sh /opt/verifier-bootstrap.sh\nRUN bash -e /opt/verifier-bootstrap.sh\n"
                 "CMD [\"sleep\", \"infinity\"]\n")

    def test_a_terminal_lego_remainder(self):
        plan = plan_build(self.TL, tree({"task_file": None, "task_file/a": b"x", "verifier-bootstrap.sh": b"true\n"}),
                          BASE)
        self.assertEqual(plan.image_config, {
            "Entrypoint": [], "Cmd": ["sleep", "infinity"], "WorkingDir": "/app", "User": "root",
            "Env": ["PATH=/opt/bin:/usr/local/bin:/usr/bin:/bin", "LANG=C.UTF-8", "UV_PYTHON_INSTALL_DIR=/opt/py",
                    "UV_CACHE_DIR=/opt/cache"]})
        self.assertEqual(plan.externals, [sandbox_build.External("ghcr.io/astral-sh/uv:0.9.5", ("/uv", "/uvx"))])
        subprocess.run(["sh", "-n"], input=plan.script.encode(), check=True)
        self.assertIn(" PATH=/opt/bin:/usr/local/bin:/usr/bin:/bin ", plan.script)
        self.assertIn("step 4 'COPY --from=ghcr.io/astral-sh/uv:0.9.5 /uv /uvx /usr/local/bin/'", plan.script)

    def test_entrypoint_resets_an_inherited_cmd_only(self):
        self.assertEqual(plan_build(FROM + "ENTRYPOINT [\"/e\"]\n", tree({}), BASE).image_config["Cmd"], [])
        config = plan_build(FROM + "CMD run\nENTRYPOINT [\"/e\"]\n", tree({}), BASE).image_config
        self.assertEqual((config["Cmd"], config["Entrypoint"]), (["/bin/sh", "-c", "run"], ["/e"]))

    def test_what_needs_a_builder(self):
        for text in ("ARG X=1\n", "ADD a /a\n", "RUN --mount=type=cache,target=/c true\n", "COPY --chown=1 a /a\n",
                     "COPY missing /m\n", "COPY --from=10.0.0.1:5000/x:1 /a /a\n", "ONBUILD RUN true\n",
                     "COPY --from=ghcr.io/a/b:1 rel /a\n", "SHELL bash\n"):
            with self.subTest(text=text), self.assertRaises(Unsupported):
                plan_build(FROM + text, tree({"a": b"x"}), BASE)

    def test_dotfiles_are_context_paths(self):
        self.assertEqual(tree({".bashrc": b"x", "d": None, "d/.hidden": b"y"}).kind(".bashrc"), "file")
        self.assertEqual(tree({"d": None, "d/.hidden": b"y"}).kind("./d"), "dir")

    def test_context_tar_owns_by_root_and_applies_changes(self):
        plain = context_tar(context({"post_install.sh": b"old", "dir": None}), {"post_install.sh": b"new"})
        with tarfile.open(fileobj=io.BytesIO(plain)) as archive:
            members = {member.name: member for member in archive}
            self.assertEqual(archive.extractfile(members["post_install.sh"]).read(), b"new")
        self.assertEqual({(m.uid, m.gid) for m in members.values()}, {(0, 0)})
        self.assertEqual(members["post_install.sh"].mode, 0o755)


def _unshare():
    try:
        return subprocess.run(["unshare", "-r", "true"], capture_output=True).returncode == 0
    except OSError:
        return False


@unittest.skipUnless(_unshare() and shutil.which("setpriv"), "needs unshare -r (namespace root)")
class ScriptTests(unittest.TestCase):
    """The generated script, run as namespace root against a temporary root."""

    def build(self, dockerfile, files, externals=()):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        text = dockerfile.replace("@ROOT@", str(root))
        build_dir, build_tmp = root / "var/tmp/.ucloud-build", root / "var/tmp/.ucloud-build-tmp"
        archive = context(files)
        plan = plan_build(text, ContextTree.of(tarfile.open(fileobj=io.BytesIO(context_tar(archive, {})))
                                               .getmembers()), BASE, build_dir=str(build_dir),
                          build_tmp=str(build_tmp), apt_proxy=("http://cache:3142", ("archive.ubuntu.com",)))
        build_dir.mkdir(parents=True)
        with gzip.open(io.BytesIO(bundle(plan, context_tar(archive, {}), list(externals), "x"))) as zipped, \
                tarfile.open(fileobj=zipped) as payload:
            payload.extractall(build_dir)
        result = subprocess.run(["unshare", "-r", "sh", str(build_dir / "build.sh")], capture_output=True, text=True)
        return root, plan, result

    def test_steps_copy_and_run_as_docker_would(self):
        root, plan, result = self.build(
            FROM + "ENV GREETING=\"hello world\"\nWORKDIR @ROOT@/app\nCOPY task task\nCOPY run.sh .\n"
                   "COPY a.txt b.txt @ROOT@/multi/\nCOPY a.txt @ROOT@/renamed.txt\n"
                   "RUN echo \"$GREETING:$(pwd):$HOME:$TMPDIR:$APT_CONFIG\" > out && ./run.sh\n"
                   "RUN [\"sh\", \"-c\", \"echo exec-form > exec\"]\nRUN rm out2\n",
            {"task": None, "task/.hidden": b"h", "task/sub": None, "task/sub/f": b"f", "run.sh": b"echo ran > out2\n",
             "a.txt": b"a", "b.txt": b"b"})
        self.assertEqual(result.returncode, 0, result.stderr)
        app = root / "app"
        self.assertEqual((app / "out").read_text().strip(),
                         f"hello world:{app}:/root:{root}/var/tmp/.ucloud-build-tmp:{root}/var/tmp/.ucloud-build/apt.conf")
        self.assertEqual((app / "task/.hidden").read_bytes(), b"h")  # A directory's contents, dotfiles too.
        self.assertEqual((app / "task/sub/f").read_bytes(), b"f")
        self.assertTrue(os.access(app / "run.sh", os.X_OK))  # Modes kept.
        self.assertEqual(sorted(p.name for p in (root / "multi").iterdir()), ["a.txt", "b.txt"])
        self.assertEqual((root / "renamed.txt").read_bytes(), b"a")
        self.assertEqual((app / "exec").read_text(), "exec-form\n")
        self.assertFalse((app / "out2").exists())
        self.assertFalse((root / "var/tmp/.ucloud-build").exists())  # The bundle and TMPDIR are gone.
        self.assertFalse((root / "var/tmp/.ucloud-build-tmp").exists())
        self.assertIn("##ucloud-done", result.stderr)

    def test_a_failing_copy_stops_the_build(self):
        root, _, result = self.build(FROM + "COPY a /proc/denied/a\nRUN touch @ROOT@/never\n", {"a": b"x"})
        self.assertEqual(result.returncode, 1)
        self.assertIn("##ucloud-failed 1", result.stderr)
        self.assertFalse((root / "never").exists())

    def test_a_failing_step_names_itself(self):
        root, _, result = self.build(FROM + "RUN true\nRUN echo boom >&2; exit 7\nRUN touch @ROOT@/never\n", {})
        self.assertEqual(result.returncode, 1)
        self.assertIn("##ucloud-failed 2 7", result.stderr)
        self.assertIn("boom", result.stderr)
        self.assertEqual((root / "var/tmp/.ucloud-build/failed").read_text().split(), ["2", "7"])
        self.assertFalse((root / "never").exists())

    def test_copy_from_files_land_like_context_files(self):
        external = io.BytesIO()
        with tarfile.open(fileobj=external, mode="w") as archive:
            for name in ("uv", "uvx"):
                info = tarfile.TarInfo(name)
                info.size, info.mode = 3, 0o755
                archive.addfile(info, io.BytesIO(b"bin"))
        root, _, result = self.build(FROM + "COPY --from=ghcr.io/astral-sh/uv:0.9.5 /uv /uvx @ROOT@/usr/local/bin/\n",
                                     {}, [external.getvalue()])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(os.access(root / "usr/local/bin/uvx", os.X_OK))


class FakeRegistry:
    """Anonymous-token registry over a fake opener: index, manifest, blobs, a redirect."""

    def __init__(self):
        layer1 = self.layer({"uv": b"uv-1", "uvx": b"uvx-1", "etc/x": b"x"})
        layer2 = self.layer({".wh.uvx": b"", "uvx": b"uvx-2"})
        self.blobs = {self.digest(layer1): layer1, self.digest(layer2): layer2}
        manifest = json.dumps({"layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                                           "digest": d} for d in self.blobs]}).encode()
        self.blobs[self.digest(manifest)] = manifest
        self.index = json.dumps({"manifests": [
            {"digest": "sha256:" + "0" * 64, "platform": {"os": "linux", "architecture": "arm64"}},
            {"digest": self.digest(manifest), "platform": {"os": "linux", "architecture": "amd64"}}]}).encode()
        self.requests = []

    @staticmethod
    def digest(data):
        return "sha256:" + hashlib.sha256(data).hexdigest()

    @staticmethod
    def layer(files):
        raw = io.BytesIO()
        with tarfile.open(fileobj=raw, mode="w") as archive:
            for name, data in files.items():
                info = tarfile.TarInfo(name)
                info.size, info.mode = len(data), 0o755
                archive.addfile(info, io.BytesIO(data))
        return gzip.compress(raw.getvalue(), mtime=0)

    def open(self, req, timeout=None):
        url, auth = req.full_url, req.headers.get("Authorization")
        self.requests.append((url, auth))
        if auth != "Bearer t0k":
            raise error.HTTPError(url, 401, "auth", {"WWW-Authenticate":
                                  'Bearer realm="https://ghcr.io/token",service="ghcr.io",scope="repository:a/uv:pull"'},
                                  None)
        path = url.split("ghcr.io", 1)[1]
        if path.endswith("/manifests/0.9.5"):
            return Response(self.index)
        if "/blobs/" in path or "/manifests/sha256:" in path:
            digest = path.rsplit("/", 1)[1]
            if "/blobs/" in path:  # Blob storage: redirected, no token there.
                raise error.HTTPError(url, 307, "redirect", {"Location": "https://blobs.example/" + digest}, None)
            return Response(self.blobs[digest])
        raise error.HTTPError(url, 404, "missing", {}, None)


class Response(io.BytesIO):
    headers = {"Content-Type": "application/json"}


class ExternalImageTests(unittest.TestCase):
    def test_paths_come_from_the_amd64_image_with_whiteouts_applied(self):
        registry = FakeRegistry()

        def urlopen(req, timeout=None):
            url = req if isinstance(req, str) else req.full_url
            if url.startswith("https://ghcr.io/token"):
                return Response(json.dumps({"token": "t0k"}).encode())
            self.assertIsNone(getattr(req, "headers", {}).get("Authorization"))
            return Response(registry.blobs[url.rsplit("/", 1)[1]])

        with TemporaryDirectory() as cache, patch.object(sandbox_build.request, "urlopen", urlopen):
            images = ExternalImages(cache, opener=registry)
            payload = images.files("ghcr.io/a/uv:0.9.5", ("/uv", "/uvx"))
            with tarfile.open(fileobj=io.BytesIO(payload)) as archive:
                contents = {m.name: archive.extractfile(m).read() for m in archive}
            self.assertEqual(contents, {"uv": b"uv-1", "uvx": b"uvx-2"})
            requests = len(registry.requests)
            self.assertEqual(images.files("ghcr.io/a/uv:0.9.5", ("/uvx", "/uv")), payload)  # Cached by digest.
            self.assertEqual(len(registry.requests), requests + 2)  # Only the index and manifest again.
            with self.assertRaises(BuildFailed):
                images.files("ghcr.io/a/uv:0.9.5", ("/missing",))

    def test_only_public_registries(self):
        self.assertEqual(sandbox_build.external_reference("python:3.12")[:2], ("registry-1.docker.io", "library/python"))
        for reference in ("10.36.101.16:5000/ucloud-managed/x:1", "localhost/x", "evil.example/x"):
            with self.subTest(reference=reference), self.assertRaises(Unsupported):
                sandbox_build.external_reference(reference)


class SpoolTests(unittest.TestCase):
    def test_a_job_runs_once_and_its_result_answers_only_its_build(self):
        with TemporaryDirectory() as directory:
            spool = Spool(directory)
            job = {"image_id": "recipe-1", "build_id": "sandbox:recipe-1:a", "repository": "r", "tag": "t:latest"}
            spool.submit(job)
            self.assertEqual(spool.status("recipe-1", "sandbox:recipe-1:a"), ("running", None))
            self.assertEqual(spool.status("recipe-1", "sandbox:recipe-1:b"), (None, None))  # Lost: resubmit.
            ran = []

            class Runner:
                def run(self, job):
                    ran.append(job["build_id"])
                    return {"status": "succeeded", "root": "sha256:" + "1" * 64}

            stop = threading.Event()
            thread = threading.Thread(target=sandbox_build.serve, args=(spool, Runner()),
                                      kwargs={"poll": 0.05, "stop": stop})
            thread.start()
            for _ in range(200):  # The result is written first, then the job removed.
                if spool.status("recipe-1", "sandbox:recipe-1:a")[0] == "succeeded" and not spool.pending():
                    break
                threading.Event().wait(0.02)
            stop.set()
            thread.join()
            status, result = spool.status("recipe-1", "sandbox:recipe-1:a")
            self.assertEqual((status, result["root"], ran), ("succeeded", "sha256:" + "1" * 64, ["sandbox:recipe-1:a"]))
            self.assertEqual(spool.pending(), [])


class FakeGateway(sandbox_build.GatewayApi):
    def __init__(self, *, exit_code=0):
        super().__init__("http://gateway", "token")
        self.calls, self.exit_code, self.polls = [], exit_code, 0

    def call(self, method, path, payload=None, *, body=None, content_type="application/json", timeout=None,
             raw=False):
        self.calls.append((method, path.split("?")[0]))
        if method == "POST" and path == "/v1/sandboxes":
            self.spec = payload
            return 200, {"sandbox": {"id": payload["id"], "generation": 3}}
        if path.endswith("/jobs"):
            return 200, {"job": {"state": "running"}}
        if "/jobs/" in path and "/logs/" in path:
            return 200, {"data": base64.b64encode(b"E: Unable to locate package nope\n").decode()}
        if "/jobs/" in path:
            self.polls += 1
            done = self.polls > 1
            return 200, {"job": {"state": "exited" if done else "running", "exit_code": self.exit_code,
                                 "stderr_bytes": 40}}
        if path.split("?")[0].endswith("/files"):
            return 200, b"4 100\n"
        if path.endswith("/commit-export"):
            self.export_request = payload
            state = "staged" if self.calls.count(("POST", path)) > 1 else "exporting"
            return 200, {"export": {"schema": "ucloud-commit-export-v1", "sandbox_id": self.spec["id"],
                                    "request": payload, "state": state, "was_paused": False, "identity": [],
                                    "repository": sandbox_build_staging(payload["image_id"]),
                                    "blob_digest": "sha256:" + "b" * 64 if state == "staged" else "",
                                    "size": 10 if state == "staged" else 0, "error_code": ""}}
        return 200, {}


def sandbox_build_staging(image_id):
    from ucloud_sandboxes.commit_policy import staging_repository
    return staging_repository(image_id)


class RunnerTests(unittest.TestCase):
    def job(self):
        archive = context({"post_install.sh": b"echo hi\n"})
        return {"image_id": "recipe-" + "c" * 40, "build_id": "sandbox:x", "repository": "ucloud-managed/recipe-x",
                "tag": "t", "dockerfile": FROM + "COPY post_install.sh /tmp/p.sh\nRUN sh /tmp/p.sh\n",
                "context_tar": base64.b64encode(context_tar(archive, {})).decode(), "base_config": BASE,
                "base_reference": FROM.split()[1], "parent_root": "sha256:" + "d" * 64}

    def runner(self, gateway, converter):
        return BuildRunner(gateway=gateway, registry_client=object(), converter=lambda: converter,
                           externals=None, work_root=self.work, poll=0)

    def setUp(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.work = Path(temporary.name)

    def test_a_build_exports_filters_and_stacks_then_deletes_its_sandbox(self):
        gateway, stacked = FakeGateway(), {}

        class Converter:
            def extend(self, parent, layer, diff_id, *, image_config, repository):
                stacked.update(parent=parent, layer=layer.read_bytes(), diff_id=diff_id, config=image_config,
                               repository=repository)
                return {"root": "sha256:" + "e" * 64, "config": "sha256:" + "f" * 64, "config_size": 9,
                        "metrics": {"chunks_new": 2}}

        def filtered(client, commit, root, timeout_seconds):
            from ucloud_sandboxes.commit_policy import FilterResult
            self.assertEqual(commit.policy.exclude, (sandbox_build.BUILD_DIR, sandbox_build.BUILD_TMP))
            self.assertTrue(commit.policy.keep_build_residue)
            self.assertEqual((commit.generation, commit.parent_root), (3, "sha256:" + "d" * 64))
            Path(root, "filtered.tar").write_bytes(b"layer")
            return FilterResult("sha256:" + "9" * 64, 5, 1, {})

        with patch("ucloud_sandboxes.environment_prepare.prepare_commit_in_subprocess", filtered):
            result = self.runner(gateway, Converter()).run(self.job())
        self.assertEqual(result["status"], "succeeded", result)
        self.assertEqual((result["root"], result["diff_id"]), ("sha256:" + "e" * 64, "sha256:" + "9" * 64))
        self.assertEqual(stacked["layer"], b"layer")
        self.assertEqual(stacked["config"]["Cmd"], ["bash"])
        self.assertEqual(gateway.export_request["resume"], False)
        self.assertEqual(gateway.calls[-1][0], "DELETE")
        self.assertTrue(result["manifest_digest"].startswith("sha256:"))
        self.assertTrue(gateway.spec["managed_process"] and gateway.spec["parkable"])
        from ucloud_sandboxes.sandbox import SandboxSpec
        self.assertEqual(SandboxSpec.from_dict(gateway.spec).filesystem.tmpfs_mb, 4096)  # Half the memory.

    def test_filtering_and_stacking_share_the_local_slots(self):
        active, peak, guard = [0], [0], threading.Lock()

        class Converter:
            def extend(self, parent, layer, diff_id, *, image_config, repository):
                with guard:
                    active[0] += 1
                    peak[0] = max(peak[0], active[0])
                threading.Event().wait(0.05)
                with guard:
                    active[0] -= 1
                return {"root": "sha256:" + "e" * 64, "config": "sha256:" + "f" * 64, "config_size": 9, "metrics": {}}

        def filtered(client, commit, root, timeout_seconds):
            from ucloud_sandboxes.commit_policy import FilterResult
            Path(root, "filtered.tar").write_bytes(b"layer")
            return FilterResult("sha256:" + "9" * 64, 5, 1, {})

        shared, results = threading.BoundedSemaphore(1), []

        def one():  # A fake gateway per build (it counts its own calls); one service's slots.
            runner = BuildRunner(gateway=FakeGateway(), registry_client=object(), converter=Converter, externals=None,
                                 work_root=self.work, poll=0)
            runner.local = shared
            results.append(runner.run(self.job()))

        with patch("ucloud_sandboxes.environment_prepare.prepare_commit_in_subprocess", filtered):
            threads = [threading.Thread(target=one) for _ in range(3)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
        self.assertEqual([result["status"] for result in results], ["succeeded"] * 3, results)
        self.assertEqual(peak[0], 1)

    def test_a_failed_step_is_the_recipes_failure_and_the_sandbox_goes(self):
        gateway = FakeGateway(exit_code=1)
        result = self.runner(gateway, None).run(self.job())
        self.assertEqual(result["status"], "failed")
        self.assertIn("step 4 exited 100", result["error"])
        self.assertIn("Unable to locate package", result["error"])
        self.assertEqual(gateway.calls[-1][0], "DELETE")
        self.assertNotIn(("POST", f"/v1/sandboxes/{gateway.spec['id']}/commit-export"), gateway.calls)


if __name__ == "__main__":
    unittest.main()


class GatewaySideTests(unittest.TestCase):
    """SandboxBuilds against a real catalog, image_roots and chunk-store root."""

    PREPARED = "private:5000/prepared:base@sha256:" + "b" * 64
    TEXT = ("FROM ubuntu:22.04\nENV DEBIAN_FRONTEND=noninteractive\nCOPY base_install.sh /tmp/base_install.sh\n"
            "RUN bash /tmp/base_install.sh && rm /tmp/base_install.sh\n")

    def setUp(self):
        from tests.chunk_store_support import REPOSITORY, ChunkStoreFixture, sample_images, signing
        from ucloud_sandboxes.build_context_store import BuildContextBlobStore
        from ucloud_sandboxes.gateway.image_roots import ImageRootsStore
        from ucloud_sandboxes.gateway.sandbox_builds import SandboxBuilds
        from ucloud_sandboxes.image_foundations import tmax_foundation
        from ucloud_sandboxes.prepared_images import PreparedImageCatalog
        self.store = ChunkStoreFixture(self, signer=signing())
        sample_images(self.store.client)
        self.parent = self.store.converter.convert(REPOSITORY, "a")["root"]
        root = self.store.root
        self.roots = ImageRootsStore(root / "image-roots.sqlite3")
        self.catalog = PreparedImageCatalog(root / "prepared.sqlite3")
        self.contexts = BuildContextBlobStore(root / "contexts", max_blob_bytes=32 * 1024 ** 2)
        foundation = tmax_foundation(self.TEXT, b"apt-get install -y git\n",
                                     ubuntu_base="docker.io/library/ubuntu@sha256:" + "a" * 64)
        self.catalog.register_foundation({"validated": True, "key": foundation.key, "family": "tmax",
                                          "base": "docker.io/library/ubuntu@sha256:" + "a" * 64,
                                          "source_prefix": foundation.source_prefix, "reference": self.PREPARED})
        self.spool = Spool(root / "spool")
        self.builds = SandboxBuilds(self.spool, catalog=self.catalog, contexts=self.contexts, roots=self.roots,
                                    environments=self.store.registry,
                                    tag_for=lambda image_id: f"10.0.0.1:5000/ucloud-managed/{image_id}:latest")

    def released_base(self):
        digest = "sha256:" + "b" * 64
        self.roots.record_converted("prepared", digest, config_digest="sha256:" + "1" * 64, old_root=self.parent,
                                    new_root=self.parent, wave="w1", build_input=True)
        for state in ("switched", "released"):
            self.roots.transition("prepared", digest, state)

    def payload(self, tail, extra=None):
        archive = context({"Dockerfile": (self.TEXT + tail).encode(), "base_install.sh": b"apt-get install -y git\n",
                           "task.txt": b"task", **(extra or {})}, owner=0)
        digest = "sha256:" + hashlib.sha256(archive).hexdigest()
        self.contexts.put_with_status(digest, io.BytesIO(archive), content_length=len(archive))
        return {"id": "recipe-" + "c" * 40, "context_archive_digest": digest, "context_archive_size": len(archive),
                "dockerfile": "Dockerfile", "build_args": {}}

    def test_a_foundation_recipe_on_a_chunk_store_root_is_spooled(self):
        self.released_base()
        job, reason = self.builds.job(self.payload("COPY task.txt /task.txt\nRUN echo task\n"))
        self.assertEqual(reason, "")
        self.assertEqual(job["dockerfile"], f"FROM {self.PREPARED}\nCOPY task.txt /task.txt\nRUN echo task\n")
        self.assertEqual((job["parent_root"], job["base_kind"]), (self.parent, "foundation"))
        self.assertEqual(job["base_config"]["Cmd"], ["sh"])
        self.assertEqual(job["repository"], "ucloud-managed/" + "recipe-" + "c" * 40)
        with tarfile.open(fileobj=io.BytesIO(base64.b64decode(job["context_tar"]))) as archive:
            self.assertIn("task.txt", archive.getnames())
        build_id = self.builds.submit(self.payload("RUN echo task\n"))
        self.assertTrue(build_id.startswith("sandbox:recipe-"))
        self.assertEqual(self.builds.status(build_id), ("running", None))

    def test_what_stays_with_the_builders(self):
        payload = self.payload("RUN echo task\n")
        self.assertEqual(self.builds.job(payload), (None, "the base has no chunk-store root"))
        self.released_base()
        self.assertEqual(self.builds.job(self.payload("COPY --from=10.0.0.1:5000/x:1 /a /b\n"))[0], None)
        self.assertEqual(self.builds.job({**payload, "build_args": {"A": "1"}})[0], None)
        self.assertEqual(self.builds.job(self.payload("RUN echo task\n", {".dockerignore": b"x"}))[0], None)
        other = context({"Dockerfile": b"FROM debian:12\nRUN true\n"})
        digest = "sha256:" + hashlib.sha256(other).hexdigest()
        self.contexts.put_with_status(digest, io.BytesIO(other), content_length=len(other))
        self.assertEqual(self.builds.job({**payload, "context_archive_digest": digest})[1], "no prepared base")

    def test_a_finished_build_is_born_released_and_resolves(self):
        from tests.test_chunk_convert import commit_layer
        from ucloud_sandboxes.images import ImageRecord
        self.released_base()
        job, _ = self.builds.job(self.payload("RUN echo task\n"))
        self.spool.submit(job)
        tar, diff_id = commit_layer(self.store.root)
        stacked = self.store.converter.extend(self.parent, tar, diff_id, image_config=job["base_config"],
                                              repository=job["repository"])
        born = sandbox_build.born_manifest_digest(stacked["config"], stacked["config_size"], stacked["root"])
        self.spool.finish(job, {"status": "succeeded", "root": stacked["root"], "config": stacked["config"],
                                "diff_id": diff_id, "manifest_digest": born})
        status, build = self.builds.status(job["build_id"])
        self.assertEqual((status, build["sandbox_build"]), ("succeeded", True))
        record = ImageRecord.from_dict(self.builds.adopt(build))
        self.assertEqual((record.manifest_digest, record.source, record.pushed), (born, "build:sandbox", True))
        self.assertEqual(self.roots.dispatch_root(job["repository"], born), stacked["root"])
        self.assertEqual(self.roots.released_digest(job["repository"], "latest"), born)
        self.assertIn((job["repository"], born), self.roots.oci_released())
        self.assertEqual(self.roots.get(job["repository"], born)["wave"], "sandbox-build")
        self.builds.adopt(build)  # Idempotent: ensure may adopt again after a crash.


class ConfigTests(unittest.TestCase):
    def test_sandbox_builds_config_round_trips_and_needs_the_chunk_store(self):
        from ucloud_sandboxes.environment_config import EnvironmentDeploymentConfig, SandboxBuildsConfig
        self.assertEqual(SandboxBuildsConfig.from_dict({"enabled": True, "slots": 4}).slots, 4)
        for bad in ({"slots": 0}, {"cpus": True}, {"timeout_seconds": 10}, {"extra": 1}):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                SandboxBuildsConfig.from_dict(bad)
        with self.assertRaises(ValueError):
            EnvironmentDeploymentConfig.from_dict({"trusted_keys_file": "/k", "signing_key_file": "/s",
                                                   "sandbox_builds": {"enabled": True}})
        off = EnvironmentDeploymentConfig.from_dict({"trusted_keys_file": "/k", "sandbox_builds": {"enabled": False}})
        self.assertEqual(off.to_dict()["sandbox_builds"]["enabled"], False)
        self.assertNotIn("sandbox_builds", EnvironmentDeploymentConfig.from_dict({"trusted_keys_file": "/k"}).to_dict())
