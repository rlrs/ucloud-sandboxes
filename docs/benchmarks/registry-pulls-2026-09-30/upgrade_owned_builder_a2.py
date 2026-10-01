#!/usr/bin/env python3
"""Inspect, or explicitly upgrade one idle owned qualification builder.

Run on the builder as root. Existing runtime/dependencies and the original unit
remain untouched. --apply requires an expected node epoch and pinned wheel.
The caller must hold the owned pool and stop qualification submissions first.
"""
from __future__ import annotations

import argparse
from email.parser import BytesParser
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import time
import urllib.request
from uuid import uuid4
import zipfile


SERVICE = "ucloud-sandbox-node.service"
LABEL = "ucloud.image-build-admission-capacity"
OVERRIDE = Path("/etc/systemd/system") / (SERVICE + ".d") / "99-owned-registry-pulls-a2.conf"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def run(argv, *, timeout=30, env=None):
    result = subprocess.run(argv, capture_output=True, text=True, timeout=timeout, env=env)
    # Never include subprocess output: argv/environment may reference secrets.
    require(result.returncode == 0, "Local command failed: " + Path(argv[0]).name)
    return result.stdout


def flag(argv, name):
    require(argv.count(name) == 1, "Missing or ambiguous service flag: " + name)
    index = argv.index(name)
    require(index + 1 < len(argv), "Missing service flag value")
    return argv[index + 1]


def finishing_args(argv):
    result = list(argv)
    name = "--max-finishing-image-builds"
    if name in result:
        require(result.count(name) == 1, "Duplicate finishing flag")
        index = result.index(name)
        require(index + 1 < len(result), "Missing finishing flag value")
        result[index + 1] = "2"
    else:
        result += [name, "2"]
    return result


def systemd_argument(value):
    # systemd performs specifier and environment expansion independently of
    # shell quoting. These are literal, already expanded /proc argv values.
    require(not any(char in value for char in "\x00\n\r"), "Unsafe service argument")
    return json.dumps(value.replace("%", "%%").replace("$", "$$"), ensure_ascii=False)


def request(state, path, payload=None):
    headers = {"Authorization": "Bearer " + state["token_file"].read_text().strip()}
    data = None if payload is None else json.dumps(payload).encode()
    if data is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(state["url"] + path, data=data, headers=headers)
    with urllib.request.urlopen(req, timeout=10) as response:
        data = response.read(2 * 1024 * 1024 + 1)
    require(len(data) <= 2 * 1024 * 1024, "Node response exceeds bound")
    return json.loads(data)


def heartbeat(state):
    result = request(state, "/v1/heartbeat")["heartbeat"]
    require(result["job_id"] == state["job_id"], "Node job identity changed")
    require(result["node_epoch"] == state["node_epoch"], "Node epoch changed")
    require("image-build" in result["capabilities"] and "sandbox" not in result["capabilities"],
            "Expected dedicated builder")
    require(result["active_image_builds"] == 0 and result["active_sandboxes"] == 0,
            "Owned builder is not idle")
    return result


def owned_fence(state, token):
    current = heartbeat(state)
    require(current["draining"] is True and current["admission_open"] is False
            and current["drain_token"] == token, "Owned idle admission fence is not established")
    return current


def discover(job_id, expected_epoch=None):
    raw = run(["systemctl", "show", SERVICE, "--property=MainPID,ActiveState,FragmentPath,User"])
    properties = dict(line.split("=", 1) for line in raw.splitlines() if "=" in line)
    require(properties["ActiveState"] == "active", "Builder service is not active")
    require(properties.get("User", "") in {"", "root"},
            "This qualification helper requires the expected root builder service")
    pid = int(properties["MainPID"])
    require(pid > 1, "Builder service has no main PID")
    argv = Path(f"/proc/{pid}/cmdline").read_bytes().decode().rstrip("\0").split("\0")
    require(argv[1:4] == ["-m", "ucloud_sandboxes.cli", "serve-builder-agent"],
            "Unexpected builder service command")
    require(flag(argv, "--job-id") == job_id, "Service is not the explicitly owned job")
    env = dict(item.split("=", 1) for item in Path(f"/proc/{pid}/environ").read_bytes()
               .decode().rstrip("\0").split("\0") if "=" in item)
    pythonpath = env.get("PYTHONPATH", "")
    require(pythonpath and ":" not in pythonpath, "Expected one explicit bundled runtime")
    source = Path(pythonpath).resolve(strict=True)
    require(source.is_dir() and source.name == "site-packages", "Unexpected runtime root")
    require((source / "ucloud_sandboxes" / "images.py").is_file(), "Runtime package absent")
    port = int(flag(argv, "--port"))
    state = {"job_id": job_id, "node_epoch": expected_epoch or "", "pid": pid,
             "argv": argv, "env": env, "source": source,
             "token_file": Path(flag(argv, "--node-control-bearer-token-file")),
             "url": f"http://127.0.0.1:{port}", "unit": Path(properties["FragmentPath"])}
    initial = request(state, "/v1/heartbeat")["heartbeat"]
    if expected_epoch is None:
        state["node_epoch"] = initial["node_epoch"]
    heartbeat(state)
    return state


def receipt(state):
    current = heartbeat(state)
    return {"job_id": state["job_id"], "node_epoch": state["node_epoch"],
            "runtime_root": str(state["source"]), "service": SERVICE,
            "images_sha256": sha(state["source"] / "ucloud_sandboxes" / "images.py"),
            "unit_sha256": sha(state["unit"]), "active_builds": 0,
            "draining": current["draining"], "admission_open": current["admission_open"],
            "advertised_capacity": current["labels"].get(LABEL),
            "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}


def wheel_members(wheel):
    with zipfile.ZipFile(wheel) as archive:
        infos = archive.infolist()
        require(len(infos) <= 2000 and sum(item.file_size for item in infos) <= 64 * 1024**2,
                "Candidate wheel exceeds bounds")
        members = {}
        for item in infos:
            path = PurePosixPath(item.filename)
            require(not path.is_absolute() and ".." not in path.parts and path.parts,
                    "Unsafe wheel member path")
            require(path.parts[0] in {"ucloud_sandboxes", "ucloud_sandboxes-0.7.0.dist-info"},
                    "Wheel contains unexpected package")
            require(not stat.S_ISLNK(item.external_attr >> 16), "Wheel contains symlink")
            if item.is_dir():
                continue
            require(item.filename not in members, "Duplicate wheel member")
            members[item.filename] = archive.read(item)
    require("ucloud_sandboxes/images.py" in members and
            "ucloud_sandboxes/build_admission.py" in members, "Candidate package incomplete")
    return members


def stage(state, wheel, root):
    require(not root.exists(), "Choose an unused owned upgrade directory")
    members = wheel_members(wheel)
    name = "ucloud_sandboxes-0.7.0.dist-info"
    old_metadata = BytesParser().parsebytes((state["source"] / name / "METADATA").read_bytes())
    new_metadata = BytesParser().parsebytes(members[name + "/METADATA"])
    require(new_metadata["Name"] == old_metadata["Name"] and new_metadata["Version"] == "0.7.0",
            "Candidate distribution identity changed")
    require(sorted(new_metadata.get_all("Requires-Dist", [])) ==
            sorted(old_metadata.get_all("Requires-Dist", [])), "Candidate dependencies changed")
    require(root.parent.is_dir(), "Owned staging parent must already exist")
    root.mkdir(mode=0o755)
    root.chmod(0o755)
    target = root / "site-packages"
    shutil.copytree(state["source"], target, symlinks=True,
                    ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    for package in ("ucloud_sandboxes", name):
        path = target / package
        require(not path.is_symlink(), "Package directory unexpectedly symlinked")
        shutil.rmtree(path)
    for relative, content in members.items():
        path = target / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        path.chmod(0o644)
    for package in ("ucloud_sandboxes", name):
        (target / package).chmod(0o755)
        for directory in (target / package).rglob("*"):
            if directory.is_dir():
                directory.chmod(0o755)
    env = {**state["env"], "PYTHONPATH": str(target)}
    run([state["argv"][0], "-c", "from ucloud_sandboxes.images import ImageManager; "
         "from ucloud_sandboxes.build_admission import BUILD_ADMISSION_CAPACITY_LABEL; "
         "assert hasattr(ImageManager, 'build_admission_snapshot')"], env=env)
    run([state["argv"][0], "-m", "ucloud_sandboxes.cli", "serve-builder-agent", "--help"], env=env)
    require(all((target / relative).read_bytes() == content for relative, content in members.items()),
            "Staged package differs from wheel")
    return target


def ready(state, *, candidate=None, timeout=45):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            current = heartbeat(state)
            if candidate is not None:
                discovered = discover(state["job_id"], state["node_epoch"])
                require(discovered["source"] == candidate, "Candidate runtime not active")
                require(flag(discovered["argv"], "--max-finishing-image-builds") == "2",
                        "Finishing capacity flag missing")
                require(current["labels"].get(LABEL) == "4", "Unexpected idle admission capacity")
            return current
        except Exception:
            time.sleep(0.5)
    raise ValueError("Builder readiness deadline expired")


def apply(state, wheel, wheel_sha, root):
    require(sha(wheel) == wheel_sha, "Candidate wheel checksum mismatch")
    require(not OVERRIDE.exists(), "Owned service override already exists")
    before = receipt(state)
    require(not before["draining"] and before["admission_open"], "Builder already fenced")
    target = stage(state, wheel, root)
    token = "pipeline-upgrade-" + uuid4().hex
    result = {"before": before, "wheel_sha256": wheel_sha, "runtime_root": str(target),
              "service_changed": False, "complete": False}
    output = root / "upgrade-receipt.json"
    output.write_text(json.dumps(result, indent=2) + "\n")
    output.chmod(0o600)
    fenced = override_written = stopped = reopening = False
    try:
        require(sha(state["unit"]) == before["unit_sha256"], "Original unit changed during staging")
        # Drain fences a new build racing our idle check. Refuse to stop if
        # any existing work was admitted; the original service stays intact.
        fenced = True
        drain = request(state, "/v1/drain", {"draining": True, "token": token})["drain"]
        require(drain["ready"] and drain["active_image_builds"] == 0
                and drain["draining"] is True and drain["admission_open"] is False
                and drain["token"] == token,
                "Builder received work before drain fence")
        owned_fence(state, token)
        stopped = True
        run(["systemctl", "stop", SERVICE], timeout=60)
        require(run(["systemctl", "show", SERVICE, "--property=MainPID", "--value"]).strip() == "0",
                "Builder process did not stop")
        command = ["/usr/bin/env", "PYTHONPATH=" + str(target), *finishing_args(state["argv"])]
        OVERRIDE.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
        with OVERRIDE.open("x") as stream:
            override_written = True
            stream.write("[Service]\nExecStart=\nExecStart=" +
                         " ".join(systemd_argument(arg) for arg in command) + "\n")
        OVERRIDE.chmod(0o600)
        run(["systemctl", "daemon-reload"])
        run(["systemctl", "restart", SERVICE], timeout=60)
        current = ready(state, candidate=target)
        require(current["draining"] and current["drain_token"] == token,
                "Drain fence lost during restart")
        # After this request may have reopened admission, automatic rollback
        # must not stop newly accepted work, even if the response is lost.
        reopening = True
        reopened = request(state, "/v1/drain", {"draining": False, "token": token})["drain"]
        require(reopened["draining"] is False and reopened["admission_open"] is True,
                "Admission did not reopen")
        fenced = False
        result.update(complete=True, service_changed=True,
                      after={"job_id": state["job_id"], "node_epoch": state["node_epoch"],
                             "runtime_root": str(target), "finishing_capacity": 2})
    except BaseException as exc:
        result["error_type"] = type(exc).__name__
        try:
            if reopening:
                result["rollback_skipped"] = "admission_reopen_attempted_preserve_possible_new_work"
                raise
            if override_written:
                try:
                    owned_fence(state, token)
                except Exception:
                    # A broken candidate may have lost the fence and accepted
                    # work. Even an unreachable heartbeat cannot prove idle.
                    result["rollback_skipped"] = "candidate_fence_or_idle_unverified"
                    raise
                run(["systemctl", "stop", SERVICE], timeout=60)
                OVERRIDE.unlink()
                run(["systemctl", "daemon-reload"])
            if stopped:
                run(["systemctl", "restart", SERVICE], timeout=60)
                current = ready(state)
                restored = discover(state["job_id"], state["node_epoch"])
                require(restored["source"] == state["source"], "Original runtime was not restored")
            else:
                # A new build racing the initial idle check prevents stopping.
                # Undo only our fence, without interrupting that work.
                current = request(state, "/v1/heartbeat")["heartbeat"]
                require(current["job_id"] == state["job_id"] and
                        current["node_epoch"] == state["node_epoch"], "Node identity changed")
            if fenced and current["draining"] and current["drain_token"] == token:
                request(state, "/v1/drain", {"draining": False, "token": token})
            result["rollback_verified"] = True
        except BaseException as rollback_error:
            result["rollback_error_type"] = type(rollback_error).__name__
        raise
    finally:
        output.write_text(json.dumps(result, indent=2) + "\n")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-job-id", required=True)
    parser.add_argument("--expected-node-epoch")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--wheel", type=Path)
    parser.add_argument("--wheel-sha256")
    parser.add_argument("--output-root", type=Path)
    args = parser.parse_args()
    require(os.geteuid() == 0, "Run on owned builder as root")
    os.umask(0o022)
    require(re.fullmatch(r"[1-9][0-9]{1,19}", args.expected_job_id), "Invalid expected job identity")
    state = discover(args.expected_job_id, args.expected_node_epoch)
    if args.apply:
        require(args.expected_node_epoch and args.wheel and args.output_root and
                re.fullmatch(r"[0-9a-f]{64}", args.wheel_sha256 or ""), "Apply arguments incomplete")
        require(args.output_root.is_absolute() and args.output_root.resolve().is_relative_to(Path("/work")),
                "Expected absolute owned /work output directory")
        result = apply(state, args.wheel, args.wheel_sha256, args.output_root)
    else:
        result = receipt(state)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(json.dumps({"complete": False, "error_type": type(error).__name__}))
        raise SystemExit(1) from None
