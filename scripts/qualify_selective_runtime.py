#!/usr/bin/env python3
"""Smoke only explicitly selected owned semantic canaries through the real SDK.

Credentials stay on the gateway. Each source is copied by a bounded managed
FROM build, acquiring its signed environment through the normal pipeline, then
tested in one 1-CPU/512-MiB sandbox at a time. Only fresh owned sandbox IDs are
deleted. The source tags, images, registry cache, and other sandboxes are kept.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sys
import time
from urllib.parse import urlsplit
from uuid import uuid4

CASES = ("links", "whiteout-opaque", "missing-parent", "cross-layer-hardlink")
SDK_WHEEL = "/work/ucloud-sandboxes/sdk-status-0.4.33-20260928/ucloud_sandboxes_sdk-0.4.33-py3-none-any.whl"

# This program only reads the generated fixture. No receipt-supplied code is
# executed; payloads are a JSON argument, never interpolated into a shell.
CHECK_PROGRAM = r'''
import hashlib,json,os,stat,subprocess,sys
from pathlib import Path
expected=json.loads(sys.argv[1])
identity={'uid':os.geteuid(),'gid':os.getegid()}
assert identity==expected['execution_identity'],('execution_identity',identity,expected['execution_identity'])
root=Path('/')/expected['prefix']
observed={}
for relative,item in sorted(expected['entries'].items(),key=lambda pair:(pair[0].count('/'),pair[0])):
    path=root/relative if relative else root
    info=path.lstat()
    kind=('directory' if stat.S_ISDIR(info.st_mode) else 'file' if stat.S_ISREG(info.st_mode)
          else 'symlink' if stat.S_ISLNK(info.st_mode) else 'other')
    assert kind==item['kind'],('kind',relative,kind,item['kind'])
    actual={'kind':kind,'mode':stat.S_IMODE(info.st_mode),'uid':info.st_uid,'gid':info.st_gid,
            'mtime_ns':info.st_mtime_ns,'inode':info.st_ino,'device':info.st_dev,'nlink':info.st_nlink}
    if not item.get('metadata_inferred'):
        for field in ('mode','uid','gid'):
            assert actual[field]==item[field],(field,relative,actual[field],item[field])
        assert info.st_mtime_ns==0,('erofs_fixed_mtime',relative,info.st_mtime_ns)
    if kind=='file':
        assert info.st_size==item['size'],('size',relative,info.st_size,item['size'])
        fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
        with os.fdopen(fd,'rb') as stream: body=stream.read(4097)
        assert len(body)<=4096,('fixture_size_bound',relative)
        actual['content_sha256']='sha256:'+hashlib.sha256(body).hexdigest()
        assert actual['content_sha256']==item['content_sha256'],('content',relative)
    elif kind=='symlink':
        actual['link']=os.readlink(path)
        assert actual['link']==item['link'],('symlink_target',relative)
    observed[relative]=actual
for relative in expected['absent']:
    assert not os.path.lexists(root/relative),('must_be_absent',relative)
for left,right in expected['hardlinks']:
    a,b=observed[left],observed[right]
    assert (a['device'],a['inode'])==(b['device'],b['inode']),('hardlink_identity',left,right)
if expected['case']=='links':
    result=subprocess.run([str(root/'links/executable')],capture_output=True,text=True,timeout=10)
    assert result.returncode==0 and result.stdout=='semantic-canary\n',('executable',result.returncode)
print(json.dumps({'verified':True,'case':expected['case'],'observed':observed,
                  'execution_identity':identity,'verified_absent':expected['absent'],'verified_hardlinks':expected['hardlinks']}))
'''


def stamp():
    return datetime.now(timezone.utc).isoformat()


def validate_receipt(receipt, selected):
    if not re.fullmatch(r"[0-9a-f]{16}", receipt.get("run_id", "")):
        raise ValueError("Invalid owned semantic run ID")
    if not re.fullmatch(r"ucloud-managed/[a-z0-9]+(?:[._-][a-z0-9]+)*", receipt.get("source_repository", "")):
        raise ValueError("Source must be an owned managed benchmark repository")
    by_case = {item.get("case"): item for item in receipt.get("cases", [])}
    if len(by_case) != len(receipt.get("cases", [])) or not 1 <= len(by_case) <= 4:
        raise ValueError("Invalid or duplicated semantic cases")
    if not selected:
        if receipt.get("complete") is not True:
            raise ValueError("Incomplete source receipt requires an explicit --cases selection")
        selected = list(by_case)
    if len(selected) != len(set(selected)):
        raise ValueError("Selected semantic cases must be unique")
    output = []
    for case in selected:
        record = by_case.get(case, {})
        if case not in CASES or record.get("equivalent") is not True:
            raise ValueError("Selected case lacks a successful exact EROFS comparison: " + case)
        if record.get("tag") != "qual-" + receipt["run_id"] + "-" + case:
            raise ValueError("Source tag does not belong to this semantic run")
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", record.get("manifest", "")):
            raise ValueError("Selected case lacks a pinned immutable manifest")
        output.append(record)
    return output


def expected_filesystem(run_id, case, tar_proof):
    # Independently regenerate only the fixed reviewed fixture, then require
    # the receipt to prove exactly these tar bytes/headers were published.
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from qualify_selective_semantics import fixture_layers
    prefix = "ucloud-qual-" + run_id
    lower, cases = fixture_layers(prefix)
    expected_proof = [{k: v for k, v in item.items() if k != "payload"} for item in (lower, cases[case])]
    # gzip headers/deflate choices can vary across Python/zlib versions on the
    # builder and gateway. Verify uncompressed tar identity and literal header
    # proof; the immutable OCI source separately binds compressed blob bytes.
    if not isinstance(tar_proof, list) or len(tar_proof) != 2 or any(
            {key: value for key, value in actual.items() if key != "descriptor"}
            != {key: value for key, value in reviewed.items() if key != "descriptor"}
            for actual, reviewed in zip(tar_proof, expected_proof)):
        raise ValueError("Receipt's tar proof differs from the reviewed semantic fixture")
    for item in tar_proof:
        descriptor = item.get("descriptor", {})
        if (descriptor.get("mediaType") != "application/vnd.oci.image.layer.v1.tar+gzip"
                or not re.fullmatch(r"sha256:[0-9a-f]{64}", descriptor.get("digest", ""))
                or type(descriptor.get("size")) is not int or not 0 < descriptor["size"] <= 64 * 1024):
            raise ValueError("Invalid bounded compressed layer descriptor")
    entries, absent, links = {}, [], []
    for layer in expected_proof:
        members = layer["members"]
        for item in members:
            name = item["name"]
            relative = name[len(prefix):].lstrip("/")
            if relative.endswith("/.wh..wh..opq"):
                parent = relative.rsplit("/", 1)[0]
                removed = [path for path in entries if path.startswith(parent + "/")]
                for path in removed:
                    entries.pop(path)
                    absent.append(path)
                absent.append(relative)
            elif "/.wh." in relative:
                parent, filename = relative.rsplit("/", 1)
                removed = parent + "/" + filename[4:]
                for path in list(entries):
                    if path == removed or path.startswith(removed + "/"):
                        entries.pop(path)
                absent.extend((removed, relative))
            elif item["kind"] == "hardlink":
                target = item["link"][len(prefix):].lstrip("/")
                if target not in entries or entries[target]["kind"] != "file":
                    raise ValueError("Fixture hardlink target is unavailable")
                entries[relative] = dict(entries[target])
                links.append([relative, target])
            else:
                entries[relative] = {key: value for key, value in item.items() if key not in {"name", "mtime"}}
    if case == "missing-parent":
        # The OCI tar omitted this header. Docker supplies this metadata; exact
        # EROFS equality has already checked it, so do not invent an OCI value.
        entries["implicit"]["metadata_inferred"] = True
    return {"case": case, "prefix": prefix, "entries": entries, "absent": sorted(set(absent)), "hardlinks": links,
            "execution_identity": {"uid": 23123, "gid": 23124}}


def error_summary(exc):
    result = {"type": type(exc).__name__}
    status = getattr(exc, "status_code", None)
    if type(status) is int:
        result["http_status"] = status
    body = getattr(exc, "body", None)
    code = body.get("error_code") if isinstance(body, dict) else None
    if isinstance(code, str) and re.fullmatch(r"[a-z0-9_]{1,80}", code):
        result["error_code"] = code
    return result


def cleanup_owned(client, sandbox_id, owner):
    """Also fence an uncertain create; never delete a pre-existing foreign ID."""
    last = None
    for _ in range(3):
        try:
            record = client.get_sandbox(sandbox_id)
            if record is not None and record.get("spec", {}).get("labels", {}).get("qualification_owner") != owner:
                return {"deleted": False, "reason": "ownership_mismatch"}
            try:
                client.delete_sandbox(sandbox_id)
            except Exception as exc:
                if getattr(exc, "status_code", None) != 404:
                    raise
            if client.get_sandbox(sandbox_id) is None:
                time.sleep(0.2)
                if client.get_sandbox(sandbox_id) is None:
                    return {"deleted": True, "absence_observed_twice": True}
        except Exception as exc:
            last = error_summary(exc)
        time.sleep(1)
    return {"deleted": False, "last_error": last, "ttl_seconds": 600}


def run(args):
    if args.receipt.stat().st_size > 2 * 1024**2:
        raise ValueError("Semantic receipt exceeds its size bound")
    raw_receipt = args.receipt.read_bytes()
    source = json.loads(raw_receipt)
    cases = validate_receipt(source, args.cases)
    expected = {item["case"]: expected_filesystem(source["run_id"], item["case"], item["tar_proof"]) for item in cases}
    args.output_root.mkdir(mode=0o700, parents=True, exist_ok=False)
    sys.path.insert(0, str(args.sdk_wheel))
    import ucloud_sandboxes_sdk.client as sdk
    from ucloud_sandboxes.config import DeploymentConfig
    config = DeploymentConfig.from_dict(json.loads(args.deployment_config.read_text()))
    token = config.sandbox_api_token_file().read_text().strip()
    registry = urlsplit(config.registry_worker_url)
    gateway = urlsplit(args.gateway_url)
    for url in (registry, gateway):
        if url.scheme not in {"http", "https"} or not url.netloc or url.username or url.password or url.query or url.fragment:
            raise ValueError("Expected a plain configured service URL")
    client = sdk.SandboxClient(args.gateway_url, api_token=token, timeout_seconds=60)
    cleanup_client = sdk.SandboxClient(args.gateway_url, api_token=token, timeout_seconds=15)
    owner = uuid4().hex[:16]
    output = {"started_at": stamp(), "source_receipt_sha256": hashlib.sha256(raw_receipt).hexdigest(),
              "source_run_id": source["run_id"], "source_receipt_complete": source.get("complete"),
              "owner": owner, "selected_cases": [item["case"] for item in cases], "cases": [], "complete": False,
              "limits": {"concurrency": 1, "cpus": 1, "memory_mb": 512, "disk_mb": 1024,
                         "sandbox_ttl_seconds": 600, "suite_seconds_excluding_cleanup": 1200,
                         "security_user": "23123:23124"}}
    deadline = time.monotonic() + 1200

    def remaining(cap):
        seconds = min(cap, deadline - time.monotonic())
        if seconds <= 0:
            raise TimeoutError("Runtime qualification budget expired")
        return seconds

    def persist():
        (args.output_root / "runtime-smoke.json").write_text(json.dumps(output, indent=2) + "\n")

    try:
        for index, case in enumerate(cases):
            name = "qs-" + owner + "-" + str(index)
            record = {"case": case["case"], "sandbox_id": name, "image_id": name,
                      "source_manifest": case["manifest"], "source_tag": case["tag"], "started_at": stamp()}
            output["cases"].append(record)
            create_attempted = False
            try:
                if client.get_sandbox(name) is not None:
                    raise ValueError("Fresh owned sandbox ID unexpectedly exists")
                context = args.output_root / (name + "-context")
                context.mkdir()
                pinned = registry.netloc + "/" + source["source_repository"] + "@" + case["manifest"]
                (context / "Dockerfile").write_text("FROM " + pinned + "\n")
                build = client.build_image(sdk.Image.from_dockerfile(name=name, context_path=context),
                    timeout_seconds=remaining(300), poll_interval_seconds=1)
                record["build"] = {key: build.get(key) for key in ("build_id", "status", "timings", "image")}
                if build.get("status") != "succeeded":
                    raise ValueError("Owned managed import did not succeed")
                create_attempted = True
                client.create_sandbox(sdk.SandboxSpec(id=name, image=sdk.Image.from_name(name), command=["sleep", "600"],
                    cpus=1, memory_mb=512, disk_mb=1024, ttl_seconds=600,
                    security=sdk.SandboxSecuritySpec(user="23123:23124"),
                    labels={"qualification_owner": owner, "qualification": "selective-erofs", "case": case["case"]}),
                    request_timeout_seconds=remaining(300))
                executed = client.exec(name, ["python", "-c", CHECK_PROGRAM, json.dumps(expected[case["case"]])],
                                       timeout_seconds=remaining(60))
                record["exit_code"] = executed.exit_code
                if executed.exit_code != 0:
                    record["fixture_failure"] = executed.stderr[-1500:]
                    raise ValueError("Runtime semantic assertion failed")
                if len(executed.stdout) > 32 * 1024:
                    raise ValueError("Runtime fixture output exceeds its bound")
                verified = json.loads(executed.stdout)
                if verified.get("verified") is not True or verified.get("case") != case["case"]:
                    raise ValueError("Invalid runtime fixture result")
                record.update(verified=True, filesystem=verified)
            except Exception as exc:
                record["error"] = error_summary(exc)
            finally:
                if create_attempted:
                    record["cleanup"] = cleanup_owned(cleanup_client, name, owner)
                record["finished_at"] = stamp()
                persist()
            if not record.get("verified") or not record.get("cleanup", {}).get("deleted"):
                break
        output["complete"] = len(output["cases"]) == len(cases) and all(
            item.get("verified") and item.get("cleanup", {}).get("deleted") for item in output["cases"])
    finally:
        output["finished_at"] = stamp()
        persist()
    print(json.dumps({"complete": output["complete"], "cases": len(output["cases"]),
                      "receipt": str(args.output_root / "runtime-smoke.json")}))
    if not output["complete"]:
        raise SystemExit(1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--cases", nargs="+", choices=CASES)
    parser.add_argument("--sdk-wheel", type=Path, default=Path(SDK_WHEEL))
    parser.add_argument("--deployment-config", type=Path, default=Path("/etc/ucloud-sandboxes/deployment.json"))
    parser.add_argument("--gateway-url", default="https://77.42.92.27")
    run(parser.parse_args())


if __name__ == "__main__":
    main()
