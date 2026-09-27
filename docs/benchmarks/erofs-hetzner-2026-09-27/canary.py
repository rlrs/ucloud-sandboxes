"""EROFS canary: managed build -> EROFS worker; external image -> import -> EROFS worker."""
import json, sys, time
sys.path.insert(0, "/Users/Rasmus/Git/ucloud-sandboxes/ucloud-sandboxes-sdk/src")
from ucloud_sandboxes_sdk.client import Image, SandboxClient, SandboxSpec
url, token, ctx = sys.argv[1], open(sys.argv[2]).read().strip(), sys.argv[3]
c = SandboxClient(url, timeout_seconds=1800, api_token=token)
out = {}
def step(name, fn):
    t = time.monotonic()
    try:
        value = fn(); out[name] = {"seconds": round(time.monotonic() - t, 1)}; return value
    except Exception as exc:
        out[name] = {"seconds": round(time.monotonic() - t, 1), "error": f"{type(exc).__name__}: {exc}"[:600]}
        print(json.dumps(out, indent=1)); raise
    finally:
        print(name, out[name], flush=True)

build = step("build_managed", lambda: c.build_image(Image.from_dockerfile(name="erofs-canary-tools", context_path=ctx), timeout_seconds=1800))
out["build_managed"]["timings"] = build.get("timings")
h = step("create_managed", lambda: c.create_sandbox(SandboxSpec(id="erofs-canary-a", image=Image.from_name("erofs-canary-tools"), cpus=1, memory_mb=1024, disk_mb=4096, command=("sleep", "infinity")), request_timeout_seconds=1800))
r = step("exec_managed", lambda: h.exec(["python3", "-c", "import requests, sqlite3, ssl; print('ok', requests.__version__)"]))
out["exec_managed"]["stdout"] = r.stdout.strip()
h2 = step("create_managed_second", lambda: c.create_sandbox(SandboxSpec(id="erofs-canary-b", image=Image.from_name("erofs-canary-tools"), cpus=1, memory_mb=1024, disk_mb=4096, command=("sleep", "infinity"))))
h3 = step("create_imported_node22", lambda: c.create_sandbox(SandboxSpec(id="erofs-canary-c", image=Image.from_registry("node:22"), cpus=1, memory_mb=1024, disk_mb=4096, command=("sleep", "infinity")), request_timeout_seconds=1800))
r3 = step("exec_imported", lambda: h3.exec(["node", "-e", "console.log('ok', process.version)"]))
out["exec_imported"]["stdout"] = r3.stdout.strip()
h4 = step("create_imported_second", lambda: c.create_sandbox(SandboxSpec(id="erofs-canary-d", image=Image.from_registry("node:22"), cpus=1, memory_mb=1024, disk_mb=4096, command=("sleep", "infinity"))))
print(json.dumps(out, indent=1))
json.dump(out, open(sys.argv[4], "w"), indent=1)
