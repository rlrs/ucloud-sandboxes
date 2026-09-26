"""Outside worker <-> public relay <-> Hetzner sandbox round trip.

usage: relay_e2e.py <gateway-url> <relay-url> <token-dir>
"""
import asyncio, json, sys, threading, uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, "/Users/Rasmus/Git/ucloud-sandboxes/ucloud-sandboxes-sdk/src")
from ucloud_sandboxes_sdk import AsyncRelayWorkerClient
from ucloud_sandboxes_sdk.client import Image, SandboxClient, SandboxSpec

gateway, relay_url, tokens = sys.argv[1], sys.argv[2], Path(sys.argv[3])


class Upstream(BaseHTTPRequestHandler):
    """Stands in for the local model server on the GPU host."""
    protocol_version = "HTTP/1.1"
    def log_message(self, *a): pass
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        out = json.dumps({"echo": json.loads(body or b"{}"), "path": self.path,
                          "auth": self.headers.get("Authorization")}).encode()
        self.send_response(200); self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out))); self.end_headers(); self.wfile.write(out)


server = ThreadingHTTPServer(("127.0.0.1", 0), Upstream)
threading.Thread(target=server.serve_forever, daemon=True).start()
upstream = f"http://127.0.0.1:{server.server_address[1]}"
sandboxes = SandboxClient(gateway, timeout_seconds=600, api_token=(tokens / "sandbox-api-token").read_text().strip())
image = Image.from_registry("python@sha256:9d2e5553305c7c7b0097999bb17187c69b921ccd6bc9d40e4bb5ebe652c00285")
PROBE = """import json,sys,urllib.request
req=urllib.request.Request(sys.argv[1]+'v1/chat/completions',data=json.dumps({'hello':'hetzner'}).encode(),
  headers={'Content-Type':'application/json','Authorization':'Bearer upstream-session'},method='POST')
print(urllib.request.urlopen(req,timeout=60).read().decode())"""


async def main():
    rollout = f"relay-e2e-{uuid.uuid4().hex[:8]}"
    async with AsyncRelayWorkerClient(relay_url, worker_token=(tokens / "relay-worker-token").read_text().strip()) as relay:
        async with relay.rollout_session(rollout, worker_id="laptop-e2e", metadata={"consumer": "e2e"}) as tunnel:
            cancel = asyncio.Event()
            worker = asyncio.create_task(tunnel.run(upstream_base_url=upstream, cancel=cancel,
                                                    max_concurrency=2, poll_timeout_seconds=10, lease_seconds=120))
            print("tunnel host:", tunnel.base_url.split("/tunnels/")[0])
            def in_sandbox():
                handle = sandboxes.create_sandbox(SandboxSpec(id=rollout, image=image, memory_mb=512, cpus=1,
                                                              disk_mb=1024, command=("sleep", "infinity")))
                try:
                    result = handle.exec(["python3", "-c", PROBE, tunnel.base_url], timeout_seconds=120)
                    return result.exit_code, result.stdout, result.stderr
                finally:
                    handle.delete()
            code, out, err = await asyncio.to_thread(in_sandbox)
            cancel.set(); worker.cancel()
            print("exit", code); print("stdout", (out.decode() if isinstance(out, bytes) else out)[:400])
            if code: print("stderr", (err.decode() if isinstance(err, bytes) else err)[-800:])

asyncio.run(main())
