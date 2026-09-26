"""Create N parkable managed-agent sandboxes running a small memory workload."""
import sys, time
sys.path.insert(0, "/Users/Rasmus/Git/ucloud-sandboxes/ucloud-sandboxes-sdk/src")
from concurrent.futures import ThreadPoolExecutor
from ucloud_sandboxes_sdk.client import Image, SandboxClient, SandboxSpec
url, token, mode, n = sys.argv[1], open(sys.argv[2]).read().strip(), sys.argv[3], int(sys.argv[4])
client = SandboxClient(url, timeout_seconds=600, api_token=token)
image = Image.from_registry("python@sha256:9d2e5553305c7c7b0097999bb17187c69b921ccd6bc9d40e4bb5ebe652c00285")
ids = [f"agent-probe-{i}" for i in range(n)]
if mode == "create":
    def make(sid):
        handle = client.create_sandbox(SandboxSpec(id=sid, image=image, memory_mb=4096, cpus=4,
                                                   disk_mb=4096, parkable=True, managed_process=True))
        # Holds ~300 MB resident, then idles like an agent between turns.
        handle.start_agent(["python3", "-c", "b = bytearray(300 * 1024 * 1024); import time; time.sleep(3600)"])
        return sid
    with ThreadPoolExecutor(8) as pool:
        print("created", list(pool.map(make, ids)))
else:
    with ThreadPoolExecutor(8) as pool:
        list(pool.map(client.delete_sandbox, ids))
    print("deleted", n)
