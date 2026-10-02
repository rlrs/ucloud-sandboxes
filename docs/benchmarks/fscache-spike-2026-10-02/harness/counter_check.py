"""Check the loopback byte counter against a known transfer through our registry client."""
import json, sys, subprocess
sys.path.insert(0, "/root/s11")
import s11lib as L
reg = L.env_registry()
comp = reg.load("sha256:6141557383eebf40509a184b43b0ba4e429b9b7495c8ebcc039b3c2bd41e07ff")
out = {}
for label, n in (("client_256KiB_x64", 64), ):
    a = L.nft_counters()["proxy_tx"]
    got = 0
    for i in range(n):
        got += len(reg.client.blob_range("environments", comp.image_digest, (i * 37 % 2800) * 262144, 262144))
    b = L.nft_counters()["proxy_tx"]
    out[label] = {"payload_bytes": got, "counter_delta": b - a}
a = L.nft_counters()["proxy_tx"]
p = subprocess.run(["curl", "-s", "-o", "/dev/null", "-w", "%{size_download}", "-r", "0-33554431",
                    f"http://{L.PROXY_REG}/v2/environments/blobs/{comp.image_digest}"], capture_output=True, text=True)
b = L.nft_counters()["proxy_tx"]
out["curl_32MiB"] = {"payload_bytes": int(p.stdout), "counter_delta": b - a}
a = L.nft_counters()["nydus_tx"]
_, boot, blobs = L.nydus_layers("scaleswe-responses-0")
p = subprocess.run(["curl", "-s", "-o", "/dev/null", "-w", "%{size_download}", "-r", "0-33554431",
                    f"http://{L.NYDUS_REG}/v2/s11/scaleswe-responses-0/blobs/{blobs[0]['digest']}"], capture_output=True, text=True)
b = L.nft_counters()["nydus_tx"]
out["curl_32MiB_nydus_registry"] = {"payload_bytes": int(p.stdout), "counter_delta": b - a}
print(json.dumps(out))
json.dump(out, open("/root/s11/out/counter-check.json", "w"))
