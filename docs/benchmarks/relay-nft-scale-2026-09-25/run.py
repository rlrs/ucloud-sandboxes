"""Run inside `unshare --net --mount`: N relay-only leases, then measure."""
import json, os, re, statistics, subprocess, sys, time
from pathlib import Path
from tempfile import TemporaryDirectory

N = int(sys.argv[1])
def run(*a, **k):
    return subprocess.run(a, check=True, capture_output=True, text=True, timeout=60, **k)
run("mount", "--make-rprivate", "/")
Path("/run/netns").mkdir(exist_ok=True)
run("mount", "-t", "tmpfs", "tmpfs", "/run/netns")
run("ip", "link", "set", "lo", "up")
run("ip", "netns", "add", "peer")
run("ip", "link", "add", "uplink", "type", "veth", "peer", "name", "eth0", "netns", "peer")
run("ip", "addr", "add", "203.0.113.1/24", "dev", "uplink")
run("ip", "link", "set", "uplink", "up")
for c in (("ip", "addr", "add", "203.0.113.2/24", "dev", "eth0"), ("ip", "link", "set", "eth0", "up"),
          ("ip", "link", "set", "lo", "up"), ("ip", "route", "add", "default", "via", "203.0.113.1")):
    run("ip", "netns", "exec", "peer", *c)
from ucloud_sandboxes.direct_network import DirectNetworkManager
from ucloud_sandboxes.network_policy import SandboxNetworkPolicy
with TemporaryDirectory() as raw:
    m = DirectNetworkManager(Path(raw) / "slots.json", network_relays={"default": "relay.test:8000"},
                             resolver=lambda _: ["203.0.113.2"])
    m.reconcile()
    times = []
    for i in range(N):
        t = time.perf_counter()
        m.ensure(f"r{i}", 1, network_policy=SandboxNetworkPolicy.relay_only())
        times.append(time.perf_counter() - t)
    direct = m.ensure("direct", 1)
    t = time.perf_counter(); m.refresh_tcp_egress(); refresh = time.perf_counter() - t
    t = time.perf_counter(); m.reconcile(); reconcile = time.perf_counter() - t
    out = subprocess.run(["ip", "netns", "exec", direct.namespace, "ping", "-f", "-q", "-c", "100000",
                          "-s", "56", "203.0.113.2"], capture_output=True, text=True, timeout=300).stdout
    rtt = re.search(r"= ([\d.]+)/([\d.]+)/([\d.]+)", out)
    total = re.search(r"time (\d+)ms", out)
    nft_lines = len(run("nft", "list", "ruleset").stdout.splitlines())
    ipt = len(run("iptables", "-S", "FORWARD").stdout.splitlines())
    last = times[-50:] or [0]
    print(json.dumps({
        "n": N, "ensure_ms_median_last50": round(statistics.median(last) * 1e3, 1),
        "ensure_ms_first": round(times[0] * 1e3, 1) if times else None,
        "refresh_ms": round(refresh * 1e3, 1), "reconcile_ms": round(reconcile * 1e3, 1),
        "ping_avg_us": round(float(rtt.group(2)) * 1e3, 1), "ping_100k_ms": int(total.group(1)),
        "nft_ruleset_lines": nft_lines, "iptables_forward_rules": ipt,
    }))
