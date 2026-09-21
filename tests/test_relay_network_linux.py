"""Opt-in packet qualification in disposable Linux network/mount namespaces.

Run as root with UCLOUD_RUN_NETNS_TESTS=1. The parent never changes its network;
all links, nftables/iptables rules and the private /run/netns mount live under
unshare. No public endpoint, cloud VM or production firewall is used.
"""

from pathlib import Path
import os
import subprocess
import sys
from tempfile import TemporaryDirectory
import time
import unittest


ENABLED = sys.platform == "linux" and os.environ.get("UCLOUD_RUN_NETNS_TESTS") == "1"


@unittest.skipUnless(ENABLED, "requires explicit Linux namespace qualification")
class RelayNetworkPacketTests(unittest.TestCase):
    def test_packet_enforcement_and_lifecycle(self):
        self.assertEqual(os.geteuid(), 0, "run this opt-in test under sudo")
        result = subprocess.run(
            [
                "unshare",
                "--net",
                "--mount",
                "--fork",
                sys.executable,
                str(Path(__file__).resolve()),
                "--worker",
            ],
            env={
                **os.environ,
                "UCLOUD_TEST_PARENT_NETNS": os.readlink("/proc/self/ns/net"),
            },
            capture_output=True,
            text=True,
            timeout=90,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("relay packet qualification passed", result.stdout)


def worker():
    assert os.readlink("/proc/self/ns/net") != os.environ["UCLOUD_TEST_PARENT_NETNS"]
    from ucloud_sandboxes.direct_network import DirectNetworkManager
    from ucloud_sandboxes.network_policy import SandboxNetworkPolicy

    def run(*args):
        return subprocess.run(
            args, check=True, capture_output=True, text=True, timeout=10
        )

    run("mount", "--make-rprivate", "/")
    Path("/run/netns").mkdir(exist_ok=True)
    run("mount", "-t", "tmpfs", "tmpfs", "/run/netns")
    run("ip", "link", "set", "lo", "up")
    run("ip", "netns", "add", "relay-peer")
    run(
        "ip",
        "link",
        "add",
        "uplink",
        "type",
        "veth",
        "peer",
        "name",
        "eth0",
        "netns",
        "relay-peer",
    )
    run("ip", "addr", "add", "10.36.0.1/24", "dev", "uplink")
    run("ip", "addr", "add", "203.0.113.1/24", "dev", "uplink")
    run("ip", "link", "set", "uplink", "up")
    for address in ("10.36.0.2/24", "10.36.0.3/24", "203.0.113.2/24"):
        run(
            "ip",
            "netns",
            "exec",
            "relay-peer",
            "ip",
            "addr",
            "add",
            address,
            "dev",
            "eth0",
        )
    run("ip", "netns", "exec", "relay-peer", "ip", "link", "set", "eth0", "up")
    run("ip", "netns", "exec", "relay-peer", "ip", "link", "set", "lo", "up")
    run(
        "ip",
        "netns",
        "exec",
        "relay-peer",
        "ip",
        "route",
        "add",
        "default",
        "via",
        "10.36.0.1",
    )
    run("iptables", "-P", "FORWARD", "DROP")
    server = """import socket, threading, time

def tcp(port, family=socket.AF_INET):
 s=socket.socket(family);
 if family == socket.AF_INET6: s.setsockopt(socket.IPPROTO_IPV6,socket.IPV6_V6ONLY,1)
 s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
 s.bind(('::' if family == socket.AF_INET6 else '0.0.0.0',port)); s.listen()
 while True:
  c,a=s.accept(); c.sendall(b'relay-ok'); c.close()
def udp():
 s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM); s.bind(('0.0.0.0',53))
 while True:
  data,addr=s.recvfrom(1024); s.sendto(data,addr)
for port in (8000,8001): threading.Thread(target=tcp,args=(port,),daemon=True).start()
threading.Thread(target=tcp,args=(8000,socket.AF_INET6),daemon=True).start()
threading.Thread(target=udp,daemon=True).start()
while True: time.sleep(1)
"""
    process = subprocess.Popen(
        ["ip", "netns", "exec", "relay-peer", sys.executable, "-c", server]
    )
    try:
        with TemporaryDirectory() as raw:
            answers = ["10.36.0.2"]
            manager = DirectNetworkManager(
                Path(raw) / "slots.json",
                network_relays={"default": "relay.test:8000"},
                resolver=lambda _: answers,
            )
            manager.reconcile()
            policy = SandboxNetworkPolicy.relay_only()
            lease = manager.ensure("restricted", 1, network_policy=policy)
            target = manager.relays["default"].virtual_ip

            def probe(namespace, host, port=8000, *, udp=False, source=None):
                code = (
                    "import socket; "
                    + f"s=socket.socket(socket.AF_INET{'6' if ':' in host else ''}, socket.SOCK_{'DGRAM' if udp else 'STREAM'}); s.settimeout(0.4); "
                    + (f"s.bind(({source!r},0)); " if source else "")
                    + f"s.connect(({host!r},{port})); "
                    + ("s.send(b'probe'); " if udp else "")
                    + "assert s.recv(32); s.close()"
                )
                return (
                    subprocess.run(
                        ["ip", "netns", "exec", namespace, sys.executable, "-c", code],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        timeout=3,
                    ).returncode
                    == 0
                )

            for _ in range(30):
                if probe(lease.namespace, target):
                    break
                assert process.poll() is None, "fixture server exited"
                time.sleep(0.05)
            else:
                raise AssertionError(
                    "relay could not be reached through enforced policy"
                )
            # Later global ACCEPTs, existing conntrack and a guest-controlled
            # source address must not expand authority beyond the relay route.
            run("iptables", "-I", "FORWARD", "1", "-j", "ACCEPT")
            for host, port in (
                ("10.36.0.2", 8000),
                ("10.36.0.3", 8000),
                ("10.36.0.1", 8000),
                ("203.0.113.2", 8000),
                (target, 8001),
            ):
                assert not probe(lease.namespace, host, port), (
                    "unexpected egress",
                    host,
                    port,
                )
            assert not probe(lease.namespace, "203.0.113.2", 53, udp=True), "DNS bypass"
            run(
                "ip",
                "netns",
                "exec",
                lease.namespace,
                "ip",
                "addr",
                "add",
                "100.96.0.99/32",
                "dev",
                "eth0",
            )
            assert not probe(lease.namespace, target, source="100.96.0.99"), (
                "source spoofing bypass"
            )
            # An ordinary sandbox is a positive control for the blocked service.
            direct = manager.ensure("direct", 1)
            assert probe(direct.namespace, "203.0.113.2"), (
                "public fixture is not reachable"
            )
            assert probe(direct.namespace, "203.0.113.2", 53, udp=True), (
                "UDP fixture is not reachable"
            )
            run("sysctl", "-qw", "net.ipv6.conf.all.forwarding=1")
            run("ip", "-6", "addr", "add", "fd00:1::1/64", "dev", "uplink", "nodad")
            run(
                "ip",
                "netns",
                "exec",
                "relay-peer",
                "ip",
                "-6",
                "addr",
                "add",
                "fd00:1::2/64",
                "dev",
                "eth0",
                "nodad",
            )
            run(
                "ip",
                "netns",
                "exec",
                "relay-peer",
                "ip",
                "-6",
                "route",
                "add",
                "default",
                "via",
                "fd00:1::1",
            )
            for item, subnet in ((lease, 2), (direct, 3)):
                run(
                    "ip",
                    "-6",
                    "addr",
                    "add",
                    f"fd00:{subnet}::1/64",
                    "dev",
                    item.host_interface,
                    "nodad",
                )
                run(
                    "ip",
                    "netns",
                    "exec",
                    item.namespace,
                    "ip",
                    "-6",
                    "addr",
                    "add",
                    f"fd00:{subnet}::2/64",
                    "dev",
                    "eth0",
                    "nodad",
                )
                run(
                    "ip",
                    "netns",
                    "exec",
                    item.namespace,
                    "ip",
                    "-6",
                    "route",
                    "add",
                    "default",
                    "via",
                    f"fd00:{subnet}::1",
                )
            assert probe(direct.namespace, "fd00:1::2"), "IPv6 fixture is not reachable"
            assert not probe(lease.namespace, "fd00:1::2"), "IPv6 bypass"
            # DNS migration preserves the virtual endpoint, and empty answers
            # close egress until the relay returns.
            answers[:] = ["10.36.0.3"]
            manager.refresh_tcp_egress()
            assert probe(lease.namespace, target), "relay DNS handoff failed"
            answers.clear()
            manager.refresh_tcp_egress()
            assert not probe(lease.namespace, target), "empty DNS answer failed open"
            answers[:] = ["10.36.0.2"]
            manager.refresh_tcp_egress()
            assert probe(lease.namespace, target)
            # Invalid atomic update must retain the previous working guard.
            bad = subprocess.run(
                ["nft", "-f", "-"],
                input=f"delete table inet ucloud_relay_{lease.slot}\ninvalid syntax\n",
                text=True,
                capture_output=True,
            )
            assert bad.returncode != 0
            assert not probe(lease.namespace, "203.0.113.2")
            assert probe(lease.namespace, target)
            # The network wiring consumed by checkpoint/restore is reconstructed
            # under the same immutable policy and lease.
            run("ip", "link", "delete", lease.host_interface)
            run("ip", "netns", "delete", lease.namespace)
            restored = manager.ensure("restricted", 1, network_policy=policy)
            assert probe(restored.namespace, target), "restore wiring failed"
            assert not probe(restored.namespace, "203.0.113.2")
            fresh = DirectNetworkManager(
                Path(raw) / "slots.json",
                network_relays={"default": "relay.test:8000"},
                resolver=lambda _: answers,
            )
            fresh.reconcile()
            assert probe(restored.namespace, target), "daemon restart failed"
            manager.release("restricted", 1)
            reused = manager.ensure("reused", 1)
            assert reused.slot == lease.slot
            assert probe(reused.namespace, "203.0.113.2"), (
                "released policy contaminated reused slot"
            )
            manager.release("reused", 1)
            manager.release("direct", 1)
    finally:
        process.terminate()
        process.wait(timeout=5)
    print("relay packet qualification passed")


if __name__ == "__main__":
    if sys.argv[1:] == ["--worker"]:
        worker()
    else:
        unittest.main()
