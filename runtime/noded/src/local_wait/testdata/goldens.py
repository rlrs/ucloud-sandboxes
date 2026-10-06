"""Regenerate python_goldens.json from ucloud_sandboxes/local_wait.py.

    .venv/bin/python runtime/noded/src/local_wait/testdata/goldens.py
"""
import ipaddress
import json
from pathlib import Path
import socket
import struct
from types import SimpleNamespace

from ucloud_sandboxes import local_wait

NETWORK = ipaddress.IPv4Network("100.96.0.0/16")


def tcp(source, destination, *, payload=0, flags=0x10, sport=40000, dport=8092, ihl=5, doff=5):
    header = struct.pack("!HHIIBBHHH", sport, dport, 1, 1, doff << 4, flags, 65535, 0, 0) + b"\0" * (4 * doff - 20)
    options = b"\x01" * (4 * ihl - 20)
    ip = struct.pack("!BBHHHBBH4s4s", 0x40 | ihl, 0, 4 * ihl + len(header) + payload, 0, 0, 64, 6, 0,
                     socket.inet_aton(source), socket.inet_aton(destination)) + options
    return (ip + header)[:local_wait.SNAPLEN]


def nflog(*packets, kind=local_wait.NFNL_SUBSYS_ULOG << 8):
    messages = b""
    for packet in packets:
        prefix = struct.pack("=HH", 4 + 3, 10) + b"abc\0"  # NFULA_PREFIX, ignored.
        attribute = struct.pack("=HH", 4 + len(packet), local_wait.NFULA_PAYLOAD | 0x8000 * (len(packet) % 2)) + packet
        attribute += b"\0" * (-len(attribute) % 4)
        body = struct.pack("=BBH", socket.AF_INET, 0, socket.htons(local_wait.NFLOG_GROUP)) + prefix + attribute
        messages += struct.pack("=IHHII", 16 + len(body), kind, 0, 0, 0) + body
    return messages


def main():
    relay_sets = [
        ["10.42.0.2:8092", "relay.example.org:443", "77.42.92.27:443"],
        ["10.42.0.10:8092", "10.42.0.2:8092", "10.42.0.2:8093", "10.42.0.2:8092", "192.168.1.5:80",
         "172.16.0.1:1", "172.32.0.1:2", "100.64.0.1:3", "127.0.0.1:4", "169.254.1.1:5", "0.1.2.3:6",
         "198.18.0.1:7", "240.0.0.1:8", "255.255.255.255:9", "192.0.0.8:10", "192.0.0.9:11", "8.8.8.8:12",
         "010.0.0.1:13", "10.0.0.256:14", "::1:15", " 10.0.0.3:16", "10.0.0.4 :17"],
        ["relay.example.org:443"],
    ]
    endpoints = []
    for relays in relay_sets:
        objects = {}
        for index, text in enumerate(relays):
            host, _, port = text.rpartition(":")
            objects[str(index)] = SimpleNamespace(host=host, port=int(port))
        endpoints.append({"relays": [[o.host, o.port] for o in objects.values()],
                          "endpoints": [list(e) for e in local_wait.relay_endpoints(objects)]})
    scripts = []
    for case in endpoints:
        for network in ("100.96.0.0/16", "10.0.0.0/8"):
            scripts.append({"endpoints": case["endpoints"], "network": network,
                            "script": local_wait.nft_script(case["endpoints"], ipaddress.IPv4Network(network))})
    configs = []
    for seq, attribute in enumerate((
            local_wait._attribute(local_wait.NFULA_CFG_CMD, bytes([local_wait.NFULNL_CFG_CMD_BIND])),
            local_wait._attribute(local_wait.NFULA_CFG_MODE, struct.pack("!IBB", local_wait.SNAPLEN,
                                                                         local_wait.NFULNL_COPY_PACKET, 0)),
            local_wait._attribute(local_wait.NFULA_CFG_QTHRESH, struct.pack("!I", 1))), start=1):
        configs.append(local_wait._config(local_wait.NFLOG_GROUP, attribute, seq=seq).hex())
    guest, relay = "100.96.0.3", "10.42.0.2"
    raw_packets = [
        tcp(guest, relay, payload=300, flags=0x18),
        tcp(relay, guest, payload=1368, sport=8092, dport=40000),
        tcp(relay, guest, sport=8092, dport=40000),
        tcp(relay, guest, flags=0x11, sport=8092),
        tcp(relay, guest, flags=0x04, sport=8092),
        tcp(relay, guest, flags=0x02, sport=8092),
        tcp(relay, "10.42.0.9"),
        tcp(guest, "100.96.0.5", payload=7),
        tcp(guest, relay, payload=40, ihl=7, doff=8),
        tcp(guest, relay, payload=0, doff=15),
        b"\x60" + b"\0" * 39,
        tcp(guest, relay)[:19],
        tcp(guest, relay, ihl=7)[:41],
        tcp(guest, relay, ihl=7)[:42],
        bytes([0x45, 0, 0, 10]) + tcp(guest, relay)[4:],
        tcp(guest, relay)[:9] + b"\x11" + tcp(guest, relay)[10:],
    ]
    packets = []
    for raw in raw_packets:
        parsed = local_wait.parse_packet(raw, NETWORK)
        packets.append({"raw": raw.hex(), "parsed": None if parsed is None else
                        [parsed.guest, parsed.outbound, parsed.payload, parsed.flags, bool(parsed.wakes)]})
    first, second = tcp(guest, relay, payload=10), tcp(relay, guest, payload=20, sport=8092)[:39]
    error = struct.pack("=IHHII", 36, local_wait.NLMSG_ERROR, 0, 1, 0) + b"\0" * 20
    buffers = [error + nflog(first, second), b"\x05\0\0", nflog(first) + b"\x03\0\0\0",
               nflog(first, kind=(local_wait.NFNL_SUBSYS_ULOG << 8) | 1) + nflog(second),
               nflog(first)[:-8], struct.pack("=IHHII", 8, 0, 0, 0, 0) + nflog(first)]
    messages = [{"data": data.hex(), "packets": [p.hex() for p in local_wait.parse_messages(data)]}
                for data in buffers]
    golden = {"table": local_wait.TABLE, "group": local_wait.NFLOG_GROUP, "snaplen": local_wait.SNAPLEN,
              "relay_endpoints": endpoints, "scripts": scripts, "configs": configs, "packets": packets,
              "messages": messages}
    out = Path(__file__).with_name("python_goldens.json")
    out.write_text(json.dumps(golden, indent=1, sort_keys=True) + "\n")


main()
