# Relay firewall scale, 2026-09-25

Per-sandbox relay nftables tables (base commit `885f8d5`, "old") against the
shared `inet ucloud_relay` table with interface-keyed verdict maps ("new").

Run on the live UCloud gateway (4 vCPU, Ubuntu 26.04, kernel 7.0.0-31,
nftables 1.1.6, iptables 1.8.11 nf_tables), entirely inside
`unshare --net --mount`; the host firewall was never touched. `run.py N`
creates N relay-only leases through `DirectNetworkManager`, then one ordinary
sandbox, and flood-pings a peer namespace from the ordinary sandbox. Its
packets cross every prerouting/forward hook, so they pay any per-sandbox rule
cost without matching it.

```
sudo env PYTHONPATH=<tree> unshare --net --mount --fork python3 run.py 500
```

| tree | N | ping RTT avg | 100k flood pings | create (median, last 50) | full reconcile | FORWARD rules |
|------|---|--------------|------------------|--------------------------|----------------|---------------|
| old  | 0   | 9 µs   | 1.3 s  | –        | 19 ms   | 9   |
| old  | 500 | 638 µs | 64.5 s | 239 ms   | 198 s   | 509 |
| new  | 0   | 11 µs  | 1.5 s  | –        | 43 ms   | 10  |
| new  | 500 | 10 µs  | 1.4 s  | 42.6 ms  | 0.44 s  | 10  |

Old: each relay sandbox hooked five base chains and one iptables
`FORWARD -i <iface> -j ACCEPT`, so every packet on the node walked
O(N) guards and create/reconcile slowed with N (iptables-nft rewrites the
chain). New: five base chains in total; each does one hash lookup on the
interface and jumps to that sandbox's regular chains. One mark-matched
iptables ACCEPT replaces the per-interface rules. The create figure is
dominated by netns/veth setup.

Qualification: `tests/test_relay_network_linux.py` passed on the same host
(relay reachability, no other egress under a global FORWARD ACCEPT, no UDP/DNS,
source-spoofing or IPv6 bypass, DNS handoff, fail-closed empty answer, rebuild
after external table deletion, legacy table/rule retirement on restart, slot
reuse). Raw rows: `results.jsonl`.
