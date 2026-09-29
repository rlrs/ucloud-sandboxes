# Gateway resize readiness

> Historical preflight review, written before the actual VM trial. It is
> superseded by the successful CCX23 qualification on 2026-09-28; see the
> [final qualification report](README.md). The actual four-CPU, 16 GiB
> machine completed all 512 agents and 6,144 cycles with the unchanged
> latency gates passing (ready-to-usable p95 0.740 seconds). Concurrent
> controlled registry/NAT traffic completed without errors; minimum
> available RAM was 12.11 GiB. CCX23 was retained after these results.

The CPU hotplug trials are not a full CCX23 qualification. They retain the
CCX33's 32 GiB-class memory allocation, warm page cache, existing boot and
network state, and nginx processes started before CPUs were disabled. Treat
four CPUs / 16 GiB / unchanged root-disk size as the proposed target supplied
for this review, not a newly verified provider catalog or availability claim.

Until the final isolated-driver run is evaluated, retain eight CPUs. The prior
four-CPU strict latency gate missed at p95 1.034 seconds. Its mixed phase had
31.48% CPU some-stall and 22.01% I/O some-stall, and achieved 122.62 MiB/s of the
requested 150 MiB/s registry upload rate. A historical roughly ten-minute cold
registry/network window consumed 4.129 CPU cores on average. Passing a narrower
synthetic workload would not erase that production burst requirement.

Memory footprint is encouraging but incomplete evidence: the earlier strong
four-CPU phase retained at least 27.53 GiB MemAvailable and used no swap. The
provisioning PostgreSQL configuration allocates 1 GiB shared buffers;
`effective_cache_size=4GB` is a planner estimate, not a 4 GiB allocation. Halving
RAM still reduces filesystem cache for registry traffic. The strong trial had
28 compaction stalls and eight movable allocation stalls even with 32 GiB-class
RAM, so neither footprint nor a zero swap counter proves low tail latency after
resize. No 16 GiB cold-boot or constrained-memory qualification was performed.

Before any resize, complete and retain the exact final latency, correctness,
registry-throughput and pressure results. A resize requires a maintenance
window: this gateway also supplies PostgreSQL, registry, relay and fleet NAT.
A reboot loses active connections and NAT conntrack state even if database
records persist. Drain/admission-control decisions must account for worker
outbound connections and in-flight model requests, not only gateway requests.
Retain a recoverable database backup and exact config/software fingerprints,
confirm source-shape capacity for rollback, preserve the server and both IPs,
and avoid any destructive root-disk replacement or external-volume change.

Boot validation must cover:

- Correct external registry volume UUID mounted at `/mnt/ucloud-registry`,
  expected filesystem/data and successful owned registry read/write. `fstab`
  uses `nofail`: a successful host boot alone does not prove registry readiness.
  Registry/GC/pressure units have `RequiresMountsFor` and a mountpoint precheck.
- PostgreSQL ready, shared/routing/relay schemas readable, and gateway, relay,
  placement, autoscaler, registry, Docker and nginx services stable with no
  failed units. Base gateway/relay unit files do not order after PostgreSQL;
  restart behavior must recover any boot race.
- Private IP/routing, IPv4 forwarding, NAT masquerade and Docker's
  `DOCKER-USER` forwarding rules. The NAT unit runs after Docker/network-online;
  verify worker public egress and private relay/registry connectivity.
- Expected CPU/RAM count and file descriptor limits, with temporary hotplug
  restoration tasks removed. `gateway_processes` remains explicitly six;
  nginx `worker_processes auto` can change from eight to four on a true reboot,
  unlike a hotplug trial. Verify connection capacity and IRQ/softirq behavior.
- Disabled unattended APT services/timers remain disabled, and registry pruning
  timers plus the intended package bundle root remain configured.
- The same 512-agent test with inventory polling and strong unique registry/NAT
  traffic, including a cold-cache/build interval and memory/CPU/I/O pressure.

If service recovery, data availability, strict latency, throughput or pressure
regresses, resize the same machine back to CCX33 while preserving its disk,
external volume, public/private IPs and optimized software. A provider shape
rollback does not require restoring an old application database or reverting
safe code optimizations. Repeat the boot and worker-connectivity checks after
rollback. Actual resize and rollback have not been executed by this review.

Evidence: `wholehost-extra-acceptance-summary.json`,
`wholehost-extra-acceptance-pressure.json`, `network-ingress.md`,
`scripts/hetzner_prod/gateway-prep.sh`, `scripts/hetzner_prod/make_config.py`,
`scripts/install_hetzner_gateway.sh`, and gateway/relay/registry systemd units.
