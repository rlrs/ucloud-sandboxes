# Four-vCPU gateway comparison, 2026-09-22

The user authorized increasing the production gateway to four vCPUs and
measuring the effect. UCloud's supported job API has no in-place product resize,
so the gateway moved from job **12379311** to **12399353**, preserving release
0.5.80 and its exact disk, PostgreSQL authority, credentials and deployment state.
The AMD Zen 5 product changed from two vCPUs / 6 GB to four vCPUs / 12 GB.
The VM also rebooted and placement may differ; this is a capacity comparison,
not an experiment that isolates CPU count from all other factors.

## Cutover

1. Created the replacement without the production network or ingress resources.
2. Confirmed production idle (zero routes, in-flight relay work or deliveries).
3. Backed up PostgreSQL and routing/control SQLite on the shared data drive,
   stopped services, and verified PostgreSQL's clean shutdown.
4. Powered off both VMs and moved the original raw boot disk to the replacement,
   retaining the replacement's unused blank disk separately. This was a metadata
   move, not a 250 GiB copy or an edit to a running disk.
5. Transferred private network `12345327` and ingress resources `12345368` and
   `12349454`, retaining the hostname `gateway-live-ucloud-20260824a`.
6. Booted the replacement. UCloud mounts each job's `work` directory separately
   from its boot disk, so `work/ucloud-sandboxes` also had to move before services
   could start. Verified `/work/data`, PostgreSQL authority, both public health
   endpoints, worker connectivity and all worker versions before resuming the
   autoscaler.

The old job remains **SUSPENDED** and has no production network endpoints.
It must **not** simply be resumed: its original disk and runtime directory now
belong to the new job. Rollback requires an idle clean shutdown of the new VM,
moving `disk.img` and `work/ucloud-sandboxes` back, transferring the three network
resources back, and then resuming the old VM. The latest PostgreSQL backup is
also on shared drive 998037. Do not terminate the new job or discard either
migration artifact during a rollback.

## Measurements

Identical 256-agent workloads and release 0.5.80 run against the same workers.
Natural retention runs eight cycles; forced parking runs three. First cycles
are excluded from latency measurements.

| Same 0.5.80 workload | Two-vCPU wake p95 | Four-vCPU wake p95 | Four-vCPU usable exec p95 |
|---|---:|---:|---:|
| Natural retention, eight cycles | 1.648 s | 5.624 s | 7.169 s |
| Forced parking, three cycles | 6.597 s | 9.195 s | 10.088 s |

Both clean four-vCPU runs completed all cycles (2,048 and 768), with zero
workload, cleanup or health-probe errors. The 0.8 s target remains unmet.
The first four-vCPU natural run had wake p95 8.566 s, but telemetry was unhealthy
for most of it: its collector still bound the prior private IP. Updated that
binding from 10.36.132.112 to 10.36.135.103 and verified collector/Tempo readiness
before repeating the test. Only the repeated run is the comparison above.

Natural runs observed zero fully parked measured cycles at response readiness;
this does not prove that no park completed later while wake requests queued.
Forced parking explicitly waits until the sandbox is observed parked.

Capacity alone did not improve the measured tail. Traces include a 23.845 s
gateway wake whose worker request took 57.5 ms, and a 20.514 s gateway wake whose
worker took 33.9 ms. Routing batch counters show much more aggregate writer
queue wait than transaction or commit time. A separate FIFO writer-admission
experiment follows; it is not part of these four-vCPU baseline results.

The collector's private-IP binding is machine-specific and must be updated on
any further VM move before accepting the observability health check.
