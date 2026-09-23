# File-transfer contention and rejected builder requests

Production remained on 0.5.107 because new application traffic started before
0.5.108 deployment. Its idle preflight stopped the deployment without restarting
the gateway. This candidate includes 0.5.108's routing writer changes.

A bounded live gateway profile found upload forwarding occupying 28 sampled
stacks, including framed reads and HTTP sends. The upload connection pool now
requests 64 KiB chunks, matching the existing bounded framed reader, instead of
urllib3's 16 KiB default. Body framing, early-rejection socket closure, isolation
from control connections, and no automatic replay are unchanged. A Linux
loopback benchmark (128 uploads of 4 MiB, concurrency 32, checksum verification)
measured 0.764–0.778 s and 1.61–1.65 CPU seconds before, versus 0.438–0.460 s
and 1.00–1.12 CPU seconds after. Body reads fell from 32,896 to 8,320. This is
component evidence, not a production wake qualification.

The realistic harness now defaults to uploading a 64 KiB tool script after each
model response, verifies the uploaded script checksum inside the guest, and
includes upload plus execution in time to a usable tool. `--tool-upload-kib 0`
reproduces the earlier inline-tool workload. Results record the setting and the
upload duration; comparisons must use the same setting. Full working-set and
file-integrity checks remain mandatory.

A separate four-CPU contention test adds six heartbeat reconciliation streams to
2,048 exec writes and concurrent JSON decoding. With the 0.5.108 isolated
heartbeat writer, maximum heartbeat time fell from 1.15–1.74 s to 0.19–0.20 s.
Exec throughput was essentially unchanged (3.66–3.78 s versus 3.69–3.72 s).
Do not interpret this as evidence of a solved end-to-end latency target.

Production also had no ready builders: two single-job creates had failed with
HTTP 502 allocation validation errors and remained uncertain, blocking the role.
The adapter now recognizes only the exact structured allocation-validation
failure for a single-item POST /api/jobs as rejected. Generic 5xx, transport
failures, multi-item requests and termination remain uncertain. UCloud performs
this validation before ResourceCreate, but iterates bulk items sequentially:
[allocation validation](https://github.com/SDU-eScience/UCloud/blob/6b4ecd44ca3345c8caa9318b6f181ca0513e70b8/core2/pkg/orchestrator/resources_allocations.go#L21),
[job creation](https://github.com/SDU-eScience/UCloud/blob/6b4ecd44ca3345c8caa9318b6f181ca0513e70b8/core2/pkg/orchestrator/job.go#L953).

The two existing journal records were separately recovered with exact operation
IDs after checking request shape, exact error, provider inventory coverage and
absence by operation label/name. A SQLite backup and an audited failed outcome
were retained. Only the autoscaler was briefly stopped; gateway and worker
traffic continued. Future failures are handled by the adapter fix after deployment.

Linux validation: 9 streaming/pool tests, 9 realistic-harness tests, and 20
provider/journal tests passed, in addition to the 78 tests for 0.5.108. Deployment
and end-to-end qualification remain pending production idle time.
