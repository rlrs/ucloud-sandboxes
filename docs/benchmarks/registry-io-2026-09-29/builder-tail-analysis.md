# Registry I/O qualification: remaining build tails

The candidate reduces physical registry writes by about 88%, including a 30-second writeback observation, but this single 48-request repeat burst does **not** demonstrate faster tail completion. All 48 builds succeed on four fresh eight-core builders with the same frozen fixture hashes; batch completion increases from 122.065 to 135.561 seconds (+11.1%) and client p95 from 118.652 to 132.432 seconds (+11.6%). Median client time improves from 84.219 to 76.800 seconds (−8.8%).

Sources: [comparison](repeat-comparison.json), [per-recipe phases](build-load-report.json), [whole-burst host findings](host-findings.json), and [narrow-window counters](builder-tail-analysis.json). This is an observational comparison of one burst per configuration, not a replicated causal experiment. Fresh hosts, placement, concurrent BuildKit graph reuse, and both candidate runtime changes can affect timing.

## What still costs time

The slowest build-and-push phases are concentrated in Node fixtures: overall p95 rises from 39.395 to 57.641 seconds. TypeScript multistage build-and-push p95 rises from 41.012 to 63.551 seconds, while Python improves from 35.836 to 29.774 seconds. Local worker queue p95 remains only 6 milliseconds; admission retries wait outside that queue and are included in client latency.

On builder `167948343`, overlapping Node builds take 49–64 seconds in build-and-push. Over the representative reconstructed interval 10:40:40.307–10:41:43.801 UTC, host CPU averages 4.65 occupied cores, including 3.85 attributed to BuildKit and descendants; CPU pressure averages 7.06%, I/O pressure 1.16%, and local disk request-weighted latency 0.90 ms. The gateway uses 0.25 cores over the same interval. This points toward work within the builder/build graph as the useful next profiling target. It does not prove CPU saturation or establish placement as the cause of the regression.

The final three tools-image builds on builder `167948344` spend 13.08–13.52 seconds publishing their environment. Their recorded phase details locate most of this time before image creation:

| Recorded operation | Three final tools builds |
|---|---:|
| Selective layer download/extraction | 5.48–5.55 s |
| Squashing the missing layer group | 6.05–6.33 s |
| `mkfs.erofs` | 0.376–0.389 s |
| Publishing the signed component | 0.230–0.261 s |
| Waiting for the group lock | 0.005–0.016 ms |

These operations occur inside the enclosing preflight/total timers; those enclosing timers must not be added again. All three reuse four groups, build one 14.23 MiB component, and download about 3.24 MiB of OCI data. In the reconstructed 10:41:56.812–10:42:10.323 UTC environment window, the builder averages 3.34 occupied cores, including 1.67 in the node-agent process group and 1.33 in BuildKit. CPU pressure is 2.22%, I/O pressure 0.63%, and local disk request-weighted latency 1.16 ms. Gateway CPU is 0.084 cores and registry-volume request-weighted latency 2.69 ms. The evidence supports investigating local extraction and squashing; it does not identify a specific interpreter lock, syscall, or algorithm as the limiting cause.

Subphase boundaries above are reconstructed from `execution_started_at` plus the recorded build-and-push duration, allowing small handoff gaps. Two-second samples capture concurrent host work rather than per-request resource ownership. The shortest builder window contains only five complete intervals (10 seconds), so its tail percentiles are not precise.

## Registry improvement survives writeback

| Gateway registry volume measurement | Baseline | Candidate |
|---|---:|---:|
| Writes within the client window | 5.518 GiB | 0.651 GiB |
| Request-weighted disk latency within that window | 31.05 ms | 2.84 ms |
| Mean gateway I/O pressure | 17.37% | 8.08% |
| Mean registry CPU | 0.199 cores | 0.102 cores |
| Additional writes in the measured 30-second tail | 4.641 MiB | 5.840 MiB |
| Writes from burst start through tail end | 5.539 GiB | 0.661 GiB |

The exact tail brackets are 09:43:57.097574–09:44:27.097570 UTC for baseline (4,866,048 bytes) and 10:42:13.211506–10:42:43.211503 UTC for candidate (6,123,520 bytes). Raw start/end counters are retained in the JSON. Complete-interval totals through tail end also include the boundary interval excluded from the original client-window report; therefore those totals are not simply the first and fifth rows added together. They cover 150 and 164 seconds respectively. Both remain well before the next benchmark burst. Counters include all volume activity, not exclusively these 48 builds; disk busy percentage is deliberately excluded because prior measurements exposed unreliable busy counters.

The write reduction is therefore not explained by moving gigabytes of pending writeback beyond the client deadline. Gateway and registry resource measurements do not show a new gateway CPU or sustained registry-volume saturation explaining the slower builds. They do not rule out individual network or metadata waits.

## Mount timing limit

The retained summaries do not contain per-build `Shared build cache mounts` metrics: the corresponding output was truncated from the retained log tails. Server logs show 846 successful mounts, with a maximum individual registry request time of 49.5 ms, but that measures server request handling rather than the entire client preparation budget. Cache preparation occurs inside `docker_build_and_push_ms`; its contribution cannot be separated from these summaries. The three-second preparation budget is not evidence that every build spends three seconds, and 48 per-build costs cannot be summed into wall time while builds overlap.

A follow-up latency comparison should retain each build's bounded preparation metrics, profile extraction/squashing on a tools fixture, and repeat the same fresh-builder burst in both configurations. The current result supports the storage optimization and correct completion; it does not support claiming an end-to-end speedup.
