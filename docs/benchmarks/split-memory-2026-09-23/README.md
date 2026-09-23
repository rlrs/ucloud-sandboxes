# Live memory and workspace I/O qualification

These isolated Linux VM tests use real storage-native ublk/XFS, the pinned
gVisor runtime, a 512 MiB guest heap with 128 MiB repeatedly dirtied, SQLite WAL
FULL transactions, and a workspace filesystem sync every two seconds. Each
30-second run verifies the entire heap and SQLite contents before cleanup.
Physical I/O counts use leaf guest-device counters and include final flushes.

The 50%-entropy ABBA comparison (`ram-active-summary.json`) measured:

| Layout | Physical writes | SQLite commits per run | Commit p99 |
| --- | --- | --- | --- |
| Coupled workspace/memory | 19.58–23.97 MB/s | 3,809–4,076 | 12.30–14.21 ms |
| RAM-active memory | 2.63–2.68 MB/s | 4,481–4,531 | 1.77–2.19 ms |

Mean physical writes fell 87.8%, while commit throughput rose 14.3%. The exact
runtime and workload hashes are retained in each report. These runs use the
RAM runtime before its final additional copy-consistency fence; final-runtime
0%- and 100%-entropy runs are recorded separately. This is neither a density
test nor a production wake latency claim.

The final attested runtime also passed ABBA at 0% and 100% high-entropy
content. Physical writes fell 86.7% and 86.8%, respectively, while completed
transactions rose 9.2% and 9.8%. These repeated endpoint tests retain all raw
results in `ram-active-final-summary.json` and its referenced reports.

The ordinary disk-file split alone is insufficient: the earlier valid
`interference-qualified-*` ABBA reduced workspace sync latency but increased
physical writes about 6.4 times for a highly compressible heap. RAM-active
backing avoids periodic live-memory writeback. Durable park still exports a
complete quota-backed component, so it remains a costly operation reserved for
actual capacity pressure.

Other reports retain native capture/abort, TCP/timer/FD restoration, project
quota exhaustion, physical reclamation, remote registry import, and deliberately
failed restore/retry evidence. `ram-large-oom-retry.json` verifies a 1.5 GiB heap
with a 128 MiB failing candidate followed by successful 2 GiB retry; the original
checkpoint survives the failed attempt. Its cold restore is about 1.35 seconds,
which reinforces the importance of retaining ordinary waits in RAM.
