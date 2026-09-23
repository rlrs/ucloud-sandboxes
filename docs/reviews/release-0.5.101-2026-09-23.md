# Account for file-backed guest working memory

A worker with roughly 62 GB mapped guest memory reported roughly 82 GB
MemAvailable out of 90 GB while direct compaction and I/O stalls delayed tools.
MemAvailable includes reclaimable file-backed guest pages; treating it as idle
capacity packs more work onto an already busy memory working set.

Runtime metrics now expose a bounded estimate: used memory plus Mapped minus
Shmem. This is a soft placement/scaling signal, not an exact resident-memory
account or a new admission rejection. Raw availability and admission evidence
are unchanged. Older heartbeat payloads default the optional field to zero.

47 Linux tests pass, including metric compatibility and a placement test that
selects a less busy worker but still accepts the only available busy worker.
Native storage and gVisor are unchanged. A separately qualified direct-I/O
candidate was rejected for throughput regression; evidence is retained.

0.5.100 clean load reports are now retained too. Its wake-only p95 passed at
256, but complete tool latency still failed; this release must be measured
under the same workload before claiming qualification.
