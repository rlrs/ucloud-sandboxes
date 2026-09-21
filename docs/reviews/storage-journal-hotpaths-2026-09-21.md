# Storage journal history and cleanup costs

Source review of 0.5.72 found three operations whose cost grew with unrelated
historical records. These changes are local and have not been deployed.

## Changes

1. Create and restore capacity admission summed the volume table using a full
   table scan. Both now use a shared query and a covering partial index over
   non-deleted volumes. The same active states, byte ceiling, retired-device
   accounting and transactional admission fence are preserved.
2. Storage metrics loaded and decoded every historical volume, including deleted
   checkpoints, on each heartbeat. Metrics now load live records and obtain the
   historical count separately in one read transaction. Deleted records remain
   available for operation replay, and `volume_count` retains its previous meaning.
3. Delete, retired-device reaping and local-layer cleanup repeatedly loaded the
   entire retired-device inventory to check one volume. A per-volume index and
   existence query replace those scans. Empty cleanup avoids opening the journal;
   layer protection checks use ancestor set membership rather than iterating every
   retired volume for every path.

The reaper still refreshes native owner identity before each device deletion and
requires the kernel's exclusive-open check. Active mount pins and files retained
by retired devices remain protected. Startup still validates stored records.
The additional indexes are created idempotently on existing journals; no record
format or schema-version change is required. Index maintenance adds a small
write cost on live state transitions in exchange for removing repeated table reads.

## Local measurements

Fixture: 20,000 deleted volumes, 512 live volumes and 10,000 retired devices.
Twelve warm-cache samples per operation, including opening and closing the SQLite
connection. Capacity measurements compare the original SQL with the new helper;
inventory measurements compare the original full record load with the new metrics
inventory; retirement measurements compare full inventory lookup with the indexed
existence query. The final samples ran after the test suite had finished.

| Operation | Before median | After median |
| --- | ---: | ---: |
| Capacity query | 9.58 ms | 0.77 ms |
| Metrics inventory and decoding | 477.33 ms | 14.90 ms |
| Retired-volume lookup | 8.10 ms | 0.51 ms |

SQLite's capacity query plan changed from `SCAN volumes` to a search using the
covering `volumes_live_capacity` index. The new inventory decodes 512 records
instead of 20,512. The benchmark's retired-device count represents a large
backlog, not an observed current production inventory.

[Raw measurements](../benchmarks/storage-journal-hotpaths-2026-09-21.json).
These are synthetic local database-path measurements, not end-to-end production
speedups or evidence of a particular production workload's bottleneck.

## Verification

Three regressions were reproduced against the original 0.5.72 methods: admission
exceeded a SQLite instruction budget when historical rows were present, metrics
decoded all tombstones, and local cleanup fetched the full retirement inventory.
All pass after the changes. Tests also cover adding indexes to an existing journal,
retained checkpoint files, historical counts, and bounded retirement lookups.

The full server suite passed **1,049 tests with six platform skips**. Ruff across
the server, tests and scripts, and `git diff --check`, passed. Existing capacity,
retirement, recycled-device identity, local-cache and compaction tests passed.
