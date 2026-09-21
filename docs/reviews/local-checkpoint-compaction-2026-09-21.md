# Independent local checkpoint compaction

Implemented locally on September 21, 2026; not committed or deployed. This is a
follow-up to the [production performance investigation](production-performance-2026-09-21-afternoon.md).

## Problem and change

Worker 12397962 accumulated 25–29 unpublished layers while publication was
repeatedly superseded by local lifecycle operations. Remote publication was the
only opportunity to compact these chains, so faster cancellation alone could
not reduce their depth. The existing dominant-base optimization also depended
on a published base.

The node service now prepares local replacements independently of publication.
A release queues maintenance, with one background export per node and requests
coalesced by volume. The default triggers are local depth greater than eight or
accumulated delta allocation greater than 4 GiB. Depth-only compaction retains
a dominant oldest local base and merges newer deltas. Byte pressure merges the
whole local chain. Already-published layers remain unchanged.

Background work pins immutable inputs with hardlinks and does not hold the
lifecycle operation gate. A wake or newly appended sealed layer does not cancel
the export while its source prefix remains valid. A completed candidate becomes
authoritative only when adopted inside a journaled mount or publication
transition; newer appended layers remain above it. Superseded ownership,
replacement inputs, deletion, and publication in progress cancel background
work. A publisher can adopt an already-completed local replacement.

## Recovery and disk use

The replacement is flushed, digest-checked against the native export result,
and recorded in an atomically replaced ready manifest. Adoption writes the
journal before deleting any original input names. A failed journal update
preserves the original checkpoint. Ready candidates survive a service restart.

Input names remain while retired devices could still read their old stack;
reaping the final retired device reclaims obsolete cached layers. Reconciliation
removes crash-left hardlink directories while a file lock protects live exports.
Uncertain output files are retained rather than risking checkpoint loss.
Physical free space and actual output growth are checked during export, retaining
256 MiB of free headroom. Maintenance failure leaves the original chain usable.

## Verification

Lifecycle regressions cover concurrent wake/appended deltas, restart, failed
journal adoption, publication adoption and failure recovery, owner/prefix fences,
retired-device cleanup, low disk headroom, damaged manifests, deletion during
export, digest mismatch, and abandoned versus live export cleanup.
The full Python suite passed: 1,029 tests, with six environment-dependent skips.
Ruff and diff whitespace checks also passed.

Native qualification used the installed pinned backend
`75a20bd1ab96e2dff63ff877d0abe63383092e34c8fabdba927128eae062a7f7`
in a separate temporary process on the idle gateway. No production service was
replaced, no device or mount was created, and no remote checkpoint was published.

| Check | Result |
| --- | --- |
| Initial local layers | 29 |
| After compaction, a concurrent wake and appended delta | 3 |
| Additional append/compact/adopt cycles | 24 |
| Maximum depth before adoption in those cycles | 9 |
| Completed/adopted compactions | 4 / 4 |
| Compaction failures | 0 |
| Logical overwrite, zero, discard and hole contents | Preserved |

The fixture has an 8 MiB base inside a 9 MiB virtual image. Its first delta merge
read an estimated 688,128 allocated bytes and produced 159,744 bytes while
retaining the base. Reference-image comparison verified the resulting stack on
every cycle. [Raw results](../benchmarks/local-checkpoint-compaction-2026-09-21.json)
and [reproduction script](../../runtime/storage_native/qualify_local_compaction.py).

## Limits and rollout measurements

This validates native layer semantics and lifecycle recovery, not production
throughput or a 256/512-sandbox capacity target. The qualification waits for each
maintenance pass; under real queueing, depth can exceed nine until a replacement
is ready and adopted. Compaction consumes local I/O, and one background export
can overlap foreground storage work and publications for other volumes. It is
not yet scheduled from measured I/O pressure.
Publication takes precedence for the same volume, so repeated publication
attempts can still defer local maintenance; this pass does not guarantee progress
under continuous publication churn.

After deployment, measure per-node maintenance queue depth, completed/adopted
counts, delta bytes, unpublished chain depth, I/O pressure and wake latency under
the same workload. Compare total storage reads/writes as well as restore time:
shorter stacks are useful only if maintenance costs less than the repeated work
it removes. Local compaction does not provide remote durability or eliminate
remaining publication and wake-coordination costs.
