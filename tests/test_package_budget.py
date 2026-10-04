"""Fail when the server package grows past its checked-in line budget.

The budget may only decrease. A PR that raises it must say why, and should
name the mechanism its new code retires (docs/rl-scale-architecture-plan.md,
C6.3). Lower it when a deletion lands so the freed lines cannot regrow.
"""
from __future__ import annotations

from pathlib import Path
import unittest

# Budget history from the 102,852-line baseline (90ce959), with what offsets it:
# + C2.2/C2.3 EROFS metadata and trace prefetch, C2.12 --MZ (the Python NBD
#   server and cache they extend go with C2.1);
# + C1.1 pause tier and its thaw-prefetch, reclaim-budget and escalation
#   follow-ups (warm_park goes once the pause flag is the only path);
# + C2.11 EROFS layout 2; C4.3 placement_choice (its wiring deletes the
#   whole-fleet scan, worker revisions and advisory turns); C5.2 netns pool;
#   C4.4 in-process heartbeat sender; C5.2 part 2 (durability levels and the
#   in-memory registry index);
# + C5.1 step 1 guest agent client (sandbox_exec's runsc-exec path and its
#   three threads per session go once exec and files are wired to it);
# + C3.1 commit worker and builder halves (the largest addition);
# - C4.7 shadow program scheduler, shared-control qualification store, and
#   the C6.1 extraction steps so far (PR4-PR6 net negative);
# + D1 reboot = process loss: exact park re-adoption, the retired-boot reaper
#   and registration-conflict codes (+187 net; the dead cross-boot reconcile
#   branch and three dead helpers went).
# + D2 heartbeat pull before any sandbox_worker_unreachable answer, and D3-D7
#   UCloud quarantine re-anchoring, provider-confirmed loss and replay
#   ordering (+151 net together; RoutingStore.delete_sandboxes_for_jobs went).
# + C2.15 upstream pull-through mirror (+265: config, mirror/trim helpers,
#   builder mirrors). What it retires, the campaign's upstream cooldowns, lives
#   in scripts/ outside this count and goes once production runs with it.
# + the environment backend's deep backlog and EAGAIN connect retry (+15),
#   and sharing a composition between config-only siblings (+1).
# + C2.13 M1 chunk store core (+2,437): pack, chunk-map and locator formats,
#   ucloud-chunk-index, the RAFS converter with crash-safe publication, the
#   worker RAFS device, single-flight attach and the unpack rollback, all
#   behind immutable_environments.chunk_store. What it retires comes with
#   migration (docs/chunk-store-design.md §8: ~3.6k lines, the EROFS builder,
#   layer groups, erofs_metadata and the component-index cache paths).
# + C2.6 chunk store node (+1,381): ucloud-chunk-store (S3 fills in extents
#   with coalescing and progress-based hedging, a crash-safe LRU extent cache,
#   warm jobs, metrics, an asyncio sendfile server), the store VM init role,
#   the store_node config and the index relocation. S12 made it a
#   precondition of M1 (S3's tail and NAT); it retires nothing yet, and C2.1's
#   native device replaces the worker read path it feeds, not this node.
# +13 attach_concurrency (0.8.3 burst fix), +24 M1 gate: verify through the node,
# hardlink groups, +60 path-ordered layers and warm-before-verify, +18 per-pack
# commits (convert race), +69 exact symlinks in rollback (an EROFS name walk),
# +63 M2 readers: SandboxSpec.environment_root, dispatched roots, capabilities,
# +171 the gateway's image_roots table, dispatch and retention, +207 chunk-migrate
# inventory and retention's dispatched view (design §8 allows the migration tool
# 250), +52 the attach-timing diagnostic (remove after the attach spike or fold
# into heartbeat metrics), +100 shared startup traces (C2.7), +353 the nydusd
# spike (C2.1 candidate: blob-toc conversion, the store node's virtual blobs,
# the opt-in nydusd device; keep or delete with the spike's verdict), +75 the
# C1.1 second pass (zswap bounded per paused cgroup, stall backoff, eviction by
# expected idle, reclaim stop counters; benchmarks/pause-reclaim-2026-10-03),
# +5 admission puts running rollouts first (every queued wake reserved; a
# swapped wake owes its prefetch), +7 net: chunk reservations close the convert
# race (+66), paid for by deleting the attach-timing diagnostic and the
# superseded chunk lookup (-59), +50 nydusd as config (chunk_store.nydusd, pinned
# by sha256; the spike's environment switches deleted). The Python RAFS reader,
# its cache and trace prefetch go once nydusd is the only RAFS path (M2).
# +28 blob-tail chunks live with their root (registration reads the tails).
# +517 node-local model waits (local_wait.py: nftables NFLOG of private relay
# flows, pause on an outstanding call, thaw on its answer; the relay's
# acknowledged delivery and delayed wake). When every trainer uses the private
# relay, the relay-driven park, the gateway's per-wait park and warm_park.py's
# relay role go. +35: a pause landing on an answered call is undone (retried,
# logged), and an answered call is never escalated to hibernate. +4 tunnel
# calls (the relay benchmark's agents) take the local path too, +17 the relay
# reads acknowledgment from a duplicated socket (a fast reader's close raced it).
# +36 locators and nydusd blob layouts stored at registration: runtime reads no
# longer query the index (gate run 3's stall), +37 nydusd in the node bundle
# (pins, VM init's verification and install), +32 nydusd's cache within
# cache_bytes (LRU detach of idle components). +245 chunk-migrate convert, record,
# switch, revert and status (M2 plan §5 steps 2-3; the plan sized the tool at
# ~400 with release). +16 the index's presigned S3 reads retry transport errors
# and 5xx (one read timeout failed a gate conversion), +8 registration range-reads
# only each tail's chunk table and the client waits 600 s for it, +14 builders'
# store-node fills (the write token) run unhedged in half the S3 slots, +44 warming
# covers nydusd blob tails and layouts, and an M2 switch dispatches only a fully
# warm image, +17 a stalled read is latency, not EIO (nydusd retries the node for
# 270 s inside a 600 s NBD timeout), and RAFS attaches have their own 8 slots,
# +3 a switch skips an image whose warm call fails, +21 a builder's store-node
# reads install first-to-evict and never promote (M2 waves outsize the cache),
# +20 only a blob's first nydusd starts alone, +10 converters split a wave
# (--shard), +7 a converter's verification warms first-to-evict (keep false),
# +3 env-io's LimitNOFILE 65536, +23 retention holds converted roots 72 h for
# recording and the build cache's client takes timeout_seconds (registry prune
# crashed hourly), +91 net: the store node as a full replica of its S3 prefix
# (mirror loop, no eviction, a residency check; batched switch), less the cold
# placement it obsoletes, +7 a switch verifies the whole closure first, +15 the
# index keeps stored locators in its database (S3 only as the permanent copy).
# +32 the store node's replica and index on a Volume (store_node.data_device:
# mounted and bound by store init; the node becomes a small, replaceable type).
# +10 net: a response's ranges are faulted in on a read thread before the loop's
# sendfile (a Volume read stalled every request), less the on-loop hit path,
# +10 served reads' queue, read and send times (the store node only timed S3),
# +27 ucloud-chunk-serve wiring: store_node.native_server_sha256, store init
# verifying and running the Go read server, the node on loopback behind it (the
# server itself is runtime/chunk_serve, Go: one GIL capped reads near one core).
# +30 the placement worker logs why it deferred commands (a burst retried 182 of
# 512 creates and nothing said why), +7 a create waits for an environment
# attach instead of polling the queue every 2 s, +8 creates in flight sized to
# every node's startup slots (a fixed 32 capped bursts near 6/s).
# Lower it on deletions (C1.3 is ~8k).
PACKAGE_LINE_BUDGET = 113_478
PACKAGE = Path(__file__).resolve().parents[1] / "ucloud_sandboxes"


def package_lines(root: Path = PACKAGE) -> int:
    total = 0
    for path in root.rglob("*.py"):
        data = path.read_bytes()
        total += data.count(b"\n") + (1 if data and not data.endswith(b"\n") else 0)
    return total


class PackageBudgetTests(unittest.TestCase):
    def test_package_stays_within_line_budget(self):
        lines = package_lines()
        self.assertGreater(lines, 0, f"no Python sources under {PACKAGE}")
        self.assertLessEqual(
            lines, PACKAGE_LINE_BUDGET,
            f"ucloud_sandboxes/ has {lines} lines, over the budget of "
            f"{PACKAGE_LINE_BUDGET}; delete code or justify raising the budget",
        )


if __name__ == "__main__":
    unittest.main()
