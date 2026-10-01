# Python source and exact EROFS qualification

The retained Python `app-change-20` image from the previous cold qualification
exceeded both old selective-materialization limits. This was an input-size
fallback, with no unsupported tar semantics found in the selected layers.

| Selected input | Compressed bytes | Unpacked tar bytes |
| --- | ---: | ---: |
| Group 3, layers `[4,10)` | 31,884,562 | 88,381,952 |
| Group 4, layer `[10,11)` | 313,043,301 | 1,014,097,920 |
| Group 5, layers `[11,15)` | 790,932 | 7,176,704 |
| All three groups | 345,718,795 | 1,109,656,576 |

The old limits were 128 MiB compressed and 1 GiB unpacked. The selected aggregate
exceeds both, so increasing only the compressed limit would still have fallen
back. The read-only inspector authenticated each compressed digest and complete
uncompressed diff ID. It scanned 41,168 members without extracting files and
found no whiteouts, unsupported hardlinks or special files, unsupported extended
metadata, duplicate paths, or missing explicit parent metadata. The 131 symlinks
were supported. See [all selected layers](python-selected-layer-inspection.json)
and the [large pip layer](python-pip-layer-inspection.json).

One new owned image republished the unchanged frozen source on the baseline
runtime before the measured baseline wave. Its build completed in 3.327 seconds,
with all BuildKit RUN steps cached and all six EROFS groups reused. All 15 source
diff IDs and compressed descriptor triples were identical to the original.
The original strict verifier intentionally failed because the new image name
changes `Labels["ucloud-sandboxes.image-id"]`. A separate
[source verification receipt](python-source/source-verification.json) checks
that exact original/new name binding, permits only that label substitution, and
requires every other runtime-config field to remain identical. The original
[failed strict receipt](python-source/summary.json) remains intact.

After the baseline wave and its 60-second writeback observation, one idle owned
builder ran the candidate wheel in a separate package directory. Its service
remained on the baseline runtime. The qualifier forced groups 3, 4, and 5 missing
simultaneously without deleting registry content. Registry writes were blocked;
publication was captured locally and every EROFS file was rehashed and its signed
metadata authenticated.

All four ABBA trials matched all six previously published component documents
exactly, including EROFS image digests and sizes. Each trial rebuilt exactly the
three selected groups (618,655,744 EROFS bytes). Both B trials used the isolated
selective subprocess, materialized 11 OCI layers, streamed exactly 345,718,795
compressed bytes, and skipped Docker. See the
[full byte comparison](python-byte-comparison.json),
[private candidate staging receipt](python-byte-stage.json), and
[post-proof live idle check](byte-live-after.json).

| Arm | Local diagnostic wall seconds | Docker path |
| --- | ---: | --- |
| A | 22.236 | Yes |
| B | 11.904 | No |
| B | 11.847 | No |
| A | 3.818 | Yes |

These are correctness diagnostics, not production speedup measurements. Docker's
local cache warmed during the first A arm; the much faster final A demonstrates
that confound. The measured fleet comparison is reported separately. The source
republish also primed one baseline builder's small local cache before the first
wave; its full-image EROFS cache was already retained in the registry.

The byte proof ran from `2026-09-29T22:32:15.111001Z` through
`22:33:05.033766Z` against candidate wheel
`3f29209df80e27d3aad2d1edb3c1677ce05bb44d50b78da1797b6b0a212c13f3`.
All four builders were directly idle with their original node epochs at
`22:33:35.369880Z` before the subsequent candidate upgrade.

Local helper validation: seven existing/multi-group/inspector tests passed;
three additional configuration-normalization tests passed. The latter reject
changed environment variables, unrelated labels, and incorrect source/name
bindings. Ruff and whitespace checks passed. This proof does not cover a mixed
500-sandbox workload or a long training soak. After the global candidate deployment, all three application assertions passed
on one newly provisioned worker: Python, TypeScript tools, and TypeScript
multistage. Each checked revision 20, the expected source-module count, grouped
rows, and score. All three owned sandboxes were deleted by
`2026-09-29T23:02:23.750449Z`; their creation took 88.27–88.30 seconds including
fresh-worker provisioning. These creation times are not build latency.

The [smoke summary](image-smokes/summary.json) and per-sandbox created receipts
preserve each resolved repository/tag/manifest, live worker job/epoch/endpoint,
and an independently read-back exact image-pull lease tuple. At
`23:03:45.726435Z`, the [fresh-worker audit](worker-audits/168019452.json) verified
all 165 installed package files and the exact file set against the candidate
wheel, matched launcher import paths, and observed the same process and worker
incarnation. All five APT/unattended-upgrade units were masked and inactive;
effective APT periodic enable, package-list update, and unattended upgrade values
were all zero. The audit changed no service or configuration.
