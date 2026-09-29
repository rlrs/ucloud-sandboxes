# Retained BuildKit progress: execution-isolation baseline

The 48 owned `exec-repeat` builds show that the remaining TypeScript time is
primarily the application's lint/build/test/smoke instruction. Image and cache
export are already small. Python's larger apparent `RUN` durations are cached
layer download/extraction, not repeated dependency installation.

Source receipts are
[`../builder-execution-2026-09-29/exec-repeat/summary.json`](../builder-execution-2026-09-29/exec-repeat/summary.json).
The raw retained logs remain outside the repository. The
[sanitized report](baseline-buildkit-progress.json) contains only known fixture
labels, numeric timings/counts, vertex IDs/statuses, and input SHA-256 hashes.

| Recipe, 16 builds each | Actual RUN seconds per build, mean | Layer materialization seconds per build, mean | Image export mean | Cache export mean |
| --- | ---: | ---: | ---: | ---: |
| Python agent | 3.394 | 11.906 | 0.581 | 0.481 |
| TypeScript tools | 24.438 | 2.813 | 1.138 | 0.300 |
| TypeScript multistage | 26.969 | 4.750 | 0.356 | 0.356 |

These are sums of observed vertex durations within each build, averaged across
all 16 builds, including cached builds. They are **not additive wall-time phase
shares**: vertices can overlap or wait on shared work. Materialization includes
both explicitly `CACHED` vertices and vertices with layer transfer/extraction
evidence but no retained `CACHED` marker.

The TypeScript tools instruction executed 13 times, averaging 30.077 seconds;
three corresponding vertices were cached. The multistage instruction executed
15 times, averaging 28.767 seconds; another vertex materialized a cached result.
There were 31 explicitly cached Node dependency-install vertices and no observed
executed Node dependency-install vertices.

Python dependency installation was explicitly cached in all 16 builds. Fourteen
native-extension `RUN`-labelled vertices spent an average 13.607 seconds loading
layers, with no command-output evidence of rerunning the native build. BuildKit
can emit `CACHED`, then repeat the same instruction header alongside layer
transfer/extraction progress and cumulative `DONE` durations. The parser retains
that history and classifies these vertices as materialization. Actual Python
compile/smoke execution averaged 3.620 seconds across 15 executions; one was
cached. Reported transfer sizes are rounded BuildKit progress values and must
not be treated as independently measured network bytes.

The last client completion, case 007 (TypeScript tools, `app-change-7`), spent
79.267 seconds in client submission/admission and 31.899 seconds in the recorded
Docker build/push phase. Its progress records contain a 29.4-second application
`RUN`, 1.0-second image export (0.9 seconds exporting layers, 0.1 seconds pushing
layers), and 0.2-second cache export. The 3.271-second environment publication
follows that Docker phase. Export sub-operation timings are nested within their
vertex totals and must not be added again.

All 48 tails have their initial BuildKit banner, are below the 64 KiB retention
cap (largest 14,151 characters), and contain no unclassified vertex headers,
unterminated vertices, or error/canceled terminal statuses. This supports useful
coverage but does not prove a complete capture: the application can trim logs
without a marker. The report makes that limitation explicit. BuildKit rounds
timings, and these plain logs do not provide a DAG, absolute vertex start times,
per-command CPU attribution, or separate lint/compiler/test subprocess timing.
They establish the expensive instruction and distinguish execution from cache
materialization; they cannot establish that cache affinity alone will eliminate
the application instruction for a changed source tree.

Reproduce with Python's standard library:

```sh
python3 scripts/analyze_buildkit_progress.py \
  --logs-dir /tmp/ucloud-buildkit-investigation-20260929/logs \
  --summary docs/benchmarks/builder-execution-2026-09-29/exec-repeat/summary.json \
  --output /tmp/buildkit-progress-reproduced.json
```

The output path must be new. `--include-vertices` retains every sanitized vertex
instead of only the longest three per build. Six focused parser tests cover
repeated renderings, nested export timings, missing/truncated records, cached
materialization, ambiguous execution/materialization, safe receipt projection,
and discarded command output.
