# Autoscaler routing snapshot

The autoscaler previously materialized every retained exec-session route each
cycle, although it only consumes sandbox ownership and capacity demand. It now
calls `RoutingStore.load(include_exec_sessions=False)`. The existing snapshot
transaction, expiration pruning, PostgreSQL serialization retries and default
full snapshot behavior are unchanged. Exec routes remain stored for clients.

`scripts/benchmark_autoscaler_snapshot.py` seeds 500 synthetic running sandboxes
over three workers and grows retained exec history. It compares the full and
autoscaler projections in alternating order, with 25 samples per projection.
The local PostgreSQL 17.11 fixture and Python 3.10.13 produced these medians:

| Retained exec sessions | Full snapshot wall | Autoscaler wall | Full client CPU | Autoscaler client CPU |
| ---: | ---: | ---: | ---: | ---: |
| 0 | 8.55 ms | 8.69 ms | 7.77 ms | 7.90 ms |
| 5,000 | 28.97 ms | 8.50 ms | 26.48 ms | 7.77 ms |
| 10,000 | 50.74 ms | 8.63 ms | 46.20 ms | 7.89 ms |

Raw samples are in `autoscaler-snapshot.json`. Client CPU excludes PostgreSQL
server CPU. This is a synthetic local snapshot benchmark, not a production
latency or 500-agent capacity result. At a five-second reconcile interval, the
measured client savings are only 0.004–0.008 CPU cores; this change alone cannot
justify downsizing the gateway.

The routing, PostgreSQL routing and autoscaler CLI suites passed all 203 tests.
The new shared contract checks that the projection retains every demand field,
still prunes expired signals, produces the same capacity demand, and leaves exec
history intact. Ruff and diff checks pass. Test schemas were unique and removed
afterward; no production access was used for this change or benchmark.

Reproduce with the repository's PostgreSQL dependencies installed and a local
test DSN in `UCLOUD_TEST_POSTGRES_DSN`:

```sh
python scripts/benchmark_autoscaler_snapshot.py --backend postgres \
  --routes 500 --exec-counts 0 5000 10000 --iterations 25
python -m unittest tests.test_routing tests.test_postgres_routing tests.test_cli
```
