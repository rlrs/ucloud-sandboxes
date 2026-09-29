# Cache import and concurrency diagnostic

**Importing 64 caches did not fix the repeated execution in this diagnostic.**
It took 120.306 seconds and showed application execution in all 12 request logs,
versus 73.380 seconds with eight imports and execution observations in six logs.
This measured release used at most **eight imports**. These results do not
support deploying the broader import set as a workaround.

[Diagnostic R1](cache-concurrency-diagnostic-r1.json) completed from 13:41:26.812
to 13:45:04.040 UTC on 2026-09-29. Each arm started with an independent empty
BuildKit store, using the pinned BuildKit 0.33.0 image and existing configuration.
The concurrent arms used four client requests at a time and the same 12 frozen
`typescript-tools` contexts, `app-change-5` through `app-change-16`. References
were bound to immutable manifest digests from one frozen cache inventory.

| Arm | Cases | Concurrent requests | Imports per case | Arm wall time | Application execution observations | Application cached observations |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Serial exact result | 1 | 1 | 1 | 1.569 s | 0 | 1 |
| Concurrent exact-first set | 12 | 4 | 8 | 73.380 s | 6 | 6 |
| Concurrent broad set | 12 | 4 | 64 | 120.306 s | 12 | 0 |

All arms reported zero cache-import error vertices, and the frozen registry
snapshot still matched after the trials. The three invocation-owned drivers
were removed with zero cleanup errors. The test made no registry image/cache
exports, manifest deletions, shared-builder changes or global prune calls.
The separate [13:58:21 final audit](final-state.json) now confirms that all ten
owned qualification nodes are retired, reservations are absent, the sampler is
inactive and production is healthy and idle.

The serial arm establishes that the selected exact result can be reused on an
empty store. It does **not** establish concurrency as the sole reason for the
misses: serial versus concurrent changed the number of imported manifests,
request concurrency and case count. The two concurrent arms more directly test
the proposed eight-to-64 import expansion, but sequential timing, shared solver
work and registry/host page-cache history still limit causal attribution.

Counts are per-request progress observations. BuildKit can share vertices or
replay progress across concurrent solves, so six execution observations are not
necessarily six independent worker executions or a measurement of CPU work.
These were `cacheonly` solves: they exclude final image push, EROFS publication,
SDK admission and runtime application verification. Their wall times must not
be substituted for the representative 48-build batch times. The earlier
[controlled semantic proof](CONTROLLED-PROOF.md) and three application smokes
provide separate output-correctness evidence.

The [completed follow-up](cache-diagnostics.md#second-diagnostic-results) separates
the variables using the same frozen immutable inputs. Both serial eight-import
cases executed (32.282 and 31.829 seconds), while all twelve concurrent exact-one
requests cached on one shared daemon (3.620 seconds) and on four exclusive
daemons (3.226 seconds). R2 removed all seven of its drivers without cleanup
errors. Multiple imports can therefore miss without concurrent solves; exact-one
imports preserved reuse under concurrency here. These cache-only results support
qualifying a narrow import policy, not substituting 3.620 seconds for a full
production batch. Combined-cache key/result selection is a supported hypothesis;
no specific upstream issue is established or claimed patched.
