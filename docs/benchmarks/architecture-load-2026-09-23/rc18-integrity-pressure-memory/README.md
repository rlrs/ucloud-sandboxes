# rc18 eventual integrity under memory pressure

Run `relay-load-e46611368211` completed **2048/2048 cycles and 256/256 unique
normal primary exits**, with zero scenario or cleanup errors. This qualifies
eventual crash/integrity behavior for the tested workload. It does **not** qualify
healthy performance: fleet health recorded 11 failures (eight entries from two
failed resource probes and three stale-heartbeat entries), and useful-action p95
was about 53.94 seconds.

The workload retained 256 primaries, eight cycles, 1.5 GiB resident heaps,
384 MiB dirtied per cycle, the same four workers and full integrity checks.
This profile explicitly used 1800-second SDK and continuation budgets within an
1800-second overall deadline. Prior runs with the original 180-second budget
remain failures. Normal primary exit is checked after the final integrity proof,
so completed workloads release heap memory and retire growth forecasts; global
sandbox cleanup still occurs after all scenarios finish.

The matched terminal records are in `normal-primary-exits.json`. Two-second RAM
samplers found minimum physical available memory of 7.83–10.91 GiB and minimum
RAM-backing available space of 9.09–12.89 GiB across the four workers. No sampler
failed. The captured worker journals contain no checkpoint command timeouts.
Periodic samples do not prove exact headroom at every allocation.

Fresh runtime counter observations bracketed the trial: completed-park counts
increased by 187, 124, 144, and 139 on workers 12400622, 12400623, 12400625, and
12400626 respectively, totaling **594 observed completed parks**. These deltas
avoid counting earlier trials on the reused fleet. The harness separately saw
parked state during 155 measured cycles; that is an observation count, not the
total number of captures.

All raw RAM samples, matched primary exits, bounded observations, and the
derived summary are retained here. Worker journals were retained privately under
`/private/tmp/rc18-integrity-pressure-evidence`. Server source remained identical
to rc18 throughout this qualification.
