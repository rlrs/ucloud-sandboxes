# rc18 pressure with normal primary completion

Run `relay-load-4ccf32378228` retained the original 180-second SDK and guest
continuation budgets, 256 primaries, eight cycles, 1.5 GiB heaps, and all integrity
checks. The only workload lifecycle change was normal primary exit after the
last proof, followed by authoritative terminal-status validation.

It failed on sandbox 0029's cycle-zero continuation deadline at 15:41:34 UTC,
before any scenario reached normal completion. This rules out retained finished
heaps as the cause of this failure. Existing failure reports remain unchanged;
this run did not qualify the original deadline.

At 15:41:57 the durable managed-process records still showed all 256 primaries
running with no terminal failures. Worker samples and observations are retained
here. Initial measured capture bursts were large (26 and 19 on two workers), then
fell to about three concurrent captures each as I/O pressure rose. The captured
journals contain no checkpoint command timeout. This evidence supports progress
and retained memory headroom, but does not establish acceptable continuation
latency or successful eventual workload completion.

A separate eventual-correctness profile may explicitly select a longer
`--continuation-timeout-seconds`, still bounded by the overall run deadline. Its
result cannot retroactively pass this run or the unchanged latency SLO.
