# Worker exec-start stage evidence

The rc13 natural-512 run uses four fixed workers. The retained file contains
100 Tempo samples selected by `node.exec.start_ms > 200`; it is a **slow-path
sample**, not the full workload latency distribution. Existing spans gained
monotonic wall and current-thread CPU stage measurements; no profiler was
attached to the acceptance run. Sample median worker exec start was 598 ms,
p95 1,258 ms, with median thread CPU 43.8 ms.

Median stage wall/CPU milliseconds: lifecycle 167/12.3; record 171/12.2;
capacity 142/10.8; command/lease 62.8/5.3; Popen 4.6/0.8; pump thread startup
14.1/1.1. Stage medians are not additive. This localizes the expensive path
to repeated lifecycle/registration/admission checks, but does not yet identify
a specific syscall, lock, or decoder as the cause.
