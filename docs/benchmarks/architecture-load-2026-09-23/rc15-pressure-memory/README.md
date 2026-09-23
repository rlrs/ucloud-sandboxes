# rc15 pressure trial memory evidence

Run `relay-load-908d18341181` began at 13:46:02 UTC on September 23. It requested
256 managed primaries, each with a 1.5 GiB heap, a 2 GiB memory limit, and eight
natural model-wait cycles. It stopped after 61 completed cycles because one
managed-start admission deadline escaped as an internal HTTP 503. It did **not**
pass the workload gate.

The two-second worker samplers found minimum physical available memory of
10.71–13.81 GiB and minimum RAM-backing available space of 12.06–15.09 GiB across
all four workers. Each backing filesystem had 83.40 GiB capacity. These samples
cover the active trial and cleanup. They are instantaneous observations, not a
proof of the value at every allocation or fault.

At 13:48:08 UTC the retained gateway managed-process journal contained 252 running
records and no signaled, failed or nonzero-exited records for this run. The retained
worker kernel journals contained no OOM-kill or block-I/O-error messages in the
13:44–13:49 UTC window. Cleanup completed, and all four nodes subsequently reported
empty inventories. The observed park completion counters totaled 172.

The data supports that this trial avoided the earlier observed guest SIGBUS
failure while exercising pressure parking and restore. It does not establish the
original SIGBUS cause, nor replace a successful sustained trial after fixing the
admission retry response.
