# Missing routes during the 13:08 UTC production run

The `super-park-harness-*` run lost worker **12396482**, which owned **103
sandbox routes** immediately before loss. UCloud's job history reports the VM
powered off at **13:10:09.018 UTC**, then transitioned to SUCCESS at
13:10:10.298. The user subsequently confirmed the worker was intentionally stopped or
restarted.

Autoscaler cycle 164 still observed the job RUNNING with 103 sandboxes and no
stop intent. Cycle 165 observed SUSPENDED, classified it as destructive node
loss, and issued a stop. This sequence does not support ordinary autoscaler
scale-down as the initial trigger. The provider's powered-off report alone does
not identify whether the cause was guest failure, provider failure, or an
external stop request.

All four specifically traced missing callers belonged to this worker:
`ab8e75aa8284`, `c7afdef64393`, `fb7c2097b2d7`, and `bdee1edd6d8d`
(with the `super-park-harness-` prefix). The first successfully parked at
13:09:29 UTC, then its wake returned gateway 404 at 13:10:10. The second
successfully woke at 13:09:54 and parked again at 13:09:56, then job polling
returned 404 after worker loss. These are not never-created sandboxes.

The autoscaler removes non-portable routes for lost workers and records
`node_lost` on program requests. Fast local park does not provide a remotely
published checkpoint, so these sandboxes cannot safely be reconstructed from
an old image without losing their process state. The relay retained model
results, but that cannot recover the lost caller. Current sandbox HTTP requests
return the generic `sandbox route not found`, hiding the durable loss reason.

The last heartbeat, received at 13:09:57.880 UTC, showed 84,746 MiB available
memory, zero memory PSI, zero swap use, zero storage errors, and 89.16% CPU.
It does not support memory exhaustion at that observation, and cannot exclude
a failure in the following eleven seconds. Other workers did exhibit memory
PSI admission rejections in retained traces; those are separate from this
worker's loss and remain to be diagnosed.

At the final routing snapshot around 13:18 UTC, zero sandbox routes remained.
The investigation performed no sandbox cleanup, service restart, or runtime
change. The user confirmation establishes the intentional stop/restart as the trigger
for this batch of missing routes. It does not make fast local checkpoints
survive node loss, or establish that the separate admission-pressure errors
are resolved.

[Structured evidence](../benchmarks/prod-worker-loss-2026-09-19.json).
