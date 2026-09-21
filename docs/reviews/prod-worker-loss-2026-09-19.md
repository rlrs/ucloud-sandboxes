# Missing routes during the 13:08 UTC production run

The `super-park-harness-*` run lost worker **12396482**, which owned **103
sandbox routes** immediately before loss. UCloud's job history reports the VM
powered off at **13:10:09.018 UTC**, then transitioned to SUCCESS at
13:10:10.298. The user clarified that this is the known UCloud VM-loss behavior and that
our responsibility is to handle the loss, not investigate the platform bug.

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
change. The earlier interpretation of a confirmation as an intentional user stop was
incorrect and is superseded by that clarification. It does not make fast local checkpoints
survive node loss, or establish that the separate admission-pressure errors
are resolved.

[Structured evidence](../benchmarks/prod-worker-loss-2026-09-19.json).

## Handling in 0.5.54

The gateway retains a loss record for seven days, fenced to the sandbox's latest
allocated generation. Lost sandbox requests now return HTTP 410 with
`error_code: node_lost`, `retryable: false`, the sandbox generation and loss time.
The record is committed atomically with removal of the route. Existing retained
terminal program losses are backfilled. Unknown IDs remain 404, deletion remains
idempotent, and a recreated ID cannot inherit the earlier generation's loss.

The relay already treats 410 as a permanently unavailable caller: it releases
the delivery hold and acknowledges the existing model result without rerunning
the model or recording a successful wake. Regression coverage checks retained
response replay across restart. The runner must replace/retry the failed agent
attempt; retrying operations on the lost incarnation cannot recover its state.

## Deployment verification

Server 0.5.54, commit `7fe61b3305876be2cb1f9809e06431428b478517`, was
installed on the gateway at 13:27:38 UTC. Both future worker bundles passed
production boot validation; existing workers did not require a restart for
this gateway-only behavior change. The relay process was preserved.

All 924 server tests (six platform skips), 118 SDK tests, canonical lint/build
checks, and CI run 35445756458 passed. Public job-status requests for all
103 incident sandbox IDs returned HTTP 410, `error_code: node_lost`, and
`retryable: false`. Both sync and async SDK 0.4.23 exposed that terminal error.
An unknown sandbox ID still returned 404. Public gateway and relay health
returned 200; gateway, relay and autoscaler services were active. The initial
verification probe used an operator-only GET endpoint with an SDK key and
correctly received 403; the successful checks used the public job-status path.
