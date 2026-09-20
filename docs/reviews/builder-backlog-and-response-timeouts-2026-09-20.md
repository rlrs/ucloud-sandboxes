# Builder backlog and response timeouts

Release 0.5.64 fixes two builder admission problems and a gateway transport error
path found while inspecting a production run.

The builder autoscaler converted any positive number of pending builds into a
one-node target. Production allowed four builders, but the controller reported
that one builder satisfied a backlog of thirteen builds. The remaining builder
admitted four builds at once and returned `builder_busy` for excess submissions.
The accepted builds eventually succeeded; repeated admission failures were not
Docker build failures.

The controller now computes its target from admitted plus pending builds and the
default per-builder execution concurrency, subject to the configured VM budget.
Builder agents queue excess submissions with their immutable contexts and existing
build IDs. Only executing builds start threads. Queued builds retain deduplication,
conflicting-spec checks, drain protection and the existing `running` API status.
No SDK contract change is required. The concurrency setting controls execution,
not whether an otherwise valid build is accepted.

Queue ownership remains local to the accepting builder. New builders can take new
submissions; they do not steal an already accepted queue. Agent restarts mark
interrupted records failed under the existing contract. Cold startup with no ready
builder still uses `builder_not_ready` and the SDK's submission retry path.
These changes do not introduce a durable gateway queue or resumable image builds.

Separately, response headers could arrive successfully and then urllib3 could
raise a timeout or protocol exception while reading the body. That exception was
outside the gateway's existing exception conversion and could close the client
connection without a structured response. Buffered responses now return the
existing structured transport error; the gateway does not replay the operation.

The run also lost three worker VMs. Provider power-off observations preceded the
controller's stop requests. Sampled park and job-status traces spent roughly sixty
seconds waiting for those workers, with only milliseconds of gateway thread CPU.
This does not establish why the VMs powered off, and release 0.5.64 does not claim
to prevent those losses. Raw production records remain outside the repository.

## Verification and deployment

The canonical checks passed: 972 server tests (six skipped), 118 SDK tests, Ruff,
shell checks, Go tests and installed-wheel checks. A further 181 targeted tests
passed against the packaged wheel on the production Linux/Python runtime.
Regression tests cover queue saturation, bounded execution, queued deduplication
and conflicts, context cleanup, backlog scaling and response-body failures.
[Release CI](https://github.com/rlrs/ucloud-sandboxes/actions/runs/35514828525) passed.

Runtime commit `9fe975b698938d556685ed4b280b635e7b709fa9` was deployed at
13:55:39 UTC. Gateway, relay and autoscaler restarted; all 93 installed package
files matched the wheel. Both node bundles validated. Production had no sandbox
routes, reservations or live workers to roll at installation. New workers and
builders were subsequently observed running 0.5.64.

A gateway test submitted sixteen distinct Docker builds from a cold pool. The
controller requested four builders, all submissions were accepted, and every
build succeeded in 72.6 seconds including about 43–45 seconds of cold startup.
There were no `builder_busy` responses. The client did retry `builder_not_ready`
while VMs booted. Twelve builds reached the first ready builder and four reached
a second; this demonstrates the local queue's placement limitation during cold
startup. Test tags were local to disposable builder VMs and were not pushed to
the shared registry. This is build-path qualification, not a new sandbox-agent
concurrency benchmark.

## Concurrent placement follow-up

Repeating the test with four ready builders exposed a second placement problem:
all sixteen simultaneous requests read the same idle load before any submission
was admitted. They selected one builder, took 36.7 seconds to complete, and left
the other three idle. Refreshing heartbeats before selection was insufficient.

Release 0.5.65 adds atomic gateway dispatch reservations. Each selection accounts
for requests dispatched since its live-load sample began, including responses
that completed before selection. It also accounts for submissions still awaiting
node acceptance. Only the short reservation update holds the shared lock;
network calls and context uploads proceed concurrently. Submissions for the same
image ID use a reference-counted lock so concurrent retries can observe the first
submission's owner before attempting another dispatch. Reservation state is
process-local; this is not coordination across multiple gateway replicas.

The forced-race regression test synchronizes sixteen idle snapshots and verifies
four reservations per builder, even when responses finish before peers select.
The full checks passed with 974 server tests (six skipped) and 118 SDK tests.

Runtime commit `51c925bf85ecb5ec10d921b8254f230a80dded3d` was deployed at
14:06:17 UTC. Both bundles validated, all 93 gateway package files matched the
wheel, and 183 targeted Linux tests passed. The follow-up
[CI run](https://github.com/rlrs/ucloud-sandboxes/actions/runs/35515364626) passed.

A final live test waited for four ready 0.5.65 builders, then submitted sixteen
distinct builds simultaneously. They distributed exactly four per builder;
all sixteen succeeded in 12.6 seconds, with no `builder_busy` responses. The
36.7-second comparison used the same sixteen-build, eight-second Docker-step
shape with four ready builders before the reservation fix. These are controlled
build tests, not a guarantee about arbitrary Dockerfiles or upstream downloads.
The temporary builder reservation was deleted, no sandbox routes were created,
and test images remained local to the disposable builder VMs.
