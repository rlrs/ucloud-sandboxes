# Builder scheduling review — 2026-09-29

The candidate keeps four execution slots per production builder and removes
the four additional admitted queue slots. New demand remains pending at the
gateway until a builder can accept it. This is a source/test review, not a
claim that the candidate has passed the live 48-request qualification.

At the time this review was saved, the frozen wheel and node bundles were
staged, the read-only EROFS canary was using one builder, and the candidate node
package had not been activated. No production changes were performed as part
of this review. See [wheel-comparison.json](wheel-comparison.json) for the
staged release digest and five changed Python modules; the scheduling runtime
changes themselves are limited to `control_plane.py` and `node_agent.py`.

## Rationale and behavior

The prior overload run completed all 48 requests, but accepted work became
bound to a busy builder while peers freed execution slots. The interval
reconstruction in [queue-analysis.md](../build-load-2026-09-29/queue-analysis.md)
found 23.880 continuous seconds with queued work behind a full owner and a free
slot elsewhere. Four TypeScript builds admitted to one builder waited about
108–109 seconds behind long Python builds. These observations identify the
placement problem; they do not establish a counterfactual speedup.

The candidate changes the following paths:

- `_select_builder_node` probes an image's running owner before applying the
  capacity gate, including in a single-builder pool. An existing owner is
  allowed at capacity so a matching request can join and a conflicting request
  still receives the existing conflict response.
- New work uses current authenticated heartbeat load even for one builder.
  Node/job/deployment/epoch identity, draining and admission checks still
  apply. In-process dispatch reservations account for concurrent assignments
  made since the heartbeat sampling began.
- `_reserve_builder_candidate` selects the busiest eligible builder with
  fewer than four active image builds. If every candidate is full, no owner
  is assigned and the existing pending-demand record remains available to
  autoscaling and client retries. Context forwarding begins only after a
  builder is selected.
- `build_builder_node_agent_server` supplies `max_queued_builds=0` while
  retaining `queue_builds=True` and `max_active_builds=4`. The existing
  transactional `ImageBuildStore.reserve_build` limits **nonterminal records**,
  including context preparation before a thread starts. Its duplicate and
  conflict checks precede the capacity check. This is the authoritative gate
  when different gateway processes act on stale samples.

The per-image host file lock continues to cover owner probing, selection and
dispatch. The generic `ImageManager` queue API and its default behavior are
unchanged. There is no new durable central work queue, held HTTP wait, context
migration or change to execution concurrency.

## Retry contract and boundaries

| Condition | Existing response contract |
| --- | --- |
| No eligible execution slot | HTTP 503, `builder_not_ready`, retryable, `Retry-After: 2` |
| Stale-sample race reaches a full node | HTTP 503, `builder_busy`, retryable, `Retry-After: 2` |
| Node admission is closed while draining | HTTP 503, `node_admission_closed`, retryable, `Retry-After: 1` |
| Matching active build at capacity | Existing build returned; no additional build reservation |
| Conflicting active specification at capacity | Existing HTTP 409 conflict |

SDK 0.4.33 supports these retryable submission responses. More submission 503s
are possible because excess work now waits before node admission. Compare
complete client latency and successful completion, including all retries;
lower node queue time alone does not prove an improvement.

Production provisioning emits no `--max-active-image-builds` override, and
both the builder CLI and gateway use `DEFAULT_MAX_ACTIVE_IMAGE_BUILDS = 4`.
A manually configured builder below four remains protected by its own atomic
limit. A manually configured builder above four would be underused by the
gateway's default limit; custom heterogeneous capacities are outside this
change.

Reservations for duplicate joins and overlapping heartbeat/image-operation
accounting can conservatively reduce apparent free capacity until a subsequent
sample/retry. Single-builder submissions now pay for owner and live-heartbeat
GETs and fail closed on ambiguous owner replies, matching existing multi-node
behavior. Draining or stale nodes remain excluded from candidate discovery;
this change does not establish fleet-wide ownership during ambiguous node
loss or drain.

The qualification invariant is **at most four admitted nonterminal builds per
builder**, with preparation counted, and at most four executing builds. Do not
require every `queue_wait_ms` value to equal zero: the retained internal queue
can handle a brief handoff while a terminal build's thread finishes cleanup.
A few milliseconds of such timing is not evidence that extra queue slots have
returned. Conversely, a fifth nonterminal reservation is a failure even if its
reported queue time is small.

## Tests and independent review

The following complete related suite passed locally on 2026-09-29:

```sh
.venv/bin/python -m unittest \
  tests.test_builder_selection tests.test_builder_packing \
  tests.test_node_agent tests.test_images tests.test_control_plane
```

Recorded result: **131 tests passed in 13.840 seconds**. Localhost API tests
were run with the required sandbox network permission. This result was
observed in the session; a separate raw test log was not saved.

New and updated coverage includes:

- Twenty simultaneous stale samples select at most four builds on each of
  four builders; the remaining four submissions stay unassigned.
- Live peer availability overrides stale periodic full-load heartbeats,
  while an entirely full fleet retains pending work.
- A full single builder still receives an existing owner's retry, and new
  work is refused after fresh heartbeat inspection.
- Eight simultaneous submissions paused during context materialization admit
  exactly four and reject four before any execution thread starts.
- Real localhost HTTP calls against a full builder preserve duplicate build
  identity, reject conflicting arguments, retain the uploaded context after
  capacity rejection, and report draining readiness only after work completes.
- Existing generic queue, atomic store reservation, gateway context forwarding
  and control-plane API tests continue to pass.

The gateway context-forwarding fixture was corrected to register the actual
builder epoch, required by the newly exercised single-node live-heartbeat
identity check. The runtime identity check was not weakened.

These checks also passed:

```sh
.venv/bin/ruff check \
  ucloud_sandboxes/control_plane.py ucloud_sandboxes/node_agent.py \
  tests/test_builder_selection.py tests/test_builder_packing.py \
  tests/test_node_agent.py tests/test_images.py tests/test_control_plane.py
git diff --check -- \
  ucloud_sandboxes/control_plane.py ucloud_sandboxes/node_agent.py \
  tests/test_builder_selection.py tests/test_builder_packing.py \
  tests/test_node_agent.py tests/test_images.py tests/test_control_plane.py
```

Two independent read-only reviews (`qualification_plan` and
`gateway_background`) found no blocking issue in the scheduling changes. Both
confirmed owner-before-capacity ordering, atomic preparation admission,
preserved duplicate/conflict semantics and cross-process protection. Their
conservatism, extra single-node probes and ambiguous node-loss/drain limits are
recorded above.

## Remaining acceptance evidence

Follow [QUALIFICATION.md](QUALIFICATION.md) with four CCX33 builders and
unchanged execution/cache policy. Verify the actual gateway and builder source
fingerprints, not only staged bundle hashes. Record all 48 successful terminal
builds and durable history entries, real-sandbox smoke results, client
p50/p95/max, batch duration, submission retries/wait, admitted and execution
overlap, preparation and queue timings, and fairness when peers have capacity.
Keep warm-cache chronology and selective EROFS changes explicit when comparing
releases. The historical 172.2-second overload batch and 162.5-second client p95
are context, not a controlled baseline for a newly warmed candidate fleet.
