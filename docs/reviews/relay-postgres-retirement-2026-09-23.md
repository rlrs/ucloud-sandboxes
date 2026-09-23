# Relay authority retirement: 0.5.114

The sole documented production deployment already uses PostgreSQL. This release
removes the live SQLite relay and its second lifecycle retry controller. Startup
requires `relay_postgres`; a legacy configuration is rejected before credentials,
routing or journal writes. No automatic empty authority is created. Gateway and
worker SQLite journals are unaffected.

The version-3 decoder remains in `shared_control/legacy_relay.py`, reachable only
from the offline idle importer. Import first checks row and encoded-byte bounds,
then checks idle state and target identity. PostgreSQL provisional commit, source
fence and target activation retain their existing ordering. A new real-database
crash test interrupts activation after source fencing and verifies the exact
import can safely finish. An old source cannot activate an unrelated empty target.

HTTP completion now has one contract: result and wake intent commit together;
acknowledgment reports durable acceptance, while pending delivery remains
recoverable independently. Transport performs one fenced attempt and returns a
typed deferral; PostgreSQL owns retry eligibility and next-attempt time. There
is no sleeping in-process retry authority.

## Behavioral test migration

`test_model_relay.py` now uses the production PostgreSQL app, including exact
HTTP bytes, headers, authentication, duplicate identity, batch polling, lease
expiry/renewal and the SDK worker contract. `test_model_relay_stateful.py` runs
the same registration/claim/renew/response/error/cancellation state machine
against real PostgreSQL, with database-clock expiry and strict changed-response
rejection. The keepalive test also uses the production PostgreSQL app.

The former SQLite implementation tests map to the following real-PG contracts
in `test_postgres_relay.py` (test names are abbreviated):

| Former behavior | Canonical contract coverage |
| --- | --- |
| Lost pending/leased callers; retain committed results | `terminal_node_loss_retires_pending_and_leased_callers`, `terminal_history_does_not_amplify_database_work` |
| Lifecycle executor saturation and retry progress | `wake_progresses_while_park_dispatch_is_full`, `deferred_park_releases_durable_claim_without_losing_intent`, transport durable-deferral tests |
| Terminal/transient wake and response release | `result_and_wake_commit_atomically_and_retry_without_client`, `http_terminal_worker_error_uses_same_acceptance_contract`, `timeout_wakes_parked_caller_to_deliver_terminal_error` |
| Committed result cannot be followed by late park | `committed_result_releases_queued_park_even_without_notifications`, `does_not_park_after_result_committed_before_dispatch`, `wake_does_not_wait_for_park_ack_and_late_transport_proof_reattaches` |
| Caller disconnect and cancellation; restart | `implicit_calls_remain_distinct_until_disconnect`, `cross_instance_enqueue_lease_result_and_reattach`, `process_replacement_recovers_uncertain_wake_same_identity`, `http_lost_ack_and_relay_restart_preserve_delivery` |
| Failure before result commit | `commit_failure_rolls_back_both_result_and_intent`, `poll_hydration_failure_rolls_back_heartbeat_and_claims` |
| Expired lease/poll wakeups without API traffic | `expired_inference_lease_requeues_without_losing_request`, `batched_poll_fallback_recovers_enqueue_and_expired_lease_without_hints`, `idle_pollers_share_readiness_queries_and_cancel_cleanly` |
| Registration metadata/generation; replacement fences | `registration_metadata_rejects_aliases_and_generation_coercion`, `registration_replacement_fences_old_leases_and_cancels_waiter`, stateful replacement/stale-token rules |
| Completed capacity and deferred pins | `storage_budget_is_admission_only_and_completed_result_survives`, `pending_delivery_cannot_expire_from_retention_gc`, `gc_reclaims_reserved_space_without_losing_retained_response` |
| Exact SQLite replay, startup decoding, cutover | `idle_cutover_preserves_tokens_and_replay_and_fences_legacy_writer`, importer refusal/atomicity/recovery tests, offline codec and size-bound tests in `test_relay_retirement.py` |
| Absolute request expiry | `timeout_wakes_parked_caller_to_deliver_terminal_error`, stateful database-clock expiry |

Old heap indices, in-memory tombstone counters, restart heap reconstruction,
synchronous notifier joining, and fixed request-count/worker-count caps are
retired implementation details. Their substitutes are durable result retention,
measured storage admission, shared claims, token fencing, and independent durable
delivery. The test suite does not emulate the removed backend to keep those old
internal assertions passing.

The isolated qualification server now mounts two PostgreSQL authorities with
distinct deployment identities. It cannot accidentally restart an old live
SQLite relay while testing the candidate. The shared-control benchmark measures
the sole PostgreSQL runtime; historical SQLite comparison reports remain archived.

## Qualification

The final focused Linux run passed **90 tests in 19.538 seconds**, including real
PostgreSQL, production HTTP, synchronous/asynchronous SDK phase calls, the generated
state machine, migration crash recovery, durable transport, keepalive and runtime
compatibility/consolidation. No tests were skipped in this focused run.

The final combined Linux gate passed **1,504 tests in 127.648 seconds**, with
12 environment-dependent skips, real PostgreSQL and the release SDK. It includes
the relay renewal fix, calibration, typed transition admission, runtime assembly,
canonical workspace identity and split storage accounting. Earlier mixed-mirror
and private-fixture failures were resolved before this complete-source run.
The log is `/tmp/combined-architecture-final-r2.log` on the qualification host.
