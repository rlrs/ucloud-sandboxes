# Create confirmation fencing and continued wake qualification

The 0.5.90 cold-fleet mixed-load run failed after 147 completed cycles. Sandbox
`relay-load-e0436b0cb7c5-0194` created successfully on worker 12399484 and registered
with the relay, then its first upload received `sandbox route not found`. The
worker retained the sandbox after gateway cleanup; it was removed by exact ID and
generation. No customer sandboxes were involved.

The matching reconciliation race is deterministic: create confirmation inherited
the placement heartbeat's old activity revision. A complete inventory sampled
before creation, received later, could delete the confirmed running route. The
receipt-time fence does not establish sampling order. Node create and targeted
recovery responses now carry a post-observation activity revision and boot epoch;
the gateway persists that fence. An older inventory cannot remove the route,
while a genuinely newer absent inventory still can. A different boot is rejected.
An inventory-removal metric now records the two revisions for future diagnosis.

The regression fails with the former confirmation behavior and passes with the
fix. 232 Linux tests pass, including routing, gateway, node runtime, real HTTP
create/recovery, and streaming upload. Two stale test assumptions were corrected:
a private registry fixture directory must be mode 0700, and adaptive park retry
intervals need not exceed ten seconds.

The load harness now separates first usable tool execution from its subsequent
512 MiB memory integrity scan. It still performs the same scan and file check on
every cycle, and any failure invalidates the run. First-tool latency includes an
external exec confirmation; full integrity completion is reported separately.
Earlier combined latency results are not directly comparable to this new metric.
Explicit forced-parking qualification is also required: naturally retained warm
sandboxes cannot qualify actual restore performance.

Qualification is still open. No subsecond production claim follows from these
component tests or the aborted load run.
