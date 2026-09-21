# Controller termination review — 2026-09-21

The supplied review identified two risks that remain in the deployed 0.5.66
source (runtime commit `d12aba7`). This review rechecked code and retained
incident evidence. The 0.5.67 implementation below addresses these risks.

## Provider readiness is not proof of permanent guest loss

In [UCloud monitor.go at 923b39e](https://github.com/SDU-eScience/UCloud/blob/923b39e/provider-integration/im2/pkg/integrations/k8s/kubevirt/monitor.go#L87),
`hasInstance && machine.Status.Ready` selects RUNNING. Its alternative selects
SUSPENDED and the powered-off message. Thus the message alone cannot prove a
guest shutdown, boot change, or irrecoverable loss. The provider's deployed
revision remains unverified.

In 0.5.66, `_instance_phase()` returns LOST for a RUNNING → SUSPENDED →
RUNNING history, regardless of the current RUNNING state. The UCloud adapter
turns that into `post_start_suspension` destructive-loss evidence. Reconcile
then fences heartbeats, applies permanent route-loss handling, and authorizes
provider termination without the ordinary empty-inventory drain handshake.
Durable destructive stop records also preserve the classification.

The retained September 20 logs show executed stop requests for workers
12397021, 12397020, 12397037, and 12397038 after their SUSPENDED reports. That
ordering does not exonerate the controller from causing permanent loss: the
provider reports could have described a recoverable condition. Conversely,
these records do not establish that recovery would actually have occurred.

## The direct-probe safeguard is incomplete

Since the reviewed older revision, heartbeat retirement was renamed to
`ucloud_unreachable_retirement` and requires a failed direct transport probe.
A successful authenticated probe recovers the heartbeat; HTTP/auth/schema
errors do not authorize retirement. However, the failed-transport branch still
accepts nonempty inventories and assigned routes as destructive loss.

A local mocked reproduction reused
`test_ucloud_heartbeat_partition_preserves_occupied_worker`, forcing
`_probe_unreachable_node()` to return `(None, True)`. Replacement provisioning
was disabled with `max_create_per_cycle=0` to isolate the stop decision. Both
subcases failed the protection assertion: an occupied worker and a worker with
only gateway-known ownership were each returned in
`destructive_node_loss_job_ids`. No real provider calls were made. The existing
regression mocks a successful probe and therefore misses this network-partition
case. The historical RUNNING → SUSPENDED → RUNNING classifier was separately
reproduced returning `lost`.

## Required safety boundary

The 0.5.67 change separates temporary unavailability, proven loss
of a sandbox incarnation, and authorization to terminate a provider VM:

- Ambiguous readiness or reachability must fence new placement without deleting
  routes, emitting terminal sandbox-loss records, or authorizing VM termination.
- Recovery requires fresh authenticated boot identity and reconciled inventory.
  `node_epoch` already derives from Linux boot_id; a changed epoch must never
  silently inherit routes belonging to the old guest.
- Old prepared destructive operations need invalidation/revalidation; changing
  only the current classifier would leave a replay path through the journal.
- Occupied workers must survive simultaneous push-heartbeat and direct-probe
  failure. Elapsed time and a failed probe are not proof that their state is gone.
- Empty-worker retirement must retain explicit ownership and drain safety checks.

Regression coverage must include historical suspension with unchanged boot and
inventory, current ambiguous suspension, both heartbeat paths failing while
occupied, changed boot identity, and replay of old stop intents. A controlled
experiment with the executing autoscaler absent can then distinguish transient
provider readiness from guest loss without the controller deleting the subject.

## 0.5.67 implementation and qualification

UCloud readiness/history now classifies the VM as unavailable. Its adapter
advertises no destructive-loss authority based on provider suspension or
heartbeat silence. Controller-owned quarantine metadata uses the existing
heartbeat labels and survives worker pushes and controller restarts. A direct
authenticated continuity check can reopen placement; current SUSPENDED state,
incomplete or mismatched inventory, an obsolete probe, and a changed boot with
old assignments cannot. Active drain intents cannot stop quarantined workers.

Prepared and uncertain obsolete destructive stop records are marked failed to
disable retries, while preserving any `providerCallStarted` evidence. Accepted
calls remain in the audit log; removing their old proof also prevents them from
latching a newly observed guest as permanently lost. An already submitted
provider termination cannot be undone by this change.

The gateway fences old routes when an authenticated heartbeat proves a changed
boot. Cleanup is scoped to the retired boot so it cannot delete newly assigned
routes; published portable snapshots retain the existing detach/recovery path.

Regression tests exercise unchanged-boot recovery after historical suspension,
current suspension, simultaneous push/direct failure with occupied or
route-only ownership, durable quarantine, stale recovery rejection, mismatched
inventory, obsolete stop replay, and authenticated boot replacement. These are
controlled fault-injection tests with provider termination mocked, not evidence
that an actual UCloud readiness interruption has been reproduced.
