# Background publication and lost relay callers

Production uploads ran for up to 235 seconds, beyond the storage client's
120-second timeout. The daemon continued publishing after the Warden released
its lock. A subsequent wake then failed with `volume is publishing; cannot
mount`, and restore cleanup failed with the same conflict for `discard`.

Release 0.5.55 gives local wake, discard and deletion precedence over a background
publication. A journal transaction retains the sealed local layers, fails the
superseded publication operation, and advances its revision. Upload completion
and failure remain fenced to their original operation/revision: neither can
replace the resumed volume's authority or remove its local checkpoint files.
The Warden no longer holds its lifecycle lock during network publication. Its
publication request carries the observed revision, so a delayed request cannot
seal a filesystem that has already resumed or been parked again.

Registry and S3 publication queues check that their snapshot is still current.
Superseded queued uploads exit without waiting for active uploads to complete.
An upload already performing remote I/O may finish, but cannot commit stale
local authority. This change does not make local checkpoints survive worker loss.

The relay now reads the gateway's durable loss identities in its maintenance
loop. It completes pending and leased requests for those exact sandbox
generations with HTTP 410 / `node_lost`, releasing queue and payload capacity.
Already committed model responses are retained and their delivery holds released;
no successful wake is fabricated. A replacement sandbox generation is unaffected.
An absent route or transient connection failure alone does not trigger this cleanup.

Regression coverage exercises blocked uploads racing with wake/discard/delete,
both upload success and failure, owner and revision fencing, a delayed upload
after another park cycle, the Unix socket path, Warden lock handoff, cancellation
in both backend queues, and durable relay cleanup with preserved model results.
