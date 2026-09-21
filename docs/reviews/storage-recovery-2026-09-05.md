# Storage recovery and admission fixes

Release 0.5.29 addresses a production restore failure that left a sandbox parked,
its completed model response undelivered, and unrelated creates retrying.

XFS mounts now explicitly use `nouuid` for independently owned COW views, which
retain the snapshot parent's filesystem UUID. This is the documented Linux
[snapshot mounting option](https://www.kernel.org/doc/html/v6.15/admin-guide/xfs.html).
The volume/generation journal and exclusive backend ownership remain the authority;
no checkpoint identity or device ownership checks are disabled.

A failed snapshot mount can be discarded back to its sealed or published parent.
Recovery is permitted only when the journal identifies the failed operation as
`MountSnapshotCow`. Cleanup resolves backend ownership before releasing a device,
because its numeric ID may already have been recycled for another sandbox.
Other terminal errors remain fenced.

Autoscaling now excludes storage-blocked capacity using the same storage admission
check as placement. Wake responses distinguish a source storage error from pending
snapshot publication. Relay statistics include pending delivery count and the age
of the oldest completed-but-undelivered response.

Regression coverage verifies recovery after a device ID is recycled, rejection of
unrelated terminal errors, replacement capacity despite an unhealthy worker,
accurate wake errors, and visibility of pending response delivery. An isolated XFS
check on UCloud reproduced duplicate-UUID rejection and verified independent writes
through the corrected snapshot mount. The full repository check passed 744 tests
(four skips), 82 SDK tests, Go tests, Ruff, shell and installed-wheel checks.
