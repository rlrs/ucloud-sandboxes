# Block device retirement after filesystem teardown

The duplicate-UUID repair did not establish that a released block device was
safe to reuse. Under concurrent sandbox activity, a device marked idle by the
backend could still reject `mkfs.xfs` with `EBUSY`. A mount disappearing from
mountinfo is insufficient evidence that all filesystem references are gone.

The storage service now checks Linux exclusive block-device access before
returning an owned device to the pool. It allows 500 ms for deferred teardown;
if the device remains busy or cannot be inspected, it journals a quarantine
instead of rebinding or synchronously deleting it. Backend ownership and backing
files remain intact. Metrics collection reclaims the device only after exclusive
access succeeds, checking the owner identity to avoid recycled numeric IDs.
Acquisition independently quarantines and rejects a busy device before formatting
or mounting it. Quarantined devices remain charged against storage and device
capacity, including after sandbox deletion. Journal schema 3 migrates schema 2
without changing volume or accounting identities. Snapshot layers remain
authoritative across retirement and restore on a fresh device.

Regression tests cover retirement with checkpoint preservation, subsequent
restore, rejection before formatting, restart recovery, retained backing files,
capacity accounting, schema migration, and recycled-ID safety. A live Linux/XFS check confirms that
an aliased filesystem remains busy after the original mount is removed and only
becomes reusable after the final mount is released. The precise source of the
remaining filesystem references in the incident has not been established;
retirement prevents those references from crossing into another device owner.
