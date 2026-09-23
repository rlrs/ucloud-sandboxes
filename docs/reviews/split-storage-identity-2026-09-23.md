# Split storage identity integration review

The legacy layout used one directory name for both workspace storage and guest
memory. Split checkpoints retain the incarnation name for memory but use a
`workspace-` volume identity. A literal audit of every production
`memory_directory` use found the heartbeat lookup still using the legacy key;
that lookup was fixed during the runtime assembly qualification.

This follow-up adds a computed `workspace_volume_id` property to the existing
sandbox and registration types. Warden ownership and batched storage inventory,
provisioner allocation/delete, overlay resume/delete, and heartbeat consumers
now use that property. There is no new serialized field. Pre-materialization
registrations resolve their planned volume identity, and legacy records preserve
their old directory. Remaining `memory_directory` accesses are memory paths,
runtime annotations, validation, serialization, or legacy normalization.

A second legacy assumption made every PUBLISHED workspace erase the entire
heartbeat disk charge. Split checkpoints still retain their separate memory
allocation locally. Heartbeat accounting now releases only workspace charge
and keeps that memory allocation's hard quota until deletion. Runtime storage
metrics already impose the combined physical-budget bound; the fix also keeps
placement conservative when those metrics are unavailable.

Snapshot publication/import readers were checked separately: route and inventory
metadata use the complete checkpoint `.reference`, while workspace-only storage
operations use `.publication`. Partial memory upload cannot become a portable
route. Split-v3 publication currently requires a Registry root; configuration
and VM bootstrap now reject the unsupported split-plus-S3 combination before
launching workers. Legacy S3 storage remains supported.

Tests use actual `DirectSandboxRegistration`, `DirectSandbox`,
`StorageVolumeRecord`, Warden inventory validation, and heartbeat capacity types.
They cover legacy/split layouts, mounted/published volumes, running/parked/waking
states, absent runtime metrics, wrong-volume alias rejection, and unchanged
serialization. The focused lifecycle/registry/overlay/provisioner suite passed
169 tests; configuration/bootstrap passed 40 tests. The separate assembled
runtime test exercises a real Unix storage client/server and HTTP heartbeat.

The combined Linux gate subsequently passed 1,504 tests in 127.648 seconds
with real PostgreSQL and the release SDK (12 environment-dependent skips).
