# Persisted heartbeat compatibility for additive metrics

The idle-fleet 0.5.101 rollout exposed a missed validation boundary before the
load test started: wire decoding accepted the optional memory-working-set
field, but the durable control-state canonical validator rejected old records
without it. The gateway health endpoint alone did not detect this.

One shared additive-field defaults table now drives both validation paths,
including existing publication metrics. Legacy canonical records remain
readable without rewriting them. Unknown fields and noncanonical serialization
remain rejected. The regression opens an old-format SQLite record through a
fresh ControlStateStore, exercises full/header/fleet reads, and checks corruption
rejection. The release also verifies the retained production heartbeat database
using the candidate wheel before installation.
