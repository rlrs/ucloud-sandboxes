# Release closeout checks

Expose the existing `max_io_psi_full_avg10` scale-policy setting in dashboard
output, alongside the other policy fields. This changes reporting only; its
value and scheduling behavior are unchanged.

Repair three outdated test fixtures exposed by the full repository CI run:
filtered node inventory now supplies the node epoch; the root-directory upload
probe executes the complete parent-selection prefix after the mkdir fast path;
and SQLite permission tests exercise the documented 0.5.99 connection-pool
contract (main-file mode checked on every read, sidecars audited when opening
connections). Permission implementation and access controls are unchanged.

72 targeted Linux tests pass. SDK 0.4.25 has been published and its complete
Linux CI passed. Runtime behavior is otherwise identical to 0.5.112, whose live
qualification and remaining 512-agent latency limitation are recorded in
`release-0.5.112-2026-09-23.md`.
