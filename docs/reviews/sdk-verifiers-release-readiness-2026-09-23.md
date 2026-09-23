# SDK and Verifiers coordinated release

SDK **0.4.26** is published from commit
`e728daf99aacf9ebf7f6ab1abc8b28a7c77ea9e9`. The nested
`ucloud-sandboxes-sdk` checkout was the canonical release source; the older sibling
SDK checkout was not used.

| Published asset | SHA256 |
| --- | --- |
| `ucloud_sandboxes_sdk-0.4.26-py3-none-any.whl` | `b39c74752c1286338bfc8904386004a7e73828219d1d3c86a631d7ba0b270724` |
| Source distribution | `57a161def0ca5697b8b1cbe287b15455423830dc96c904704d9dbdab9d12abbc` |

Verifiers `main` commit `d3917175a6dd6efa5c1cdfc28e50f618716a1ce2`
is pushed. It pins the immutable 0.4.26 release wheel, removes the local SDK source
override, and includes the regenerated lock. Clean-install qualification passed
all **16 plugin tests**, Ruff and type checking. Its existing local Verifiers
framework dependency is separate from the SDK release pin.

The release provides shared relay request admission, durable-response acceptance
and the common sync/async advisory resource-phase contract. Verifiers exposes
`max_inflight_requests`, optional `resource_phase_hints`, and actual integration
phase hooks. Hints remain nonfatal advice, preserve cancellation, and grant no
parking or execution authority. No runtime version-probing fallback was added.

The backend's resident-wait and RAM backing changes are server controlled and do
not require a different create shape. Verifiers already uses
`SandboxSpec.benchmark`, preserving the managed/parkable benchmark profile. These
SDK release checks do not establish backend forced-restore or loaded density
qualification; those remain in the architecture implementation ledger.

## Closeout verification

Read-only verification on 2026-09-23 confirmed the GitHub `v0.4.26` tag and
published release target `e728daf99aacf9ebf7f6ab1abc8b28a7c77ea9e9`; both asset
digests still match the table above. Remote Verifiers `main` remains `d391717`,
and its lock pins the matching wheel SHA256. The canonical nested SDK checkout
and Verifiers checkout are clean. The older sibling SDK checkout has unrelated
uncommitted work and is not a release source.

The subsequent worker growth forecasts, backing-capacity admission, registry
validation optimization and gateway transport changes require **no SDK upgrade
beyond 0.4.26 and no additional Verifiers change**. They preserve the existing
managed-process, file/exec and relay protocol; growth ownership and memory
headroom are enforced on the server. Optional advisory phase hints still grant
no lifecycle authority. This compatibility check is separate from pressure and
sustained-load acceptance, whose final outcomes remain pending.

Managed-start growth admission uses the already-published retry contract:
an exhausted pre-dispatch admission wait returns HTTP 503 with
`error_code=node_startup_busy` and `retryable=true`. SDK 0.4.26 preserves the
same job identity/body and retries within the caller's existing deadline, in
both sync and async clients. The server classifies only the admission step;
an ambiguous supervisor RPC failure is not relabeled as safely retryable. A
bare 503 is not equivalent to this explicit pre-dispatch fence.

Transient managed status/log reads use the existing
`managed_process_read_unavailable` HTTP 503 contract. Their handlers must catch
that subtype before the semantic `ManagedProcessError` mapping to HTTP 409.
Released sync/async clients already retry the structured read failure within
their caller deadline; a semantic job conflict remains nonretryable. Actual HTTP
regressions cover both mappings, without widening mutation retries.
