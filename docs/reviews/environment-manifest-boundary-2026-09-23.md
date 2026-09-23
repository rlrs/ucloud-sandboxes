# Immutable environment boundary

This change implements the first structural part of P4. It does not introduce an
EROFS backend, a new public sandbox creation API, or a memory/workspace split.

`EnvironmentManifest` gives immutable components one strict identity: base,
optional immutable workspace seed, ordered toolkits, and OCI overlay composition
semantics. The workspace seed is an immutable image component, not the sandbox's
writable workspace revision. Toolkit order affects identity. Unresolved tags,
unknown schema fields, and unknown composition semantics are rejected.

The existing Docker adapter still resolves one already-composed image. Its
rootfs fingerprint remains exactly SHA256(`ucloud-overlay2-rootfs-v1` + NUL +
Docker image ID), preserving all existing checkpoint fingerprints. Manifest
identity is deliberately distinct from this backend compatibility identity.
The Docker adapter rejects additional workspace/toolkit components and unknown
backend ABIs. Describing a composition does not qualify its execution.

The provisioner now obtains resolution leases, preparation, recovery, release,
and image collection through its single rootfs manager. It no longer accepts a
second independently injected Docker store. Node warmup and operation metrics
also enter through that manager. The concrete Docker adapter owns mount paths,
image pinning, and its existing fresh-reference check under the GC digest lock.
No generic plugin framework or alternate ownership journal was added.

Rootfs bundles continue to write metadata schema 1 with unchanged fields and
fingerprints. One decoder resolves them to the canonical in-memory manifest,
without rewriting persisted metadata. The schema-2 reader binds a manifest and
backend ABI for qualification fixtures only; no production writer or backend
selector is added. This permits reader-first rollout without creating a rollback
barrier for a structural extraction. Tests exercise an old bundle through
park/remount, prove its bytes are unchanged, and reject changed components/ABIs
in the new reader before remount. Remove schema 1 only after a separately
qualified writer rollout and drain of old durable/importable incarnations.

Validation: 95 unit tests across environment manifests, immutable rootfs,
provisioning and OCI translation pass; Ruff passes. Mount behavior is tested with
command-runner doubles, not asserted as native EROFS or kernel qualification.
Existing Linux lifecycle qualification remains required for release.

Still gated: EROFS builder/mounter and pinned kernel/runsc qualification;
whiteout, opaque directory, hardlink, xattr, ownership, symlink and copy-up
semantics for independently built components; capture/restore, publication/import
and referenced-artifact GC. No speculative EROFS path is selectable in production.
