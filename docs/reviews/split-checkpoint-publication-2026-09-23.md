# Split checkpoint publication and import

The v3 migration descriptor has one complete checkpoint root. Its OCI index binds
an existing immutable workspace publication and an immutable memory manifest, plus
the exact portable runtime manifest digest. Memory files use a bounded sparse
extent format; the allowlist is the runtime's artifact inventory, manifest.json,
and COMPLETE. Local allocator ownership markers and lock files are not exported.
V2 manifests and v1 migration serialization keep their existing bytes.

Publication holds an allocator read lease while source descriptors are open. It
checks the exact parked lifecycle record (including revision), registration and
file identity before each extent/chunk and before promoting the complete result.
A restore followed by repark cannot resurrect an upload's ownership. No lifecycle
lock spans network uploads. Partial uploads and ambiguous root commits remain
unadvertised orphans; a failed publication never replaces a portable route.
The allocator retains its hard claim until every open source descriptor closes,
even if deletion has already been requested. There is no second full memory copy.
One cached complete publication per incarnation avoids reuploading memory after
worker restart; the cache is usable only for the exact current parked manifest.

Import verifies the complete root before allocation, reserves the existing full
phase-aware hard claim, prepares separate workspace and memory owners, downloads
and authenticates sparse files into an unadvertised generation, then atomically
exposes and locally rebinds COMPLETE. An interrupted import keeps ownership and
its quota reservation. Retrying the same operation cleans incomplete staging and
continues; it cannot silently create a fresh sandbox. Metadata rebinding changes
only the ordinary memory component, so the imported workspace remains the exact
original remote publication rather than adding an empty block layer.

Routes, inventory and detached wake use migration.reference as their portable
identity. migration.publication deliberately continues to mean workspace block
storage. The existing RegistryUsageStore protects every member of
migration.references: root, workspace and memory manifest. Partial lease
acquisition is conservative. Lease release preserves all dependencies shared by
a successor route. V3 destination selection requires the explicit split checkpoint
capability, advertised only with both the allocator and Registry transport.

Qualification includes sparse transport roundtrip and local rebind, provisioner
import/retry with actual quota ownership journal, resume/repark during upload,
source replacement/unlink, corrupt downloads, ambiguous root commit, final-fence
invalidation, restart cache reuse and dependency-closure lease release. Production
activation still requires the native full-product/physical-quota qualification;
these metadata and transport tests do not substitute for that kernel/runtime test.
