# Release 0.5.28 deployment verification

Release `0.5.28` was deployed successfully on 2026-09-05. Fresh sandbox and
builder workers reported the new release, and installed gVisor binaries matched
the qualified artifacts. Previous release artifacts were preserved for rollback.

Validation passed:

- Service health and rejection of unauthenticated sandbox access.
- Sandbox create, file upload, exact readback, execution, and deletion.
- Ten park/resume cycles preserving process identity and counter progress.
- Builder image build and push followed by sandbox pull and execution.
- 741 repository tests (four skips), 82 SDK tests, Go tests, Ruff, shell checks,
  and installed-wheel checks.

Validation sandboxes and temporary capacity reservations were removed. The
artifact-transfer VM was stopped. Detailed operational evidence is retained
outside the repository.

This release verification does not establish full Prime taskset coverage or
cross-worker checkpoint migration compatibility. July and August checkpoint
memory formats remain incompatible; a rollback must account for checkpoints
created after deployment.
