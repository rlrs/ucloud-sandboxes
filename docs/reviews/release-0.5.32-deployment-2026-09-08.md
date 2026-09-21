# Release 0.5.32 deployment verification

Release **0.5.32** was deployed and verified on September 8, 2026.
The runtime code matches the accepted density-test candidate; only the package
version changed. The accepted density baseline and remaining compatibility
gaps are recorded in the existing performance and compatibility reviews.

Verification passed:

- Gateway and relay release health, registry health and access-control checks.
- Fresh sandbox and builder workers running the new release.
- Exact installed runtime and companion executable hashes.
- Sandbox create, file upload, exact readback, execution and deletion.
- Ten park/wake cycles preserving process identity and counter progress.
- Builder image build/push followed by sandbox pull and execution.
- Cleanup with no remaining test sandboxes, active disks or storage errors.
- Full repository checks: 830 main tests (five skips), 82 SDK tests, Go tests,
  lint, shell checks, builds and installed-wheel verification.

The single-sandbox deployment check measured median park/wake times of
0.290 / 0.667 seconds, with zero lifecycle retries. These are smoke-test
measurements, separate from the 128-sandbox density results.

Detailed operational evidence and rollback backups are retained privately.
