# Release 0.5.33 deployment verification

Server **0.5.33**, code commit `3fb9c73`, and SDK **0.4.18** were deployed and
verified in DFM Pretraining production on September 16, 2026.
The SDK wheel and source distribution are published in the
[0.4.18 release](https://github.com/rlrs/ucloud-sandboxes-sdk/releases/tag/v0.4.18).

The named `default` policy uses the model relay's dedicated private TCP
endpoint. A shared public HTTPS ingress is unsuitable for this isolation
boundary because different virtual hosts share its IP and port. Clients opt in
with `SandboxNetworkPolicy.relay_only("default")`; direct networking remains
the backward-compatible default. See [network policy](../network-policy.md).

Verification passed:

- Full server/SDK CI, including real Linux namespace packet filtering,
  Docker Distribution and S3-compatible contracts.
- Fresh worker bootstrap from rebuilt sandbox and builder package bundles.
- Production gateway, relay, registry and autoscaler health.
- Exact match of installed gateway Python sources to the committed release.
- Restricted sandbox creation, file upload/readback and command execution.
- Relay health access, with public IPv4, metadata, direct relay-IP access,
  other ports, external DNS and IPv6 blocked.
- Explicit park/wake followed by the same successful isolation checks.
- Test sandbox deletion and an empty final sandbox inventory.

The bootstrap check found an archive readability failure under a restrictive
shell umask. The release now normalizes bundle permissions independently of
the invoking shell; a regression test verifies readable data and executable
runtime files. The complete production gVisor and storage-native binaries were
reused from the previously qualified 0.5.32 bundle and verified before packaging.

Final production verification completed at **11:41 UTC**. Configuration and
SQLite backups, bundle digests, smoke-test output and detailed deployment
records are retained privately. Workers remain managed by the autoscaler.

CI evidence: [server](https://github.com/rlrs/ucloud-sandboxes/actions/runs/35091255087),
[SDK](https://github.com/rlrs/ucloud-sandboxes-sdk/actions/runs/35090084165).
