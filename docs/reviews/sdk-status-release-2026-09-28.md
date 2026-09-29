# SDK 0.4.33 compact status release

Published [SDK v0.4.33](https://github.com/rlrs/ucloud-sandboxes-sdk/releases/tag/v0.4.33)
at 2026-09-28 19:48:57 UTC from commit
[`fecf8930e35ac3f5a5832eddde34a975bd2d6431`](https://github.com/rlrs/ucloud-sandboxes-sdk/commit/fecf8930e35ac3f5a5832eddde34a975bd2d6431).
Both remote `main` and the release tag resolved to that commit after publishing.
Wheel and source archive were downloaded again and compared byte for byte with
the tested artifacts. The SDK is distributed through GitHub releases.

The synchronous and asynchronous clients now expose `list_sandbox_statuses()`
and `get_sandbox_status(id)`. Optional `sandbox_ids=[...]` selects exact IDs;
an explicit empty list makes no HTTP request. The methods require the gateway's
`view=status` marker and validate records, rejecting unsupported, malformed,
duplicate or unrequested responses. Existing full-inventory methods retain
their existing behavior. See [the gateway contract](../fleet-status.md).

The production gateway already supports this endpoint. The service load
harness now uses the public SDK method for `--inventory-view status`, which
requires SDK 0.4.33 or newer. Full inventory remains the harness default.

## Validation

- All 176 SDK tests passed locally on Python 3.10.13 and 3.13.2 with all
  optional integrations installed. Ruff and the package build passed.
- [GitHub CI](https://github.com/rlrs/ucloud-sandboxes-sdk/actions/runs/36474662970)
  passed on the exact release commit for both Python 3.10 and 3.13, including
  minimal import, lint, the complete test suite, build and installed-wheel smoke.
- A fresh Python 3.10 environment installed the downloaded published wheel
  without dependencies and passed `tests/minimal_install_smoke.py`.
- The 38 service inventory/relay benchmark tests passed using the new SDK.
- At 19:46:56 UTC both clients loaded the release wheel in an isolated process
  on the gateway and successfully called the verified public HTTPS endpoint
  `https://77.42.92.27`. Checks covered compact and full inventory, empty
  filters, missing-ID filters and single missing-ID lookups. The production
  inventory was empty; populated records and error cases are covered by SDK
  protocol tests. Credentials stayed on the gateway, all requests were reads,
  and no service restart or installation into the gateway environment occurred.

This release provides the client API for the deployed optimization. These
checks do not constitute a new 500- or 1,000-sandbox capacity qualification.

## Artifact hashes

```text
d15b65fbb5e1570fde69cb9d571789a9b61d4418682efc17732bdc9c2ca8414c  ucloud_sandboxes_sdk-0.4.33-py3-none-any.whl
f7128a710f5f8307baea3f7a45cf4aaaa8e2fe8de26cb8e0fc9d6592e786a43d  ucloud_sandboxes_sdk-0.4.33.tar.gz
```
