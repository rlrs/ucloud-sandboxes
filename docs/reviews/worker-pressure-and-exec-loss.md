# Worker pressure and exec loss handling

Server 0.5.61 keeps dedicated image builders out of sandbox pressure signals. Builder CPU, memory and materialization pressure cannot be relieved by provisioning sandbox workers. Mixed-role workers still contribute, and historical metrics without capabilities remain compatible.

When a worker is confirmed lost, the gateway retains a bounded durable record for its accepted exec sessions before removing their routes. Subsequent status, event and input requests return HTTP 410 with `exec_worker_lost` and `retryable: false`, rather than an ambiguous missing route. Records survive gateway restart and sandbox-ID reuse and expire with the existing terminal retention policy. They describe the original process incarnation; commands are never replayed on a replacement worker. Portable sandbox snapshots may still recover independently.

A complete inventory from a replacement boot also records terminal sandbox loss when the former process is absent. A stale heartbeat or transient DNS failure alone does not prove worker loss. Previously deleted exec routes cannot be reconstructed by this release.

Tests cover role-specific pressure, mixed-role and legacy observations, durable exec loss, retention, sandbox generation reuse, portable snapshot preservation and the HTTP contract. These changes improve scheduling feedback and failure reporting; they do not prevent worker loss or claim to eliminate all DNS and resume timeouts.

Validation: 946 server tests (6 skips), 118 SDK tests, Go tests, lint and installed-wheel checks passed. The release also passed 370 Linux tests (1 skip), both bundle boot validators, and [CI run 35504063222](https://github.com/rlrs/ucloud-sandboxes/actions/runs/35504063222).

Production verification on server and worker 0.5.61 with SDK 0.4.23 passed all 64 sandbox creates, 32 concurrent large uploads plus 320 small writes, tool calls, park/resume and restored checksum checks. Small-write p95 during the upload burst was 1.881 seconds; resume-and-tool p95 was 11.550 seconds. Health probes and cleanup passed. Cold creates waited for worker provisioning and retried admission; this is successful overload recovery, not zero admission responses. This canary does not certify 256 or 512 concurrent agents.
