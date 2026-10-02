# Images, placement and cold-demand forecasts

An agentic test on Hetzner on 2026-09-26 (rc49) used about 50 distinct 2.5–12.6
GB images across 100 concurrent sandboxes, and found three problems:
- Creates hung for up to 12 minutes because each worker's Docker image store
  (64 GB at the time) filled, and pulls failed with ENOSPC while Docker retried.
- The images were spread over all three workers.
- The autoscaler bought three CCX63 workers for sandboxes that each used about
  1.5 GB.

This note covers the three fixes, released in rc51.

## Worker image store: keep recently used, evict under pressure

Sandbox root filesystems mount Docker's overlay2 layers directly, so the store
holds each image once, with shared layers stored once. Deleting a sandbox
releases its rootfs cache entry and private pin but keeps the pulled image, so
the next sandbox of the same task starts without a pull. Before rc51 nothing
ever removed those images.

`ImageCacheEvictor` (`ucloud_sandboxes/image_eviction.py`):
- **Watermarks:** when the image filesystem is at least 85% full, it evicts
  images until usage is below 70%.
- **Order:** least recently used first. Last use is when the image's last
  sandbox was deleted, falling back to Docker's pull/tag time.
- **What is never evicted:** an image a registration references, an image
  whose digest lock a create or mount holds, and an image used or pulled in the
  last 10 minutes, which may be about to back a create.
- **Removal:** `DockerOverlay2RootfsStore.evict_image` removes the rootfs cache
  entry and the Docker image. It holds the exclusive digest lock and re-checks
  references first.
- **Triggers:** the worker's 5-second maintenance tick, and every image pull.
- **Heartbeats:** after a sweep, image records whose tag no longer resolves are
  dropped. Otherwise the gateway would skip the pull for an image it still
  thought was present.

Store size is `sandbox.docker_quota_image_gb`: 256 GB on Docker-store Hetzner
workers. EROFS workers (`immutable_environments.worker_enabled`, the production
default in `scripts/hetzner_prod/make_config.py`) pull no large images and use
32 GB. VM init grows a snapshot's smaller store to it.

## Placement: image locality below a load band

Create placement ranked nodes by assigned requested shapes, then live pressure,
and only then image locality. Every 4-vCPU create added 4/48 to its node's
assigned shapes, so creates round-robined and each worker pulled most images.

`Placement.rank` (`gateway/placement.py`) now computes a load per node: live
pressure (0..1) plus in-flight creates ÷ the per-node target concurrency. It
ranks:
1. **Not busy first:** load below 0.6 (`_AFFINITY_LOAD_BAND`). Busy nodes keep
   the previous order, assigned shapes then load, so stale cached pressure
   cannot funnel a burst onto one peer.
2. **Image locality:** has the image, then image in flight, then neither.
3. **Missing layer bytes.**
4. **Assigned shapes:** these only spread otherwise-equal nodes.
5. **Load, in-flight creates, slack, node ID.**

In-flight creates count toward load, so a same-image burst stays on the node
that holds the image only until about five creates are in flight (at the
default target of 8), then overflows.

## Autoscaler: forecast cold demand from remembered usage

Running sandboxes were already charged their measured memory. Announced
(prepared) and pending ones were charged their full request: memory ÷ 0.8 and
the full requested disk.

Runs usually start cold, so the evidence has to come from earlier runs.
`UsageHistory` (`ucloud_sandboxes/usage_history.py`) keeps the peak observed
memory per (image, requested shape):
- **Source:** heartbeat inventory samples from running sandboxes.
- **Storage:** the autoscaler's `usage-history.json` state file.
- **Retention:** 30 days.

Each cycle, cold demand is resized:
- **Memory:** min(request, 1.25 × remembered peak). The lookup tries the exact
  image and shape, then the same image at any shape, then the same shape across
  images. With no history it uses the full request.
- **Disk:** what a worker charges at create, which is the initial workspace
  grant plus the idle memory claim (576 MiB on Hetzner). It comes from worker
  heartbeats, or from the deployment when no worker exists yet. Growth is
  reserved as the sandbox writes.

CPU still only has to fit the node shape.

For the test above, 100 announced sandboxes now plan two workers instead of
three (`tests/test_usage_history.py`). A low forecast only delays scale-up:
worker admission still owns execution safety, and placement checks real free
disk.
