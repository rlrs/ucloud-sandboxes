# Routing and gateway

The public gateway is the only client-facing sandbox endpoint. Node agents are
private deployment services and accept only control-plane credentials.

## Ownership

The gateway owns:

- authentication and request limits;
- generation allocation and durable operation intent;
- resource placement and pending demand;
- sandbox-to-node routes;
- forwarding exec, file, SSH, image, park, wake, and delete requests;
- translating transport failures into structured API errors.

The sandbox node owns:

- one direct gVisor Warden;
- OCI bundle, cgroup, process, and storage-native lifecycle;
- node-local image materialization;
- generation- and operation-fenced sandbox mutations;
- bounded exec sessions and file transfer;
- authenticated inventory and resource heartbeats.

Docker and containerd are image infrastructure. They do not own sandbox
processes or writable volumes.

## Network and authentication

The gateway VM and worker VMs join one UCloud private network. A public link
binds only the gateway port. Heartbeats advertise each node's private URL, and
the gateway forwards to that URL without exposing it to clients.

Four credentials define separate trust channels:

- the sandbox API key authenticates least-privileged public SDK callers;
- the gateway control token authenticates operators and internal controllers;
- the heartbeat token authenticates node heartbeat publication;
- the node-control token authenticates gateway and autoscaler calls to nodes.

The SDK key reaches only the documented sandbox, exec, file, image, build, and
prepared-capacity routes. Node inventory, metrics, registry state, and explicit
park/wake/detach/migration routes require the gateway control token. This keeps
an SDK credential from becoming an infrastructure-administration credential.

Every node endpoint except `/healthz` requires the node-control token. The
gateway strips external authorization headers before attaching its private node
credential. Network reachability alone never authorizes a node mutation.

## Route identity

A route names an exact deployment, sandbox ID, positive generation, node ID,
and node epoch. The gateway persists an operation intent before dispatching a
mutation and reuses the same operation ID for retries. A timeout does not move
the route or create another generation; the gateway first resolves the original
intent against authenticated node inventory.

`GET /v1/sandboxes` is served from the gateway route index. The canonical routing
store is the durable recovery and pending-demand authority: PostgreSQL in
production, or SQLite for standalone deployments. Public creates and wakes
requiring placement use the [durable placement queue](placement-authority.md), with
worker RPCs outside routing transactions. An explicit
`?refresh=true` request fans out to nodes and reconciles their inventories.

Fleet monitors can opt into `GET /v1/sandboxes?view=status`. It reads current
route state and heartbeat freshness while omitting full user specifications
and attached snapshot descriptors. Records retain `id`, `spec.id`, `generation`,
`state`, `cached_state`, `node`, `created_at` and `updated_at`. The envelope sets
`view` to `status` and `refresh_supported` to `false`. The default full response
is unchanged. Add repeated exact IDs, such as
`?view=status&id=agent-a&id=agent-b`, to read only those routes (at most 256 IDs).
Missing IDs are omitted. Status requests cannot use `refresh=true`; use the
ordinary full endpoint for explicit reconciliation. Existing authentication,
generation, heartbeat expiry and detached-snapshot validation still apply.

A late heartbeat push is not silence. Before the gateway answers exec, file,
park, wake, DELETE, create-replay or exec-session traffic with a retryable 503
`sandbox_worker_unreachable`, it pulls `GET /v1/heartbeat` from the stored
node URL (2 s timeout). Concurrent requests to one worker boot share that
pull, and the next pull waits at least 2 s after it ends, doubling up to 32 s
while the worker does not answer. A sample counts only when its node, job,
deployment, agent version and URL match the stored heartbeat. It is ingested
like a push, so its inventory reconciles routes, and a new boot epoch retires
the old boot's routes as `node_lost` (410).

A final provider state is node loss: the gateway reports affected
non-portable work as `node_lost`. A reboot (a new authenticated boot epoch)
loses only the old guest's processes. Running, paused and half-captured
sandboxes answer `node_lost` with `reason: rebooted`; a complete local park
that the new boot reports with its exact incarnation keeps its route and
wakes there. The gateway deletes the remaining old-boot registrations,
delivering recorded deletes, so the same ids can be created again.

## Storage-native migration

A parked route is portable only when it has a verified `storage-native-v1`
snapshot manifest and both source and destination advertise
`sandbox-migrate-storage-native-v1`. The gateway reserves destination disk,
persists one migration id, stages the snapshot, atomically switches the route,
activates the destination, and then finalizes the source. Each phase is
generation- and digest-fenced.

The route switch is the ownership commit point. Before it, cancellation aborts
the destination and restores the source. After it, retries finish activation
and source cleanup against the same journal. Autoscaler drain uses this exact
path for parked routes; it never infers portability from a generic capability
or from route absence.

## Exec sessions

Exec uses one sequence-numbered HTTP protocol:

- `POST /v1/sandboxes/<sandbox-id>/exec`
- `GET /v1/exec/<session-id>`
- `GET /v1/exec/<session-id>/events?after=<sequence>&wait_seconds=<seconds>`
- `POST /v1/exec/<session-id>/stdin`
- `POST /v1/exec/<session-id>/close-stdin`
- `POST /v1/exec/<session-id>/signal`

An exec start accepts a command, environment, working directory, stdin flag,
and TTY flag. Events use monotonically increasing sequence numbers and bounded
retention. Long-poll readers wait on the session notification rather than
scanning all sessions or spinning while idle.

The session ID is opaque and bound to its origin node. When the gateway
forwards an exec start, it sends `X-UCloud-Exec-Session-Prefix`: an HMAC-signed
binding of the sandbox, its generation and worker job, keyed by the gateway
credential. A current worker names the session `<prefix>.<random>`. Follow-up
polls, stdin, signals and closes verify the prefix and route by the worker's
heartbeat, with no exec-route row and no routing-database read. The durable
route table is read only when that worker is stale or absent, to distinguish a
lost owner (410 `exec_worker_lost`), a deleted or replaced incarnation (404)
and a temporarily silent worker (503). A session is never redirected to another
owner. Rotating the gateway credential invalidates outstanding prefixes.

A worker that ignores the header names the session `exec-<uuid>`. The gateway
then stores the node URL in its durable exec-route table before returning it,
and every follow-up read loads that indexed row; there is no process-local
route cache, because another process may retire the route.

## File and SSH routes

File transfer is separate from exec:

- `PUT /v1/sandboxes/<sandbox-id>/files?path=<absolute-path>`
- `GET /v1/sandboxes/<sandbox-id>/files?path=<absolute-path>`

Bodies are raw `application/octet-stream`. The node validates absolute paths
and enforces the configured body limit in both directions.

SSH-enabled sandboxes request SSH when created. The node returns the sandbox's
node-local target through `GET /v1/sandboxes/<sandbox-id>/ssh`. Public clients
must use the authenticated gateway/tunnel layer; VM-local SSH ports are never
public ingress resources.

## Failure contract

Gateway errors are JSON and preserve whether a retry is safe. DNS absence,
connection failure, request timeout, admission closure, and upstream non-JSON
responses have distinct codes. A failure before a create reaches a node releases
the provisional route. An ambiguous failure retains the original generation and
operation ID until node inventory proves its outcome.

The gateway must bound request bodies, exec output, session count, file size,
and per-tenant concurrency. Node-side bounds remain necessary because the node
credential protects authorization, not resource exhaustion by an already
authorized control-plane process.
