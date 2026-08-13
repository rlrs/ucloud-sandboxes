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

`GET /v1/sandboxes` is served from the gateway route index. The SQLite route
store is the durable recovery and pending-demand authority. An explicit
`?refresh=true` request fans out to nodes and reconciles their inventories.

A post-start worker suspension or final provider state is node loss. The
gateway removes that node from placement and reports affected non-portable work
as `node_lost`. It never routes traffic to a rebooted copy of the earlier guest
disk.

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

An exec start accepts a command, environment, working directory, stdin flag,
and TTY flag. Events use monotonically increasing sequence numbers and bounded
retention. Long-poll readers wait on the session notification rather than
scanning all sessions or spinning while idle.

The session ID is opaque and bound to its origin node. Before returning it, the
gateway stores the node URL in its durable exec-route table and bounded
in-memory cache. Follow-up reads therefore do not depend on a fresh heartbeat
lookup.

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
