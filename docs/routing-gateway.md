# Routing and gateway

Date: 2026-06-28

The routing layer should keep user-facing access stable while sandbox containers
move across UCloud VM nodes. The control plane should route to node agents over
the UCloud private network using the `node_url` advertised in heartbeats.

## Responsibilities

The control-plane gateway owns:

- resolving a sandbox id to the VM node that hosts it
- forwarding exec API calls to that node's private `node_url`
- proxying exec stdout, stderr, stdin, and exit events
- proxying raw file upload/download requests to the owning node
- returning SSH connection metadata for a sandbox
- applying authentication, authorization, tenant limits, audit logging, and rate
  limits before traffic reaches a VM node

The VM node agent owns:

- creating/deleting sandbox containers
- starting per-sandbox exec sessions with `docker exec`
- accepting stdin for exec sessions
- producing ordered stdout/stderr/status/exit events
- copying files into and out of sandbox containers
- returning the node-local SSH target for SSH-enabled sandboxes

## Public ingress

UCloud public links should expose the gateway/control-plane service API, not raw
node-agent ports. The gateway VM should join the same UCloud private network as
the sandbox nodes and bind one public link to its gateway HTTP port. Gateway to
node traffic then stays on private-network hostnames such as
`http://sandbox-node-...:8090`.

The live `DFM Pretraining` project currently has this gateway link:

```text
id: 12345368
domain: app-sandboxes.cloud.sdu.dk
product: ucloud/u1-publiclink/u1-publiclink
state: READY
bound job: 12349450
bound port: 8090
```

The same all-in-one VM also runs the relay, registry, and autoscaler.

The target job resource fragment is:

```json
{
  "resources": [
    {
      "type": "ingress",
      "id": "12345368",
      "port": 8090
    }
  ]
}
```

The target VM product advertises `jobs.vm.bindLinkToPort`, so the port is the
VM-local service port exposed by the link. UCloud public links are single-bind
resources, so autoscaler-created sandbox nodes should not consume the gateway
link. After the VM is running and the gateway service is listening, call
`ucloud-sandboxes open-vm-web <job-id> --port 8090`; otherwise UCloud may show
the link as bound while the public endpoint returns `449`.

Current smoke-test shape:

- gateway VM job `12349450` runs `serve-control-plane --host 0.0.0.0 --port 8090`
  with `/work/ucloud-sandboxes/state/heartbeats.json` and
  `/work/ucloud-sandboxes/state/routes.sqlite`
- gateway route lookups are served from an in-memory index; `routes.sqlite` is
  a write-through recovery and pending-demand database shared with the
  autoscaler
- gateway VM job `12349450` also runs `autoscaler-loop` as a systemd service
  with create execution, label-gated stop execution, a 5-second reconcile
  interval, a 600-second sandbox idle grace, and a 900-second builder idle
  grace enabled
- the gateway requires `X-UCloud-Sandbox-Token: <token>` for non-health routes
  over UCloud public links; private/direct callers may also use
  `Authorization: Bearer <token>`
- sandbox and builder pools are currently scaled to zero when there is no
  pending sandbox demand, pending image-build demand, or unconsumed prepared
  capacity signal
- `GET /healthz` works publicly without auth and returns `service` plus
  package `version`
- authenticated `GET /v1/sandboxes` works publicly with
  `X-UCloud-Sandbox-Token`, while unauthenticated access returns `401`
- `GET /v1/sandboxes` is intentionally served from cached gateway routing
  state; use `GET /v1/sandboxes?refresh=true` only for explicit node
  reconciliation because it fans out to sandbox nodes

## Performance shape

The load-bearing path is the aiohttp-based async node agent and async gateway
client:

- `ucloud_sandboxes.async_node_agent.create_async_node_agent_app`
- `ucloud_sandboxes.async_exec.AsyncExecSessionManager`
- `ucloud_sandboxes.async_gateway.AsyncNodeGatewayClient`

This path uses one asyncio event loop per node-agent process, not one thread per
request or stream. Real exec sessions use `asyncio.create_subprocess_exec`.
Stdout and stderr are pumped with asyncio tasks into bounded per-session queues.
When a gateway or user stops reading, the queue applies backpressure to the
process pipe instead of accumulating unbounded memory.

The compatibility stdlib node-agent endpoints can still be useful for simple
tests and dry-runs, but they should not be the final 100-concurrent-session data
plane.

## Exec contract

Exec is session based. The node-agent API is:

- `POST /v1/sandboxes/<sandbox-id>/exec`
- `GET /v1/exec/<session-id>`
- `GET /v1/exec/<session-id>/events?after=<sequence>&wait_seconds=<seconds>`
- `GET /v1/exec/<session-id>/ws`
- `POST /v1/exec/<session-id>/stdin`
- `POST /v1/exec/<session-id>/close-stdin`

`POST /v1/sandboxes/<sandbox-id>/exec` accepts:

```json
{
  "command": ["python", "-c", "print('ok')"],
  "env": {
    "REQUEST_ID": "req-1"
  },
  "working_dir": "/workspace",
  "stdin": true,
  "tty": false
}
```

Events are ordered by `sequence` and use `stream` values:

- `status`
- `stdout`
- `stderr`
- `stdin`
- `stdin_closed`
- `exit`
- `error`

The high-performance exec transport is WebSocket:

- server-to-client binary frames: first byte is stream id, remaining bytes are
  payload
- stream id `1`: stdout
- stream id `2`: stderr
- server-to-client JSON frames: `session`, `status`, `exit`, and `error`
- client-to-server binary frames: stdin bytes
- client-to-server JSON frame: `{"type": "close_stdin"}`

The async gateway client exposes this as a WebSocket-backed stream:

```python
from ucloud_sandboxes.async_gateway import AsyncNodeGatewayClient
from ucloud_sandboxes.sandbox_exec import SandboxExecSpec

async with AsyncNodeGatewayClient("http://sandbox-node-1:8090") as client:
    stream = await client.open_exec_stream(
        "sandbox-1",
        SandboxExecSpec(
            sandbox_id="sandbox-1",
            command=("python", "-c", "print('ok')"),
            stdin=True,
        ),
    )
    async with stream:
        await stream.write_stdin(b"input\n")
        await stream.close_stdin()
        async for event in stream.events():
            ...
```

Long-poll JSON remains available as a compatibility/debug path. It should not be
used for high-volume stdout/stderr because it does not relieve the WebSocket
backpressure queue.

## File Transfer

File transfer is separate from exec:

- `PUT /v1/sandboxes/<sandbox-id>/files?path=/absolute/container/path`
- `GET /v1/sandboxes/<sandbox-id>/files?path=/absolute/container/path`

Uploads and downloads use raw HTTP request/response bodies with
`application/octet-stream`. The node agent validates absolute container file
paths and uses Docker copy operations on the VM. This avoids base64 encoding and
keeps file transfer out of the exec event stream.

## SSH contract

SSH is separate from exec. Sandboxes opt into SSH at create time:

```json
{
  "id": "ssh-demo-1",
  "image": "local/sandbox-ssh:latest",
  "network": "bridge",
  "ssh": {
    "enabled": true,
    "user": "sandbox",
    "authorized_keys": ["ssh-ed25519 AAAA... user@example"]
  }
}
```

The node agent allocates a VM-local host port and returns it through:

- `GET /v1/sandboxes/<sandbox-id>/ssh`
- `GET /v1/sandboxes/<sandbox-id>/ssh/ws`

The gateway should not expose raw VM-local ports directly to users. For SSH, the
node agent exposes a WebSocket-to-TCP bridge to the sandbox's VM-local SSH port.
The public gateway should expose either:

- a normal TCP listener for an SSH client and forward bytes to the node-agent
  `/ssh/ws` bridge, or
- a generated `ProxyCommand` that connects to the public gateway and asks it to
  bridge to the sandbox.

SSH bytes are not JSON encoded and do not share the exec event protocol.

## Expected scaling behavior

For around 100 sandboxes, the target shape is:

- occasional exec sessions: straightforward
- 100 concurrent interactive exec sessions: one subprocess plus a few asyncio
  tasks per session, bounded output queues, no stream pump threads
- 100 SSH sessions: handled as TCP/WebSocket byte proxying, independent of exec
  event storage

The first hard limits are likely Docker/runsc process overhead, VM CPU/RAM, and
the amount of stdout each session produces. The gateway should enforce per-user
and per-node concurrency limits rather than letting a single tenant open
unbounded exec or SSH sessions.

## Next steps

- Add route-entry TTL cleanup and recovery reconciliation after gateway restart.
- Add a public gateway process that authenticates users and forwards WebSocket
  exec streams to the selected node agent.
- Add a public TCP/ProxyCommand gateway for SSH over the node-agent SSH
  WebSocket bridge.
- Add session cleanup and TTLs for completed exec sessions.
- Add per-tenant authorization checks for exec and SSH target access.
- Add load tests for 100, 500, and 1000 concurrent exec/SSH sessions with
  bounded stdout volume.
