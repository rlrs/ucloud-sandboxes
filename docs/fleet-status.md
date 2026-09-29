# Compact fleet status reads

Clients that poll sandbox lifecycle or placement can request a smaller, freshly
read projection instead of downloading each sandbox's complete specification:

```http
GET /v1/sandboxes?view=status
```

Authentication and authorization are the same as the existing fleet endpoint.
The default `GET /v1/sandboxes` response remains unchanged. Clients must opt in
to obtain this reduction in SQL, JSON and transfer work.

```json
{
  "sandboxes": [{
    "id": "agent-1",
    "spec": {"id": "agent-1"},
    "generation": 2,
    "state": "parked",
    "cached_state": "parked",
    "node": {
      "node_id": "10.42.0.5",
      "job_id": "worker-job",
      "node_url": "http://10.42.0.5:8090",
      "active_sandboxes": 0,
      "fresh": true,
      "attached": true
    },
    "created_at": "2026-09-28T12:00:00+00:00",
    "updated_at": "2026-09-28T12:01:00+00:00"
  }],
  "cached": true,
  "refresh_supported": false,
  "view": "status"
}
```

`cached` means the gateway reports durable observations; each completed request
reads the current routing and heartbeat state. There is no response TTL. Only
identical requests that overlap in time share one read. Worker freshness,
inventory absence, quarantine and detached snapshot validation use the same
rules as the default full view. `state` can therefore be `unknown` while
`cached_state` retains the last durable worker state. `generation` identifies
the sandbox incarnation; a deleted and recreated ID may have a new generation.

For a small working set, repeat the `id` parameter to avoid reading and rendering
unrelated routes. IDs are exact matches, sorted in the response; duplicate IDs
collapse and unknown IDs are absent:

```http
GET /v1/sandboxes?view=status&id=agent-1&id=agent-2
```

The filter accepts at most 256 nonempty IDs, each at most 512 characters, with
no NUL characters. The encoded internal command is limited to 256 KiB, including
Unicode escaping. Larger fleets can read the unfiltered status view or batch
their IDs. The unfiltered view still reads all routes and complete heartbeats;
it does not implement incremental changes or a weaker freshness policy.

Unknown or repeated `view` values, invalid filters, filters on the full view,
and `view=status&refresh=true` return HTTP 400. Use the existing full
`?refresh=true` path when an explicit worker refresh is needed. The compact view
omits full specs, labels, resource requirements, images and snapshot descriptors;
use the default full view for those fields.
