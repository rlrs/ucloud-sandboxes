# Sandbox network policies

`network` selects connectivity (`bridge` or `none`). `network_policy` selects
which egress is permitted. These are separate so a relay can later provide a
filtering proxy without changing the sandbox isolation mechanism.

The default policy is `{"egress":"direct"}`: the existing bridge/NAT behaviour,
including its private-address denies and administrator-configured exceptions.
The SDK omits this default field, preserving compatibility with older gateways.

`{"egress":"relay","relay":"default"}` permits only the named relay's TCP
endpoint. The name refers to trusted worker configuration. Sandbox callers
cannot supply destination addresses, firewall rules, or exceptions.

## Configure a relay

Add the optional `network_relays` map inside the deployment's existing
`sandbox` configuration:

```json
{
  "network_relays": {
    "default": "relay.example.org:443"
  }
}
```

Use the hostname and port that sandbox clients use, including the hostname
required by the relay's TLS certificate. An IPv4 literal and port also work
for a fixed endpoint. The relay must run on a different host from the worker;
worker-local services remain blocked. Use a dedicated endpoint: this is TCP
isolation, not HTTP path, virtual-host, or application authorization.

Deploy the updated service package to the gateway and workers. Node package
bundles now include `nftables`, `iptables`, and `iproute2`; rebuild the bundle
when upgrading. Existing schema-5 deployment files without `network_relays`
remain valid. Configuring a relay does not change existing direct sandboxes.
For a standalone node, the equivalent option is:

```sh
ucloud-sandboxes serve-direct-node-agent \
  --network sandbox \
  --network-relays-json '{"default":"relay.example.org:443"}' \
  ...
```

The worker checks nftables support at startup and advertises
`network-policy-relay-v1:default`. The gateway requires that capability during
placement and migration. Older nodes cannot silently accept a restricted
sandbox as an unrestricted one. A gateway predating this extension rejects the
unknown policy field. An unknown relay on a current gateway has no eligible
worker; a direct node request rejects an unconfigured relay.

Use the same named relay definition throughout a worker pool. Names are stable
policy identities: introduce a new name when changing the service hostname or
port. Changing that hostname's IPv4 DNS answers is supported without changing
the policy. Migration preserves the policy in the existing sandbox manifest.

## Use the SDK

```python
from ucloud_sandboxes_sdk import (
    Image, SandboxClient, SandboxNetworkPolicy, SandboxSpec, model_relay_env,
)

client = SandboxClient.from_env()
sandbox = client.create_sandbox(SandboxSpec(
    id="restricted-rollout",
    image=Image.from_registry("your-prepared-agent-image"),
    command=["python", "/app/agent.py"],
    memory_mb=1024,
    cpus=1,
    disk_mb=2048,
    network="bridge",
    network_policy=SandboxNetworkPolicy.relay_only("default"),
    env=model_relay_env(
        "https://relay.example.org", "run-001",
        api_key="<sandbox-scoped-relay-token>",
    ),
))
```

The async client and `SandboxSpec.benchmark()` accept the same policy. Inspect
AI can select it with `UCLOUD_SANDBOX_RELAY=default`; when no network mode is
specified, this selects bridge connectivity. An explicit `none` or inbound SSH
conflicts with relay-only mode and is rejected. Custom guest DNS servers are
also rejected. Install dependencies in the image before starting a restricted
workload; package downloads from inside it will be blocked.

## Enforcement and lifecycle

Each restricted network lease owns an nftables table on the host. Rules match
the sandbox's host-side veth and expected source address, rather than trusting
its claimed source IP alone. The pre-DNAT guard permits only the selected relay
route and drops everything else, including IPv6, UDP/DNS, other ports, direct
internet access, and source spoofing. The host INPUT and OUTPUT paths remain
blocked. The forward guard accepts only the configured relay endpoints and
established TCP replies from them. A scoped iptables exception allows these
already-filtered packets through the older private-address denies and Docker's
FORWARD policy; a later broad ACCEPT cannot override an earlier nftables DROP.

For a DNS relay, the guest's `/etc/hosts` maps the original hostname to a stable
virtual IPv4 address in `198.18.0.0/15`. Host-side DNAT translates that address
to a resolved relay address, preserving the client hostname and TLS SNI. No
public resolver is exposed to the guest. Clients must honour `/etc/hosts`;
clients that implement their own DNS queries will fail closed. Changing the
guest files or using a numeric address does not grant another destination.

The worker resolves the relay on the host at its network reconciliation
interval (currently two seconds). It atomically replaces that lease's filter
and DNAT rules when the answer changes. New connections use a deterministic
address from the current A records; existing connections can continue while
their address remains authorized. Removing an address revokes those old flows,
so clients must reconnect. Empty, invalid, or failed DNS answers install a
policy that denies all traffic; a later valid answer restores the route.
IPv6 relay upstreams are not supported in this first policy version.

Policy intent is recorded durably before the veth is activated. A failed
firewall transaction prevents activation and preserves the prior rules.
Create, restore, and migration all install the policy before execution.
Node-agent restart rebuilds rules from durable leases before broad forwarding
rules are reconciled. Deletion keeps the policy and slot reserved until the
interface is gone, then removes the owned rules before allowing slot reuse.
Like other host-enforced isolation, this assumes the privileged host and its
firewall administration remain trusted.

## Future filtering relays

A named endpoint can already be an application relay or a dedicated HTTP
CONNECT/SOCKS/filtering proxy. Proxy-aware clients can use that endpoint while
the same host policy prevents direct bypass. The current model relay is not a
general-purpose internet proxy; this change does not turn it into one or
intercept arbitrary HTTP/TLS transparently.

Future work can add explicit proxy discovery/configuration and authenticated
per-sandbox filtering at the relay, or a distinct transport for arbitrary TCP
and UDP. Those features should extend the versioned policy and worker
capability contract rather than reinterpret existing `relay` permissions.
Relay authorization, rollout scoping, destination filtering, and auditing
belong at that service boundary. Do not expose an unrestricted forwarding
endpoint when the intended policy is limited model access.

## Verification

Unit and protocol tests cover serialization, validation, placement requirements,
create/migration propagation, atomic-update failure, DNS handoff/failure,
restart, name-file safety, and slot cleanup. The opt-in Linux packet test runs
in a disposable network and mount namespace, with private fixture services:

```sh
sudo env UCLOUD_RUN_NETNS_TESTS=1 PYTHONPATH="$PWD" \
  "$(uv python find)" -m unittest tests.test_relay_network_linux
```

It requires `unshare`, `mount`, `ip`, `iptables`, and `nft`. CI runs this test on
Linux. It exercises the actual host firewall and namespace wiring; it does not
replace a deployment smoke test with the pinned gVisor runtime and relay TLS.
