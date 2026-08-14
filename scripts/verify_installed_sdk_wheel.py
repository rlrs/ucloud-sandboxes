"""Smoke-check the installed sibling SDK without optional dependencies."""

from ucloud_sandboxes_sdk import (
    Image,
    SandboxClient,
    SandboxSpec,
    http_tunnel_url,
    model_relay_env,
    sandbox_auth_headers,
)


def main() -> None:
    payload = SandboxSpec(
        id="wheel-smoke",
        image=Image.from_registry("registry.example/image:latest"),
        command=("true",),
        memory_mb=128,
        cpus=1,
        disk_mb=256,
    ).to_dict()
    assert payload["id"] == "wheel-smoke"
    assert payload["image"] == "registry.example/image:latest"
    assert SandboxClient("https://gateway.example", api_token="token")
    assert sandbox_auth_headers("token") == {"X-UCloud-Sandbox-Token": "token"}
    assert model_relay_env("https://relay.example", "rollout-one")[
        "OPENAI_BASE_URL"
    ].endswith("/rollouts/rollout-one/v1")
    assert http_tunnel_url(
        "https://relay.example",
        "rollout-one",
        "/health",
    ).endswith("/tunnels/rollout-one/health")


if __name__ == "__main__":
    main()
