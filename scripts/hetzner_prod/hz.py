"""Minimal Hetzner Cloud helper for the production bring-up; records every created resource."""

import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

API = "https://api.hetzner.cloud/v1"
LEDGER = Path(__file__).resolve().parents[2] / "build" / "hetzner-prod" / "resources.json"
LABELS = {"purpose": "sandboxes-production"}


def call(method, path, body=None):
    request = urllib.request.Request(
        API + path,
        method=method,
        data=None if body is None else json.dumps(body).encode(),
        headers={
            "Authorization": "Bearer " + os.environ["HETZNER_API_KEY"],
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            raw = response.read()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        raise SystemExit(f"{method} {path}: HTTP {exc.code}: {exc.read().decode()[:500]}")


def ledger():
    return json.loads(LEDGER.read_text()) if LEDGER.exists() else {"servers": {}, "primary_ips": {}, "images": {}}


def record(kind, name, value):
    state = ledger()
    state[kind][name] = value
    LEDGER.write_text(json.dumps(state, indent=2) + "\n")


def forget(kind, name):
    state = ledger()
    state[kind].pop(name, None)
    LEDGER.write_text(json.dumps(state, indent=2) + "\n")


def wait_action(action_id, timeout=600):
    deadline = time.time() + timeout
    while time.time() < deadline:
        action = call("GET", f"/actions/{action_id}")["action"]
        if action["status"] == "success":
            return action
        if action["status"] == "error":
            raise SystemExit(f"action {action_id} failed: {action['error']}")
        time.sleep(2)
    raise SystemExit(f"action {action_id} timed out")


def primary_ip(name):
    body = {"name": name, "type": "ipv4", "assignee_type": "server",
            "location": "hel1", "auto_delete": False, "labels": LABELS}
    result = call("POST", "/primary_ips", body)["primary_ip"]
    record("primary_ips", name, {"id": result["id"], "ip": result["ip"]})
    return result


def server(name, server_type, image, *, private_ip, firewall_ids, primary_ipv4=None, public_ipv4=False, user_data=None):
    body = {
        "name": name, "server_type": server_type, "image": image,
        "location": "hel1", "ssh_keys": [116985947],
        "firewalls": [{"firewall": item} for item in firewall_ids],
        "public_net": {"enable_ipv4": bool(primary_ipv4 or public_ipv4), "enable_ipv6": False,
                       **({"ipv4": primary_ipv4} if primary_ipv4 else {})},
        "labels": LABELS, "start_after_create": True,
    }
    if user_data:
        body["user_data"] = user_data
    started = time.time()
    result = call("POST", "/servers", body)
    created = result["server"]
    record("servers", name, {"id": created["id"], "type": server_type, "private_ip": private_ip})
    wait_action(result["action"]["id"])
    attach = call("POST", f"/servers/{created['id']}/actions/attach_to_network",
                  {"network": 12539764, "ip": private_ip})
    wait_action(attach["action"]["id"])
    current = call("GET", f"/servers/{created['id']}")["server"]
    print(json.dumps({
        "id": current["id"], "name": name, "status": current["status"],
        "public_ipv4": (current["public_net"].get("ipv4") or {}).get("ip"),
        "private_ip": [n["ip"] for n in current["private_net"]],
        "create_to_running_seconds": round(time.time() - started, 1),
    }))
    return current


def delete_server(name):
    entry = ledger()["servers"].get(name)
    if entry is None:
        raise SystemExit(f"unknown server {name}")
    call("DELETE", f"/servers/{entry['id']}")
    forget("servers", name)
    print("deleted server", name)


if __name__ == "__main__":
    command, *args = sys.argv[1:]
    if command == "gateway":
        ip = ledger()["primary_ips"].get("sandboxes-gateway-ipv4") or primary_ip("sandboxes-gateway-ipv4")
        server("sandboxes-gateway", "cpx32", "ubuntu-26.04", private_ip="10.42.0.2",
               firewall_ids=[11457260], primary_ipv4=ip["id"])
    elif command == "server":
        name, server_type, image, private_ip, public = args
        server(name, server_type, image, private_ip=private_ip,
               firewall_ids=[11454113], public_ipv4=public == "public")
    elif command == "snapshot":
        # Power off for consistency, then snapshot; prints the image ID.
        name, description = args
        entry = ledger()["servers"][name]
        wait_action(call("POST", f"/servers/{entry['id']}/actions/poweroff")["action"]["id"])
        result = call("POST", f"/servers/{entry['id']}/actions/create_image",
                      {"type": "snapshot", "description": description, "labels": LABELS})
        wait_action(result["action"]["id"], timeout=1800)
        record("images", description, {"id": result["image"]["id"]})
        print(json.dumps({"image_id": result["image"]["id"], "description": description}))
    elif command == "delete-server":
        delete_server(args[0])
    elif command == "list":
        print(json.dumps(ledger(), indent=2))
    else:
        raise SystemExit(f"unknown command {command}")
