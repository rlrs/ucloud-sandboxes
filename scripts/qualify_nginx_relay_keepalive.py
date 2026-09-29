"""Exercise generated TLS ingress against a local aiohttp relay fixture.

Uses only disposable local Docker nginx instances. No production credentials or
traffic. Reports backend connection counts, not production CPU capacity.
"""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter
import json
from pathlib import Path
import socket
import subprocess
import sys
from tempfile import TemporaryDirectory
import time
from uuid import uuid4

import aiohttp
from aiohttp import web
from yarl import URL


def run(*args):
    return subprocess.run(args, check=True, capture_output=True, text=True).stdout


def port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def tcp_counters():
    lines = Path("/proc/net/snmp").read_text().splitlines()
    for index, line in enumerate(lines):
        if line.startswith("Tcp:"):
            values = dict(zip(line.split()[1:], lines[index + 1].split()[1:]))
            return {key: int(values[key]) for key in ("ActiveOpens", "PassiveOpens", "CurrEstab")}
    raise ValueError("TCP counters unavailable")


def configuration(*, relay_port, https_port, http_port, pooled):
    source = Path("scripts/configure_hetzner_sdk_ingress.sh").read_text()
    site = source.split('cat >"$temporary_site" <<EOF\n')[2].split("\nEOF", 1)[0]
    for name, value in {
        "relay_port": str(relay_port), "gateway_port": str(relay_port),
        "public_host": "localhost", "acme_webroot": "/qualification/acme",
        "certificate_dir": "/qualification",
    }.items():
        site = site.replace("$" + name, value)
    site = site.replace("\\$", "$")
    site = site.replace("    listen [::]:80;\n", "")
    site = site.replace("    listen [::]:443 ssl;\n", "")
    site = site.replace("listen 80;", f"listen 127.0.0.1:{http_port};")
    site = site.replace("listen 443 ssl;", f"listen 127.0.0.1:{https_port} ssl;")
    if not pooled:
        site = site.replace("http://ucloud_model_relay$ucloud_relay_uri",
                            f"http://127.0.0.1:{relay_port}$ucloud_relay_uri")
        site = site.replace('        proxy_set_header Connection "";\n', "")
    return "worker_processes 1;\nevents { worker_connections 768; }\nhttp {\naccess_log off;\n" + site + "\n}\n"


async def qualify(root, image, *, pooled):
    transports = {}
    accepted = Counter()

    async def backend(request):
        transport = request.transport
        connection = transports.setdefault(transport, len(transports) + 1)
        accepted[request.raw_path] += 1
        body = await request.read()
        if request.path == "/abort":
            transport.close()
            return web.Response()
        if request.path == "/batch":
            await asyncio.sleep(0.01)
        response = web.json_response({
            "connection": connection, "raw_path": request.raw_path,
            "authorization": request.headers.get("Authorization"),
            "proxy_authorization": request.headers.get("Proxy-Authorization"),
            "connection_header": request.headers.get("Connection"),
            "body": body.decode(),
        }, status=400 if request.path == "/error" else 200)
        if request.path == "/close":
            response.force_close()
        return response

    app = web.Application(handler_args={"keepalive_timeout": 5})
    app.router.add_route("*", "/{path:.*}", backend)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    relay_port = site._server.sockets[0].getsockname()[1]
    https_port, http_port = port(), port()
    (root / "original.conf").write_text(configuration(
        relay_port=relay_port, https_port=https_port, http_port=http_port, pooled=pooled,
    ))
    renderer = Path("scripts/configure_hetzner_sdk_ingress.sh").read_text().split(
        "<<'PY_NGINX_CAPACITY'\n", 1
    )[1].split("\nPY_NGINX_CAPACITY", 1)[0]
    subprocess.run(
        [sys.executable, "-", str(root / "original.conf"), str(root / "nginx.conf")],
        input=renderer, check=True, text=True, capture_output=True,
    )
    name = "ucloud-nginx-qualification-" + uuid4().hex[:12]
    docker = ("sudo", "-n", "docker")
    common = ("--network", "host", "--mount", f"type=bind,src={root},dst=/qualification,readonly")
    try:
        await asyncio.to_thread(run, *docker, "run", "--rm", *common, image,
                                "nginx", "-t", "-c", "/qualification/nginx.conf")
        await asyncio.to_thread(run, *docker, "run", "-d", "--rm", "--name", name,
                                *common, image, "nginx", "-c", "/qualification/nginx.conf",
                                "-g", "daemon off;")
        base = f"https://127.0.0.1:{https_port}/relay"
        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=False, limit=0),
            timeout=aiohttp.ClientTimeout(total=15),
        ) as client:
            for attempt in range(100):
                try:
                    async with client.get(base + "/ready") as response:
                        await response.read()
                    break
                except aiohttp.ClientConnectorError:
                    if attempt == 99:
                        raise
                    await asyncio.sleep(0.05)

            async def request(path, *, status=200, method="GET", body=None,
                              authorization="Bearer fixture-only"):
                async with client.request(
                    method, URL(base + path, encoded=True), data=body,
                    headers={"Authorization": authorization,
                             "Proxy-Authorization": "Bearer must-be-stripped",
                             "Connection": "keep-alive"},
                ) as response:
                    assert response.status == status, (path, response.status)
                    if status == 502:
                        await response.read()
                        return None
                    payload = await response.json()
                    assert payload["raw_path"] == path, payload
                    assert payload["authorization"] == authorization
                    assert payload["proxy_authorization"] is None
                    assert payload["connection_header"] == (None if pooled else "close")
                    assert payload["body"] == (body or "")
                    return payload["connection"]

            sequential = [await request(
                "/sequential", method="POST", body=f"request-{index}",
                authorization=f"Bearer fixture-{index}",
            ) for index in range(20)]
            assert len(set(sequential)) == (1 if pooled else 20)
            await asyncio.sleep(1.2)
            assert await request("/after-idle") != sequential[-1]
            await request("/v1/rollouts/a%2Fb%3Fc/tunnel?x=%252F&y=a+b")
            await request("/error", status=400)
            await request("/after-error")
            closed = await request("/close", method="POST", body="one mutation")
            assert await request("/after-close") != closed
            await request("/abort", method="POST", body="ambiguous mutation", status=502)
            await request("/after-abort")
            assert accepted["/close"] == accepted["/abort"] == 1
            batches = []
            for concurrency in (64, 512):
                before = len(transports)
                tcp_before = tcp_counters()
                started = time.perf_counter()

                async def worker():
                    for _ in range(4):
                        await request("/batch")

                await asyncio.gather(*(worker() for _ in range(concurrency)))
                tcp_after = tcp_counters()
                batches.append({
                    "concurrency": concurrency, "requests": concurrency * 4,
                    "backend_connections_opened": len(transports) - before,
                    "wall_seconds": time.perf_counter() - started,
                    "host_tcp_counter_delta": {
                        key: value - tcp_before[key] for key, value in tcp_after.items()
                    },
                })
            return {
                "pooled": pooled, "sequential_requests": 20,
                "sequential_backend_connections": len(set(sequential)),
                "raw_uri_auth_error_framing_and_reconnect_passed": True,
                "aborted_mutation_backend_attempts": accepted["/abort"],
                "batches": batches,
            }
    finally:
        await asyncio.to_thread(subprocess.run, (*docker, "rm", "-f", name),
                                capture_output=True, check=False)
        await runner.cleanup()


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nginx-image", default="nginx:1.28-alpine")
    args = parser.parse_args()
    with TemporaryDirectory(prefix="ucloud-nginx-qualification-") as raw:
        root = Path(raw)
        run("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
            "-keyout", str(root / "privkey.pem"), "-out", str(root / "fullchain.pem"),
            "-days", "1", "-subj", "/CN=localhost")
        reports = [await qualify(root, args.nginx_image, pooled=pooled)
                   for pooled in (False, True)]
        print(json.dumps({"nginx_image": args.nginx_image,
                          "fixture": "local aiohttp, one nginx worker; not a CPU capacity benchmark",
                          "capacity": "both variants use generated worker_connections4096/worker_rlimit_nofile65536",
                          "tcp_counters_scope": "whole local host; background traffic can contribute",
                          "reports": reports}, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
