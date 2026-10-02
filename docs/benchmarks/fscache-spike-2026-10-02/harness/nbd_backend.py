"""Our serve-environment-io storage process (EnvironmentBackend over NBD), production policy,
with producer signature checks stubbed (see s11lib.stub_trust)."""
import json
import sys
from threading import Thread

sys.path.insert(0, "/root/s11")
import s11lib  # noqa: E402

settings = json.loads(sys.argv[1])
registry = s11lib.env_registry()
from ucloud_sandboxes.environment_backend import (  # noqa: E402
    EnvironmentBackend, EnvironmentBackendServer, PrefetchPolicy)

backend = EnvironmentBackend(s11lib.ENVIO_ROOT, registry, cache_bytes=settings["cache_bytes"],
                             prefetch=PrefetchPolicy(enabled=settings["prefetch"]))
with EnvironmentBackendServer(s11lib.ENVIO_SOCK, backend) as server:
    server.serve_forever()
