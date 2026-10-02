"""Node agents in their own processes, forked from a preloaded zygote.

``python node_process.py`` imports the node code once, then reads one JSON
request per line on stdin and answers each with the PID of a forked child
running that node agent. Forking skips the interpreter start and imports a
restart would otherwise pay. The zygote is single-threaded when it forks,
and the kernel reaps its children.

SIGKILL of a child models a node-agent crash: no cleanup runs, and flocks,
pooled connections and in-flight runtime invocations are severed as on a
VM. SIGTERM stops serving. A child writes its bound port to the request's
``ready`` path once startup reconciliation finished and it accepts requests.

The zygote exits at end of input, so with its test process, and its
children die with it: an aborted test run leaves no agent serving.
"""

import ctypes
import json
import os
from pathlib import Path
import signal
import sys
import threading
import traceback

sys.path.insert(0, str(Path(__file__).resolve().parent))

from assembly import NodeAgentConfig, assemble_node_agent, fixed_metrics  # noqa: E402
from images import ImageCatalog, LocalRootfsStore  # noqa: E402
from pidfd import SyscallPidfdFencer, native_pidfd_available  # noqa: E402

_PR_SET_PDEATHSIG = 1


def serve(config: dict, ready_path: str, catalog_root: str) -> int:
    agent = NodeAgentConfig(**config)
    server, _service = assemble_node_agent(
        agent,
        rootfs_store=LocalRootfsStore(
            Path(agent.state_root) / "image-cache", ImageCatalog(Path(catalog_root))
        ),
        fencer=None if native_pidfd_available() else SyscallPidfdFencer(proc_root=Path(agent.proc_root)),
        sample_metrics=fixed_metrics,
    )
    signal.signal(signal.SIGTERM, lambda *_: threading.Thread(target=server.shutdown).start())
    staging = Path(ready_path + ".tmp")
    staging.write_text(f"{server.server_address[1]}\n", encoding="ascii")
    os.replace(staging, ready_path)
    server.serve_forever(poll_interval=0.01)
    server.server_close()
    return 0


def _die_with(parent: int) -> None:
    """SIGKILL this agent when the zygote exits.

    The setting is cleared in forked children, so runtime invocations and
    sentries this agent starts still outlive it, as a crash requires.
    """
    if ctypes.CDLL(None, use_errno=True).prctl(_PR_SET_PDEATHSIG, int(signal.SIGKILL), 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "prctl(PR_SET_PDEATHSIG) failed")
    if os.getppid() != parent:
        raise RuntimeError("the zygote exited before its agent started")


def _child(request: dict, parent: int) -> None:
    # Runtime invocations need their exit status back.
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)
    os.dup2(os.open(os.devnull, os.O_RDONLY), 0)
    log = os.open(request["log"], os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    os.dup2(log, 1)
    os.dup2(log, 2)
    code = 1
    try:
        _die_with(parent)
        code = serve(request["config"], request["ready"], request["catalog"])
    except BaseException:
        traceback.print_exc()
    finally:
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(code)


def zygote() -> int:
    signal.signal(signal.SIGCHLD, signal.SIG_IGN)
    for line in sys.stdin:
        request = json.loads(line)
        parent = os.getpid()
        pid = os.fork()
        if pid == 0:
            _child(request, parent)
        sys.stdout.write(f"{pid}\n")
        sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(zygote())
