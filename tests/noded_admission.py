"""A real ``ucloud-noded --admission-only`` behind a test's DirectSandboxService.

Step 3c.3 of docs/admission-port-3c.md runs the admission service tests over
both backends: ``LocalAdmission`` (the agent's own ledger) and
``NodedAdmission`` against noded's admission service, built from this
worktree with cargo. ``NodedHarness`` owns that process and keeps it fed
with what a test controls on the Python side, which noded reads from files:

- the admission configuration (``admission_config`` of the live service,
  plus its RAM backing root): a change restarts noded, and the agent's hello
  then reasserts the claims it holds, as after a real noded restart;
- runtime metrics, from the service's metrics provider (``--metrics-file``);
- memory placement and resident samples, from the Warden's
  ``application_memory_mode`` and the service's sampler
  (``--observations-file``): a new set of samples is one sampler pass.

The files are written before every call to noded and every 20 ms while a
call waits, so a test that changes its sample while an admission waits is
seen as the local guard would see it.
"""

from __future__ import annotations

from contextlib import contextmanager, nullcontext
from dataclasses import asdict
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import tempfile
import threading
import time
import unittest

from ucloud_sandboxes.admission_backend import NodedAdmission
from ucloud_sandboxes.create_handoff import admission_config
from ucloud_sandboxes.noded_client import NodedClient, NodedLink

REPO = Path(__file__).resolve().parents[1]
NODED = REPO / "runtime" / "noded"
TOKEN = "noded-test-token"
_PUMP_SECONDS = 0.02
_BUILD_GUARD = threading.Lock()
_BINARY: Path | None = None


def noded_binary() -> Path:
    """``ucloud-noded`` built from this worktree (``UCLOUD_NODED_BINARY`` overrides)."""
    global _BINARY
    configured = os.environ.get("UCLOUD_NODED_BINARY")
    if configured:
        return Path(configured)
    with _BUILD_GUARD:
        if _BINARY is not None:
            return _BINARY
        cargo = shutil.which("cargo") or str(Path.home() / ".cargo" / "bin" / "cargo")
        if not Path(cargo).exists():
            raise unittest.SkipTest("NodedAdmission tests build runtime/noded and need cargo, which is not installed")
        built = subprocess.run([cargo, "build", "--offline", "--locked", "--bin", "ucloud-noded"], cwd=NODED,
                               capture_output=True, text=True, check=False)
        if built.returncode != 0:
            raise AssertionError(f"cargo build of ucloud-noded failed:\n{built.stderr[-4000:]}")
        target = Path(os.environ.get("CARGO_TARGET_DIR") or NODED / "target")
        _BINARY = target / "debug" / "ucloud-noded"
        return _BINARY


def _write(path: Path, document) -> None:
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(document), encoding="utf-8")
    os.replace(temporary, path)


def _metrics(metrics) -> dict | None:
    if metrics is None:
        return None
    backing = metrics.memory_backing
    return {
        "memory_total_mb": metrics.memory_total_mb,
        "memory_available_mb": metrics.memory_available_mb,
        "swap_total_mb": metrics.swap_total_mb,
        "swap_free_mb": metrics.swap_free_mb,
        "memory_psi_full_avg10": metrics.memory_psi_full_avg10,
        "memory_backing": None if backing is None else {
            "total_bytes": backing.total_bytes, "available_bytes": backing.available_bytes},
    }


class HarnessedNodedAdmission(NodedAdmission):
    """NodedAdmission that brings the test's environment to noded first."""

    def __init__(self, service, link: NodedLink, harness: "NodedHarness") -> None:
        super().__init__(service, link)
        self.harness = harness

    def _call(self, method, path, body=None, *, timeout=None):
        self.harness.sync(self.service)
        with self.harness.waiting():
            return super()._call(method, path, body, timeout=timeout)


class NodedHarness:
    """One ``--admission-only`` noded over a test's registry."""

    def __init__(self, registry_path: Path) -> None:
        self.registry_path = Path(registry_path)
        # AF_UNIX paths are short: the socket lives in its own short directory.
        self.root = Path(tempfile.mkdtemp(prefix="noded-"))
        self.socket = self.root / "noded.sock"
        self.token_file = self.root / "token"
        self.token_file.write_text(TOKEN, encoding="ascii")
        self.config_file = self.root / "admission.json"
        self.metrics_file = self.root / "metrics.json"
        self.observations_file = self.root / "observations.json"
        self.log = self.root / "noded.log"
        self.process: subprocess.Popen | None = None
        self.backends: list[HarnessedNodedAdmission] = []
        self._config: dict | None = None
        self._guard = threading.RLock()
        self._observations: tuple | None = None
        self._seq = 0
        self._waiting = 0
        self._changed = threading.Condition()
        self._service = None
        self._stopped = False
        self._pump = threading.Thread(target=self._pump_loop, name="noded-harness-pump", daemon=True)
        self._pump.start()

    # -- the process --------------------------------------------------------

    def _start(self, config: dict) -> None:
        _write(self.config_file, config)
        with self.log.open("ab") as log:
            self.process = subprocess.Popen(
                [str(noded_binary()), "--admission-only", "--internal-unix", str(self.socket),
                 "--registry", str(self.registry_path), "--node-control-token-file", str(self.token_file),
                 "--admission-config", str(self.config_file), "--metrics-file", str(self.metrics_file),
                 "--observations-file", str(self.observations_file)],
                stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        deadline = time.monotonic() + 20
        while True:
            if self.process.poll() is not None:
                raise AssertionError(f"noded exited at start:\n{self.log.read_text(errors='replace')[-4000:]}")
            probe = socket.socket(socket.AF_UNIX)
            try:
                probe.connect(str(self.socket))
                break
            except OSError:
                if time.monotonic() > deadline:
                    raise AssertionError("noded did not listen in time") from None
                time.sleep(0.01)
            finally:
                probe.close()
        self._config = config

    def _stop_process(self) -> None:
        process, self.process = self.process, None
        if process is None or process.poll() is not None:
            return
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()

    def restart(self) -> None:
        """noded restarts with the same configuration; each agent says hello again."""
        with self._guard:
            config = self._config
            self._stop_process()
            self._start(config)
            for backend in self.backends:
                backend.link.hello_now()

    def close(self) -> None:
        with self._changed:
            self._stopped = True
            self._changed.notify_all()
        self._pump.join(timeout=2)
        for backend in self.backends:
            backend.client.close()
        self._stop_process()
        shutil.rmtree(self.root, ignore_errors=True)

    # -- what noded reads -----------------------------------------------------

    @staticmethod
    def config_of(service) -> dict:
        config = admission_config(service, rust_admission_enabled=True)
        # Every caller sends its own wait; noded's bound must not cut it short.
        config["admission_wait_seconds"] = 3600.0
        root = getattr(service.warden.config, "application_memory_root", None)
        if root is not None:
            config["application_memory_root"] = str(root)
        return config

    def _write_environment(self, service) -> None:
        provider = service._runtime_metrics_provider
        _write(self.metrics_file, _metrics(provider()) if provider is not None else None)
        warden = service.warden
        keys = [(item.sandbox_id, item.sandbox_generation) for item in service.provisioner.registry.list()]
        modes = [{"sandbox_id": key[0], "generation": key[1], "mode": warden.application_memory_mode(*key)}
                 for key in keys]
        default = warden.application_memory_mode("", 0)
        sampler = service._resident_memory
        guard = getattr(sampler, "_guard", None)
        with guard if guard is not None else nullcontext():
            samples = dict(getattr(sampler, "_samples", {}))
        listed = [{"sandbox_id": key[0], "generation": key[1], **asdict(sample)}
                  for key, sample in sorted(samples.items())]
        observed = (default, json.dumps(modes), json.dumps(listed))
        if observed != self._observations:
            if self._observations is None or observed[2] != self._observations[2]:
                self._seq += 1
            self._observations = observed
            _write(self.observations_file, {"seq": self._seq, "default_memory_mode": default,
                                            "memory_modes": modes, "samples": listed})

    def sync(self, service) -> None:
        """noded runs this service's configuration and sees its environment."""
        with self._guard:
            self._service = service
            self._write_environment(service)
            config = self.config_of(service)
            if self.process is None:
                self._start(config)
            elif config != self._config:
                self._stop_process()
                self._start(config)
                for backend in self.backends:
                    backend.link.hello_now()

    @contextmanager
    def waiting(self):
        """A call is in flight: keep noded's files current while it waits."""
        with self._changed:
            self._waiting += 1
            self._changed.notify_all()
        try:
            yield
        finally:
            with self._changed:
                self._waiting -= 1

    def _pump_loop(self) -> None:
        while True:
            with self._changed:
                while not self._waiting and not self._stopped:
                    self._changed.wait()
                if self._stopped:
                    return
            service = self._service
            if service is not None:
                try:
                    with self._guard:
                        self._write_environment(service)
                except Exception:  # noqa: BLE001 - a test's provider may fail on purpose.
                    pass
            time.sleep(_PUMP_SECONDS)

    # -- agents --------------------------------------------------------------

    def attach(self, service) -> HarnessedNodedAdmission:
        """Give ``service`` noded's admission under a new agent session."""
        client = NodedClient(self.socket, TOKEN)
        backend = HarnessedNodedAdmission(service, NodedLink(client), self)
        self.backends.append(backend)
        service.use_noded_admission(backend)
        self.sync(service)
        backend.link.hello_now()
        return backend


def install(testcase: unittest.TestCase, service, *, harness: NodedHarness | None = None) -> NodedHarness:
    """Run ``service``'s admission in a real noded for the rest of ``testcase``."""
    if harness is None:
        harness = NodedHarness(service.provisioner.registry.path)
        testcase.addCleanup(harness.close)
    harness.attach(service)
    return harness
