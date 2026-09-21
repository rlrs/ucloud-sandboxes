"""Qualify local compaction on temporary files using an isolated native daemon.

No mounts, devices, remote publications, production journals or provider changes.
"""
from __future__ import annotations

import argparse
from dataclasses import replace
import json
from pathlib import Path
import subprocess
import tempfile
import threading
import time

from runtime.storage_native.benchmark_delta_compaction import apply_layer, write_layer
from ucloud_sandboxes.storage_native import AgentEnvUblkClient
from ucloud_sandboxes.storage_native_compaction import LocalCheckpointCompactor
from ucloud_sandboxes.storage_native_daemon import StorageVolumeRecord, StorageVolumeState


def qualify(root: Path, backend: Path, exercise_fn=None) -> dict:
    config = root / "global.json"
    config.write_text(json.dumps({
        "cacheConfig": {"cacheDir": str(root / "cache"), "cacheSizeGB": 1,
                        "cacheType": "file", "refillSize": 262144},
        "download": {"enable": False}, "nrIoRings": 1, "registryFsVersion": "v2",
    }))
    control = root / "backend.sock"
    with (root / "backend.log").open("w+") as log:
        daemon = subprocess.Popen([
            str(backend), "--socket-path", str(control), "--global-config", str(config),
            "--resize-global-config", str(config), "--metrics-listen-addr", "", "--log-level", "warn",
        ], stdout=log, stderr=log)
        try:
            deadline = time.monotonic() + 10
            while not control.exists():
                if daemon.poll() is not None or time.monotonic() >= deadline:
                    log.seek(0)
                    raise RuntimeError("isolated backend did not start: " + log.read()[-2000:])
                time.sleep(.02)
            return (exercise_fn or exercise)(root, config, AgentEnvUblkClient(control))
        finally:
            daemon.terminate()
            try:
                daemon.wait(timeout=10)
            except subprocess.TimeoutExpired:
                daemon.kill()
                daemon.wait(timeout=5)


def exercise(root, config, client):
    volume = root / "volume"
    volume.mkdir()
    virtual_size = 9 * 1024**2
    expected = bytearray(virtual_size)
    paths = []
    for index in range(29):
        path = volume / f"layer-{index}.commit"
        if index == 0:
            extents = [(0, b"A" * (8 * 1024**2))]
        else:
            # Overwrites, explicit zero writes, discards and previously empty
            # space must retain their meaning when merged without a base.
            extents = [(4096 * (index % 7), bytes([index]) * 4096),
                       (65536, bytes(4096)), (131072, 4096),
                       (8 * 1024**2 + index * 4096, b"H" * 4096)]
        write_layer(path, virtual_size, extents)
        apply_layer(path, expected)
        paths.append(str(path))
    current = [StorageVolumeRecord(
        volume_id="volume", sandbox_id="sandbox", sandbox_generation=1,
        revision=1, state=StorageVolumeState.RELEASED, operation_id="qualification",
        virtual_size=virtual_size, runtime_dir=str(volume), mount_path=str(root / "mount"),
        source_image_config=str(root / "source.json"), sealed_layer_paths=tuple(paths),
        accounting_id=200001, device_owner_id="",
    )]
    started, resume = threading.Event(), threading.Event()

    class Exporter:
        def export_compacted_image(self, **kwargs):
            started.set()
            if not resume.wait(10):
                raise TimeoutError("qualification did not resume exporter")
            return client.export_compacted_image(**kwargs)

    def persist(record):
        current[0] = record

    def remove(paths):
        for path in paths:
            path.unlink(missing_ok=True)

    compactor = LocalCheckpointCompactor(root=root, global_config=config, exporter=Exporter(),
                                         load=lambda _: current[0], remove_layers=remove)
    start = time.monotonic()
    try:
        compactor.submit(current[0])
        assert started.wait(5), "compaction did not start"
        current[0] = replace(current[0], state=StorageVolumeState.MOUNTED, revision=2)
        appended = volume / "appended.commit"
        write_layer(appended, virtual_size, [(0, b"Z" * 4096)])
        apply_layer(appended, expected)
        current[0] = replace(current[0], state=StorageVolumeState.RELEASED, revision=3,
                             sealed_layer_paths=(*current[0].sealed_layer_paths, str(appended)))
        resume.set()
        compactor.wait(15)
        assert compactor.metrics()["local_compaction_completed"] == 1, compactor.metrics()
        current[0] = replace(current[0], state=StorageVolumeState.ACQUIRING, revision=4)
        current[0] = compactor.adopt(current[0], persist)
        assert len(current[0].sealed_layer_paths) == 3
        first_result = compactor.metrics()
        first_seconds = time.monotonic() - start
        maximum_layers = 0
        for cycle in range(24):
            observed = bytearray(virtual_size)
            for path in current[0].sealed_layer_paths:
                apply_layer(Path(path), observed)
            assert observed == expected, f"logical data changed at cycle {cycle}"
            path = volume / f"cycle-{cycle}.commit"
            write_layer(path, virtual_size, [(196608 + 4096 * (cycle % 5), bytes([cycle + 1]) * 4096)])
            apply_layer(path, expected)
            current[0] = replace(current[0], state=StorageVolumeState.RELEASED,
                                 revision=current[0].revision + 1,
                                 sealed_layer_paths=(*current[0].sealed_layer_paths, str(path)))
            maximum_layers = max(maximum_layers, len(current[0].sealed_layer_paths))
            compactor.submit(current[0])
            compactor.wait(15)
            current[0] = replace(current[0], state=StorageVolumeState.ACQUIRING,
                                 revision=current[0].revision + 1)
            current[0] = compactor.adopt(current[0], persist)
        observed = bytearray(virtual_size)
        for path in current[0].sealed_layer_paths:
            apply_layer(Path(path), observed)
        assert observed == expected
        assert maximum_layers <= 9
        return {"initial_layers": 29, "layers_after_first_adoption_with_new_delta": 3,
                "first_compaction_seconds": first_seconds, "first_metrics": first_result,
                "additional_cycles": 24, "maximum_layers_before_adoption": maximum_layers,
                "final_layers": len(current[0].sealed_layer_paths), "metrics": compactor.metrics(),
                "checks": {"overwrite": True, "zero_write": True, "discard": True,
                           "holes": True, "wake_during_compaction": True, "appended_layer": True,
                           "remote_publications": 0, "created_devices": 0}}
    finally:
        resume.set()
        compactor.wait(20)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", type=Path, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="local-compact-qualification-", dir="/tmp") as raw:
        print(json.dumps(qualify(Path(raw), args.backend), indent=2))
