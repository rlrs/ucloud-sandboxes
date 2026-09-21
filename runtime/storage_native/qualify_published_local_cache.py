"""Qualify local published-layer reuse with an isolated native backend, without devices."""
import argparse
import json
from pathlib import Path
import tempfile

from runtime.storage_native.benchmark_delta_compaction import apply_layer, write_layer
from runtime.storage_native.qualify_local_compaction import qualify
from ucloud_sandboxes.storage_native_local_cache import PublishedLocalCache
from ucloud_sandboxes.storage_native_registry import consume_export_stream


def exercise(root, config, client):
    volume = root / "volume"
    volume.mkdir()
    source, delta = volume / "source.commit", volume / "delta.commit"
    size = 9 * 1024**2
    write_layer(source, size, [(0, b"A" * (8 * 1024**2))])
    write_layer(delta, size, [(0, b"B" * 4096), (16384, bytes(4096)),
                              (65536, 4096), (8 * 1024**2, b"H" * 4096)])
    expected = bytearray(size)
    apply_layer(source, expected)
    apply_layer(delta, expected)
    # Native dense export supplies the actual uploaded representation/digest.
    descriptors = []
    dense_paths = []
    cache = PublishedLocalCache(root / "local-cache", capacity_bytes=16 * 1024**2)
    pins = []
    for index, path in enumerate((source, delta)):
        dense = root / f"dense-{index}.commit"
        with dense.open("wb") as stream:
            descriptor = consume_export_stream(
                lambda sock: client.export_dense_layer(source_layer_path=path, stream_socket_path=sock),
                stream_socket_root=root, chunk_bytes=1024**2, timeout_seconds=30, consume=stream.write,
            )
        descriptors.append(descriptor)
        dense_paths.append(dense)
        cache.remember("test-origin", descriptor.digest, path)
        pin = cache.pin("test-origin", descriptor.digest, volume, mount_revision=5)
        assert pin is not None and pin.stat().st_ino == path.stat().st_ino
        pins.append(pin)
        path.unlink()
    # Remove all idle cache entries. In-use mount pins must survive eviction.
    cache.capacity_bytes = 0
    cache.maintain()
    assert cache.metrics()["published_local_cache_entries"] == 0
    for name, paths in (("dense", dense_paths), ("local", pins)):
        cfg = root / f"{name}.json"
        # Any accidental remote lookup fails; native export must use the pins.
        cfg.write_text(json.dumps({"lowers": [{"file": str(path)} for path in paths],
                                   "upper": {}, "resultFile": "", "repoBlobUrl": "http://127.0.0.1:1/unreachable"}))
        result = root / f"{name}-merged.commit"
        with result.open("wb") as stream:
            consume_export_stream(
                lambda sock: client.export_compacted_image(source_image_config=cfg, global_config=config, stream_socket_path=sock),
                stream_socket_root=root, chunk_bytes=1024**2, timeout_seconds=30, consume=stream.write,
            )
        observed = bytearray(size)
        apply_layer(result, observed)
        assert observed == expected, f"{name} native restack changed logical contents"
    return {"virtual_bytes": size, "published_layer_bytes": sum(item.size for item in descriptors),
            "locally_reused_layers": len(pins), "copied_cache_data_bytes": 0,
            "checks": {"same_inode": True, "original_names_removed": True,
                       "evicted_cache_pins_survive": True, "native_restack_matches_dense": True,
                       "overwrites_zeros_discards_holes_preserved": True,
                       "remote_origin_unreachable": True, "created_devices": 0},
            "metrics": cache.metrics()}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", type=Path, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="local-cache-qual-", dir="/tmp") as raw:
        print(json.dumps(qualify(Path(raw), args.backend, exercise), indent=2))
