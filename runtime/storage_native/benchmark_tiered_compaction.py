#!/usr/bin/env python3
"""Compare repeated native exports with old and size-tiered merge selection.

Uses a private daemon, temporary layers and no devices. Synthetic overwrite
trace, not a production latency benchmark. Run on isolated Linux with PYTHONPATH.
"""
import argparse
import json
from pathlib import Path
import tempfile
import time

from benchmark_delta_compaction import apply_layer, write_layer
from qualify_volume import Qualifier
from ucloud_sandboxes.storage_native_publication import snapshot_compaction_start
from ucloud_sandboxes.storage_native_registry import consume_export_stream

MIB = 1024**2


def run(binary, root, cycles):
    qualifier = Qualifier(daemon_binary=binary, work_root=root, output=root / 'unused.json',
                          virtual_size=65*MIB, upper_mode='sparse')
    qualifier.test_root = root
    try:
        qualifier._start_daemon()
        base = root / 'base.lsmt'
        write_layer(base, 65*MIB, [(0, b'A' * (64*MIB))])
        deltas = []
        for i in range(cycles):
            layer = root / f'delta-{i}.lsmt'
            # Repeated hot writes, explicit zeros and discard over older data.
            write_layer(layer, 65*MIB, [((i % 8)*2*MIB, bytes([i % 250 + 1])*2*MIB),
                                       (32*MIB, bytes(4096) if i % 2 else 4096)])
            deltas.append(layer)
        results = {}
        for mode in ('previous', 'tiered'):
            chain = [base]
            expected = bytearray(65*MIB)
            apply_layer(base, expected)
            exported = merges = 0
            started = time.monotonic()
            for i, layer in enumerate(deltas):
                chain.append(layer)
                apply_layer(layer, expected)
                sizes = tuple(path.stat().st_size for path in chain)
                if mode == 'tiered':
                    start = snapshot_compaction_start(sizes, max_layers=8,
                        max_delta_bytes=4*MIB, reusable_base=True)
                elif len(sizes) > 8 or sum(sizes[1:]) > 4*MIB:
                    start = 1 if sum(sizes[1:]) <= 4*MIB and sizes[0] > sum(sizes[1:]) else 0
                else:
                    start = None
                if start is not None:
                    config = root / f'{mode}-{i}.json'
                    config.write_text(json.dumps({'lowers': [{'file': str(p)} for p in chain[start:]],
                                                  'upper': {}, 'repoBlobUrl': '', 'resultFile': ''}))
                    output = root / f'{mode}-{i}.lsmt'
                    with output.open('wb') as stream:
                        result = consume_export_stream(lambda sock: qualifier.client.export_compacted_image(
                            source_image_config=config, global_config=root / 'global.json',
                            stream_socket_path=sock), stream_socket_root=root, chunk_bytes=MIB,
                            timeout_seconds=120, consume=stream.write)
                    exported += result.size
                    merges += 1
                    chain = [*chain[:start], output]
                actual = bytearray(65*MIB)
                for path in chain:
                    apply_layer(path, actual)
                assert actual == expected, (mode, i, 'logical mismatch')
                assert len(chain) <= 8
            results[mode] = {'exported_bytes': exported, 'merges': merges,
                             'seconds_including_verification': time.monotonic()-started,
                             'final_layers': len(chain)}
        return {'cycles': cycles, 'results': results, 'logical_verification_each_cycle': True,
                'scope': 'Synthetic local overwrite/export trace; excludes publication and production load.'}
    except Exception as exc:
        log = root / 'ublk-daemon.log'
        tail = log.read_text(errors='replace')[-5000:] if log.exists() else ''
        raise RuntimeError(f'{exc}\nNative daemon log:\n{tail}') from exc
    finally:
        qualifier._cleanup()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--backend-binary', type=Path, required=True)
    parser.add_argument('--cycles', type=int, default=32)
    parser.add_argument('--work-root', type=Path, default=Path('/var/tmp'))
    args = parser.parse_args()
    if args.cycles < 1:
        parser.error('cycles must be positive')
    with tempfile.TemporaryDirectory(prefix='tiered-', dir=args.work_root) as directory:
        print(json.dumps(run(args.backend_binary.resolve(), Path(directory), args.cycles), indent=2))
