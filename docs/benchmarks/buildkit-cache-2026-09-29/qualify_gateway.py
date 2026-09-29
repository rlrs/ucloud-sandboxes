"""Public SDK canary; run on the gateway so credentials never leave it."""
# ruff: noqa: E402 -- use the released SDK wheel staged on the gateway.
import argparse
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import time

sys.path.insert(0, '/work/ucloud-sandboxes/sdk-status-0.4.33-20260928/ucloud_sandboxes_sdk-0.4.33-py3-none-any.whl')
from ucloud_sandboxes_sdk import SandboxClient, SandboxSpec, Image
from ucloud_sandboxes.config import DeploymentConfig

ROOT = Path('/work/ucloud-sandboxes/buildkit-cache-optimization-20260929-r3')
NAME = 'buildkit-cache-production-20260929'
SANDBOX = 'buildkit-cache-exec-20260929'
PREPARE = 'build-cache-qualification-20260929'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('phase', choices=['prepare', 'release', 'build', 'sandbox'])
    args = parser.parse_args()
    config = DeploymentConfig.from_dict(json.loads(Path('/etc/ucloud-sandboxes/deployment.json').read_text()))
    client = SandboxClient('https://77.42.92.27', api_token=config.sandbox_api_token_file().read_text().strip())
    receipt = {'phase': args.phase, 'started_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}
    if args.phase == 'prepare':
        client.prepare_builder(count=1, ttl_seconds=3600, prepare_id=PREPARE)
        receipt['prepare_id'] = PREPARE
    elif args.phase == 'release':
        client.delete_prepared_builder(PREPARE)
        receipt['released'] = True
    elif args.phase == 'build':
        context = ROOT / 'context'
        context.mkdir(exist_ok=True)
        payload = hashlib.shake_256(b'ucloud-buildkit-cache-qualification-20260929-v1').digest(32 * 1024**2)
        (context / 'payload').write_bytes(payload)
        (context / 'Dockerfile').write_text('FROM busybox:1.37.0\nCOPY payload /payload\nRUN for i in $(seq 1 32); do sha256sum /payload; done > /proof\nCMD ["sh"]\n')
        before = time.monotonic()
        image = Image.from_dockerfile(name=NAME, context_path=context)
        submitted = client.submit_image_build(image, timeout_seconds=600)
        done = client.wait_for_image_build(submitted['build_id'], timeout_seconds=600, poll_interval_seconds=.5)
        assert done['status'] == 'succeeded', done.get('error')
        receipt['wall_seconds_including_upload'] = time.monotonic() - before
        receipt['build'] = {k:done.get(k) for k in ('build_id','image_id','status','created_at','started_at','queued_at','execution_started_at','finished_at','timings')}
        receipt['cached_step_lines'] = [line for line in done.get('log_tail','').splitlines() if 'CACHED' in line]
        assert receipt['cached_step_lines'], 'Fresh builder must import shared build steps'
        assert done['timings']['environment'].get('docker_pull_skipped') == 1
        db = sqlite3.connect(config.metrics_path().with_name('build-history.sqlite').as_uri() + '?mode=ro', uri=True)
        row = db.execute('SELECT summary_json FROM terminal_builds WHERE build_id=?', (done['build_id'],)).fetchone()
        assert row is not None
        summary = json.loads(row[0])
        assert summary['status'] == 'succeeded' and 'queue_wait_ms' in summary['timings']
        receipt['durable_history'] = summary
    else:
        expected = hashlib.sha256(hashlib.shake_256(b'ucloud-buildkit-cache-qualification-20260929-v1').digest(32 * 1024**2)).hexdigest()
        try:
            client.create_sandbox(SandboxSpec(id=SANDBOX, image=Image.from_name(NAME),
                command=['sleep','600'], memory_mb=512, cpus=1, disk_mb=1024,
                ttl_seconds=600, labels={'qualification':'buildkit-cache-20260929'}), request_timeout_seconds=600)
            result = client.exec(SANDBOX, ['sh','-c','sha256sum /payload; wc -l < /proof; cat /proof | sort -u'], timeout_seconds=60)
            assert result.exit_code == 0
            assert result.stdout.splitlines() == [expected + '  /payload', '32', expected + '  /payload'], result.stdout
            receipt['exit_code'] = result.exit_code
            receipt['stdout'] = result.stdout
            receipt['payload_verified'] = True
        finally:
            client.delete_sandbox(SANDBOX)
            receipt['sandbox_deleted'] = True
    (ROOT / ('public-' + args.phase + '-receipt.json')).write_text(json.dumps(receipt,indent=2) + '\n')
    print(json.dumps(receipt, indent=2))


if __name__ == '__main__':
    main()
