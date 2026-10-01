from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
import runpy
import sqlite3
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from ucloud_sandboxes.control_plane import _registry_operation_lease_owner
from ucloud_sandboxes.managed_registry import RegistryUsageStore

HELPER = runpy.run_path(str(Path(__file__).parents[1] / 'docs/benchmarks/registry-pulls-2026-09-30/release-owned-smoke-pull-leases.py'))
DIGEST = 'sha256:' + '1' * 64


class SmokePullLeaseTests(unittest.TestCase):
    def case(self):
        ref = '10.42.0.2:5000/ucloud-managed/owned:latest'
        row = dict(image_id='owned', recipe='python-agent', variant='app-change-20',
                   image_ref=ref, repository='ucloud-managed/owned', tag='latest', manifest_digest=DIGEST)
        worker = dict(job_id='123', node_id='10.42.0.7', node_epoch='epoch-a', node_url='http://10.42.0.7:8081')
        resolved = ref+'@'+DIGEST
        owner = _registry_operation_lease_owner('create-image-pull', ('123','epoch-a',worker['node_url'],resolved))
        captured = dict(repository=row['repository'], tag='latest', owner=owner, digest=DIGEST,
                        acquired_at='2000-01-01T00:00:00+00:00', renewed_at='2000-01-01T00:00:00+00:00',
                        expires_at='2000-01-01T01:00:00+00:00')
        case = dict(image_id='owned', recipe=row['recipe'], variant=row['variant'],
                    published_image_tag=ref, published_manifest_digest=DIGEST,
                    resolved_image_ref=resolved, worker=worker, create_image_pull_lease_owner=owner,
                    create_image_pull_lease_owner_verified=True, create_image_pull_lease=captured,
                    finished_at='2000-01-01T00:00:10+00:00')
        return row, case

    def test_recomputed_owner_binds_incarnation_endpoint_and_full_repository(self):
        row, case = self.case()
        self.assertEqual(HELPER['expected_lease'](case,row),case['create_image_pull_lease'])
        changes = [('epoch', lambda c:c['worker'].update(node_epoch='epoch-b')),
                   ('node', lambda c:c['worker'].update(job_id='456')),
                   ('url', lambda c:c['worker'].update(node_url='http://10.42.0.8:8081')),
                   ('repository', lambda c:c.update(resolved_image_ref='other@'+DIGEST)),
                   ('owner', lambda c:c.update(create_image_pull_lease_owner='other')),
                   ('digest', lambda c:c['create_image_pull_lease'].update(digest='sha256:'+'2'*64))]
        for name, change in changes:
            modified = deepcopy(case)
            change(modified)
            with self.subTest(change=name), self.assertRaises(ValueError):
                HELPER['expected_lease'](modified,row)

    def store(self, root):
        store = RegistryUsageStore(root/'usage.sqlite')
        expected = {}
        for i in range(3):
            lease = store.acquire_lease(f'ucloud-managed/owned-{i}', 'latest', f'create-image-pull:v1:{i}',
                ttl_seconds=3600, digest=DIGEST, now=datetime(2000,1,1,tzinfo=timezone.utc))
            expected[str(i)] = asdict(lease)
        store.acquire_lease('environments', 'component', 'unrelated', ttl_seconds=1,
                            digest=DIGEST, now=datetime(2000,1,1,tzinfo=timezone.utc))
        return store, expected

    def test_readonly_plan_preserves_expired_unrelated_rows_and_missing_database(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            store, expected = self.store(root)
            before = store.path.read_bytes()
            values = HELPER['current_selection'](store.path,expected)
            self.assertEqual([v['state'] for v in values],['expired']*3)
            self.assertEqual(store.path.read_bytes(),before)
            missing = root/'missing.sqlite'
            with self.assertRaises(sqlite3.OperationalError):
                HELPER['current_selection'](missing,expected)
            self.assertFalse(missing.exists())

    def test_owner_digest_and_every_timestamp_change_fail_closed(self):
        for field, value in [('owner','other'),('digest','sha256:'+'2'*64),
                             ('acquired_at','2000-01-01T00:00:01+00:00'),
                             ('renewed_at','2000-01-01T00:00:01+00:00'),
                             ('expires_at','2000-01-01T02:00:00+00:00')]:
            with self.subTest(field=field), TemporaryDirectory() as tmp:
                store, expected = self.store(Path(tmp))
                with sqlite3.connect(store.path) as db:
                    db.execute(f'UPDATE registry_leases SET {field}=? WHERE repository=?',
                               (value,expected['1']['repository']))
                with self.assertRaisesRegex(ValueError,'Exact owned pull lease changed'):
                    HELPER['current_selection'](store.path,expected)

    def test_apply_revalidates_all_before_first_release(self):
        with TemporaryDirectory() as tmp:
            store, expected = self.store(Path(tmp))
            selected = HELPER['current_selection'](store.path,expected)
            with sqlite3.connect(store.path) as db:
                db.execute('UPDATE registry_leases SET renewed_at=? WHERE repository=?',
                           ('2000-01-01T00:00:01+00:00',expected['2']['repository']))
            with patch.object(store,'release_lease',wraps=store.release_lease) as release:
                with self.assertRaises(ValueError):
                    HELPER['release_selected'](store,selected,[])
                release.assert_not_called()

    def test_expired_exact_release_and_absence_are_idempotent_and_preserve_other_rows(self):
        with TemporaryDirectory() as tmp:
            store, expected = self.store(Path(tmp))
            with sqlite3.connect(store.path) as db:
                unrelated = db.execute('SELECT * FROM registry_leases WHERE repository=?',('environments',)).fetchone()
            rows=[]
            HELPER['release_selected'](store,HELPER['current_selection'](store.path,expected),rows)
            self.assertEqual(sum(v['released'] for v in rows),3)
            again=[]
            HELPER['release_selected'](store,HELPER['current_selection'](store.path,expected),again)
            self.assertEqual([v['state'] for v in again],['already_absent']*3)
            with sqlite3.connect(store.path) as db:
                self.assertEqual(db.execute('SELECT * FROM registry_leases').fetchall(),[unrelated])

    def test_additional_owned_repository_lease_blocks_release_without_touching_it(self):
        with TemporaryDirectory() as tmp:
            store, expected = self.store(Path(tmp))
            store.acquire_lease(expected['0']['repository'],'latest','unrelated-reader',ttl_seconds=3600,digest=DIGEST)
            before=store.path.read_bytes()
            with self.assertRaisesRegex(ValueError,'Additional lease'):
                HELPER['current_selection'](store.path,expected)
            self.assertEqual(store.path.read_bytes(),before)

    def test_provider_receipt_requires_every_worker_404_after_smoke_finished(self):
        _,case=self.case()
        provider=dict(all_absent=True,provider='hetzner',mutations=0,verified_at='2000-01-01T00:00:11Z',
                      nodes=[dict(id='123',absent=True,http_status=404)])
        self.assertEqual(HELPER['verify_provider'](provider,[case]),['123'])
        for update in (dict(verified_at='2000-01-01T00:00:09Z'),dict(nodes=[]),
                       dict(nodes=[dict(id='123',absent=True,http_status=500)]),dict(all_absent=False)):
            with self.subTest(change=update),self.assertRaises(ValueError):
                HELPER['verify_provider']({**provider,**update},[case])
