from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
import runpy
import tempfile
import unittest

from ucloud_sandboxes.images import ImageRecord, ImageStore
from ucloud_sandboxes.managed_registry import RegistryRequestError, digest_protection_tag

HELPER = runpy.run_path(str(Path(__file__).parents[1] / 'docs/benchmarks/build-pipeline-2026-09-29/cleanup-owned-images.py'))
DIGEST = 'sha256:' + '1' * 64
ROW = {'repository': 'ucloud-managed/slotq-owned-123', 'manifest_digest': DIGEST,
       'image_id': 'slotq-owned', 'image_ref': '10.42.0.2:5000/ucloud-managed/slotq-owned-123:latest'}


class FakeRegistry:
    def __init__(self, tags):
        self.values = tags
        self.calls = []

    def tags(self, repository):
        self.calls.append(repository)
        return list(self.values)

    def manifest_digest(self, repository, reference):
        self.calls.append(repository)
        if reference in self.values:
            return self.values[reference]
        if reference in self.values.values():
            return reference
        raise RegistryRequestError(404, 'HEAD', '/owned-fixture', '')


class OwnedImageCleanupTests(unittest.TestCase):
    def test_only_expected_same_digest_aliases_are_eligible(self):
        client = FakeRegistry({'latest': DIGEST, digest_protection_tag(DIGEST): DIGEST,
                               'unrelated-older': 'sha256:' + '2' * 64})
        observed = HELPER['inventory'](client, ROW)
        self.assertEqual(observed['aliases'], sorted(['latest', digest_protection_tag(DIGEST)]))
        self.assertEqual(observed['unrelated_aliases'], 1)
        self.assertEqual(set(client.calls), {ROW['repository']})
        client.values['unrelated-name'] = DIGEST
        with self.assertRaisesRegex(ValueError, 'Unknown alias'):
            HELPER['inventory'](client, ROW)

    def test_absence_and_changed_reference_are_distinct(self):
        self.assertEqual(HELPER['inventory'](FakeRegistry({}), ROW)['state'], 'absent')
        with self.assertRaisesRegex(ValueError, 'latest tag changed'):
            HELPER['inventory'](FakeRegistry({'latest': 'sha256:'+'2'*64}), ROW)
        with self.assertRaisesRegex(ValueError, 'without its declared latest'):
            HELPER['inventory'](FakeRegistry({digest_protection_tag(DIGEST): DIGEST}), ROW)

    def test_gateway_record_alias_and_replacement_fail_closed(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = ImageStore(Path(temporary) / 'images.sqlite')
            own = ImageRecord(id=ROW['image_id'], tag=ROW['image_ref'], source='registry', state='available',
                              created_at=datetime.now(timezone.utc), updated_at=datetime.now(timezone.utc),
                              pushed=True, manifest_digest=DIGEST)
            store.upsert(own)
            with store._transaction(write=True) as conn:
                HELPER['image_record_guard'](store, conn, ROW)
            store.upsert(replace(own, id='unrelated-image'))
            with self.assertRaisesRegex(ValueError, 'Unrelated image record'):
                with store._transaction(write=True) as conn:
                    HELPER['image_record_guard'](store, conn, ROW)
            self.assertEqual(len(store.load()), 2)
            with store._transaction(write=True) as conn:
                conn.execute('DELETE FROM image_state_v1_images WHERE record_id=?', ('unrelated-image',))
            store.upsert(replace(own, manifest_digest='sha256:'+'2'*64))
            with self.assertRaisesRegex(ValueError, 'was replaced'):
                with store._transaction(write=True) as conn:
                    HELPER['image_record_guard'](store, conn, ROW)

    def test_smokes_require_three_distinct_verified_deleted_bound_images(self):
        rows = [{'phase':'slotq-b-cold','image_id':str(i),'recipe':recipe}
                for i,recipe in enumerate(sorted(HELPER['RECIPES']))]
        smokes = {'passed':True,'results':[{'sandbox_id':f'slotq-smoke-{i}','image_id':row['image_id'],
                  'recipe':row['recipe'],'verified':True,'deleted':True,'exit_code':0}
                 for i,row in enumerate(rows)]}
        HELPER['verify_smokes'](smokes, rows)
        smokes['results'][0]['deleted'] = False
        with self.assertRaises(ValueError):
            HELPER['verify_smokes'](smokes, rows)
        smokes['results'][0]['deleted'] = True
        smokes['results'][0]['recipe'] = smokes['results'][1]['recipe']
        with self.assertRaises(ValueError):
            HELPER['verify_smokes'](smokes, rows)


class OwnedSmokeLeaseTests(unittest.TestCase):
    def test_only_exact_persisted_smoke_lease_is_accepted(self):
        from ucloud_sandboxes.managed_registry import RegistryImageLease
        helper = runpy.run_path(str(Path(__file__).parents[1] / 'docs/benchmarks/build-pipeline-2026-09-29/release-owned-smoke-pull-leases.py'))
        image_id = 'slotq-b-cold-71e13d91-045'
        suffix, micros = helper['EXPECTED'][image_id]
        row = dict(ROW, image_id=image_id)
        lease = RegistryImageLease(repository=row['repository'], tag='latest',
            owner='create-image-pull:v1:'+suffix, digest=row['manifest_digest'],
            acquired_at='2026-09-29T21:18:56.'+micros+'+00:00',
            renewed_at='2026-09-29T21:18:56.'+micros+'+00:00',
            expires_at='2026-09-29T22:18:56.'+micros+'+00:00')
        self.assertEqual(helper['validate_lease'](lease,row)['owner'],lease.owner)
        for change in ({'owner':lease.owner+':environment'}, {'digest':'sha256:'+'2'*64},
                       {'renewed_at':'2026-09-29T21:30:00+00:00'}, {'tag':'other'}):
            with self.subTest(change=tuple(change)):
                with self.assertRaisesRegex(ValueError,'Exact owned pull lease changed'):
                    helper['validate_lease'](replace(lease, **change), row)

    def test_lease_plan_reads_only_owned_rows_without_expiry_cleanup(self):
        import sqlite3
        helper = runpy.run_path(str(Path(__file__).parents[1] / 'docs/benchmarks/build-pipeline-2026-09-29/release-owned-smoke-pull-leases.py'))
        with tempfile.TemporaryDirectory() as temporary:
            path=Path(temporary)/'leases.sqlite'
            with sqlite3.connect(path) as db:
                db.execute('CREATE TABLE registry_leases(repository,tag,owner,acquired_at,renewed_at,expires_at,digest)')
                db.executemany('INSERT INTO registry_leases VALUES(?,?,?,?,?,?,?)',
                    [(repository,'latest','fixture','2000-01-01','2000-01-01','2000-01-02',DIGEST)
                     for repository in (ROW['repository'],'unrelated')])
            before=path.read_bytes()
            values=helper['exact_leases'](path,{ROW['repository']})
            self.assertEqual(len(values),1)
            self.assertEqual(values[0].repository,ROW['repository'])
            self.assertEqual(path.read_bytes(),before)
            missing=Path(temporary)/'missing.sqlite'
            with self.assertRaises(sqlite3.OperationalError):
                helper['exact_leases'](missing,{ROW['repository']})
            self.assertFalse(missing.exists())
