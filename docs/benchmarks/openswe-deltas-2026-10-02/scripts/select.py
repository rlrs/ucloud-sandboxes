import sqlite3, json, re
c = sqlite3.connect('/home/alex-admin/ucloud-sandboxes/build/openswe-foundations-expanded-20260930/coverage.sqlite')
rows = {s: json.loads(r)['dockerfile'] for s, f, r, p in c.execute('select * from images')}
pandas = [20422, 63232, 22261, 62428, 54945, 43447, 45642, 34736, 21799, 47327, 50682, 58452]
pandas_backup = [39341, 60697, 52264]
others = ['duck-dynasty__duckbot-1203', 'OasisLMF__OasisLMF-1406', 'androguard__androguard-411', 'mps-gmbh__hl7-parser-32',
          'napari__napari-tiff-31', 'newam__idasen-340', 'aiidateam__reentry-50', 'fake-useragent__fake-useragent-216',
          'rustedpy__result-58', 'scikit-learn__scikit-learn-30152', 'astropy__astropy-16038', 'getmoto__moto-7212']
extra = ['getmoto__moto-7752', 'scikit-learn__scikit-learn-29021']
backup_other = ['conan-io__conan-15665', 'pydata__xarray-9194']
sel = []
def add(name, group, role):
    t = 'openswe--' + name
    d = rows[t]
    froms = re.findall(r'^\s*FROM\s+(\S+)', d, re.M | re.I)
    ok = froms and all(f.startswith('10.42.0.2:5000/') and '@sha256:' in f for f in froms)
    sel.append({'task': t, 'group': group, 'role': role, 'dockerfile': d, 'froms': froms, 'registry_from': bool(ok)})
for n in pandas: add(f'pandas-dev__pandas-{n}', 'pandas', 'main')
for n in pandas_backup: add(f'pandas-dev__pandas-{n}', 'pandas', 'backup')
for n in others: add(n, 'other', 'main')
for n in extra: add(n, 'other-later', 'extra')
for n in backup_other: add(n, 'other', 'backup')
for s in sel: print(s['task'], s['group'], s['role'], s['registry_from'], [f.split('/')[-1][:45] for f in s['froms']])
json.dump(sel, open('tasks.json', 'w'), indent=1)
