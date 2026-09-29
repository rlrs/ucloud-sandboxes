import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path
import re
import shlex
import subprocess
from urllib.parse import parse_qs, urlsplit

def duration(value):
    units = {'ns': 1e-9, 'µs': 1e-6, 'us': 1e-6, 'ms': 1e-3, 's': 1, 'm': 60, 'h': 3600}
    return sum(float(n) * units[u] for n, u in re.findall(r'([0-9.]+)(ns|µs|us|ms|s|m|h)', value))

def run(start, end):
    result = subprocess.run(['docker','logs','--since',start,'--until',end,'ucloud-sandbox-registry'],
                            capture_output=True,text=True,timeout=60,check=True)
    groups = defaultdict(lambda: {'requests': 0, 'response_bytes': 0, 'seconds': [], 'committed_blob_bytes': 0})
    commits = Counter()
    committed_sizes = {}
    mounts = Counter()
    malformed = 0
    for line in result.stderr.splitlines():
        try:
            fields = dict(item.split('=',1) for item in shlex.split(line) if '=' in item)
        except ValueError:
            malformed += 1
            continue
        if fields.get('msg') != 'response completed' or 'http.response.status' not in fields:
            continue
        method = fields.get('http.request.method', '')
        status = int(fields['http.response.status'])
        uri = urlsplit(fields.get('http.request.uri', ''))
        query = parse_qs(uri.query)
        repository = fields.get('vars.name', '')
        kind = ('build-cache' if repository.startswith('ucloud-build-cache') else
                'managed-image' if repository.startswith('ucloud-managed/') else
                'environments' if repository == 'environments' else 'other')
        route = ('upload' if '/blobs/uploads/' in uri.path else
                 'blob' if '/blobs/' in uri.path else
                 'manifest' if '/manifests/' in uri.path else
                 'tags' if '/tags/list' in uri.path else 'other')
        key = '|'.join((kind, method, route, str(status), 'mount' if 'mount' in query else ''))
        group = groups[key]
        group['requests'] += 1
        group['response_bytes'] += int(fields.get('http.response.written', '0'))
        group['seconds'].append(duration(fields.get('http.response.duration', '0s')))
        if 'mount' in query:
            mounts[str(status)] += 1
        digest = query.get('digest', [''])[0]
        if method == 'PUT' and route == 'upload' and status == 201 and re.fullmatch(r'sha256:[0-9a-f]{64}',digest):
            path = Path('/mnt/ucloud-registry/docker-registry/docker/registry/v2/blobs/sha256') / digest[7:9] / digest[7:] / 'data'
            if path.is_file():
                size = path.stat().st_size
                group['committed_blob_bytes'] += size
                commits[(kind, digest)] += 1
                committed_sizes[digest] = size
    for group in groups.values():
        values = sorted(group.pop('seconds'))
        group['duration_seconds'] = {'sum':sum(values), 'p50':values[len(values)//2],
                                    'p95':values[min(len(values)-1,int(len(values)*.95))], 'max':max(values)}
    totals = Counter()
    for (kind,digest), count in commits.items():
        totals[kind] += count * committed_sizes[digest]
    return {'start':start,'end':end,'registry_log_bytes_read':len(result.stdout)+len(result.stderr),
            'groups':dict(groups),'mount_statuses':dict(mounts),'committed_blob_bytes_by_kind':dict(totals),
            'unique_committed_blob_bytes':sum(committed_sizes.values()), 'malformed_lines':malformed,
            'largest_repeated_commits':[{'kind':kind,'digest':digest,'count':count,'bytes':committed_sizes[digest],
                                        'total_committed_bytes':count*committed_sizes[digest]}
                                      for (kind,digest),count in sorted(commits.items(),key=lambda x:x[1]*committed_sizes[x[0][1]],reverse=True)[:20]],
            'limits':['Committed bytes use current immutable blob file sizes; they are not measured request body or physical disk bytes.',
                      'Only completed registry log events within the selected window are counted; uploads spanning boundaries may be incomplete.',
                      'Duration sums span concurrent requests and are not elapsed wall time. No raw logs or headers are exported.']}

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', required=True)
    parser.add_argument('--end', required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.start, args.end), indent=2))
