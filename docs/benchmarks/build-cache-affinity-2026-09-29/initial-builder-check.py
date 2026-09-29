#!/usr/bin/env python3
"""Read-only candidate source and empty local BuildKit check before load."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
bundle = sys.argv[1]
site = Path('/var/cache/ucloud-sandboxes/init-packages') / bundle / 'agent-runtime/site-packages'
expected = json.loads(sys.argv[2])
observed = {name: hashlib.sha256((site/'ucloud_sandboxes'/name).read_bytes()).hexdigest() for name in expected}
assert observed == expected, 'Candidate source mismatch'
active = subprocess.run(['systemctl','is-active','--quiet','ucloud-sandbox-node'], check=False).returncode == 0
assert active, 'Node agent is not active'
service_user = subprocess.run(['systemctl','show','ucloud-sandbox-node','--property=User','--value'], check=True, text=True, capture_output=True).stdout.strip() or 'root'
command = ['runuser','-u',service_user,'--','docker','buildx','du','--builder','ucloud-shared-cache']
du = subprocess.run(command, check=True, text=True, capture_output=True).stdout
assert 'Total:\t\t0B' in du or 'Total:          0B' in du or any(line.startswith('Total:') and line.split()[-1]=='0B' for line in du.splitlines()), 'Local BuildKit cache is not empty'
config = Path('/etc/ucloud-sandboxes/buildkit/buildkitd.toml').read_bytes()
version = subprocess.run(['docker','exec','buildx_buildkit_ucloud-shared-cache0','buildkitd','--version'],check=True,capture_output=True,text=True).stdout.strip()
print(json.dumps({'source_hashes': observed, 'node_active': active, 'buildkit_cache_empty': True,
                  'buildkit_version': version, 'buildkit_config_sha256': hashlib.sha256(config).hexdigest(),
                  'buildkit_config': config.decode()}))
