#!/bin/bash
set -euxo pipefail
W=/root/s11; cd $W
rm -rf bundle agent; mkdir -p bundle agent
tar -xzf sandbox-node-package.tar.gz -C bundle --exclude='runtime/debs/*'
tar -xf bundle/runtime/agent/node-agent-runtime.tar -C agent
find agent -maxdepth 3 | head -30
ls -la bundle/runtime/direct bundle/runtime/direct/gvisor-bin
bundle/runtime/direct/runsc --version
python3 -c "import json;d=json.load(open('bundle/runtime/direct/build-manifest.json'));print(json.dumps(d)[:1500])"
