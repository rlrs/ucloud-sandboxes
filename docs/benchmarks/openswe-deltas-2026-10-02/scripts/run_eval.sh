#!/bin/bash
# What the OpenSWE harness runs (verifiers OpenSWETaskSet): setup symlink, optionally the gold patch
# (validate_instance: git apply --whitespace=fix, falling back to patch --fuzz=5 -p1), then /eval.sh.
# Usage: run_eval.sh <label> [gold]
ln -sf /opt/conda/envs/testbed /root/.venv
export PATH=/opt/conda/envs/testbed/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin PAGER=cat MANPAGER=cat LESS=-R PIP_PROGRESS_BAR=off TQDM_DISABLE=1
cd /testbed
if [ "${2:-}" = gold ]; then
  if git apply --whitespace=fix /e/gold.patch 2>/tmp/gold.err || patch --fuzz=5 -p1 -i /e/gold.patch >>/tmp/gold.err 2>&1; then echo gold_apply=ok; else echo gold_apply=failed; fi
fi
bash /e/eval.sh > /tmp/test_output.txt 2>&1
echo "eval_shell_rc=$?"
grep -o 'OPENSWE_EXIT_CODE=[0-9]*' /tmp/test_output.txt | tail -1
tail -c 3000 /tmp/test_output.txt > /out/$1.tail 2>/dev/null || true
