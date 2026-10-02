#!/bin/bash
# Harness-level checks inside a container from a (slimmed) task image. Prints key=value lines.
# /e/base_commit, /e/module, /e/collect.txt (pytest node ids / paths), /e/test.patch
export PATH=/opt/conda/envs/testbed/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export PAGER=cat MANPAGER=cat PIP_PROGRESS_BAR=off TQDM_DISABLE=1
cd /testbed
t0=$(date +%s%N); git status --porcelain=v1 > /tmp/st 2>/tmp/st.err; echo "git_status_rc=$?"
echo "git_status_ms=$(( ($(date +%s%N) - t0) / 1000000 ))"
echo "git_status_lines=$(wc -l < /tmp/st)"; echo "git_status_sha=$(sha256sum < /tmp/st | cut -c1-16)"
head -c 300 /tmp/st.err | tr '\n' ' ' | sed 's/^/git_status_err=/'; echo
echo "head=$(git rev-parse HEAD 2>&1)"; echo "head_is_base=$([ "$(git rev-parse HEAD 2>/dev/null)" = "$(cat /e/base_commit)" ] && echo 1 || echo 0)"
echo "base_commit_present=$(git cat-file -e "$(cat /e/base_commit)^{commit}" 2>/dev/null && echo 1 || echo 0)"
echo "commits=$(git rev-list --count HEAD 2>/dev/null)"; echo "describe=$(git describe --tags 2>&1 | head -1)"
git diff --quiet; echo "git_diff_rc=$?"
echo "fsck_rc=$(git fsck --connectivity-only --no-dangling >/dev/null 2>&1; echo $?)"
. /opt/conda/etc/profile.d/conda.sh 2>/dev/null; conda activate testbed 2>/dev/null
m=$(cat /e/module 2>/dev/null)
if [ -n "$m" ]; then echo "version=$(timeout 300 python -c "import $m as x; print(getattr(x,'__version__','?'))" 2>&1 | tail -1)"; fi
git apply --check /e/test.patch 2>/tmp/ap.err; echo "test_patch_check_rc=$?"
git apply /e/test.patch 2>/dev/null
if [ -s /e/collect.txt ]; then
  timeout 900 python -m pytest --collect-only -q -p no:cacheprovider $(cat /e/collect.txt) > /tmp/co 2>&1; echo "collect_rc=$?"
  echo "collect_tail=$(grep -E '[0-9]+ (tests? )?(collected|selected)|error|no tests ran' /tmp/co | tail -2 | tr '\n' ' ' | cut -c1-300)"
  echo "collected=$(grep -cE '::' /tmp/co)"
fi
git checkout -q -- . 2>/dev/null
