#!/bin/bash
# Slimming applied as a final build step inside a container started from a built task image.
# Usage: slim.sh <variant>; variants are cumulative as described in the README:
#   a                 caches, /tmp, logs, apt lists (no behaviour change)
#   a_gc              a + git gc --prune=now (full history kept)
#   a_shallow         a + history cut to HEAD (.git/shallow), all refs dropped, repacked
#   a_shallow_loose   a_shallow, but objects left loose (one zlib file per object), gc.auto=0
#   a_placeholder     a + .git re-initialised with one placeholder commit of the tracked files
#   a_sharedpack      a + objects replaced by a per-project canonical pack mounted at /mirror
#   c                 a_shallow_loose + checked-hash .pyc for the delta + strip -g of in-tree .so
#                     + setuptools build/temp* object files removed (see /pyc.list)
set -u
v=$1
log() { echo "[slim $v] $*"; }
do_a() {
  [ -n "${SLIM_TEST:-}" ] && return 0   # local self-test of the git steps only
  rm -rf /root/.cache /root/.npm/_cacache /root/.cargo/registry/cache /root/.conda/pkgs /home/*/.cache 2>/dev/null
  if [ -x /opt/conda/bin/conda ]; then /opt/conda/bin/conda clean -a -y -q >/dev/null 2>&1 || log "conda clean failed"; fi
  rm -rf /opt/conda/pkgs/*.tar.bz2 /opt/conda/pkgs/*.conda /opt/conda/pkgs/cache /opt/conda/pkgs/urls* 2>/dev/null
  apt-get clean >/dev/null 2>&1; rm -rf /var/lib/apt/lists/* /var/cache/apt/*.bin 2>/dev/null
  rm -rf /tmp/* /tmp/.[!.]* /var/tmp/* /var/tmp/.[!.]* 2>/dev/null
  find /var/log -type f -exec truncate -s 0 {} + 2>/dev/null
  rm -f /root/.wget-hsts /root/.python_history /root/.bash_history 2>/dev/null
  true
}
cd "${TESTBED:-/testbed}" || exit 3
gitq() { git -c gc.auto=0 "$@"; }
drop_refs() {
  git for-each-ref --format='%(refname)' | while read -r r; do git update-ref -d "$r"; done
  rm -rf .git/packed-refs .git/FETCH_HEAD .git/ORIG_HEAD .git/refs/remotes .git/logs/refs/remotes
  git reflog expire --expire=now --all
}
shallow() {
  h=$(git rev-parse HEAD)
  t=$(git describe --tags --abbrev=0 2>/dev/null)
  drop_refs
  # keep `git describe` (versioneer, setuptools_scm) resolving: nearest tag re-created at HEAD
  [ -n "$t" ] && git tag "$t" "$h"
  git rev-parse HEAD >/dev/null || { log "HEAD lost"; exit 4; }
  echo "$h" > .git/shallow
  git -c gc.auto=0 gc --prune=now -q
}
case $v in
  a) do_a ;;
  a_gc) do_a; git reflog expire --expire=now --all; git gc --prune=now -q ;;
  a_shallow) do_a; shallow ;;
  a_shallow_loose|c) do_a; shallow
     mkdir -p /tmp/unpack; mv .git/objects/pack/pack-*.pack /tmp/unpack/ && rm -f .git/objects/pack/pack-*
     for p in /tmp/unpack/*.pack; do git unpack-objects -q < "$p"; done; rm -rf /tmp/unpack
     git config gc.auto 0 ;;
  a_placeholder) do_a
     git ls-files -z > /tmp/tracked
     t=$(git describe --tags --abbrev=0 2>/dev/null)
     rm -rf .git
     git init -q && git add --pathspec-from-file=/tmp/tracked --pathspec-file-nul -f
     GIT_AUTHOR_DATE="2000-01-01T00:00:00+0000" GIT_COMMITTER_DATE="2000-01-01T00:00:00+0000" git -c user.name=openswe -c user.email=openswe@localhost commit -q -m "OpenSWE task base" --no-verify || { log "placeholder commit failed"; exit 7; }
     [ -n "$t" ] && git tag "$t" HEAD
     git gc --prune=now -q; rm -f /tmp/tracked ;;
  a_sharedpack) do_a
     [ -d /mirror ] || { log "no /mirror"; exit 5; }
     rm -rf .git/objects/pack/* ; find .git/objects -mindepth 1 -maxdepth 1 -name '??' -exec rm -rf {} +
     cp /mirror/objects/pack/pack-*.pack /mirror/objects/pack/pack-*.idx .git/objects/pack/
     git reflog expire --expire=now --all
     git cat-file -e 'HEAD^{tree}' || { log "HEAD not in shared pack"; exit 6; } ;;
  *) log "unknown variant"; exit 2 ;;
esac
if [ "$v" = c ]; then
  # 1) .pyc of the task delta recompiled as checked-hash (deterministic, validated against the source on import)
  if [ -s /pyc.list ]; then
    for py in /opt/conda/envs/testbed/bin/python /opt/conda/bin/python; do
      [ -x $py ] || continue
      $py - <<'PY'
import os, sys, py_compile, importlib.util
tag = sys.implementation.cache_tag
ok = bad = 0
mode = getattr(py_compile, "PycInvalidationMode", None)
for line in open("/pyc.list"):
    pyc = line.rstrip("\n")
    if not pyc.endswith(".pyc") or ("." + tag + ".") not in os.path.basename(pyc) or not os.path.exists(pyc):
        continue
    d, b = os.path.split(pyc)
    src = os.path.join(os.path.dirname(d), b.split(".")[0] + ".py")
    opt = b.split(".")[2] if b.count(".") == 3 else ""
    if not os.path.exists(src):
        continue
    try:
        if mode is None:
            os.unlink(pyc)   # Python < 3.7: no hash-based pycs; regenerated on import
        else:
            py_compile.compile(src, cfile=pyc, doraise=True, invalidation_mode=mode.CHECKED_HASH,
                               optimize={"": 0, "opt-1": 1, "opt-2": 2}.get(opt, 0))
        ok += 1
    except Exception:
        bad += 1
print("[slim c] pyc", tag, "recompiled", ok, "failed", bad)
PY
    done
  fi
  # 2) debug info stripped from extension modules built in the source tree (mtime preserved)
  find /testbed -path /testbed/.git -prune -o -type f -name '*.so' -print0 | xargs -0 -r strip -p --strip-debug 2>/dev/null
  # 3) setuptools object files: only in build/temp.*; never inside a meson/ninja build directory
  for d in /testbed/build/temp.*; do [ -d "$d" ] && [ ! -e "$d/build.ninja" ] && find "$d" -name '*.o' -delete; done
fi
sync
log done
