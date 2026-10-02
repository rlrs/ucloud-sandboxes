#!/usr/bin/env python3
"""VM driver: build OpenSWE task images FROM registry foundations, derive slimming variants, walk trees, verify.

Builds use the Docker legacy builder (as in S10) with --network host; every FROM is a digest-pinned
10.42.0.2:5000 foundation (read-only pulls). Nothing is pushed anywhere.
"""
import json, os, re, subprocess, sys, threading, time
from concurrent.futures import ThreadPoolExecutor

W = "/data/w"
PY = "python3"
TASKS = {t["task"]: t for t in json.load(open(f"{W}/tasks.json"))}
ORDER = json.load(open(f"{W}/order.json"))
FOUND = json.load(open(f"{W}/foundations.json"))
VARIANTS = ["a", "a_gc", "a_shallow", "a_shallow_loose", "a_placeholder", "a_sharedpack", "c"]
EVAL_VARIANTS = ["base", "a_shallow_loose", "a_placeholder", "c"]
MIRRORS = {"pandas-dev__pandas": "https://github.com/pandas-dev/pandas.git",
           "getmoto__moto": "https://github.com/getmoto/moto.git",
           "scikit-learn__scikit-learn": "https://github.com/scikit-learn/scikit-learn.git"}
lock = threading.Lock()
logf = open(f"{W}/log/pipeline.jsonl", "a")


def log(**r):
    r["t"] = round(time.time(), 1)
    with lock:
        logf.write(json.dumps(r) + "\n"); logf.flush()
        print(json.dumps(r)[:400], flush=True)


def sh(cmd, timeout=None, **kw):
    t = time.time()
    try:
        p = subprocess.run(cmd, shell=isinstance(cmd, str), capture_output=True, text=True, timeout=timeout, **kw)
        return p.returncode, p.stdout, p.stderr, round(time.time() - t, 1)
    except subprocess.TimeoutExpired as e:
        return 124, (e.stdout or b"").decode(errors="replace") if isinstance(e.stdout, bytes) else (e.stdout or ""), "timeout", round(time.time() - t, 1)


def tag(task):
    return "ow/" + task.replace("openswe--", "").lower().replace("__", ".")


def foundations():
    os.makedirs(f"{W}/found", exist_ok=True)
    def one(k):
        out = f"{W}/found/{k}.jsonl.gz"
        if os.path.exists(out):
            return
        ref = FOUND[k]
        rc, o, e, s = sh(["docker", "pull", ref])
        log(step="pull_foundation", key=k, rc=rc, s=s, err=e[-300:] if rc else "")
        sh(["docker", "rm", "-f", f"f-{k}"])
        sh(["docker", "create", "--name", f"f-{k}", ref, "true"])
        rc, o, e, s = sh(f"set -o pipefail; docker export f-{k} | {PY} {W}/scripts/walk.py --full --out {out}.tmp && mv {out}.tmp {out}")
        log(step="walk_foundation", key=k, rc=rc, s=s, out=o.strip()[-300:], err=e[-300:])
        sh(["docker", "rm", "-f", f"f-{k}"])
    with ThreadPoolExecutor(6) as ex:
        list(ex.map(one, FOUND))


def mirrors():
    os.makedirs(f"{W}/mirror", exist_ok=True)
    for p, url in MIRRORS.items():
        d = f"{W}/mirror/{p}.git"
        if os.path.exists(d + "/ok"):
            continue
        rc, o, e, s = sh(["git", "clone", "-q", "--bare", url, d])
        if rc == 0:
            rc2, *_ = sh(["git", "-C", d, "repack", "-a", "-d", "-q"])
            open(d + "/ok", "w").write("")
        size = sh(f"du -sb {d}/objects/pack | cut -f1")[1].strip()
        log(step="mirror", project=p, rc=rc, s=s, pack_bytes=size, err=e[-300:])


def build(task):
    t = TASKS[task]
    d = f"{W}/ctx/{task}"
    os.makedirs(d, exist_ok=True)
    open(f"{d}/Dockerfile", "w").write(t["dockerfile"])
    for attempt in (1, 2):
        rc, o, e, s = sh(["docker", "build", "--network", "host", "-t", tag(task) + ":base", d],
                         env=dict(os.environ, DOCKER_BUILDKIT="0"), timeout=3600)
        out = (o + e)
        retry = rc != 0 and attempt == 1 and re.search(r"(RPC failed|early EOF|Could not resolve|unable to access|"
                                                         r"Connection reset|timed out|fetch-pack|TLS)", out[-4000:])
        log(step="build", task=task, attempt=attempt, rc=rc, s=s, tail="" if rc == 0 else out[-1500:])
        open(f"{W}/log/build-{task}.log", "w").write(out[-200000:])
        if rc == 0:
            return True, s
        if not retry:
            return False, s
    return False, s


def walk_container(name, task, v):
    fk = next(o["foundation_key"] for o in ORDER if o["task"] == task)
    out = f"{W}/trees/{task}/{v}.jsonl.gz"
    rc, o, e, s = sh(f"set -o pipefail; docker export {name} | {PY} {W}/scripts/walk.py --base {W}/found/{fk}.jsonl.gz --out {out}.tmp && mv {out}.tmp {out}")
    log(step="walk", task=task, variant=v, rc=rc, s=s, out=o.strip()[-300:], err=e[-300:])


def project(task):
    return re.match(r"openswe--(.+)-\d+$", task).group(1)


def variants(task):
    img = tag(task)
    os.makedirs(f"{W}/trees/{task}", exist_ok=True)
    base_out = f"{W}/trees/{task}/base.jsonl.gz"
    if not os.path.exists(base_out):
        sh(["docker", "rm", "-f", f"{task}-base"])
        sh(["docker", "create", "--name", f"{task}-base", img + ":base", "true"])
        walk_container(f"{task}-base", task, "base")
        sh(["docker", "rm", "-f", f"{task}-base"])
    rc, o, e, s = sh(f"docker run --rm --entrypoint git {img}:base -C /testbed ls-files -z > {W}/trees/{task}/tracked.txt")
    sh(f"{PY} -c \"import gzip,json; [print(r[0]) for r in map(json.loads, gzip.open('{base_out}','rt')) "
       f"if isinstance(r,list) and r[0].endswith('.pyc')]\" > {W}/trees/{task}/pyc.list")
    for v in VARIANTS:
        if v == "a_sharedpack" and project(task) not in MIRRORS:
            continue
        out = f"{W}/trees/{task}/{v}.jsonl.gz"
        have_img = sh(["docker", "image", "inspect", f"{img}:{v}"])[0] == 0
        if os.path.exists(out) and have_img:
            continue
        name = f"{task}-{v}"
        sh(["docker", "rm", "-f", name])
        mounts = ["-v", f"{W}/scripts:/slim:ro", "-v", f"{W}/trees/{task}/pyc.list:/pyc.list:ro"]
        if v == "a_sharedpack":
            mounts += ["-v", f"{W}/mirror/{project(task)}.git:/mirror:ro"]
        rc, o, e, s = sh(["docker", "run", "--name", name, "--network", "none", "--entrypoint", "bash", *mounts,
                          img + ":base", "/slim/slim.sh", v], timeout=3600)
        log(step="slim", task=task, variant=v, rc=rc, s=s, out=(o + e)[-600:])
        if rc != 0:
            sh(["docker", "rm", "-f", name]); continue
        if not os.path.exists(out):
            walk_container(name, task, v)
        rc, o, e, s = sh(["docker", "commit", name, f"{img}:{v}"])
        if rc:
            log(step="commit", task=task, variant=v, rc=rc, out=e[-300:])
        sh(["docker", "rm", "-f", name])


def verify(task):
    img = tag(task)
    os.makedirs(f"{W}/verify/{task}", exist_ok=True)
    e_in = f"{W}/evalin/{task}"
    for v in ["base"] + VARIANTS:
        out = f"{W}/verify/{task}/{v}.txt"
        if os.path.exists(out) or not sh(["docker", "image", "inspect", f"{img}:{v}"])[0] == 0:
            continue
        rc, o, e, s = sh(["docker", "run", "--rm", "--network", "none", "--entrypoint", "bash", "-v", f"{W}/scripts:/slim:ro",
                          "-v", f"{e_in}:/e:ro", f"{img}:{v}", "/slim/verify.sh"], timeout=1800)
        open(out, "w").write(o + f"\nverify_rc={rc}\nverify_s={s}\n" + "stderr_tail=" + e[-500:].replace("\n", " ") + "\n")
        log(step="verify", task=task, variant=v, rc=rc, s=s)


def evaluate(task, mode=""):
    img = tag(task)
    os.makedirs(f"{W}/eval/{task}", exist_ok=True)
    for v in EVAL_VARIANTS:
        label = v + (".gold" if mode else "")
        out = f"{W}/eval/{task}/{label}.txt"
        if os.path.exists(out) or not sh(["docker", "image", "inspect", f"{img}:{v}"])[0] == 0:
            continue
        rc, o, e, s = sh(["docker", "run", "--rm", "--network", "host", "--cpus", "4", "--entrypoint", "bash",
                          "-v", f"{W}/scripts:/slim:ro", "-v", f"{W}/evalin/{task}:/e:ro", "-v", f"{W}/eval/{task}:/out",
                          f"{img}:{v}", "/slim/run_eval.sh", label, mode], timeout=1800)
        open(out, "w").write(o + f"\nrun_rc={rc}\nrun_s={s}\n")
        log(step="eval" + mode, task=task, variant=v, rc=rc, s=s, out=o.strip()[-200:])


def task_pipeline(task):
    ok = sh(["docker", "image", "inspect", tag(task) + ":base"])[0] == 0
    if not ok:
        ok, s = build(task)
        if not ok:
            return
    variants(task)
    verify(task)
    if "noeval" not in sys.argv:
        evaluate(task)
        evaluate(task, "gold")


if __name__ == "__main__":
    os.makedirs(f"{W}/log", exist_ok=True)
    stage = sys.argv[1]
    if stage == "found":
        foundations(); mirrors()
    elif stage == "gold":
        names = [o["task"] for o in ORDER if sh(["docker", "image", "inspect", tag(o["task"]) + ":base"])[0] == 0]
        with ThreadPoolExecutor(int(sys.argv[2])) as ex:
            list(ex.map(lambda t: (variants(t), verify(t), evaluate(t), evaluate(t, "gold")), names))
        log(step="gold_done")
    elif stage == "tasks":
        names = [a for a in sys.argv[3:] if a.startswith("openswe--")] or [o["task"] for o in ORDER]
        heavy = lambda n: (0 if "pandas" in n or "scikit" in n or "astropy" in n else 1)
        names.sort(key=heavy)
        with ThreadPoolExecutor(int(sys.argv[2])) as ex:
            list(ex.map(task_pipeline, names))
        log(step="tasks_done")
