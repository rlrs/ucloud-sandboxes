#!/usr/bin/env python3
"""Per-task harness inputs from the GAIR/OpenSWE openswe_oss rows: eval.sh, test patch, collect targets."""
import json, os, re, sys
sel = json.load(open(sys.argv[1])); out = sys.argv[2]
MOD = {"pandas-dev/pandas": "pandas", "scikit-learn/scikit-learn": "sklearn", "astropy/astropy": "astropy",
       "getmoto/moto": "moto", "OasisLMF/OasisLMF": "oasislmf", "androguard/androguard": "androguard",
       "mps-gmbh/hl7-parser": "hl7parser", "napari/napari-tiff": "napari_tiff", "newam/idasen": "idasen",
       "aiidateam/reentry": "reentry", "fake-useragent/fake-useragent": "fake_useragent", "rustedpy/result": "result",
       "duck-dynasty/duckbot": "", "conan-io/conan": "conans", "pydata/xarray": "xarray"}
HD = re.compile(r"<<-?\s*'?(EOF[_A-Za-z0-9]*|TESTEOF)'?\n.*?\n\1\n", re.S)
PATH = re.compile(r"(?<![\w/.-])((?:/testbed/)?[A-Za-z_][\w./-]*?(?:\.py|/)(?:::[\w\[\]\-.:]+)?)(?=[\s\"'\\]|$)")
for name, j in sel.items():
    d = f"{out}/{name}"; os.makedirs(d, exist_ok=True)
    open(f"{d}/eval.sh", "w").write(j["eval_script"])
    open(f"{d}/test.patch", "w").write(j["test_patch"] or "")
    open(f"{d}/gold.patch", "w").write(j["patch"] or "")
    open(f"{d}/base_commit", "w").write(j["base_commit"])
    open(f"{d}/module", "w").write(MOD.get(j["repo"], ""))
    body = HD.sub("\n", j["eval_script"])
    targets = []
    for line in re.sub(r"\\\n", " ", body).splitlines():
        if not re.search(r"\bpytest\b", line) or "--co" in line.split() or "--collect-only" in line:
            continue
        for m in PATH.findall(line):
            m = m.replace("/testbed/", "")
            if m.startswith(("/", "tmp")) or m in targets or "/dev/null" in m:
                continue
            targets.append(m)
    if not targets:   # unittest/poetry scripts: test files touched by the test patch
        targets = [f for f in re.findall(r"^\+\+\+ b/(\S+)", j["test_patch"] or "", re.M) if f.endswith(".py") and "test" in f]
    open(f"{d}/collect.txt", "w").write(" ".join(targets))
    print(name, "|", " ".join(targets)[:200])
