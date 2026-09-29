#!/usr/bin/env python3
"""Generate deterministic, dependency-heavy build contexts without building images.

Direct dependencies are pinned. Resolve each Node dependency variant's lock once
before measuring builds; reuse that lock for baseline and application changes.
Python records its resolved dependency graph in the image, and accepts an
optional fully pinned requirements lock. See docs/benchmarks/build-load-fixtures.md.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
from typing import Any


RECIPES = ("python-agent", "typescript-tools", "typescript-multistage")
DEFAULT_SOURCE_FILES = {"python-agent": 1500, "typescript-tools": 2000, "typescript-multistage": 2000}
PYTHON_REQUIREMENTS = {
    "requests": "2.32.4",  # Existing scripts/live_load_benchmark.py pin.
    "pydantic": "2.11.7",  # Existing scripts/live_load_benchmark.py pin.
    "numpy": "2.2.6",
    "scipy": "1.15.3",
    "pandas": "2.2.3",
    "pyarrow": "20.0.0",
    "scikit-learn": "1.6.1",
    "matplotlib": "3.10.3",
    "opencv-python-headless": "4.11.0.86",
    "pillow": "11.2.1",
    "sympy": "1.14.0",
    "jupyterlab": "4.4.3",
    "ipython": "9.2.0",
    "pytest": "8.3.5",
    "rich": "14.0.0",
    "httpx": "0.28.1",
    "fastapi": "0.115.12",
    "uvicorn": "0.34.2",
    "setuptools": "80.9.0",
    "wheel": "0.45.1",
}
NODE_DEPENDENCIES = {"lodash": "4.17.21", "zod": "3.24.4"}
NODE_DEV_DEPENDENCIES = {
    "typescript": "5.8.3",  # Existing scripts/live_load_benchmark.py pin.
    "@types/node": "22.15.29",
    "@types/lodash": "4.17.17",
    "esbuild": "0.25.5",
    "vite": "6.3.5",
    "webpack": "5.99.9",
    "webpack-cli": "6.0.1",
    "ts-loader": "9.5.2",
    "jest": "29.7.0",
    "eslint": "9.28.0",
    "typescript-eslint": "8.33.0",
    "prettier": "3.5.3",
}
SOURCE_EPOCH = 1750000000


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, indent=2) + "\n"


def _write(root: Path, name: str, value: str) -> None:
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def _variant(variant: str) -> tuple[int, bool]:
    if variant == "base":
        return 0, False
    if variant == "dependency-change":
        return 0, True
    if variant == "app-change":
        return 1, False
    match = re.fullmatch(r"app-change-([1-9][0-9]{0,5})", variant)
    if match:
        return int(match[1]), False
    raise ValueError("variant must be base, dependency-change, app-change, or app-change-N (1..999999)")


def _base(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9./_:@-]*", value):
        raise ValueError("base references must be plain OCI image references without whitespace")
    return value


def _score(count: int, revision: int) -> int:
    return sum((index % 101) * (index % 17 + 1) + index % 13 + revision for index in range(count))


def _python_context(root: Path, *, base: str, count: int, revision: int, dependency_change: bool, python_lock: Path | None) -> dict[str, Any]:
    dependencies = dict(PYTHON_REQUIREMENTS)
    if dependency_change:
        dependencies["rich"] = "14.1.0"
    requirements = "".join(f"{name}=={version}\n" for name, version in sorted(dependencies.items()))
    _write(root, "requirements.in", requirements)
    if python_lock is not None:
        requirements = python_lock.read_text()
        for name, version in dependencies.items():
            if not re.search(rf"(?mi)^{re.escape(name)}=={re.escape(version)}(?:\s|$)", requirements):
                raise ValueError(f"Python lock must preserve the fixture direct pin {name}=={version}")
    _write(root, "requirements.txt", requirements)
    _write(root, "Dockerfile", f'''FROM {base}
ENV PYTHONUNBUFFERED=1 PYTHONHASHSEED=0 SOURCE_DATE_EPOCH={SOURCE_EPOCH} \\
    OPENBLAS_NUM_THREADS=2 OMP_NUM_THREADS=2 MKL_NUM_THREADS=2 \\
    PIP_DISABLE_PIP_VERSION_CHECK=1 PYTHONPATH=/opt/fixture/src
WORKDIR /opt/fixture
COPY requirements.txt ./requirements.txt
RUN python -m pip install --no-cache-dir --only-binary=:all: --report /opt/fixture/pip-install-report.json -r requirements.txt \\
 && python -m pip check \\
 && python -m pip freeze --all > /opt/fixture/resolved-requirements.txt
COPY native ./native
RUN python -m pip install --no-cache-dir --no-build-isolation --no-deps ./native
COPY src ./src
RUN python -m compileall -q -j 4 --invalidation-mode checked-hash src \\
 && python -m agent.smoke > /opt/fixture/build-smoke.json
CMD ["python", "-m", "agent.smoke"]
''')
    _write(root, "native/setup.py", '''from setuptools import Extension, setup
setup(name="fixture-faststats", version="1.0.0", ext_modules=[Extension("fixture_faststats", ["faststats.c"])])
''')
    _write(root, "native/faststats.c", r'''#define PY_SSIZE_T_CLEAN
#include <Python.h>
static PyObject *sum_squares(PyObject *self, PyObject *input) {
    PyObject *items = PySequence_Fast(input, "expected numeric sequence");
    if (!items) return NULL;
    double total = 0.0;
    for (Py_ssize_t i = 0; i < PySequence_Fast_GET_SIZE(items); i++) {
        double value = PyFloat_AsDouble(PySequence_Fast_GET_ITEM(items, i));
        if (PyErr_Occurred()) { Py_DECREF(items); return NULL; }
        total += value * value;
    }
    Py_DECREF(items);
    return PyFloat_FromDouble(total);
}
static PyMethodDef methods[] = {{"sum_squares", sum_squares, METH_O, "Compute a feature norm."}, {NULL, NULL, 0, NULL}};
static struct PyModuleDef module = {PyModuleDef_HEAD_INIT, "fixture_faststats", NULL, -1, methods};
PyMODINIT_FUNC PyInit_fixture_faststats(void) { return PyModule_Create(&module); }
''')
    _write(root, "src/agent/__init__.py", '"""Generated data-processing agent workload."""\n')
    _write(root, "src/agent/steps/__init__.py", "")
    _write(root, "src/agent/revision.py", f"REVISION = {revision}\n")
    for index in range(count):
        _write(root, f"src/agent/steps/step_{index:05d}.py", f'''"""Feature transform {index}: validate and score one task record."""
from dataclasses import dataclass
from agent.revision import REVISION

@dataclass(frozen=True)
class Task:
    task_id: str
    value: int
    group: str

def transform(task: Task) -> dict[str, object]:
    if not task.task_id or task.value < 0:
        raise ValueError("invalid task")
    return {{"id": task.task_id, "group": task.group.strip().lower(),
            "score": task.value * {index % 17 + 1} + {index % 13} + REVISION}}

def sample() -> dict[str, object]:
    return transform(Task("task-{index}", {index % 101}, " Group-{index % 11} "))
''')
    _write(root, "src/agent/smoke.py", f'''"""Validate generated modules, native code, data frames and CPU model fitting."""
import importlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from pydantic import BaseModel
from scipy.linalg import norm
from sklearn.ensemble import RandomForestRegressor
from fixture_faststats import sum_squares
from agent.revision import REVISION

class Summary(BaseModel):
    source_modules: int
    revision: int
    score: int
    grouped_rows: int

rows = [importlib.import_module(f"agent.steps.step_{{index:05d}}").sample() for index in range({count})]
frame = pd.DataFrame(rows)
grouped = frame.groupby("group")["score"].agg(["sum", "mean", "count"])
with TemporaryDirectory() as directory:
    path = Path(directory) / "tasks.parquet"
    frame.to_parquet(path, engine="pyarrow", compression="zstd")
    restored = pq.read_table(path).to_pandas()
    pd.testing.assert_frame_equal(frame, restored)
x = np.arange(8192, dtype=np.float64).reshape(1024, 8) / 8192
y = x[:, 0] * 2 + x[:, 3]
model = RandomForestRegressor(n_estimators=12, max_depth=6, random_state=7, n_jobs=2).fit(x, y)
assert model.score(x, y) > 0.98
assert abs(sum_squares([1., 2., 3.]) - float(norm([1., 2., 3.]) ** 2)) < 1e-9
summary = Summary(source_modules={count}, revision=REVISION, score=int(frame.score.sum()), grouped_rows=len(grouped))
assert summary.score == sum((i % 101) * (i % 17 + 1) + i % 13 + REVISION for i in range({count}))
print(json.dumps(summary.model_dump(), sort_keys=True))
''')
    return {
        "direct_dependencies": dependencies,
        "dependency_change": {"package": "rich", "from": "14.0.0", "to": "14.1.0"} if dependency_change else None,
        "dependency_resolution": "provided-requirements-lock" if python_lock else "exact-direct-pins; transitive-resolution-recorded-in-image",
        "dependency_evidence_paths": ["/opt/fixture/pip-install-report.json", "/opt/fixture/resolved-requirements.txt"],
        "smoke_command": ["python", "-m", "agent.smoke"],
        "smoke_expected_json": {"source_modules": count, "revision": revision, "score": _score(count, revision), "grouped_rows": min(count, 11)},
    }


def _node_context(root: Path, *, recipe: str, base: str, runtime_base: str, count: int, revision: int, dependency_change: bool, node_lock: Path | None) -> dict[str, Any]:
    dependencies = dict(NODE_DEPENDENCIES)
    if dependency_change:
        dependencies["zod"] = "3.25.76"
    package = {
        "name": "ucloud-build-load-fixture", "version": "1.0.0", "private": True,
        "dependencies": dependencies, "devDependencies": NODE_DEV_DEPENDENCIES,
        "scripts": {
            "build": "tsc -p tsconfig.json && node build.cjs && webpack --config webpack.config.cjs",
            "test": "jest --runInBand",
            "lint": "eslint src",
        },
    }
    _write(root, "package.json", _json(package))
    if node_lock is not None:
        lock = json.loads(node_lock.read_text())
        _validate_node_lock(package, lock)
        _write(root, "package-lock.json", _json(lock))
    _write(root, "tsconfig.json", _json({
        "compilerOptions": {"target": "ES2022", "module": "CommonJS", "moduleResolution": "Node", "rootDir": "src", "outDir": "dist/tsc", "strict": True, "esModuleInterop": True, "declaration": True, "sourceMap": True, "skipLibCheck": True},
        "include": ["src/**/*.ts"],
    }))
    _write(root, "src/revision.ts", f"export const revision = {revision};\n")
    _write(root, "src/types.ts", '''export interface Task { id: string; value: number; group: string; }
export interface Result { id: string; score: number; group: string; }
''')
    for index in range(count):
        _write(root, f"src/steps/step_{index:05d}.ts", f'''import type {{ Task, Result }} from '../types';
import {{ revision }} from '../revision';
/** Feature transform {index}: validate and score one work item. */
export function transform(task: Task): Result {{
  if (!task.id || task.value < 0) throw new Error('invalid task');
  return {{ id: task.id, score: task.value * {index % 17 + 1} + {index % 13} + revision,
    group: task.group.trim().toLowerCase() }};
}}
''')
    imports = "".join(f"import {{ transform as step{index} }} from './steps/step_{index:05d}';\n" for index in range(count))
    transforms = ", ".join(f"step{index}" for index in range(count))
    _write(root, "src/index.ts", imports + f'''import {{ z }} from 'zod';
import _ from 'lodash';
import {{ revision }} from './revision';
const transforms = [{transforms}];
const schema = z.object({{ id: z.string(), score: z.number().int(), group: z.string() }});
export function summarize() {{
  const rows = transforms.map((transform, i) => schema.parse(transform({{ id: `task-${{i}}`, value: i % 101, group: ` Group-${{i % 11}} ` }})));
  return {{ source_modules: transforms.length, revision,
    score: _.sumBy(rows, 'score'), grouped_rows: Object.keys(_.groupBy(rows, 'group')).length }};
}}
if (require.main === module) console.log(JSON.stringify(summarize()));
''')
    _write(root, "build.cjs", '''const fs = require('node:fs');
const esbuild = require('esbuild');
esbuild.buildSync({entryPoints: ['src/index.ts'], bundle: true, platform: 'node', target: 'node22', format: 'cjs', outfile: 'dist/bundle.cjs', sourcemap: true, metafile: true});
fs.copyFileSync('package-lock.json', 'dist/resolved-package-lock.json');
''')
    _write(root, "webpack.config.cjs", '''const path = require('node:path');
module.exports = {mode: 'production', target: 'node22', entry: './src/index.ts',
  module: {rules: [{test: /\\.tsx?$/, use: 'ts-loader', exclude: /node_modules/}]},
  resolve: {extensions: ['.ts', '.js']}, optimization: {minimize: false},
  output: {filename: 'webpack.cjs', path: path.resolve(__dirname, 'dist/webpack'), library: {type: 'commonjs2'}}};
''')
    _write(root, "eslint.config.cjs", '''const tseslint = require('typescript-eslint');
module.exports = [{files: ['src/**/*.ts'], languageOptions: {parser: tseslint.parser},
  rules: {'no-unreachable': 'error', 'no-constant-condition': 'error', 'no-duplicate-imports': 'error'}}];
''')
    _write(root, "tests/pipeline.test.cjs", f'''const {{summarize}} = require('../dist/tsc/index.js');
test('every generated transform contributes to validated pipeline output', () => {{
  const summary = summarize();
  expect(summary.source_modules).toBe({count});
  expect(summary.score).toBe(Array.from({{length: {count}}}, (_, i) => (i % 101) * (i % 17 + 1) + i % 13 + summary.revision).reduce((a, b) => a + b, 0));
  expect(summary.grouped_rows).toBe({min(count, 11)});
}});
''')
    _write(root, "jest.config.cjs", "module.exports = {testMatch: ['**/tests/*.test.cjs'], testEnvironment: 'node'};\n")
    build_stage = f'''FROM {base} AS compile
ENV CI=1 SOURCE_DATE_EPOCH={SOURCE_EPOCH} NODE_OPTIONS=--max-old-space-size=2048
WORKDIR /opt/fixture
COPY package.json package-lock.json ./
RUN npm ci --no-audit --no-fund --cache /tmp/npm-cache \\
 && npm ls --all --json > /opt/fixture/npm-resolved.json \\
 && rm -rf /tmp/npm-cache
COPY tsconfig.json build.cjs webpack.config.cjs eslint.config.cjs jest.config.cjs ./
COPY tests ./tests
COPY src ./src
RUN npm run lint && npm run build && npm test \\
 && node dist/bundle.cjs > /opt/fixture/build-smoke.json
'''
    if recipe == "typescript-multistage":
        dockerfile = build_stage + f'''
FROM {runtime_base}
WORKDIR /opt/fixture
COPY --from=compile /opt/fixture/dist/bundle.cjs ./bundle.cjs
COPY --from=compile /opt/fixture/package-lock.json ./resolved-package-lock.json
COPY --from=compile /opt/fixture/npm-resolved.json ./npm-resolved.json
COPY --from=compile /opt/fixture/build-smoke.json ./build-smoke.json
CMD ["node", "/opt/fixture/bundle.cjs"]
'''
        command = ["node", "/opt/fixture/bundle.cjs"]
    else:
        dockerfile = build_stage + 'CMD ["node", "/opt/fixture/dist/bundle.cjs"]\n'
        command = ["node", "/opt/fixture/dist/bundle.cjs"]
    _write(root, "Dockerfile", dockerfile)
    return {
        "direct_dependencies": dependencies, "direct_dev_dependencies": NODE_DEV_DEPENDENCIES,
        "dependency_change": {"package": "zod", "from": "3.24.4", "to": "3.25.76"} if dependency_change else None,
        "dependency_resolution": "npm-lock-provided" if node_lock else "npm-lock-required-before-build",
        "dependency_evidence_paths": ["/opt/fixture/npm-resolved.json", "/opt/fixture/resolved-package-lock.json" if recipe == "typescript-multistage" else "/opt/fixture/package-lock.json"],
        "smoke_command": command,
        "smoke_expected_json": {"source_modules": count, "revision": revision, "score": _score(count, revision), "grouped_rows": min(count, 11)},
        "intermediate_install_in_final_image": recipe != "typescript-multistage",
    }


def _validate_node_lock(package: dict[str, Any], lock: dict[str, Any]) -> None:
    if lock.get("lockfileVersion") not in {2, 3}:
        raise ValueError("Node fixtures require an npm v2/v3 package lock")
    root = lock.get("packages", {}).get("") or {}
    for field in ("dependencies", "devDependencies"):
        if root.get(field) != package[field]:
            raise ValueError(f"Node lock {field} differ from this fixture variant")


def context_inventory(root: Path) -> dict[str, Any]:
    files = []
    total = 0
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root).as_posix()
        if not path.is_file() or relative == "fixture.json" or any(part in {"node_modules", ".git", "__pycache__"} for part in path.parts):
            continue
        payload = path.read_bytes()
        digest.update(relative.encode() + b"\0" + hashlib.sha256(payload).digest())
        files.append(relative)
        total += len(payload)
    return {"context_sha256": digest.hexdigest(), "context_files": len(files), "context_bytes": total}


def generate_context(
    root: Path | str,
    recipe: str,
    variant: str,
    *,
    base_ref: str | None = None,
    runtime_base_ref: str | None = None,
    source_files: int | None = None,
    node_lock: Path | str | None = None,
    python_lock: Path | str | None = None,
) -> dict[str, Any]:
    """Write a fresh context at root; never build, install dependencies or contact a registry."""
    if recipe not in RECIPES:
        raise ValueError(f"unknown recipe: {recipe}")
    revision, dependency_change = _variant(variant)
    count = source_files if source_files is not None else DEFAULT_SOURCE_FILES[recipe]
    if type(count) is not int or not 1 <= count <= 5000:
        raise ValueError("source_files must be between 1 and 5000")
    base = _base(base_ref or ("python:3.12-bookworm" if recipe == "python-agent" else "node:22-bookworm"))
    runtime_base = _base(runtime_base_ref or base)
    target = Path(root)
    if target.exists() and (not target.is_dir() or any(target.iterdir())):
        raise ValueError("fixture output must be a new or empty directory")
    target.mkdir(parents=True, exist_ok=True)
    _write(target, ".dockerignore", "fixture.json\n.git\nnode_modules\n__pycache__\n*.pyc\n")
    if recipe == "python-agent":
        details = _python_context(target, base=base, count=count, revision=revision, dependency_change=dependency_change, python_lock=Path(python_lock) if python_lock else None)
    else:
        details = _node_context(target, recipe=recipe, base=base, runtime_base=runtime_base, count=count, revision=revision, dependency_change=dependency_change, node_lock=Path(node_lock) if node_lock else None)
    manifest = {
        "schema_version": 1, "recipe": recipe, "variant": variant, "revision": revision,
        "base_ref": base, "runtime_base_ref": runtime_base if recipe == "typescript-multistage" else None,
        "bases_digest_pinned": "@sha256:" in base and (recipe != "typescript-multistage" or "@sha256:" in runtime_base),
        "source_files": count, "expected_unpacked_image_gib": [1, 3],
        "image_size_is_estimate": True, "generated_large_padding_files": False,
        "context_path": str(target.resolve()), "smoke_output_format": "one JSON object; compare expected fields",
        "local_build_parallelism": {"python_blas_threads": 2, "typescript_heap_mb": 2048, "jest_workers": 1},
        **details, **context_inventory(target),
    }
    _write(target, "fixture.json", _json(manifest))
    return manifest


def lock_node_dependencies(root: Path | str, *, npm: str = "npm", timeout_seconds: int = 600) -> dict[str, Any]:
    """Resolve metadata once before a benchmark; no package scripts or installation.

    This is intentionally separate from generate_context so cold/warm build
    phases never silently resolve different transitive dependency versions.
    """
    target = Path(root).resolve()
    manifest = json.loads((target / "fixture.json").read_text())
    if manifest["recipe"] not in {"typescript-tools", "typescript-multistage"}:
        raise ValueError("npm lock generation is only for Node fixtures")
    executable = shutil.which(npm)
    if not executable:
        raise ValueError(f"npm executable not found: {npm}")
    with tempfile.TemporaryDirectory(prefix="ucloud-fixture-npm-") as cache:
        environment = dict(os.environ)
        environment.update({"npm_config_cache": cache, "npm_config_userconfig": os.devnull})
        subprocess.run([executable, "install", "--package-lock-only", "--ignore-scripts", "--no-audit", "--no-fund", "--registry=https://registry.npmjs.org/"], cwd=target, env=environment, check=True, timeout=timeout_seconds, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    package = json.loads((target / "package.json").read_text())
    lock = json.loads((target / "package-lock.json").read_text())
    _validate_node_lock(package, lock)
    # Canonical serialization makes copying this lock through generate_context
    # byte-identical to its original baseline context.
    _write(target, "package-lock.json", _json(lock))
    manifest.update(context_inventory(target))
    manifest["dependency_resolution"] = "npm-lock-resolved-before-build"
    _write(target, "fixture.json", _json(manifest))
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path, help="New or empty context directory")
    parser.add_argument("--recipe", required=True, choices=RECIPES)
    parser.add_argument("--variant", default="base")
    parser.add_argument("--base-ref", help="Prefer an explicit image@sha256:digest")
    parser.add_argument("--runtime-base-ref", help="Final Node base for the multistage recipe")
    parser.add_argument("--source-files", type=int)
    parser.add_argument("--node-lock", type=Path, help="Reuse the same dependency variant's prepared package-lock.json")
    parser.add_argument("--python-lock", type=Path, help="Optional resolved Python requirements preserving direct pins")
    parser.add_argument("--resolve-node-lock", action="store_true", help="Resolve public npm metadata before measuring builds; executes no package scripts")
    args = parser.parse_args()
    if args.resolve_node_lock and (args.node_lock or args.recipe == "python-agent"):
        parser.error("--resolve-node-lock requires a Node recipe without --node-lock")
    result = generate_context(args.output, args.recipe, args.variant, base_ref=args.base_ref, runtime_base_ref=args.runtime_base_ref, source_files=args.source_files, node_lock=args.node_lock, python_lock=args.python_lock)
    if args.resolve_node_lock:
        result = lock_node_dependencies(args.output)
    print(_json(result), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
