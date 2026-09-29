# Dependency-heavy image build fixtures

`scripts/build_load_fixtures.py` generates three repeatable build contexts using
public packages and generated application source. It performs no image builds.
The Python/Node choices extend the existing SDK load benchmark's Python agent
and TypeScript toolchain workloads; `requests==2.32.4`, `pydantic==2.11.7` and
`typescript@5.8.3` reuse that benchmark's pins.

| Recipe | Work performed during the build | Default application modules |
| --- | --- | ---: |
| `python-agent` | Install the scientific/data/API/Jupyter/test stack, compile a small C feature extension and Python bytecode, validate Arrow/Parquet round trips and a CPU random forest | 1,500 |
| `typescript-tools` | Install the locked npm development toolchain, lint, type-check and emit declarations, bundle with esbuild and webpack, run Jest | 2,000 |
| `typescript-multistage` | Compile and test the same TypeScript application in an intermediate stage; copy only the bundled runtime and dependency evidence to the final image | 2,000 |

These represent dependency and filesystem work rather than real customer
repositories. Each generated module validates and transforms a task record, and
the build checks that every module contributes to the final result. There are
no large random padding files, GPU packages, browser downloads or private
dependencies. The full Debian language bases plus real dependencies target
roughly 1–3 GiB unpacked; measure actual image sizes. A slim runtime override
will make the multistage final image smaller while leaving its build work intact.

The multistage case is useful for observing `mode=min`: dependency and compiler
layers in the intermediate stage are absent from the final image, so replacement
builders may need to repeat that work after a source edit. Report the observed
cache behavior rather than assuming all stages will be cached remotely.

## Generate and lock before measurement

The callable API writes to a new or empty directory:

```python
from pathlib import Path
from scripts.build_load_fixtures import generate_context, lock_node_dependencies

base = generate_context(
    Path("/tmp/fixtures/node-base"), "typescript-tools", "base",
    base_ref="node:22-bookworm@sha256:<resolved-digest>",
)
base = lock_node_dependencies(base["context_path"])
edited = generate_context(
    Path("/tmp/fixtures/node-edit"), "typescript-tools", "app-change-7",
    base_ref=base["base_ref"],
    node_lock=Path(base["context_path"]) / "package-lock.json",
)
```

`--resolve-node-lock` provides the equivalent CLI workflow. Lock generation
contacts the public npm registry for metadata with `--package-lock-only` and
`--ignore-scripts`; it installs no packages and must happen before measured
build phases. Reuse the base lock for all application variants and both Node
recipes. Resolve a separate lock for `dependency-change`. Node Dockerfiles fail
if the lock is absent, instead of silently resolving dependencies during a
measured build.

Python direct dependencies are exact pins. For a frozen transitive graph,
resolve `requirements.in` for Python 3.12/Linux amd64, then pass the resulting
file through `python_lock=` or `--python-lock`. Resolve one base lock and one
dependency-change lock, and reuse the base lock for every application edit.
The generated image retains pip's installation report and resolved versions;
without the optional lock, transitive resolution may change between cold runs.
The manifest identifies which mode was used.

Use `--base-ref` and, for multistage, `--runtime-base-ref` to supply resolved
image digests. Defaults are moving language-version tags and the manifest
explicitly reports whether bases are digest-pinned. The Python build requires a
base with a C compiler, such as the default full Bookworm Python image.

## Variants and evidence

`base` has application revision zero. `app-change` means revision one, and
`app-change-N` supports revisions 1 through 999999. An application edit changes
only the late-copied revision source file; the Dockerfile, dependency inputs and
all other source files stay byte-identical. The output score changes, so sandbox
validation can detect accidentally running the previous application's image.

`dependency-change` keeps the application at revision zero and changes one
direct dependency: Python `rich` 14.0.0 to 14.1.0, or Node `zod` 3.24.4 to
3.25.76. The full lock may also change dependent versions when resolved; retain
both locks and compare them when attributing performance differences.

Every context includes `fixture.json` with the recipe, variant, base references,
dependency change, context digest, source count and expected smoke result. This
metadata is excluded by `.dockerignore`, so evidence does not invalidate build
layers. `context_sha256` excludes the metadata itself and measures the generated
input files; it is not an OCI image digest.

Run the manifest's `smoke_command` inside a sandbox created from the published
image and compare its JSON fields with `smoke_expected_json`. Python validates
data processing/native code/model fitting, and Node executes every generated
transform. `dependency_evidence_paths` locates the resolved package evidence in
the final image, including the multistage image.

The generated workload limits Python numerical libraries to two threads, Jest
to one worker and Node's old-space heap to 2 GiB. The generator accepts 1–5,000
source modules and rejects nonempty output directories. These limits do not
replace fleet CPU, memory, disk, registry or workload-concurrency controls.
Measure cold, warm, source-edit, dependency-edit and replacement-builder cases
separately; a repeated submitted image name/context may be deduplicated before
Docker runs, so the runner must distinguish accepted builds from reuse.
