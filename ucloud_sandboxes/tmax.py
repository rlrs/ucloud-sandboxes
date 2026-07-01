from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import re
import shlex
from typing import Any, Iterable


SECTION_RE = re.compile(r"^%([A-Za-z][A-Za-z0-9_-]*)(?:\s+.*)?$")
SAFE_IMAGE_COMPONENT_RE = re.compile(r"[^a-z0-9_.-]+")


@dataclass(frozen=True)
class TMaxFileMapping:
    source: str
    destination: str


@dataclass(frozen=True)
class TMaxContainerDefinition:
    bootstrap: str
    base_image: str
    sections: dict[str, str]

    @property
    def post(self) -> str:
        return self.sections.get("post", "")

    @property
    def file_mappings(self) -> tuple[TMaxFileMapping, ...]:
        return parse_file_mappings(self.sections.get("files", ""))


@dataclass(frozen=True)
class TMaxBuildContext:
    row_idx: int
    task_id: str
    image_id: str
    tag: str
    context_path: Path
    skipped_reason: str = ""

    @property
    def buildable(self) -> bool:
        return not self.skipped_reason


def parse_container_definition(text: str) -> TMaxContainerDefinition:
    header: dict[str, str] = {}
    sections: dict[str, list[str]] = {}
    current_section: str | None = None
    for raw_line in text.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        stripped = raw_line.strip()
        section_match = SECTION_RE.match(stripped)
        if section_match is not None:
            current_section = section_match.group(1).lower()
            sections.setdefault(current_section, [])
            continue
        if current_section is None:
            if ":" in raw_line:
                key, value = raw_line.split(":", 1)
                header[key.strip().lower()] = value.strip()
        else:
            sections[current_section].append(raw_line)

    bootstrap = header.get("bootstrap", "")
    base_image = header.get("from", "")
    return TMaxContainerDefinition(
        bootstrap=bootstrap,
        base_image=base_image,
        sections={key: "\n".join(value).strip("\n") for key, value in sections.items()},
    )


def parse_file_mappings(files_section: str) -> tuple[TMaxFileMapping, ...]:
    mappings: list[TMaxFileMapping] = []
    for raw_line in files_section.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        parts = shlex.split(line)
        if len(parts) < 2:
            continue
        mappings.append(TMaxFileMapping(source=parts[0], destination=parts[1]))
    return tuple(mappings)


def materialize_tmax_context(
    row: dict[str, Any],
    *,
    row_idx: int,
    output_root: Path,
    registry_prefix: str,
    tag_suffix: str,
    allow_file_mappings: bool = False,
) -> TMaxBuildContext:
    task_id = str(row.get("task_id") or f"row-{row_idx}")
    image_id = image_id_for_task(task_id)
    tag = f"{registry_prefix.rstrip('/')}/{safe_image_component(task_id)}:{tag_suffix}"
    context_path = output_root / safe_image_component(task_id)
    container_def = str(row.get("container_def") or "")
    if not container_def.strip():
        return TMaxBuildContext(
            row_idx=row_idx,
            task_id=task_id,
            image_id=image_id,
            tag=tag,
            context_path=context_path,
            skipped_reason="missing container_def",
        )

    definition = parse_container_definition(container_def)
    skipped_reason = skip_reason(definition, allow_file_mappings=allow_file_mappings)
    if skipped_reason:
        return TMaxBuildContext(
            row_idx=row_idx,
            task_id=task_id,
            image_id=image_id,
            tag=tag,
            context_path=context_path,
            skipped_reason=skipped_reason,
        )

    context_path.mkdir(parents=True, exist_ok=True)
    (context_path / "Dockerfile").write_text(
        render_dockerfile(definition, row=row),
        encoding="utf-8",
    )
    (context_path / "post.sh").write_text(render_post_script(definition), encoding="utf-8")
    (context_path / "test_initial_state.py").write_text(
        str(row.get("test_initial_state") or ""),
        encoding="utf-8",
    )
    (context_path / "tmax_task.json").write_text(
        json.dumps(task_metadata(row, row_idx=row_idx), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return TMaxBuildContext(
        row_idx=row_idx,
        task_id=task_id,
        image_id=image_id,
        tag=tag,
        context_path=context_path,
    )


def skip_reason(
    definition: TMaxContainerDefinition,
    *,
    allow_file_mappings: bool = False,
) -> str:
    if definition.bootstrap.lower() != "docker":
        return f"unsupported bootstrap: {definition.bootstrap or '(missing)'}"
    if not definition.base_image:
        return "missing base image"
    if definition.file_mappings and not allow_file_mappings:
        return "requires external %files fixtures"
    return ""


def render_dockerfile(definition: TMaxContainerDefinition, *, row: dict[str, Any]) -> str:
    labels = {
        "org.opencontainers.image.source": "https://huggingface.co/datasets/allenai/TMax-15K",
        "org.opencontainers.image.title": str(row.get("task_id") or ""),
        "ucloud-sandboxes.tmax.task-id": str(row.get("task_id") or ""),
    }
    lines = [
        f"FROM {definition.base_image}",
        "",
        "SHELL [\"/bin/bash\", \"-o\", \"pipefail\", \"-c\"]",
        "ENV PIP_DEFAULT_TIMEOUT=120",
        "ENV PIP_RETRIES=10",
        "ENV PIP_DISABLE_PIP_VERSION_CHECK=1",
    ]
    for key, value in labels.items():
        if value:
            lines.append(f"LABEL {shlex.quote(key)}={json.dumps(value)}")
    lines.extend(
        [
            "",
            "COPY post.sh /tmp/ucloud-tmax-post.sh",
            "RUN chmod +x /tmp/ucloud-tmax-post.sh && /tmp/ucloud-tmax-post.sh && rm /tmp/ucloud-tmax-post.sh",
            "",
            "RUN mkdir -p /opt/tmax",
            "COPY test_initial_state.py /opt/tmax/test_initial_state.py",
            "COPY tmax_task.json /opt/tmax/task.json",
            "",
            "WORKDIR /home/user",
        ]
    )
    return "\n".join(lines) + "\n"


def render_post_script(definition: TMaxContainerDefinition) -> str:
    body = harden_post_script(definition.post).strip("\n")
    if not body:
        body = "true"
    return "\n".join(
        [
            "#!/usr/bin/env bash",
            "set -eo pipefail",
            "cd /",
            body,
            "",
        ]
    )


def harden_post_script(post: str) -> str:
    lines: list[str] = []
    for line in post.splitlines():
        stripped = line.strip()
        if stripped in {"pip3 install pytest", "python3 -m pip install pytest"}:
            indent = line[: len(line) - len(line.lstrip())]
            lines.append(f"{indent}apt-get install -y python3-pytest")
        else:
            lines.append(line)
    return "\n".join(lines)


def task_metadata(row: dict[str, Any], *, row_idx: int) -> dict[str, Any]:
    keys = (
        "task_id",
        "domain",
        "skill_type",
        "task_complexity",
        "command_complexity",
        "scenario",
        "language",
    )
    payload = {"row_idx": row_idx}
    payload.update({key: row.get(key) for key in keys if key in row})
    return payload


def image_id_for_task(task_id: str) -> str:
    return f"tmax-{safe_image_component(task_id)}"[:64].rstrip(".-_")


def safe_image_component(value: str) -> str:
    lowered = value.strip().lower().replace("/", "-")
    cleaned = SAFE_IMAGE_COMPONENT_RE.sub("-", lowered).strip(".-_")
    return cleaned or "unnamed"


def buildable_contexts(contexts: Iterable[TMaxBuildContext]) -> list[TMaxBuildContext]:
    return [context for context in contexts if context.buildable]
