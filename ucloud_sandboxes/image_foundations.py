"""Factor explicit, task-independent image prefixes into immutable foundations."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import posixpath
import re
import shlex


_BASE_COPY = "COPY base_install.sh /tmp/base_install.sh\n"
_BASE_RUN = "RUN bash /tmp/base_install.sh && rm /tmp/base_install.sh\n"
_PINNED = re.compile(r"[^\s@]+@sha256:[0-9a-f]{64}\Z")


def require_pinned_reference(reference: str) -> str:
    if not _PINNED.fullmatch(reference):
        raise ValueError("foundation references must be immutable manifest digests")
    return reference


@dataclass(frozen=True)
class ImageFoundation:
    dockerfile: str
    installer: bytes
    source_prefix: str
    family: str = "tmax"

    @property
    def key(self) -> str:
        inputs = {"schema": 1, "platform": "linux/amd64", "dockerfile": self.dockerfile}
        if self.family in {"tmax", "tmax-inline"}:
            inputs["base_install_sha256"] = hashlib.sha256(self.installer).hexdigest()
        return hashlib.sha256(json.dumps(inputs, sort_keys=True).encode()).hexdigest()

    @property
    def image_id(self) -> str:
        return "foundation-" + self.family + "-" + self.key[:32]

    def task_dockerfile(self, original: str, reference: str) -> str:
        require_pinned_reference(reference)
        offset = self.prefix_offset(original)
        if offset is None:
            raise ValueError("task does not have the qualified foundation prefix")
        return original[:offset] + "FROM " + reference + "\n" + original[offset + len(self.source_prefix):]

    def prefix_offset(self, original: str) -> int | None:
        offset = 0
        for line in original.splitlines(keepends=True):
            if re.match(r"\s*#\s*(syntax|escape|check)\s*=", line, re.I):
                return None
            if line.strip() and not line.lstrip().startswith("#"):
                break
            offset += len(line)
        return offset if original.startswith(self.source_prefix, offset) else None


def tmax_foundation(dockerfile: str, installer: bytes, *, ubuntu_base: str) -> ImageFoundation:
    """Accept only the explicit upstream base stage; never reorder shell commands.

    The remaining task instructions stay byte-for-byte intact. The caller must
    retain the resulting image and use its digest when replacing this prefix.
    """
    require_pinned_reference(ubuntu_base)
    marker = _BASE_COPY + _BASE_RUN
    if dockerfile.count(marker) != 1:
        raise ValueError("expected one explicit TMax base installer stage")
    before, _ = dockerfile.split(marker, 1)
    lines = before.splitlines()
    if not lines or lines[0] != "FROM ubuntu:22.04":
        raise ValueError("unsupported TMax source base")
    if any(line.strip() and not line.startswith("ENV ") for line in lines[1:]):
        raise ValueError("task operations before the base installer cannot be factored")
    if not installer:
        raise ValueError("base installer is empty")
    prefix = before + marker
    pinned = prefix.replace("FROM ubuntu:22.04\n", "FROM " + ubuntu_base + "\n", 1)
    return ImageFoundation(pinned, installer, prefix)


def openswe_foundation(python_version: str, *, miniconda_base: str) -> ImageFoundation:
    """The exact common prefix emitted by the pinned OpenSWE recipe generator."""
    require_pinned_reference(miniconda_base)
    if not re.fullmatch(r"(?:2|3)\.[0-9]{1,2}", python_version):
        raise ValueError("expected a Python major.minor version")
    prefix = (
        "FROM continuumio/miniconda3:25.3.1-1\n"
        "RUN apt-get update && apt-get install -y --no-install-recommends git patch bash ca-certificates ripgrep build-essential\n"
        f"RUN conda create --override-channels -c conda-forge -n testbed python={python_version} -y\n"
        "RUN conda config --system --set auto_activate_base false\n"
        "ENV PATH=/opt/conda/envs/testbed/bin:/opt/conda/bin:$PATH\n"
    )
    pinned = prefix.replace("FROM continuumio/miniconda3:25.3.1-1\n", "FROM " + miniconda_base + "\n", 1)
    return ImageFoundation(pinned, b"", prefix, "openswe")


def terminal_foundation(dockerfile: str, *, source_base: str, resolved_base: dict) -> ImageFoundation:
    """Freeze a context-free initial stage, without moving task instructions.

    Base ONBUILD triggers must have been inspected from the pinned config: an
    inherited COPY could otherwise silently consume the foundation's context.
    Comments outside heredocs are ignored for identity, but task bytes survive.
    """
    base = require_pinned_reference(resolved_base["reference"])
    if resolved_base.get("onbuild") != []:
        raise ValueError("base ONBUILD configuration is unknown or nonempty")
    if re.search(r"^\s*#\s*(syntax|escape|check)\s*=", dockerfile, re.I | re.M):
        raise ValueError("custom Dockerfile frontend is unsupported")
    if len(re.findall(r"^\s*FROM\b", dockerfile, re.I | re.M)) != 1:
        raise ValueError("only a single unnamed stage can be factored")
    # The pinned Terminal-Lego tooling copies task_file and its verifier script.
    # A broad COPY could include the rewritten Dockerfile itself in task state.
    for copied in re.finditer(r"^\s*(?:COPY|ADD)\s+(.+)$", dockerfile.replace("\\\n", " "), re.I | re.M):
        fields = shlex.split(copied[1])
        options = []
        while fields and fields[0].startswith("--"):
            options.append(fields.pop(0))
        if any(option.startswith("--from=") for option in options):
            continue
        if (len(fields) < 2 or any(posixpath.normpath(source).split("/", 1)[0]
                                  not in {"task_file", "verifier-bootstrap.sh"} for source in fields[:-1])):
            raise ValueError("unsupported terminal context copy")
    instructions = []
    pending = ""
    cursor = 0
    start = None
    end = 0
    runs = 0
    for line in dockerfile.splitlines(keepends=True):
        before = cursor
        cursor += len(line)
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if start is None:
            start = before
        pending += line
        if line.rstrip().endswith("\\"):
            continue
        match = re.match(r"\s*([A-Za-z]+)\s+", pending)
        if match is None:
            break
        kind = match[1].upper()
        if not instructions:
            if not re.fullmatch(r"FROM[ \t]+" + re.escape(source_base) + r"\s*", pending, re.I):
                raise ValueError("source base must be literal and unnamed")
            instructions.append("FROM " + base + "\n")
        elif kind not in {"RUN", "ENV", "WORKDIR", "USER", "SHELL"}:
            break
        elif kind == "RUN" and (re.match(r"\s*RUN\s+--", pending, re.I) or "<<" in pending):
            break
        else:
            instructions.append(pending.rstrip("\r\n") + "\n")
            runs += kind == "RUN"
        end = cursor
        pending = ""
    if not runs or start is None:
        raise ValueError("no supported dependency RUN prefix")
    return ImageFoundation("".join(instructions), b"", dockerfile[start:end], "terminal-prefix")


def tmax_inline_foundation(dockerfile: str, script: bytes, *, ubuntu_base: str) -> tuple[ImageFoundation, bytes]:
    """Factor only literal leading apt/pip statements from monolithic installers.

    No shell parsing guesses: substitutions, continuations, redirects, arbitrary
    options, local requirements, variables, and context operations stop matching.
    Shell options remain in the task script. Unrecognized statements retain their
    order and bytes, and scripts observing prior command status are rejected.
    """
    require_pinned_reference(ubuntu_base)
    marker = "COPY post_install.sh /tmp/post_install.sh\nRUN bash /tmp/post_install.sh && rm /tmp/post_install.sh\n"
    if dockerfile.count(marker) != 1:
        raise ValueError("expected one explicit monolithic TMax installer")
    before, _ = dockerfile.split(marker, 1)
    lines = before.splitlines()
    if not lines or lines[0] != "FROM ubuntu:22.04" or any(
        line.strip() and not line.startswith("ENV ") for line in lines[1:]
    ):
        raise ValueError("task operations precede the installer")
    text = script.decode("utf-8")
    if "$?" in text or "PIPESTATUS" in text:
        raise ValueError("installer observes previous command status")
    # Literal package names/version pins only; no pip -r/-e/local source inputs.
    package = r"[A-Za-z0-9][A-Za-z0-9_.+=!~,-]*"
    apt = rf"(?:apt-get update && )?apt-get install -y(?: --no-install-recommends)?(?: {package})+"
    pip = rf"(?:pip3|python3 -m pip) install(?: {package})+"
    retained = []
    shell_setup = []
    commands = []
    offset = 0
    for line in text.splitlines(keepends=True):
        stripped = line.strip()
        if stripped in {"set -e", "set -eu", "set -euo pipefail"}:
            if commands:
                break
            shell_setup.append(stripped + "\n")
            retained.append(line)
        elif not stripped or stripped.startswith("#"):
            retained.append(line)
        elif stripped == "apt-get update" or re.fullmatch(apt, stripped) or re.fullmatch(pip, stripped):
            commands.append(stripped + "\n")
        else:
            break
        offset += len(line)
    if not commands or not any("apt-get install " in line for line in commands):
        raise ValueError("no supported initial dependency installation")
    # Keep shell setup and comments in both scripts. Only provisioning commands
    # are removed from the task remainder; there is no command reordering.
    preamble = "".join(retained)
    installer = "".join(shell_setup + commands).encode()
    remaining = (preamble + text[offset:]).encode()
    pinned = before.replace("FROM ubuntu:22.04\n", "FROM " + ubuntu_base + "\n", 1)
    foundation = ImageFoundation(pinned + _BASE_COPY + _BASE_RUN, installer, before, "tmax-inline")
    return foundation, remaining
