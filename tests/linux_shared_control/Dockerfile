FROM ghcr.io/astral-sh/uv:bookworm-slim
ENV UV_PYTHON_INSTALL_DIR=/opt/python
RUN uv python install 3.13
WORKDIR /deps
COPY pyproject.toml uv.lock ./
RUN uv sync --python 3.13 --locked --extra postgres --no-install-project
COPY README.md ./
COPY ucloud_sandboxes ./ucloud_sandboxes
COPY scripts/verify_installed_wheel.py /verify_installed_wheel.py
RUN uv build --wheel --out-dir /wheels \
    && uv venv --python 3.13 /wheel-check \
    && uv pip install --python /wheel-check/bin/python /wheels/*.whl \
    && cd /tmp && /wheel-check/bin/python /verify_installed_wheel.py
ENV PATH="/deps/.venv/bin:$PATH" PYTHONPATH=/src
WORKDIR /src
