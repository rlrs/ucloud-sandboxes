# The verifiers harness toolkit (docs/toolkit-layers.md): uv, a managed Python and
# the prebuilt uv script environment of each harness program in programs/ (copied
# from the pinned verifiers checkout by build.sh), at the path every sandbox sees.
FROM ubuntu:22.04 AS build
ARG UV_VERSION
ARG UV_SHA256
ARG PYTHON_VERSION=3.12
ARG TOOLKIT=vf-harness
ARG VERIFIERS_COMMIT
ENV T=/opt/ucloud/toolkits/${TOOLKIT}
RUN apt-get update -qq && apt-get install -y -qq curl ca-certificates >/dev/null
RUN mkdir -p $T/bin && cd /tmp \
 && curl -LsSfO https://github.com/astral-sh/uv/releases/download/${UV_VERSION}/uv-x86_64-unknown-linux-musl.tar.gz \
 && echo "${UV_SHA256}  uv-x86_64-unknown-linux-musl.tar.gz" | sha256sum -c - \
 && tar xzf uv-x86_64-unknown-linux-musl.tar.gz && cp uv-x86_64-unknown-linux-musl/uv $T/bin/uv
# As the harness will run it (its env carries these): only the toolkit's Python.
ENV PATH=$T/bin:$PATH UV_CACHE_DIR=$T/uv-cache UV_PYTHON_INSTALL_DIR=$T/python UV_PYTHON_PREFERENCE=only-managed
COPY programs/ /tmp/programs/
RUN uv python install ${PYTHON_VERSION} && mkdir -p /tmp/vf-scripts \
 && for program in /tmp/programs/*.py; do \
      digest=$(sha256sum "$program" | cut -d' ' -f1); cp "$program" /tmp/vf-scripts/$digest.py; \
      uv sync --script /tmp/vf-scripts/$digest.py -q --no-config; \
      echo "$digest $(basename "$program") $(uv python find --script /tmp/vf-scripts/$digest.py --no-config)"; \
    done > $T/programs.txt \
 && printf '{"name": "%s", "verifiers_commit": "%s", "uv": "%s", "python": "%s", "libc": "glibc", "programs": "programs.txt"}\n' \
      "${TOOLKIT}" "${VERIFIERS_COMMIT}" "${UV_VERSION}" "$(uv python find ${PYTHON_VERSION})" > $T/toolkit.json \
 && cat $T/programs.txt && du -sh $T

# Only the toolkit's own tree: publish-environment's allow path keeps exactly it.
FROM scratch
ARG TOOLKIT=vf-harness
COPY --from=build /opt/ucloud/toolkits/${TOOLKIT} /opt/ucloud/toolkits/${TOOLKIT}
