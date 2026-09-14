FROM registry.access.redhat.com/ubi9/python-311:9.6

WORKDIR /opt/app-root/src
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN pip install --no-cache-dir uv==0.6.12 \
    && uv sync --frozen --no-dev

USER 1001
ENV PATH="/opt/app-root/src/.venv/bin:${PATH}"
ENTRYPOINT ["lineage-demo"]
