FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir uv==0.11.7

COPY pyproject.toml uv.lock README.md ./
COPY src ./src

RUN uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:$PATH"

CMD ["feat-stream", "--help"]
