FROM python:3.13-slim

WORKDIR /app

ENV UV_PROJECT_ENVIRONMENT="/usr/local/"
ENV UV_COMPILE_BYTECODE=1

COPY pyproject.toml .
COPY uv.lock .
RUN pip install --no-cache-dir uv
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/* \
	&& uv sync --frozen --no-install-project --no-dev

COPY src/ ./src/
COPY main.py .

EXPOSE 8080
CMD ["python", "main.py"]