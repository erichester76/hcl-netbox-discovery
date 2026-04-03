ARG PYTHON_VER=3.12
ARG PYNETBOX_WRAPPER_REPO=https://github.com/erichester76/pynetbox-wrapper.git
ARG PYNETBOX_WRAPPER_REF=main

# ---------------------------------------------------------------------------
# Stage 1 – install Python dependencies via Poetry into a virtual environment
# ---------------------------------------------------------------------------
FROM python:${PYTHON_VER}-slim AS builder

# Proxy build args – pass with --build-arg HTTP_PROXY=... if needed
ARG HTTP_PROXY
ARG HTTPS_PROXY
ARG NO_PROXY
ARG PYNETBOX_WRAPPER_REPO
ARG PYNETBOX_WRAPPER_REF

WORKDIR /app

# Install build-time OS packages needed by some Python deps (e.g. ldap3, cryptography)
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        git \
        libldap2-dev \
        libsasl2-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN pip install --no-cache-dir poetry

# Configure Poetry: place the venv inside the project dir, no prompts
ENV POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_NO_INTERACTION=1

# Dependency source of truth: pyproject.toml (and poetry.lock when present)
COPY pyproject.toml poetry.lock* ./

RUN poetry install --only main --no-root

RUN git clone --depth 1 --branch "${PYNETBOX_WRAPPER_REF}" "${PYNETBOX_WRAPPER_REPO}" /tmp/pynetbox-wrapper

# ---------------------------------------------------------------------------
# Stage 2 – lean runtime image
# ---------------------------------------------------------------------------
FROM python:${PYTHON_VER}-slim

# Proxy build args – pass with --build-arg HTTP_PROXY=... if needed
ARG HTTP_PROXY
ARG HTTPS_PROXY
ARG NO_PROXY
ARG PYNETBOX_WRAPPER_REPO
ARG PYNETBOX_WRAPPER_REF

# Runtime LDAP libraries required by ldap3
RUN apt-get update && apt-get install -y --no-install-recommends \
        libldap2 \
        libsasl2-2 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the virtual environment from the builder stage
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /tmp/pynetbox-wrapper/pynetbox2.py /app/lib/pynetbox2.py
ENV PATH="/app/.venv/bin:$PATH"

# Copy application source
COPY collector/ collector/
COPY mappings/  mappings/
COPY regex/     regex/
COPY web/       web/
COPY main.py    .
COPY web_server.py .

# Run as a non-root user
RUN useradd -r -u 1001 -g root appuser
# Ensure the data directory for the job DB is writable
RUN mkdir -p /app/data && chown 1001 /app/data
USER appuser

ENTRYPOINT ["python", "main.py"]
CMD ["--run-scheduler"]
