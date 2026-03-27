ARG PYTHON_VER=3.12

# ---------------------------------------------------------------------------
# Stage 1 – install Python dependencies into a virtual environment
# ---------------------------------------------------------------------------
FROM python:${PYTHON_VER}-slim AS builder

WORKDIR /app

# Install build-time OS packages needed by some Python deps (e.g. ldap3, cryptography)
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libldap2-dev \
        libsasl2-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --upgrade pip \
    && /opt/venv/bin/pip install --no-cache-dir -r requirements.txt

# ---------------------------------------------------------------------------
# Stage 2 – lean runtime image
# ---------------------------------------------------------------------------
FROM python:${PYTHON_VER}-slim

# Runtime LDAP libraries required by ldap3
RUN apt-get update && apt-get install -y --no-install-recommends \
        libldap-2.5-0 \
        libsasl2-2 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the virtual environment from the builder stage
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy application source
COPY collector/ collector/
COPY lib/       lib/
COPY mappings/  mappings/
COPY regex/     regex/
COPY main.py    .

# Run as a non-root user
RUN useradd -r -u 1001 -g root appuser
USER appuser

ENTRYPOINT ["python", "main.py"]
CMD []
