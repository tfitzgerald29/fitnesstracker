# ── Build stage ───────────────────────────────────────────────────────────────
FROM python:3.13-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM python:3.13-slim

# Patch all system packages to remove known vulnerabilities
RUN apt-get update && apt-get upgrade -y && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application code
COPY . .

# Default port
ENV PORT=8080

# Gunicorn: 1 worker is correct for Dash (callbacks share in-process state).
# --timeout 120 gives the startup FIT processing pipeline time to finish.
# In S3 mode the startup pipeline is skipped so startup is fast.
CMD ["sh", "-c", "gunicorn app:server --bind 0.0.0.0:${PORT} --workers 1 --threads 8 --timeout 120"]
