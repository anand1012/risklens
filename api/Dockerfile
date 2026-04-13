# ── Build / install stage ─────────────────────────────────────────────────────
FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# System deps needed by some Python packages (e.g. grpcio for BigQuery)
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── App stage ─────────────────────────────────────────────────────────────────
FROM base AS app

WORKDIR /app

# Copy only the packages needed at runtime
COPY api/       ./api/
COPY indexing/  ./indexing/

EXPOSE 8000

# Use Gunicorn + Uvicorn workers in production
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
