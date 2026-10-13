# ===========================================================================
# Distributed Job Platform — Multi-Stage Production Dockerfile
# Phase 2 — Real Multi-Process Cluster and Durable Persistence
# ===========================================================================

FROM python:3.12-slim

# Prevent generation of .pyc files and buffer stdout/stderr for immediate logging
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install minimal OS dependencies for healthchecks (curl) and PostgreSQL connectivity
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application directories and assets
COPY common/ ./common/
COPY scheduler/ ./scheduler/
COPY worker/ ./worker/
COPY tools/ ./tools/
COPY docs/ ./docs/
COPY prometheus.yml ./prometheus.yml

# Create non-root system user for secure container execution
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Expose HTTP port for FastAPI Scheduler and Observability Dashboard
EXPOSE 8000

# Default command starts the Scheduler service
CMD ["python", "-m", "uvicorn", "scheduler.main:app", "--host", "0.0.0.0", "--port", "8000"]
