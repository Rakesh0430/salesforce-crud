# Dockerfile

# Use an official Python runtime as a parent image
FROM python:3.11-slim as builder

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set work directory
WORKDIR /app

# Install system dependencies that might be needed (e.g., for psycopg2, lxml, etc.)
# Add here if your requirements.txt needs them. Example:
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     build-essential libpq-dev libxml2-dev libxslt1-dev \
#     && rm -rf /var/lib/apt/lists/*

# Install poetry (or pip if you prefer direct pip install)
# Using Poetry for dependency management example
# RUN pip install poetry
# COPY poetry.lock pyproject.toml /app/
# RUN poetry config virtualenvs.create false && poetry install --no-dev --no-interaction --no-ansi

# Using pip (more common for simpler Dockerfiles)
COPY requirements.txt /app/
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY ./src /app/src
COPY .env.example /app/.env.example
# If you have other top-level files needed at runtime, copy them too.
# e.g. COPY alembic.ini /app/

# --- Final Stage ---
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
# ENV APP_MODULE "src.app.main:app" # Example, can be set in docker-compose or K8s

WORKDIR /app

# Create a non-root user and group
RUN addgroup --system app && adduser --system --group app

# Copy installed packages from builder stage
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application code from builder stage
COPY --from=builder /app/src /app/src
COPY --from=builder /app/.env.example /app/.env.example
# Ensure .env is NOT copied if it contains secrets. It should be mounted or managed by orchestrator.

# Create data directories and set permissions (if using local paths for I/O, map to K8s volumes)
# These paths should align with core.config.py settings
RUN mkdir -p /app/data/input /app/data/output /app/data/failed && \
    chown -R app:app /app/data
ENV DATA_PATH_INPUT="/app/data/input"
ENV DATA_PATH_OUTPUT="/app/data/output"
ENV DATA_PATH_FAILED="/app/data/failed"


# Switch to non-root user
USER app

# Expose the port the app runs on
EXPOSE 8000

# Command to run the application
# This can be overridden by docker-compose or Kubernetes manifests
# Ensure your main.py is executable or called via python -m
# Using uvicorn directly:
CMD ["uvicorn", "src.app.main:app", "--host", "0.0.0.0", "--port", "8000"]

# If using Gunicorn with Uvicorn workers (common for production):
# CMD ["gunicorn", "-k", "uvicorn.workers.UvicornWorker", "-c", "gunicorn_conf.py", "src.app.main:app"]
# You would need a gunicorn_conf.py file. Example:
# # gunicorn_conf.py
# bind = "0.0.0.0:8000"
# workers = 4 # Or calculate based on CPU cores: (2 * cpu_count()) + 1
# worker_class = "uvicorn.workers.UvicornWorker"
# loglevel = "info"
# accesslog = "-" # Log to stdout
# errorlog = "-"  # Log to stderr
