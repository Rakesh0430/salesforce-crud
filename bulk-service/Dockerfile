# bulk-service/Dockerfile
FROM python:3.11-slim as builder

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy source code for both the service and the shared library
COPY ./bulk-service/src /app/bulk-service/src
COPY ./shared/src /app/shared/src
COPY .env.example /app/.env.example

# --- Final Stage ---
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
# Set the PYTHONPATH to include the root of the service and the shared library
ENV PYTHONPATH="/app:/app/shared/src"

WORKDIR /app

# Create a non-root user
RUN addgroup --system app && adduser --system --group app

# Copy installed packages from builder stage
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application code
COPY --from=builder /app/bulk-service/src /app/bulk-service/src
COPY --from=builder /app/shared/src /app/shared/src
COPY --from=builder /app/.env.example /app/.env.example

# Create and set permissions for data directories
RUN mkdir -p /app/data/input /app/data/output /app/data/failed && \
    chown -R app:app /app/data
ENV DATA_PATH_INPUT="/app/data/input"
ENV DATA_PATH_OUTPUT="/app/data/output"
ENV DATA_PATH_FAILED="/app/data/failed"

USER app

EXPOSE 8001

# Command to run the Bulk Service application
CMD ["uvicorn", "bulk-service.src.app.main:app", "--host", "0.0.0.0", "--port", "8001"]
