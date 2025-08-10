# record-service/src/app/main.py
from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import time
import logging
from datetime import datetime

from app.routers import records as records_router
from core.config import settings
from utils.logger import setup_logging

# Setup logging
setup_logging()
logger = logging.getLogger(settings.APP_NAME)

app = FastAPI(
    title="Record Service API",
    version=settings.APP_VERSION,
    description="Microservice for single-record CRUD operations with Salesforce.",
    openapi_url=f"{settings.API_V1_STR}/openapi.json"
)

# CORS Middleware
if settings.BACKEND_CORS_ORIGINS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[str(origin) for origin in settings.BACKEND_CORS_ORIGINS],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

# Middleware to add process time header
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.perf_counter()
    response = await call_next(request)
    process_time = time.perf_counter() - start_time
    response.headers["X-Process-Time"] = f"{process_time:.4f}"
    logger.info(
        f"Request: {request.method} {request.url.path} - Status: {response.status_code} - Process Time: {process_time:.4f}s"
    )
    return response

# Exception handlers
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    logger.error(f"Validation error: {exc.errors()} for request: {request.method} {request.url.path}")
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={"detail": exc.errors(), "body": exc.body},
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {exc} for request: {request.method} {request.url.path}", exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "An unexpected error occurred. Please try again later."},
    )

# Include routers
app.include_router(records_router.router, prefix=settings.API_V1_STR, tags=["Record Operations"])

@app.get("/health", tags=["Health"])
async def health_check():
    return {"status": "healthy", "application": "RecordServiceAPI", "version": settings.APP_VERSION, "timestamp": datetime.utcnow().isoformat()}

# Detailed Metrics Endpoint
import psutil
import platform

@app.get("/metrics", tags=["Metrics"], summary="Get detailed system and application metrics")
async def get_detailed_metrics():
    try:
        disk_usage = psutil.disk_usage('/')
        disk_metrics = {
            "total": disk_usage.total,
            "used": disk_usage.used,
            "free": disk_usage.free,
            "percent": disk_usage.percent,
        }
    except Exception as e:
        logger.warning(f"Could not retrieve disk usage metrics: {e}")
        disk_metrics = None

    try:
        cpu_freq = psutil.cpu_freq()
        cpu_freq_metrics = {
            "current": cpu_freq.current,
            "min": cpu_freq.min,
            "max": cpu_freq.max,
        }
    except Exception:
        cpu_freq_metrics = None

    metrics = {
        "application_name": "RecordServiceAPI",
        "application_version": settings.APP_VERSION,
        "timestamp": datetime.utcnow().isoformat(),
        "cpu_logical_count": psutil.cpu_count(logical=True),
        "cpu_physical_count": psutil.cpu_count(logical=False),
        "cpu_usage_percent": psutil.cpu_percent(interval=0.1),
        "cpu_load_average": psutil.getloadavg() if hasattr(psutil, "getloadavg") else None,
        "cpu_frequency": cpu_freq_metrics,
        "memory_virtual": {
            "total": psutil.virtual_memory().total,
            "available": psutil.virtual_memory().available,
            "percent": psutil.virtual_memory().percent,
            "used": psutil.virtual_memory().used,
            "free": psutil.virtual_memory().free,
        },
        "memory_swap": {
            "total": psutil.swap_memory().total,
            "used": psutil.swap_memory().used,
            "free": psutil.swap_memory().free,
            "percent": psutil.swap_memory().percent,
        } if hasattr(psutil, "swap_memory") else None,
        "disk_.env.example": disk_metrics,
        "network_io_counters": {
            "bytes_sent": psutil.net_io_counters().bytes_sent,
            "bytes_recv": psutil.net_io_counters().bytes_recv,
            "packets_sent": psutil.net_io_counters().packets_sent,
            "packets_recv": psutil.net_io_counters().packets_recv,
            "errin": psutil.net_io_counters().errin,
            "errout": psutil.net_io_counters().errout,
            "dropin": psutil.net_io_counters().dropin,
            "dropout": psutil.net_io_counters().dropout,
        } if hasattr(psutil, "net_io_counters") else None,
        "system_boot_time": datetime.utcfromtimestamp(psutil.boot_time()).isoformat() if hasattr(psutil, "boot_time") else None,
        "operating_system": platform.platform(),
        "python_version": platform.python_version(),
    }
    return metrics

if __name__ == "__main__":
    import uvicorn
    # This is for local development. For production, use a Gunicorn server.
    # The PYTHONPATH needs to be configured to find the 'shared' directory.
    # Example: PYTHONPATH=./shared/src uvicorn record-service.src.app.main:app --reload
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=settings.DEBUG_MODE)
