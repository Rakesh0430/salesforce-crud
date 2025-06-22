# src/app/main.py
from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import time
import logging

from src.app.routers import sfdc as sfdc_router
from src.core.config import settings
from src.utils.logger import setup_logging

# Setup logging
setup_logging()
logger = logging.getLogger(settings.APP_NAME)

app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="Production-grade API for dynamic Salesforce object integration and CRUD operations.",
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
app.include_router(sfdc_router.router, prefix=settings.API_V1_STR, tags=["Salesforce Operations"])

@app.get("/health", tags=["Health"])
async def health_check():
    # Basic health check, can be expanded with psutil if needed for quick status
    # For more detailed metrics, see /metrics endpoint
    return {"status": "healthy", "application": settings.APP_NAME, "version": settings.APP_VERSION, "timestamp": datetime.utcnow().isoformat()}

# Detailed Metrics Endpoint (inspired by salesforce_custom_object_metric.py)
import psutil
import platform
from datetime import datetime # ensure datetime is imported if not already

@app.get("/metrics", tags=["Metrics"], summary="Get detailed system and application metrics")
async def get_detailed_metrics():
    """
    Returns detailed system metrics including CPU, memory, disk, network,
    and basic application information.
    """
    # Application uptime (example, would need to store start time)
    # For now, placeholder or could be calculated if app start time is stored globally
    # global app_start_time # Needs to be set at app startup
    # uptime_seconds = time.time() - app_start_time if 'app_start_time' in globals() else None

    try:
        disk_usage = psutil.disk_usage('/')
        disk_metrics = {
            "total": disk_usage.total,
            "used": disk_usage.used,
            "free": disk_usage.free,
            "percent": disk_usage.percent,
        }
    except Exception as e: # Handle potential errors like permission denied or path not found
        logger.warning(f"Could not retrieve disk usage metrics: {e}")
        disk_metrics = None

    try:
        cpu_freq = psutil.cpu_freq()
        cpu_freq_metrics = {
            "current": cpu_freq.current,
            "min": cpu_freq.min,
            "max": cpu_freq.max,
        }
    except Exception: # Some systems might not support this or raise errors
        cpu_freq_metrics = None


    metrics = {
        "application_name": settings.APP_NAME,
        "application_version": settings.APP_VERSION,
        "timestamp": datetime.utcnow().isoformat(),
        "cpu_logical_count": psutil.cpu_count(logical=True),
        "cpu_physical_count": psutil.cpu_count(logical=False),
        "cpu_usage_percent": psutil.cpu_percent(interval=0.1), # Short interval for responsiveness
        "cpu_load_average": psutil.getloadavg() if hasattr(psutil, "getloadavg") else None, # (1, 5, 15 min avg)
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
        "disk_usage_root": disk_metrics,
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
        # "application_uptime_seconds": uptime_seconds,
    }
    return metrics


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=settings.DEBUG_MODE)
