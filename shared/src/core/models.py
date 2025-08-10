# src/core/models.py
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List

# Generic models, can be expanded or specified further

class HealthCheck(BaseModel):
    status: str = "healthy"
    timestamp: float

class ErrorDetail(BaseModel):
    field: Optional[str] = None
    message: str

class ErrorResponse(BaseModel):
    success: bool = False
    message: str
    error_code: Optional[int] = None
    details: Optional[List[ErrorDetail]] = None

# Placeholder for Salesforce-specific data models if needed for strong typing
# For truly dynamic objects, data will often be Dict[str, Any]
# but specific operations might benefit from models.

class SalesforceRecord(BaseModel):
    Id: Optional[str]
    # Other common fields can be added if most objects share them,
    # or use __root__ = Dict[str, Any] for completely dynamic fields
    # For now, assume data is largely handled as Dict[str, Any] in operations

    class Config:
        extra = "allow" # Allow arbitrary fields for SObject data

class BulkApiResultDetail(BaseModel):
    success: bool
    created: bool
    id: Optional[str] = None
    errors: Optional[List[Dict[str, Any]]] = None # Salesforce error structure

class BulkJobInfo(BaseModel):
    id: str
    operation: str
    object: str
    state: str # e.g., Open, UploadComplete, InProgress, JobComplete, Failed, Aborted
    errorMessage: Optional[str] = None
    numberRecordsProcessed: Optional[int] = None
    numberRecordsFailed: Optional[int] = None
    # Add other relevant fields from Bulk API 2.0 JobInfo resource
    # https://developer.salesforce.com/docs/atlas.en-us.api_bulk_v2.meta/api_bulk_v2/job_info.htm

class SystemMetrics(BaseModel):
    cpu_usage_percent: float
    memory_usage_percent: float
    disk_usage_percent: Optional[float] = None # May not always be relevant/available
    system_load_avg: Optional[Tuple[float, float, float]] = None # For Unix-like systems
    # Add other metrics as needed from psutil or platform
    operating_system: Optional[str] = None
    python_version: Optional[str] = None
    app_uptime_seconds: Optional[float] = None
