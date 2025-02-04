import os
import logging
import asyncio
import psutil
import time
from logging.handlers import RotatingFileHandler
from datetime import datetime, date, timedelta
from functools import lru_cache
from typing import Optional, Dict, Any, Tuple

import httpx  # Asynchronous HTTP client
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Depends, status, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel, EmailStr, Field, validator

# Environment Variables

# Load environment variables from a .env file if present
load_dotenv()

# Logging Configuration

LOG_FILENAME = "salesforce_api_io.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Add rotating file handler to manage log size and retention
file_handler = RotatingFileHandler(
    LOG_FILENAME,
    maxBytes=10 * 1024 * 1024,  # 10 MB
    backupCount=5               # Retain 5 backup log files
)
file_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
logger.addHandler(file_handler)

# Settings and Configuration

class Settings:
    """
    Holds Salesforce configuration values such as client IDs, client secrets, usernames, and tokens.
    Loaded from environment variables for enhanced security and flexibility.
    """
    CLIENT_ID: str = os.getenv("SALESFORCE_CLIENT_ID")
    CLIENT_SECRET: str = os.getenv("SALESFORCE_CLIENT_SECRET")
    USERNAME: str = os.getenv("SALESFORCE_USERNAME")
    PASSWORD: str = os.getenv("SALESFORCE_PASSWORD")
    TOKEN_URL: str = os.getenv("SALESFORCE_TOKEN_URL", "https://login.salesforce.com/services/oauth2/token")
    API_VERSION: str = "v57.0"

    # Time (in seconds) to consider token refresh before its actual expiry.
    # This provides a buffer so you don't hit expiry mid-request.
    TOKEN_REFRESH_BUFFER: int = 60

    def validate(self) -> None:
        """
        Ensures that all required environment variables are present.
        Raises a ValueError if any are missing.
        """
        missing_vars = [
            var for var, value in vars(self).items()
            if not value and not var.startswith("_")
        ]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

@lru_cache()
def get_settings() -> Settings:
    """
    Provides a cached instance of Settings to avoid repeatedly
    loading and validating environment variables.
    """
    settings = Settings()
    settings.validate()
    return settings

# Pydantic Models

class ISCSBase(BaseModel):
    """Represents the base schema for the ISCS Salesforce object."""
    Customer_Name__c: str = Field(..., min_length=1, max_length=100)
    Email_Address__c: EmailStr
    Phone_Number__c: str = Field(..., pattern=r'^\+?1?\d{9,15}$')
    Registration_Date__c: date
    Account_Balance__c: float = Field(..., ge=0)

    @validator('Registration_Date__c')
    def validate_date(cls, v: date) -> str:
        """
        Validates that the registration date is not in the future.
        Returns date in ISO format if valid.
        """
        if v > date.today():
            raise ValueError('Registration date cannot be in the future')
        return v.isoformat()

class ISCSResponse(BaseModel):
    """
    Simple response model to convey whether an operation was successful,
    along with a descriptive message, an optional record ID,
    and any error details.
    """
    success: bool
    message: str
    record_id: Optional[str] = None
    error_details: Optional[Dict[str, Any]] = None

class ISCSUpdate(BaseModel):
    """
    Model for updating specific fields of an ISCS record. All fields are optional,
    allowing partial updates without overwriting unspecified fields.
    """
    Customer_Name__c: Optional[str] = Field(None, min_length=1, max_length=100)
    Email_Address__c: Optional[EmailStr] = None
    Phone_Number__c: Optional[str] = Field(None, pattern=r'^\+?1?\d{9,15}$')
    Registration_Date__c: Optional[date] = None
    Account_Balance__c: Optional[float] = Field(None, ge=0)

    @validator('Registration_Date__c')
    def validate_date(cls, v: Optional[date]) -> Optional[str]:
        """
        Validates that the updated registration date is not in the future.
        Returns date in ISO format if valid, otherwise None.
        """
        if v and v > date.today():
            raise ValueError('Registration date cannot be in the future')
        return v.isoformat() if v else None

# FastAPI Application Configuration

app = FastAPI(
    title="Salesforce ISCS Integration API",
    description="Production-grade API for Salesforce ISCS object integration",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Middleware to add process time header in every response
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.perf_counter()
    response = await call_next(request)
    process_time = time.perf_counter() - start_time
    response.headers["X-Process-Time"] = f"{process_time:.4f}"
    return response

# Salesforce Authentication Management with Token Refresh and Concurrency

# Lock to ensure concurrency safety when refreshing tokens
token_lock = asyncio.Lock()

class SalesforceAuth:
    """
    Manages Salesforce authentication by retrieving and caching an access token.
    Automatically re-authenticates if the token is missing, expired, or invalid.
    Also supports refreshing the token close to expiry or upon 401 errors.
    """
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._access_token: Optional[str] = None
        self._instance_url: Optional[str] = None
        self._token_expiry: Optional[datetime] = None

    async def get_auth_details(self) -> Tuple[str, str]:
        """
        Retrieves the currently cached access token and instance URL,
        re-authenticating first if necessary or if token is close to expiry.

        Returns:
            (access_token, instance_url)
        """
        # Acquire lock to prevent multiple coroutines
        # from refreshing simultaneously
        async with token_lock:
            if not self._access_token or await self._is_token_expired():
                await self.authenticate()
            return self._access_token, self._instance_url

    async def _is_token_expired(self) -> bool:
        """
        Determines whether the token is close to expiration or already expired.
        Applies a configurable buffer to avoid mid-request expiry.
        """
        if not self._token_expiry or not self._access_token:
            return True
        now = datetime.utcnow()
        if now >= (self._token_expiry - timedelta(seconds=self.settings.TOKEN_REFRESH_BUFFER)):
            logger.info("Token is close to or past expiry threshold; refreshing.")
            return True
        return False

    async def authenticate(self) -> None:
        """
        Performs an authentication request against Salesforce
        using the OAuth 2.0 Password flow.
        On success, caches the access token, instance URL,
        and expiry time for future requests.
        Raises an HTTPException if authentication fails.
        """
        try:
            payload = {
                'grant_type': 'password',
                'client_id': self.settings.CLIENT_ID,
                'client_secret': self.settings.CLIENT_SECRET,
                'username': self.settings.USERNAME,
                'password': self.settings.PASSWORD
            }

            logger.info("Authenticating with Salesforce...")
            async with httpx.AsyncClient() as client:
                response = await client.post(self.settings.TOKEN_URL, data=payload)
                response.raise_for_status()

            auth_response = response.json()
            self._access_token = auth_response['access_token']
            self._instance_url = auth_response['instance_url']

            issued_at = auth_response.get("issued_at")
            valid_for = auth_response.get("access_token_validity")

            if issued_at and issued_at.isdigit():
                # 'issued_at' is returned in milliseconds from epoch
                issued_at_dt = datetime.utcfromtimestamp(int(issued_at) / 1000.0)
                # If 'access_token_validity' was provided in seconds:
                expires_in_seconds = int(valid_for) if valid_for else 3600
                self._token_expiry = issued_at_dt + timedelta(seconds=expires_in_seconds)
            else:
                # Fallback: simply set token expiry to 1 hour from now.
                self._token_expiry = datetime.utcnow() + timedelta(hours=1)

            logger.info(
                "Authentication with Salesforce was successful. "
                f"Token set to expire at {self._token_expiry}"
            )

        except httpx.RequestError as e:
            logger.error(f"Salesforce authentication request failed: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Failed to authenticate with Salesforce (network issue)"
            )
        except httpx.HTTPStatusError as e:
            logger.error(
                f"Salesforce authentication failed with status {e.response.status_code}: {e}"
            )
            raise HTTPException(
                status_code=e.response.status_code,
                detail=(
                    f"Failed to authenticate with Salesforce "
                    f"(HTTP error {e.response.status_code})"
                )
            )

    async def handle_401(self):
        """
        Handles a 401 Unauthorized error from Salesforce by forcing a token refresh.
        Subsequent calls will use the new token.
        """
        async with token_lock:
            logger.info("Received 401 from Salesforce, refreshing token...")
            await self.authenticate()

# Dependency Injector

@lru_cache()
def get_salesforce_auth() -> SalesforceAuth:
    """
    Provides a cached instance of SalesforceAuth to ensure tokens
    are reused when valid.
    """
    return SalesforceAuth(get_settings())

# Utility Functions

async def make_request_with_retries(
    method: str,
    url: str,
    headers: Dict[str, str],
    json_data: Optional[Dict] = None,
    auth_instance: SalesforceAuth = None,
    max_retries: int = 1
) -> httpx.Response:
    """
    Makes an HTTP request to the Salesforce REST API,
    with token refresh logic on 401 errors.
    Automatically retries once if unauthorized.
    """
    async with httpx.AsyncClient() as client:
        for attempt in range(max_retries + 1):
            try:
                if method.upper() == "GET":
                    resp = await client.get(url, headers=headers)
                elif method.upper() == "POST":
                    resp = await client.post(url, headers=headers, json=json_data)
                elif method.upper() == "PATCH":
                    resp = await client.patch(url, headers=headers, json=json_data)
                elif method.upper() == "DELETE":
                    resp = await client.delete(url, headers=headers)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")

                if resp.status_code == 401 and auth_instance and attempt < max_retries:
                    logger.warning(
                        f"401 Unauthorized encountered. "
                        f"Attempt {attempt+1} of {max_retries+1}"
                    )
                    await auth_instance.handle_401()
                    new_token, _ = await auth_instance.get_auth_details()
                    headers["Authorization"] = f"Bearer {new_token}"
                    continue

                resp.raise_for_status()
                return resp

            except httpx.HTTPStatusError as e:
                # Raise immediately for status errors (other than 401 handled above)
                raise e
    # If we exhausted all retries and still have 401 or other failures:
    return resp

# API Endpoints

@app.post(
    "/api/v1/iscs",
    response_model=ISCSResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["ISCS"],
)
async def create_iscs_record(
    data: ISCSBase,
    sf_auth: SalesforceAuth = Depends(get_salesforce_auth)
) -> ISCSResponse:
    """
    Creates a new ISCS record in Salesforce.

    Returns:
        ISCSResponse indicating success and providing the newly created record ID.
    Raises:
        HTTPException in case of Salesforce API errors or unexpected failures.
    """
    wall_start = time.perf_counter()
    proc = psutil.Process(os.getpid())
    cpu_start = proc.cpu_times()
    try:
        access_token, instance_url = await sf_auth.get_auth_details()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        endpoint = f"{instance_url}/services/data/{get_settings().API_VERSION}/sobjects/ISCS__c"

        logger.info(f"Inserting record into Salesforce ISCS object: {data.dict()}")
        response = await make_request_with_retries(
            method="POST",
            url=endpoint,
            headers=headers,
            json_data=data.dict(),
            auth_instance=sf_auth,
            max_retries=1
        )

        record_id = response.json().get("id")
        logger.info(f"Record created successfully. ID: {record_id}")

        return ISCSResponse(
            success=True,
            message="Record created successfully",
            record_id=record_id
        )
    except httpx.HTTPError as e:
        error_detail = _extract_salesforce_error(e)
        logger.error(f"Failed to create record in ISCS object: {error_detail}")
        raise HTTPException(
            status_code=_extract_status_code(e),
            detail=f"Salesforce API error: {error_detail}"
        )
    except Exception as e:
        logger.error(f"Unexpected error creating ISCS record: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )
    finally:
        wall_end = time.perf_counter()
        cpu_end = proc.cpu_times()
        wall_time = wall_end - wall_start
        cpu_time = (cpu_end.user - cpu_start.user) + (cpu_end.system - cpu_start.system)
        logger.info(f"[Metrics][POST /api/v1/iscs] Wall time: {wall_time:.4f} seconds, CPU time: {cpu_time:.4f} seconds.")

@app.get(
    "/api/v1/iscs/{record_id}",
    response_model=ISCSBase,
    status_code=status.HTTP_200_OK,
    tags=["ISCS"],
)
async def get_iscs_record(
    record_id: str,
    sf_auth: SalesforceAuth = Depends(get_salesforce_auth)
) -> ISCSBase:
    """
    Retrieves an ISCS record from Salesforce by its record ID.

    Returns:
        ISCSBase model populated with the Salesforce data.
    Raises:
        HTTPException if the record is not found or if an API error occurs.
    """
    wall_start = time.perf_counter()
    proc = psutil.Process(os.getpid())
    cpu_start = proc.cpu_times()
    try:
        access_token, instance_url = await sf_auth.get_auth_details()
        headers = {
            "Authorization": f"Bearer {access_token}"
        }
        endpoint = f"{instance_url}/services/data/{get_settings().API_VERSION}/sobjects/ISCS__c/{record_id}"

        logger.info(f"Retrieving record from Salesforce ISCS object: {record_id}")
        response = await make_request_with_retries(
            method="GET",
            url=endpoint,
            headers=headers,
            auth_instance=sf_auth
        )

        record_data = response.json()
        logger.info(f"Record retrieved successfully: {record_id}")

        # Remove Salesforce-specific metadata and ID from the response
        record_data.pop("attributes", None)
        record_data.pop("Id", None)
        return ISCSBase(**record_data)
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            logger.error(f"ISCS record not found: {record_id}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Record not found"
            )
        error_detail = _extract_salesforce_error(e)
        logger.error(f"Failed to retrieve ISCS record: {error_detail}")
        raise HTTPException(
            status_code=e.response.status_code,
            detail=f"Salesforce API error: {error_detail}"
        )
    except Exception as e:
        logger.error(f"Unexpected error retrieving ISCS record: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )
    finally:
        wall_end = time.perf_counter()
        cpu_end = proc.cpu_times()
        wall_time = wall_end - wall_start
        cpu_time = (cpu_end.user - cpu_start.user) + (cpu_end.system - cpu_start.system)
        logger.info(f"[Metrics][GET /api/v1/iscs/{record_id}] Wall time: {wall_time:.4f} seconds, CPU time: {cpu_time:.4f} seconds.")

@app.put(
    "/api/v1/iscs/{record_id}",
    response_model=ISCSResponse,
    status_code=status.HTTP_200_OK,
    tags=["ISCS"],
)
async def update_iscs_record(
    record_id: str,
    data: ISCSUpdate,
    sf_auth: SalesforceAuth = Depends(get_salesforce_auth)
) -> ISCSResponse:
    """
    Updates an existing ISCS record in Salesforce.
    Only provided fields will be overwritten.

    Returns:
        ISCSResponse with success status and a message.
    Raises:
        HTTPException if the update fails (Salesforce API error or other issues).
    """
    wall_start = time.perf_counter()
    proc = psutil.Process(os.getpid())
    cpu_start = proc.cpu_times()
    try:
        access_token, instance_url = await sf_auth.get_auth_details()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        endpoint = f"{instance_url}/services/data/{get_settings().API_VERSION}/sobjects/ISCS__c/{record_id}"

        update_data = {k: v for k, v in data.dict().items() if v is not None}

        logger.info(f"Updating record {record_id} with data: {update_data}")
        await make_request_with_retries(
            method="PATCH",
            url=endpoint,
            headers=headers,
            json_data=update_data,
            auth_instance=sf_auth,
            max_retries=1
        )

        logger.info(f"Record updated successfully: {record_id}")
        return ISCSResponse(success=True, message="Record updated successfully")
    except httpx.HTTPError as e:
        error_detail = _extract_salesforce_error(e)
        logger.error(f"Failed to update ISCS record {record_id}: {error_detail}")
        raise HTTPException(
            status_code=_extract_status_code(e),
            detail=f"Salesforce API error: {error_detail}"
        )
    except Exception as e:
        logger.error(f"Unexpected error updating ISCS record: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )
    finally:
        wall_end = time.perf_counter()
        cpu_end = proc.cpu_times()
        wall_time = wall_end - wall_start
        cpu_time = (cpu_end.user - cpu_start.user) + (cpu_end.system - cpu_start.system)
        logger.info(f"[Metrics][PUT /api/v1/iscs/{record_id}] Wall time: {wall_time:.4f} seconds, CPU time: {cpu_time:.4f} seconds.")

@app.delete(
    "/api/v1/iscs/{record_id}",
    response_model=ISCSResponse,
    status_code=status.HTTP_200_OK,
    tags=["ISCS"],
)
async def delete_iscs_record(
    record_id: str,
    sf_auth: SalesforceAuth = Depends(get_salesforce_auth)
) -> ISCSResponse:
    """
    Deletes an ISCS record in Salesforce by its record ID.

    Returns:
        ISCSResponse indicating success if the record was deleted.
    Raises:
        HTTPException if the record does not exist or a Salesforce API error occurs.
    """
    wall_start = time.perf_counter()
    proc = psutil.Process(os.getpid())
    cpu_start = proc.cpu_times()
    try:
        access_token, instance_url = await sf_auth.get_auth_details()
        headers = {
            "Authorization": f"Bearer {access_token}"
        }
        endpoint = f"{instance_url}/services/data/{get_settings().API_VERSION}/sobjects/ISCS__c/{record_id}"

        logger.info(f"Deleting record from Salesforce ISCS object: {record_id}")
        await make_request_with_retries(
            method="DELETE",
            url=endpoint,
            headers=headers,
            auth_instance=sf_auth,
            max_retries=1
        )

        logger.info(f"Record deleted successfully: {record_id}")
        return ISCSResponse(success=True, message="Record deleted successfully")
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            logger.error(f"ISCS record not found for deletion: {record_id}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Record not found"
            )
        error_detail = _extract_salesforce_error(e)
        logger.error(f"Failed to delete ISCS record {record_id}: {error_detail}")
        raise HTTPException(
            status_code=e.response.status_code,
            detail=f"Salesforce API error: {error_detail}"
        )
    except Exception as e:
        logger.error(f"Unexpected error deleting ISCS record: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )
    finally:
        wall_end = time.perf_counter()
        cpu_end = proc.cpu_times()
        wall_time = wall_end - wall_start
        cpu_time = (cpu_end.user - cpu_start.user) + (cpu_end.system - cpu_start.system)
        logger.info(f"[Metrics][DELETE /api/v1/iscs/{record_id}] Wall time: {wall_time:.4f} seconds, CPU time: {cpu_time:.4f} seconds.")

# Health Check Endpoint

@app.get("/health", tags=["Health"])
async def health_check() -> Dict[str, Any]:
    """
    Health check endpoint for uptime monitoring and basic health inspection.
    Returns additional system metrics such as CPU usage and memory statistics.
    """
    cpu_usage = psutil.cpu_percent(interval=0.1)
    mem = psutil.virtual_memory()
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "cpu_usage": cpu_usage,
        "memory": {
            "total": mem.total,
            "available": mem.available,
            "used": mem.used,
            "percent": mem.percent
        }
    }

# Global Error Handlers

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    """
    Catches all HTTPExceptions to provide a consistent JSON response format.
    """
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "success": False,
            "message": str(exc.detail),
            "error_code": exc.status_code
        }
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """
    Catches all unhandled exceptions and logs them,
    returning a generic error to the client.
    """
    logger.error(f"Unhandled exception: {str(exc)}")
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "success": False,
            "message": "Internal server error",
            "error_code": status.HTTP_500_INTERNAL_SERVER_ERROR
        }
    )

# Helper Functions

def _extract_salesforce_error(exc: httpx.HTTPError) -> str:
    """
    Extracts SFDC error details from an httpx HTTPError if present,
    returning a user-friendly string.
    """
    try:
        errors = exc.response.json()
        if isinstance(errors, list) and len(errors) > 0:
            # Example of possible Salesforce error structure
            return (
                f"{errors[0].get('message')} "
                f"(Code: {errors[0].get('errorCode')}, "
                f"Fields: {errors[0].get('fields')})"
            )
        elif isinstance(errors, dict) and "error" in errors:
            return errors.get("error")
        return str(errors)
    except Exception:
        return str(exc)

def _extract_status_code(exc: httpx.HTTPError) -> int:
    """
    Safely extracts status code from an httpx HTTPError.
    """
    return exc.response.status_code if exc.response else status.HTTP_500_INTERNAL_SERVER_ERROR

# Application Entry Point

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "salesforce_metric:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        workers=4
    )