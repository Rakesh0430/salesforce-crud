from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel, EmailStr, Field, validator
from typing import Optional, Dict, Any
from datetime import datetime, date
import requests
import logging
import os
from logging.handlers import RotatingFileHandler
from functools import lru_cache
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging with rotation
LOG_FILENAME = "salesforce_api.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add rotating file handler
file_handler = RotatingFileHandler(
    LOG_FILENAME, maxBytes=10485760, backupCount=5  # 10MB per file, keep 5 files
)
file_handler.setFormatter(
    logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
)
logger.addHandler(file_handler)

# Environment Variables
class Settings:
    CLIENT_ID: str = os.getenv("SALESFORCE_CLIENT_ID")
    CLIENT_SECRET: str = os.getenv("SALESFORCE_CLIENT_SECRET")
    USERNAME: str = os.getenv("SALESFORCE_USERNAME")
    PASSWORD: str = os.getenv("SALESFORCE_PASSWORD")
    TOKEN_URL: str = os.getenv("SALESFORCE_TOKEN_URL", "https://login.salesforce.com/services/oauth2/token")
    API_VERSION: str = "v57.0"

    def validate(self):
        missing_vars = [
            var for var, value in vars(self).items()
            if not value and not var.startswith('_')
        ]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

@lru_cache()
def get_settings() -> Settings:
    settings = Settings()
    settings.validate()
    return settings

# Pydantic Models
class ISCSBase(BaseModel):
    Customer_Name__c: str = Field(..., min_length=1, max_length=100)
    Email_Address__c: EmailStr
    Phone_Number__c: str = Field(..., pattern=r'^\+?1?\d{9,15}$')
    Registration_Date__c: date
    Account_Balance__c: float = Field(..., ge=0)

    @validator('Registration_Date__c')
    def validate_date(cls, v):
        if v > date.today():
            raise ValueError('Registration date cannot be in the future')
        return v.isoformat()

class ISCSResponse(BaseModel):
    success: bool
    message: str
    record_id: Optional[str] = None
    error_details: Optional[Dict[str, Any]] = None

class ISCSUpdate(BaseModel):
    Customer_Name__c: Optional[str] = Field(None, min_length=1, max_length=100)
    Email_Address__c: Optional[EmailStr] = None
    Phone_Number__c: Optional[str] = Field(None, pattern=r'^\+?1?\d{9,15}$')
    Registration_Date__c: Optional[date] = None
    Account_Balance__c: Optional[float] = Field(None, ge=0)

    @validator('Registration_Date__c')
    def validate_date(cls, v):
        if v and v > date.today():
            raise ValueError('Registration date cannot be in the future')
        return v.isoformat() if v else None

# FastAPI App Configuration
app = FastAPI(
    title="Salesforce ISCS Integration API",
    description="Production-grade API for Salesforce ISCS object integration",
    version="1.0.0"
)

# CORS Configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Salesforce Authentication Class
class SalesforceAuth:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._access_token = None
        self._instance_url = None

    async def get_auth_details(self):
        if not self._access_token:
            await self.authenticate()
        return self._access_token, self._instance_url

    async def authenticate(self):
        try:
            payload = {
                'grant_type': 'password',
                'client_id': self.settings.CLIENT_ID,
                'client_secret': self.settings.CLIENT_SECRET,
                'username': self.settings.USERNAME,
                'password': self.settings.PASSWORD
            }
            logger.info("Authenticating with Salesforce...")
            response = requests.post(self.settings.TOKEN_URL, data=payload)
            response.raise_for_status()
            auth_response = response.json()
            
            self._access_token = auth_response['access_token']
            self._instance_url = auth_response['instance_url']
            logger.info("Authentication successful")
            
        except requests.RequestException as e:
            logger.error(f"Salesforce authentication failed: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Failed to authenticate with Salesforce"
            )

# Dependencies
@lru_cache()
def get_salesforce_auth() -> SalesforceAuth:
    return SalesforceAuth(get_settings())

# API Endpoints
@app.post(
    "/api/v1/iscs",
    response_model=ISCSResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["ISCS"]
)
async def create_iscs_record(
    data: ISCSBase,
    sf_auth: SalesforceAuth = Depends(get_salesforce_auth)
):
    """Creates a new ISCS record in Salesforce."""
    try:
        access_token, instance_url = await sf_auth.get_auth_details()
        
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        endpoint = f"{instance_url}/services/data/{get_settings().API_VERSION}/sobjects/ISCS__c"
        
        logger.info(f"Inserting record into Salesforce ISCS object: {data.dict()}")
        response = requests.post(endpoint, headers=headers, json=data.dict())
        response.raise_for_status()
        
        record_id = response.json()['id']
        logger.info(f"Record created successfully! ID: {record_id}")
        
        return ISCSResponse(
            success=True,
            message="Record created successfully",
            record_id=record_id
        )
        
    except requests.HTTPError as e:
        error_detail = e.response.json()[0] if e.response.json() else str(e)
        logger.error(f"Failed to insert record into ISCS object: {error_detail}")
        raise HTTPException(
            status_code=e.response.status_code,
            detail=f"Salesforce API error: {error_detail}"
        )
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )

@app.get(
    "/api/v1/iscs/{record_id}",
    response_model=ISCSBase,
    status_code=status.HTTP_200_OK,
    tags=["ISCS"]
)
async def get_iscs_record(
    record_id: str,
    sf_auth: SalesforceAuth = Depends(get_salesforce_auth)
):
    """Retrieves an ISCS record from Salesforce by its ID."""
    try:
        access_token, instance_url = await sf_auth.get_auth_details()
        headers = {'Authorization': f'Bearer {access_token}'}
        endpoint = f"{instance_url}/services/data/{get_settings().API_VERSION}/sobjects/ISCS__c/{record_id}"

        logger.info(f"Retrieving record from Salesforce ISCS object: {record_id}")
        response = requests.get(endpoint, headers=headers)
        response.raise_for_status()

        record_data = response.json()
        logger.info(f"Record retrieved successfully: {record_id}")

        # Remove unnecessary fields before returning
        del record_data['attributes']
        del record_data['Id']

        return ISCSBase(**record_data)

    except requests.HTTPError as e:
        if e.response.status_code == 404:
            logger.error(f"Record not found: {record_id}")
            raise HTTPException(status_code=404, detail="Record not found")
        else:
            error_detail = e.response.json()[0] if e.response.json() else str(e)
            logger.error(f"Failed to retrieve record from ISCS object: {error_detail}")
            raise HTTPException(
                status_code=e.response.status_code,
                detail=f"Salesforce API error: {error_detail}"
            )
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )

@app.put(
    "/api/v1/iscs/{record_id}",
    response_model=ISCSResponse,
    status_code=status.HTTP_200_OK,
    tags=["ISCS"]
)
async def update_iscs_record(
    record_id: str,
    data: ISCSUpdate,
    sf_auth: SalesforceAuth = Depends(get_salesforce_auth)
):
    """Updates an existing ISCS record in Salesforce."""
    try:
        access_token, instance_url = await sf_auth.get_auth_details()
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        endpoint = f"{instance_url}/services/data/{get_settings().API_VERSION}/sobjects/ISCS__c/{record_id}"

        # Filter out None values for partial updates
        update_data = {k: v for k, v in data.dict().items() if v is not None}

        logger.info(f"Updating record in Salesforce ISCS object: {record_id} with data: {update_data}")
        response = requests.patch(endpoint, headers=headers, json=update_data)
        response.raise_for_status()

        logger.info(f"Record updated successfully: {record_id}")
        return ISCSResponse(success=True, message="Record updated successfully")

    except requests.HTTPError as e:
        error_detail = e.response.json()[0] if e.response.json() else str(e)
        logger.error(f"Failed to update record in ISCS object: {error_detail}")
        raise HTTPException(
            status_code=e.response.status_code,
            detail=f"Salesforce API error: {error_detail}"
        )
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )

@app.delete(
    "/api/v1/iscs/{record_id}",
    response_model=ISCSResponse,
    status_code=status.HTTP_200_OK,
    tags=["ISCS"]
)
async def delete_iscs_record(
    record_id: str,
    sf_auth: SalesforceAuth = Depends(get_salesforce_auth)
):
    """Deletes an ISCS record from Salesforce by its ID."""
    try:
        access_token, instance_url = await sf_auth.get_auth_details()
        headers = {'Authorization': f'Bearer {access_token}'}
        endpoint = f"{instance_url}/services/data/{get_settings().API_VERSION}/sobjects/ISCS__c/{record_id}"

        logger.info(f"Deleting record from Salesforce ISCS object: {record_id}")
        response = requests.delete(endpoint, headers=headers)
        response.raise_for_status()

        logger.info(f"Record deleted successfully: {record_id}")
        return ISCSResponse(success=True, message="Record deleted successfully")

    except requests.HTTPError as e:
        if e.response.status_code == 404:
            logger.error(f"Record not found: {record_id}")
            raise HTTPException(status_code=404, detail="Record not found")
        else:
            error_detail = e.response.json()[0] if e.response.json() else str(e)
            logger.error(f"Failed to delete record from ISCS object: {error_detail}")
            raise HTTPException(
                status_code=e.response.status_code,
                detail=f"Salesforce API error: {error_detail}"
            )
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )

@app.get("/health", tags=["Health"])
async def health_check():
    """Health check endpoint for monitoring"""
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

# Error Handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "success": False,
            "message": str(exc.detail),
            "error_code": exc.status_code
        }
    )

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    logger.error(f"Unhandled exception: {str(exc)}")
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "success": False,
            "message": "Internal server error",
            "error_code": status.HTTP_500_INTERNAL_SERVER_ERROR
        }
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "crudsalesforce:app",
        host="0.0.0.0",
        port=8000,
        reload=False,  # Disable in production
        workers=4  # Adjust based on your needs
    )