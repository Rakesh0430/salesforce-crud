# src/core/config.py
import os
from pydantic import BaseSettings, AnyHttpUrl, EmailStr, validator
from typing import List, Optional, Union, Dict, Any
from dotenv import load_dotenv

# Load .env file
load_dotenv()

class Settings(BaseSettings):
    APP_NAME: str = "SalesforceIntegrationAPI"
    APP_VERSION: str = "1.0.0"
    API_V1_STR: str = "/api/v1"
    DEBUG_MODE: bool = os.getenv("DEBUG_MODE", "False").lower() == "true"

    # Salesforce Configuration
    SALESFORCE_CLIENT_ID: str
    SALESFORCE_CLIENT_SECRET: str
    SALESFORCE_USERNAME: str
    SALESFORCE_PASSWORD: str # This includes the security token if needed, or handle token separately
    SALESFORCE_TOKEN_URL: AnyHttpUrl = "https://login.salesforce.com/services/oauth2/token"
    SALESFORCE_API_VERSION: str = "v58.0" # Specify current API version
    SALESFORCE_TOKEN_REFRESH_BUFFER: int = 300 # Seconds before expiry to refresh token

    # Logging Configuration
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()
    LOG_FILENAME: Optional[str] = os.getenv("LOG_FILENAME", "sfdc_api.log") # None for console only
    LOG_MAX_BYTES: int = 10 * 1024 * 1024  # 10 MB
    LOG_BACKUP_COUNT: int = 5

    # CORS
    BACKEND_CORS_ORIGINS: List[AnyHttpUrl] = []
    # Example: BACKEND_CORS_ORIGINS=["http://localhost", "http://localhost:4200"]

    # Paths for data (relevant if using PV/PVC in K8s for file uploads/downloads)
    DATA_PATH_INPUT: str = "/app/data/input"
    DATA_PATH_OUTPUT: str = "/app/data/output"
    DATA_PATH_FAILED: str = "/app/data/failed"


    @validator("BACKEND_CORS_ORIGINS", pre=True)
    def assemble_cors_origins(cls, v: Union[str, List[str]]) -> Union[List[str], str]:
        if isinstance(v, str) and not v.startswith("["):
            return [i.strip() for i in v.split(",")]
        elif isinstance(v, (list, str)):
            return v
        raise ValueError(v)

    class Config:
        case_sensitive = True
        env_file = ".env"
        env_file_encoding = 'utf-8'

settings = Settings()

# Ensure necessary data paths exist (primarily for local dev, K8s handles volumes)
if settings.DEBUG_MODE: # Or based on some other condition if these paths are always local
    os.makedirs(settings.DATA_PATH_INPUT, exist_ok=True)
    os.makedirs(settings.DATA_PATH_OUTPUT, exist_ok=True)
    os.makedirs(settings.DATA_PATH_FAILED, exist_ok=True)
