# shared/src/core/config.py
import os
from pydantic import AnyHttpUrl, EmailStr, field_validator
from pydantic_settings import BaseSettings
from typing import List, Optional, Union
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
    SALESFORCE_PASSWORD: str
    SALESFORCE_TOKEN_URL: AnyHttpUrl = "https://login.salesforce.com/services/oauth2/token"
    SALESFORCE_API_VERSION: str = "v58.0"
    SALESFORCE_TOKEN_REFRESH_BUFFER: int = 300

    # Logging Configuration
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()
    LOG_FILENAME: Optional[str] = os.getenv("LOG_FILENAME")
    LOG_MAX_BYTES: int = 10 * 1024 * 1024
    LOG_BACKUP_COUNT: int = 5

    # CORS
    BACKEND_CORS_ORIGINS: List[AnyHttpUrl] = []

    # Paths
    DATA_PATH_INPUT: str = "/app/data/input"
    DATA_PATH_OUTPUT: str = "/app/data/output"
    DATA_PATH_FAILED: str = "/app/data/failed"

    @field_validator("BACKEND_CORS_ORIGINS", mode='before')
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

if settings.DEBUG_MODE:
    os.makedirs(settings.DATA_PATH_INPUT, exist_ok=True)
    os.makedirs(settings.DATA_PATH_OUTPUT, exist_ok=True)
    os.makedirs(settings.DATA_PATH_FAILED, exist_ok=True)
