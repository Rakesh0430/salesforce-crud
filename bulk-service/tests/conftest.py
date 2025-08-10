# bulk-service/tests/conftest.py
import pytest
from fastapi.testclient import TestClient
from typing import Generator, Any
from unittest.mock import AsyncMock, patch
import os

# Set environment variables for tests
os.environ["SALESFORCE_CLIENT_ID"] = "test_client_id"
os.environ["SALESFORCE_CLIENT_SECRET"] = "test_client_secret"
os.environ["SALESFORCE_USERNAME"] = "test_username"
os.environ["SALESFORCE_PASSWORD"] = "test_password"
os.environ["API_V1_STR"] = "/api/v1"

# Imports assume PYTHONPATH is set to include bulk-service/src and shared/src
from app.main import app as fastapi_app
from salesforce.auth import SalesforceAuth, get_salesforce_auth_instance
from salesforce.client import SalesforceApiClient, get_salesforce_api_client

@pytest.fixture(scope="module")
def client() -> Generator[TestClient, Any, None]:
    with TestClient(fastapi_app) as c:
        yield c

@pytest.fixture
def mock_salesforce_auth_instance():
    mock_auth = AsyncMock(spec=SalesforceAuth)
    mock_auth.get_auth_details = AsyncMock(return_value=("test_access_token", "https://test.salesforce.com"))
    mock_auth.authenticate = AsyncMock() # Prevent real authentication
    return mock_auth

@pytest.fixture
def mock_salesforce_api_client():
    return AsyncMock(spec=SalesforceApiClient)

@pytest.fixture(autouse=True)
def override_dependencies(mock_salesforce_auth_instance: AsyncMock, mock_salesforce_api_client: AsyncMock):
    with patch('salesforce.auth._auth_instance', mock_salesforce_auth_instance):
        async def mock_get_auth():
            return mock_salesforce_auth_instance

        async def mock_get_client():
            return mock_salesforce_api_client

        fastapi_app.dependency_overrides[get_salesforce_auth_instance] = mock_get_auth
        fastapi_app.dependency_overrides[get_salesforce_api_client] = mock_get_client

        yield

    fastapi_app.dependency_overrides = {}
