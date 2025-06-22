# src/tests/conftest.py
import pytest
from fastapi.testclient import TestClient
from typing import Generator, Any
from unittest.mock import AsyncMock, MagicMock

# To allow tests to run from the root directory and import src modules
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))


# Mock settings before they are imported by other modules
# This is tricky because settings are often imported at the module level.
# One way is to set environment variables before other imports.
os.environ["SALESFORCE_CLIENT_ID"] = "test_client_id"
os.environ["SALESFORCE_CLIENT_SECRET"] = "test_client_secret"
os.environ["SALESFORCE_USERNAME"] = "test_username"
os.environ["SALESFORCE_PASSWORD"] = "test_password"
os.environ["SALESFORCE_TOKEN_URL"] = "https://login.salesforce.com/services/oauth2/token"
os.environ["API_V1_STR"] = "/api/v1"
os.environ["DEBUG_MODE"] = "True"
os.environ["LOG_LEVEL"] = "DEBUG"
os.environ["LOG_FILENAME"] = "" # No file logging during tests
os.environ["BACKEND_CORS_ORIGINS"] = "[]"


from src.app.main import app as fastapi_app
from src.salesforce.auth import SalesforceAuth, get_salesforce_auth_instance
from src.salesforce.client import SalesforceApiClient, get_salesforce_api_client


@pytest.fixture(scope="session")
def event_loop():
    """Overrides pytest-asyncio event_loop fixture to use asyncio.get_event_loop()."""
    import asyncio
    return asyncio.get_event_loop()

@pytest.fixture(scope="module")
def client() -> Generator[TestClient, Any, None]:
    """
    Test client for the FastAPI application.
    """
    with TestClient(fastapi_app) as c:
        yield c


@pytest.fixture
def mock_salesforce_auth_instance():
    mock_auth = AsyncMock(spec=SalesforceAuth)
    mock_auth.get_auth_details = AsyncMock(return_value=("test_access_token", "https://test.salesforce.com"))
    mock_auth.handle_401_unauthorized = AsyncMock()
    return mock_auth

@pytest.fixture
def mock_salesforce_api_client(mock_salesforce_auth_instance: AsyncMock):
    # This mock will be for the SalesforceApiClient instance itself
    mock_api_client = AsyncMock(spec=SalesforceApiClient)
    # We can mock its specific methods as needed in individual tests, e.g.:
    # mock_api_client.create_sobject_record = AsyncMock(return_value={"id": "mock_record_id", "success": True, "errors": []})
    # For DI, we need to ensure this mock is injected
    return mock_api_client


# Override FastAPI dependencies for testing
@pytest.fixture(autouse=True) # autouse to apply to all tests
async def override_dependencies(
    mock_salesforce_auth_instance: AsyncMock,
    mock_salesforce_api_client: AsyncMock
):
    # This is the dependency that get_salesforce_auth_instance in auth.py will use
    # SalesforceAuth.get_auth_details = mock_salesforce_auth_instance.get_auth_details
    # SalesforceAuth.handle_401_unauthorized = mock_salesforce_auth_instance.handle_401_unauthorized
    # SalesforceAuth.authenticate = AsyncMock() # Prevent actual authentication calls

    # Better: Override the dependency injector function itself
    async def mock_get_auth():
        # print("Mock get_salesforce_auth_instance called")
        return mock_salesforce_auth_instance

    async def mock_get_api_client():
        # print("Mock get_salesforce_api_client called")
        # This mock_salesforce_api_client is an AsyncMock instance of SalesforceApiClient
        # It's not the class itself. So we return this instance.
        return mock_salesforce_api_client

    fastapi_app.dependency_overrides[get_salesforce_auth_instance] = mock_get_auth
    fastapi_app.dependency_overrides[get_salesforce_api_client] = mock_get_api_client

    yield

    # Clean up overrides after tests if necessary, though usually not for session-scoped fixtures
    fastapi_app.dependency_overrides = {}


# Example usage in a test:
# async def test_create_something(client: TestClient, mock_salesforce_api_client: AsyncMock):
#     mock_salesforce_api_client.create_sobject_record = AsyncMock(return_value={"id": "mock_id", "success": True})
#     response = client.post("/api/v1/sobjects/create", json={...})
#     assert response.status_code == 201
#     mock_salesforce_api_client.create_sobject_record.assert_called_once()
