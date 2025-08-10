# record-service/tests/test_sfdc_operations.py
import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch, MagicMock
import httpx
import sys
import os

# Add shared and service-specific modules to Python path for testing
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../shared/src")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.config import settings

pytestmark = pytest.mark.asyncio

# --- Test Create Operation ---
async def test_handle_create_record_success(client: TestClient, mock_salesforce_api_client: AsyncMock):
    mock_salesforce_api_client.create_sobject_record = AsyncMock(
        return_value={"id": "mock_created_id", "success": True, "errors": []}
    )
    payload = {"object_name": "Account", "data": {"Name": "Test Account"}}
    response = client.post(f"{settings.API_V1_STR}/records/create", json=payload)
    assert response.status_code == 201
    json_response = response.json()
    assert json_response["success"] is True
    assert json_response["record_id"] == "mock_created_id"

async def test_handle_create_record_sfdc_error(client: TestClient, mock_salesforce_api_client: AsyncMock):
    mock_salesforce_api_client.create_sobject_record = AsyncMock(
        return_value={"id": None, "success": False, "errors": [{"message": "Required field missing"}]}
    )
    payload = {"object_name": "Contact", "data": {"LastName": "Test"}}
    response = client.post(f"{settings.API_V1_STR}/records/create", json=payload)
    assert response.status_code == 400

# --- Test Get Operation ---
async def test_handle_get_record_success(client: TestClient, mock_salesforce_api_client: AsyncMock):
    mock_salesforce_api_client.get_sobject_record = AsyncMock(
        return_value={"Id": "mock_retrieved_id", "Name": "Test Account"}
    )
    response = client.get(f"{settings.API_V1_STR}/records/Account/mock_retrieved_id")
    assert response.status_code == 200
    json_response = response.json()
    assert json_response["success"] is True
    assert json_response["data"]["Id"] == "mock_retrieved_id"

async def test_handle_get_record_not_found(client: TestClient, mock_salesforce_api_client: AsyncMock):
    mock_response = httpx.Response(404, json=[{"message": "Not Found"}])
    mock_salesforce_api_client.get_sobject_record = AsyncMock(
        side_effect=httpx.HTTPStatusError(message="Not Found", request=MagicMock(), response=mock_response)
    )
    response = client.get(f"{settings.API_V1_STR}/records/Account/non_existent_id")
    assert response.status_code == 404

# --- Test Update Operation ---
async def test_handle_update_record_success(client: TestClient, mock_salesforce_api_client: AsyncMock):
    mock_salesforce_api_client.update_sobject_record = AsyncMock(return_value=True)
    payload = {"object_name": "Opportunity", "record_id": "opp_id", "data": {"StageName": "Closed Won"}}
    response = client.patch(f"{settings.API_V1_STR}/records/update", json=payload)
    assert response.status_code == 200
    assert response.json()["success"] is True

# --- Test Delete Operation ---
async def test_handle_delete_record_success(client: TestClient, mock_salesforce_api_client: AsyncMock):
    mock_salesforce_api_client.delete_sobject_record = AsyncMock(return_value=True)
    response = client.delete(f"{settings.API_V1_STR}/records/Task/task_id_to_delete")
    assert response.status_code == 200
    assert response.json()["success"] is True

# --- Test Describe Operation ---
async def test_handle_describe_sobject_success(client: TestClient, mock_salesforce_api_client: AsyncMock):
    mock_describe_data = {"name": "Account", "fields": [{"name": "Name", "type": "string"}]}
    mock_salesforce_api_client.get_sobject_describe = AsyncMock(return_value=mock_describe_data)
    response = client.get(f"{settings.API_V1_STR}/sobjects/Account/describe")
    assert response.status_code == 200
    assert response.json()["data"] == mock_describe_data
