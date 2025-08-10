# bulk-service/tests/test_sfdc_operations.py
import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch
import sys
import os

# Add shared and service-specific modules to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../shared/src')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.config import settings

pytestmark = pytest.mark.asyncio

@patch("app.routers.bulk.read_file_data_for_bulk", new_callable=AsyncMock)
@patch("app.routers.bulk.perform_bulk_operation", new_callable=AsyncMock)
async def test_handle_bulk_operation_file_success(
    mock_perform_bulk: AsyncMock,
    mock_read_file: AsyncMock,
    client: TestClient
):
    mock_read_file.return_value = [{"Name": "Bulk Acc 1"}]
    mock_perform_bulk.return_value = ("mock_job_id", [])
    file_content = b"Name\nBulk Acc 1"

    response = client.post(
        f"{settings.API_V1_STR}/bulk/dml-file-upload?object_name=Account&operation_type=insert",
        files={"file": ("test.csv", file_content, "text/csv")}
    )

    assert response.status_code == 200
    json_response = response.json()
    assert json_response["success"] is True
    assert json_response["job_id"] == "mock_job_id"

@patch("app.routers.bulk.perform_bulk_operation", new_callable=AsyncMock)
async def test_handle_batch_record_processing_use_bulk_api_true(
    mock_perform_bulk: AsyncMock,
    client: TestClient,
):
    mock_perform_bulk.return_value = ("mock_bulk_job_id", [])
    payload = {
        "object_name": "Account",
        "operation_type": "insert",
        "records": [{"Name": "Batch Acc 1"}],
        "use_bulk_api": True
    }
    response = client.post(f"{settings.API_V1_STR}/records/batch", json=payload)
    assert response.status_code == 200
    assert response.json()["job_id"] == "mock_bulk_job_id"

@patch("app.routers.bulk.get_bulk_job_status_and_results", new_callable=AsyncMock)
async def test_handle_get_bulk_job_status_success(
    mock_get_status: AsyncMock,
    client: TestClient,
):
    job_id = "some_job_id_123"
    mock_get_status.return_value = {
        "job_info": {"id": job_id, "state": "JobComplete", "operation": "insert", "object": "Account", "numberRecordsProcessed": 1, "numberRecordsFailed": 0, "errorMessage": ""},
        "results_data": None
    }

    response = client.get(f"{settings.API_V1_STR}/bulk/job/{job_id}/status")
    assert response.status_code == 200
    json_response = response.json()
    assert json_response["state"] == "JobComplete"
