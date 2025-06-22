# src/tests/test_sfdc_operations.py
import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch, MagicMock
import httpx # For simulating httpx.HTTPStatusError

from src.core.schemas import SalesforceOperationPayload
from src.core.config import settings # To use settings.API_V1_STR

# Test client and overridden dependencies are provided by conftest.py

# Mark all tests in this module as asyncio
pytestmark = pytest.mark.asyncio

# --- Test Create Operation ---
async def test_handle_create_record_success(client: TestClient, mock_salesforce_api_client: AsyncMock):
    # Configure the mock SalesforceApiClient's create_sobject_record method
    mock_salesforce_api_client.create_sobject_record = AsyncMock(
        return_value={"id": "mock_created_id", "success": True, "errors": []}
    )

    payload = {
        "object_name": "Account",
        "data": {"Name": "Test Account"}
    }
    response = client.post(f"{settings.API_V1_STR}/sobjects/create", json=payload)

    assert response.status_code == 201
    json_response = response.json()
    assert json_response["success"] is True
    assert json_response["message"] == "Record created successfully in Account."
    assert json_response["record_id"] == "mock_created_id"
    mock_salesforce_api_client.create_sobject_record.assert_called_once_with(
        "Account", {"Name": "Test Account"}
    )

async def test_handle_create_record_sfdc_error(client: TestClient, mock_salesforce_api_client: AsyncMock):
    # Simulate Salesforce returning an error
    mock_salesforce_api_client.create_sobject_record = AsyncMock(
        return_value={"id": None, "success": False, "errors": [{"message": "Required field missing", "errorCode": "REQUIRED_FIELD_MISSING"}]}
    )

    payload = {
        "object_name": "Contact",
        "data": {"LastName": "Test"} # Missing FirstName, for example
    }
    response = client.post(f"{settings.API_V1_STR}/sobjects/create", json=payload)

    assert response.status_code == 400 # Or whatever status code your operation layer maps this to
    json_response = response.json()
    assert json_response["success"] is False # Assuming your error handler sets this
    assert "Required field missing" in json_response["detail"] # Or json_response["message"] depending on error model

async def test_handle_create_record_exception(client: TestClient, mock_salesforce_api_client: AsyncMock):
    # Simulate an unexpected exception in the client call
    mock_salesforce_api_client.create_sobject_record = AsyncMock(side_effect=Exception("Internal SF client error"))

    payload = {
        "object_name": "Lead",
        "data": {"Company": "Test Corp"}
    }
    response = client.post(f"{settings.API_V1_STR}/sobjects/create", json=payload)

    assert response.status_code == 500
    json_response = response.json()
    assert "Internal SF client error" in json_response["detail"]


# --- Test Get Operation ---
async def test_handle_get_record_success(client: TestClient, mock_salesforce_api_client: AsyncMock):
    mock_salesforce_api_client.get_sobject_record = AsyncMock(
        return_value={"Id": "mock_retrieved_id", "Name": "Test Account", "attributes": {"type": "Account"}}
    )

    object_name = "Account"
    record_id = "mock_retrieved_id"
    response = client.get(f"{settings.API_V1_STR}/sobjects/{object_name}/{record_id}")

    assert response.status_code == 200
    json_response = response.json()
    assert json_response["success"] is True
    assert json_response["message"] == "Record retrieved successfully."
    assert json_response["data"]["Id"] == "mock_retrieved_id"
    assert json_response["data"]["Name"] == "Test Account"
    assert "attributes" not in json_response["data"] # Check if popped
    mock_salesforce_api_client.get_sobject_record.assert_called_once_with(
        object_name, record_id, None # No specific fields requested
    )

async def test_handle_get_record_with_fields(client: TestClient, mock_salesforce_api_client: AsyncMock):
    mock_salesforce_api_client.get_sobject_record = AsyncMock(
        return_value={"Id": "mock_id", "Name": "Specific Field Test", "Phone": "12345", "attributes": {"type": "Account"}}
    )

    object_name = "Account"
    record_id = "mock_id"
    fields_query = "Name,Phone"
    response = client.get(f"{settings.API_V1_STR}/sobjects/{object_name}/{record_id}?fields={fields_query}")

    assert response.status_code == 200
    json_response = response.json()
    assert json_response["data"]["Name"] == "Specific Field Test"
    assert json_response["data"]["Phone"] == "12345"
    mock_salesforce_api_client.get_sobject_record.assert_called_once_with(
        object_name, record_id, ["Name", "Phone"]
    )

async def test_handle_get_record_not_found(client: TestClient, mock_salesforce_api_client: AsyncMock):
    # Simulate 404 from Salesforce client
    # The client's _request method would typically raise an HTTPException for a 404
    mock_response = httpx.Response(404, json=[{"message": "The requested resource does not exist", "errorCode": "NOT_FOUND"}])
    mock_salesforce_api_client.get_sobject_record = AsyncMock(
        side_effect=httpx.HTTPStatusError(
            message="Not Found", request=MagicMock(), response=mock_response
        )
    )
    # Or, if your operation layer catches and re-raises:
    # from fastapi import HTTPException
    # mock_salesforce_api_client.get_sobject_record = AsyncMock(side_effect=HTTPException(status_code=404, detail="Record not found"))


    object_name = "Account"
    record_id = "non_existent_id"
    response = client.get(f"{settings.API_V1_STR}/sobjects/{object_name}/{record_id}")

    assert response.status_code == 404
    assert "Record not found" in response.json()["detail"] # Or the SF error message

# --- Test Update Operation ---
async def test_handle_update_record_success(client: TestClient, mock_salesforce_api_client: AsyncMock):
    mock_salesforce_api_client.update_sobject_record = AsyncMock(return_value=True) # SF returns 204, client maps to True

    payload = {
        "object_name": "Opportunity",
        "record_id": "opp_id_to_update",
        "data": {"StageName": "Closed Won"}
    }
    response = client.patch(f"{settings.API_V1_STR}/sobjects/update", json=payload)

    assert response.status_code == 200 # Or 204 if your endpoint returns that directly
    json_response = response.json()
    assert json_response["success"] is True
    assert json_response["message"] == "Record opp_id_to_update in Opportunity updated successfully."
    mock_salesforce_api_client.update_sobject_record.assert_called_once_with(
        "Opportunity", "opp_id_to_update", {"StageName": "Closed Won"}
    )

async def test_handle_update_record_missing_record_id(client: TestClient):
    payload = {
        "object_name": "Account",
        # "record_id": "some_id", # Missing record_id
        "data": {"Name": "Updated Name"}
    }
    response = client.patch(f"{settings.API_V1_STR}/sobjects/update", json=payload)
    assert response.status_code == 400 # As per router validation
    assert "record_id is required" in response.json()["detail"]


# --- Test Delete Operation ---
async def test_handle_delete_record_success(client: TestClient, mock_salesforce_api_client: AsyncMock):
    mock_salesforce_api_client.delete_sobject_record = AsyncMock(return_value=True)

    object_name = "Task"
    record_id = "task_id_to_delete"
    response = client.delete(f"{settings.API_V1_STR}/sobjects/{object_name}/{record_id}")

    assert response.status_code == 200 # Or 204
    json_response = response.json()
    assert json_response["success"] is True
    assert json_response["message"] == f"Record {record_id} in {object_name} deleted successfully."
    mock_salesforce_api_client.delete_sobject_record.assert_called_once_with(
        object_name, record_id
    )


# --- Test Describe Operation ---
async def test_handle_describe_sobject_success(client: TestClient, mock_salesforce_api_client: AsyncMock):
    mock_describe_data = {"name": "Account", "fields": [{"name": "Name", "type": "string"}]}
    mock_salesforce_api_client.get_sobject_describe = AsyncMock(return_value=mock_describe_data)

    object_name = "Account"
    response = client.get(f"{settings.API_V1_STR}/sobjects/{object_name}/describe")

    assert response.status_code == 200
    json_response = response.json()
    assert json_response["success"] is True
    assert json_response["message"] == f"Successfully described SObject {object_name}."
    assert json_response["data"] == mock_describe_data
    mock_salesforce_api_client.get_sobject_describe.assert_called_once_with(object_name)


# --- Test Bulk Operation (File Upload) ---
# These tests are more complex due to file handling and multi-step bulk process.
# We'll mock the high-level `perform_bulk_operation` from `src.salesforce.operations` for router tests.
# Lower-level tests for `perform_bulk_operation` itself would mock the `SalesforceApiClient` methods.

@patch("src.app.routers.sfdc.perform_bulk_operation", new_callable=AsyncMock)
@patch("src.app.routers.sfdc.read_file_data_for_bulk", new_callable=AsyncMock)
async def test_handle_bulk_operation_file_success(
    mock_read_file: AsyncMock,
    mock_perform_bulk: AsyncMock,
    client: TestClient
):
    mock_read_file.return_value = [{"Name": "Bulk Acc 1"}, {"Name": "Bulk Acc 2"}]
    mock_perform_bulk.return_value = ("mock_job_id", []) # job_id, results (empty for now)

    object_name = "Account"
    operation_type = "create"
    file_content = b"Name\nBulk Acc 1\nBulk Acc 2" # Dummy CSV content

    response = client.post(
        f"{settings.API_V1_STR}/sobjects/bulk/{operation_type}?object_name={object_name}",
        files={"file": ("test.csv", file_content, "text/csv")}
    )

    assert response.status_code == 200 # Or 202 if you prefer for accepted jobs
    json_response = response.json()
    assert json_response["success"] is True
    assert json_response["job_id"] == "mock_job_id"
    assert f"Bulk {operation_type} job submitted for {object_name}" in json_response["message"]

    mock_read_file.assert_called_once()
    # Check that mock_perform_bulk was called with the correct SalesforceBulkOperationPayload
    # This requires inspecting the arguments passed to the mock.
    # args, kwargs = mock_perform_bulk.call_args
    # called_payload = kwargs.get('payload') or args[1] # Assuming payload is the second arg or kwarg
    # assert called_payload.object_name == object_name
    # assert called_payload.operation == operation_type
    # assert called_payload.records == [{"Name": "Bulk Acc 1"}, {"Name": "Bulk Acc 2"}]
    mock_perform_bulk.assert_called_once()


async def test_handle_bulk_operation_file_invalid_op_type(client: TestClient):
    object_name = "Account"
    operation_type = "invalid_op"
    response = client.post(
        f"{settings.API_V1_STR}/sobjects/bulk/{operation_type}?object_name={object_name}",
        files={"file": ("test.csv", b"Name\nTest", "text/csv")}
    )
    assert response.status_code == 400
    assert "Invalid operation_type" in response.json()["detail"]


async def test_handle_bulk_upsert_missing_external_id(client: TestClient):
    object_name = "Contact"
    operation_type = "upsert" # Upsert requires external_id_field
    response = client.post(
        f"{settings.API_V1_STR}/sobjects/bulk/{operation_type}?object_name={object_name}", # No external_id_field query param
        files={"file": ("test.csv", b"ExtId__c,LastName\n123,Test", "text/csv")}
    )
    assert response.status_code == 400
    assert "external_id_field is required for upsert operation" in response.json()["detail"]


# --- Test /records/batch endpoint ---
@patch("src.app.routers.sfdc.perform_bulk_operation", new_callable=AsyncMock)
@patch("src.app.routers.sfdc.create_record", new_callable=AsyncMock)
async def test_handle_batch_record_processing_use_bulk_api_true(
    mock_create_record: AsyncMock, # Not used if bulk is true
    mock_perform_bulk: AsyncMock,
    client: TestClient,
    mock_salesforce_auth_instance: AsyncMock # To ensure auth is mocked
):
    mock_perform_bulk.return_value = ("mock_bulk_job_id", []) # job_id, results
    payload = {
        "object_name": "Account",
        "operation_type": "create",
        "records": [{"Name": "Batch Acc 1"}, {"Name": "Batch Acc 2"}],
        "use_bulk_api": True
    }
    response = client.post(f"{settings.API_V1_STR}/records/batch", json=payload)

    assert response.status_code == 200 # Endpoint returns 200 for job submission
    json_response = response.json()
    assert json_response["success"] is True
    assert json_response["job_id"] == "mock_bulk_job_id"
    assert "Bulk create job submitted" in json_response["message"]
    mock_perform_bulk.assert_called_once()
    # args, kwargs = mock_perform_bulk.call_args
    # called_payload = kwargs['payload']
    # assert called_payload.object_name == "Account"
    # assert called_payload.operation == "create"
    # assert called_payload.records == [{"Name": "Batch Acc 1"}, {"Name": "Batch Acc 2"}]
    mock_create_record.assert_not_called()


@patch("src.app.routers.sfdc.perform_bulk_operation", new_callable=AsyncMock)
@patch("src.app.routers.sfdc.create_record", new_callable=AsyncMock)
@patch("src.app.routers.sfdc.update_record", new_callable=AsyncMock)
@patch("src.app.routers.sfdc.upsert_record", new_callable=AsyncMock)
@patch("src.app.routers.sfdc.delete_record", new_callable=AsyncMock)
async def test_handle_batch_record_processing_use_bulk_api_false_create(
    mock_delete: AsyncMock, mock_upsert: AsyncMock, mock_update: AsyncMock, mock_create: AsyncMock,
    mock_perform_bulk: AsyncMock,
    client: TestClient,
    mock_salesforce_auth_instance: AsyncMock
):
    mock_create.return_value = "mock_created_id_1" # Simulate create_record returning an ID
    payload = {
        "object_name": "Lead",
        "operation_type": "create",
        "records": [{"Company": "Batch Lead 1"}, {"Company": "Batch Lead 2"}],
        "use_bulk_api": False
    }
    response = client.post(f"{settings.API_V1_STR}/records/batch", json=payload)

    assert response.status_code == 200
    json_response = response.json()
    assert json_response["success"] is True
    assert json_response["job_id"] is None # Not a bulk job
    assert "Processed 2 records via REST API" in json_response["message"]
    assert len(json_response["results"]) == 2
    assert json_response["results"][0]["success"] is True
    assert json_response["results"][0]["id"] == "mock_created_id_1"

    assert mock_create.call_count == 2
    mock_perform_bulk.assert_not_called()
    mock_update.assert_not_called()
    mock_upsert.assert_not_called()
    mock_delete.assert_not_called()

async def test_handle_batch_record_processing_empty_records(client: TestClient):
    payload = {
        "object_name": "Account",
        "operation_type": "create",
        "records": [], # Empty records list
        "use_bulk_api": False
    }
    response = client.post(f"{settings.API_V1_STR}/records/batch", json=payload)
    assert response.status_code == 422 # Pydantic validation error
    assert "Records list cannot be empty" in response.text


# --- Test /bulk/local-file-process endpoint ---
@patch("src.app.routers.sfdc.read_data_from_local_file", new_callable=AsyncMock)
@patch("src.app.routers.sfdc.perform_bulk_operation", new_callable=AsyncMock)
@patch("src.app.routers.sfdc.create_record", new_callable=AsyncMock) # For non-bulk path
async def test_handle_local_file_process_use_bulk_api_true(
    mock_create_record: AsyncMock,
    mock_perform_bulk: AsyncMock,
    mock_read_local_file: AsyncMock,
    client: TestClient,
    mock_salesforce_auth_instance: AsyncMock
):
    mock_read_local_file.return_value = [{"Name": "Local File Acc 1"}]
    mock_perform_bulk.return_value = ("local_file_job_id", [])

    payload = {
        "object_name": "CustomObj__c",
        "use_bulk_api": True,
        "file_path": "/test/data/sample.csv", # Path is symbolic for mock
        "operation_type": "insert" # Assuming 'insert' is a valid op for FileProcessingPayload
    }
    response = client.post(f"{settings.API_V1_STR}/bulk/local-file-process", json=payload)

    assert response.status_code == 200
    json_response = response.json()
    assert json_response["success"] is True
    assert json_response["job_id"] == "local_file_job_id"
    assert "Bulk insert job submitted" in json_response["message"]

    mock_read_local_file.assert_called_once_with("/test/data/sample.csv")
    mock_perform_bulk.assert_called_once()
    # args_list = mock_perform_bulk.call_args_list
    # called_payload = args_list[0][1]['payload'] # Accessing payload from kwargs
    # assert called_payload.object_name == "CustomObj__c"
    # assert called_payload.records == [{"Name": "Local File Acc 1"}]
    mock_create_record.assert_not_called()


@patch("src.app.routers.sfdc.read_data_from_local_file", new_callable=AsyncMock)
@patch("src.app.routers.sfdc.create_record", new_callable=AsyncMock) # For non-bulk path
async def test_handle_local_file_process_use_bulk_api_false(
    mock_create_record: AsyncMock,
    mock_read_local_file: AsyncMock,
    client: TestClient,
    mock_salesforce_auth_instance: AsyncMock
):
    mock_read_local_file.return_value = [{"Name": "Local REST Acc 1"}]
    mock_create_record.return_value = "created_via_local_rest"

    payload = {
        "object_name": "Contact",
        "use_bulk_api": False,
        "file_path": "/test/data/sample_contacts.csv",
        "operation_type": "create"
    }
    response = client.post(f"{settings.API_V1_STR}/bulk/local-file-process", json=payload)

    assert response.status_code == 200
    json_response = response.json()
    assert json_response["success"] is True
    assert json_response["job_id"] is None
    assert "Processed 1 records via REST API" in json_response["message"]
    assert json_response["results"][0]["id"] == "created_via_local_rest"

    mock_read_local_file.assert_called_once_with("/test/data/sample_contacts.csv")
    mock_create_record.assert_called_once_with(
        auth=mock_salesforce_auth_instance, # Check if auth object is passed
        object_name="Contact",
        data={"Name": "Local REST Acc 1"}
    )

@patch("src.app.routers.sfdc.read_data_from_local_file", new_callable=AsyncMock)
async def test_handle_local_file_process_file_not_found(
    mock_read_local_file: AsyncMock,
    client: TestClient,
    mock_salesforce_auth_instance: AsyncMock
):
    mock_read_local_file.side_effect = FileNotFoundError("File /test/data/notfound.csv not found")
    # Or if read_data_from_local_file raises HTTPException(404)
    # from fastapi import HTTPException
    # mock_read_local_file.side_effect = HTTPException(status_code=404, detail="Local file not found")


    payload = {
        "object_name": "Account",
        "use_bulk_api": True,
        "file_path": "/test/data/notfound.csv",
        "operation_type": "create"
    }
    response = client.post(f"{settings.API_V1_STR}/bulk/local-file-process", json=payload)

    # This depends on how read_data_from_local_file surfaces the error.
    # If it raises FileNotFoundError, the general exception handler in main.py catches it (500).
    # If it raises HTTPException(404), then 404.
    # The current read_data_from_local_file raises HTTPException(404).
    assert response.status_code == 404
    assert "Local file not found" in response.json()["detail"]


# --- Test Bulk DML Direct Payload ---
@patch("src.app.routers.sfdc.perform_bulk_operation", new_callable=AsyncMock)
async def test_handle_bulk_dml_direct_payload_success(
    mock_perform_bulk: AsyncMock,
    client: TestClient,
    mock_salesforce_auth_instance: AsyncMock
):
    mock_perform_bulk.return_value = ("direct_payload_job_id", [])
    payload = {
        "object_name": "Opportunity",
        "operation": "update",
        "records": [{"Id": "opp1", "StageName": "Closed Won"}]
    }
    response = client.post(f"{settings.API_V1_STR}/bulk/dml-direct-payload", json=payload)

    assert response.status_code == 200
    json_response = response.json()
    assert json_response["job_id"] == "direct_payload_job_id"
    mock_perform_bulk.assert_called_once()
    # Further assertions on payload passed to mock_perform_bulk can be added


# --- Test Bulk Query Submit ---
@patch("src.app.routers.sfdc.perform_bulk_query", new_callable=AsyncMock)
async def test_handle_submit_bulk_query_job_success(
    mock_perform_bulk_query: AsyncMock,
    client: TestClient,
    mock_salesforce_auth_instance: AsyncMock
):
    mock_perform_bulk_query.return_value = ("query_job_id", [])
    payload = {
        "object_name": "Account", # Optional for query schema, but good to test
        "soql_query": "SELECT Id, Name FROM Account LIMIT 10",
        "operation": "query"
    }
    response = client.post(f"{settings.API_V1_STR}/bulk/query-submit", json=payload)
    assert response.status_code == 200
    json_response = response.json()
    assert json_response["job_id"] == "query_job_id"
    mock_perform_bulk_query.assert_called_once()


# --- Test Bulk Job Status ---
@patch("src.app.routers.sfdc.get_bulk_job_status_and_results", new_callable=AsyncMock)
async def test_handle_get_bulk_job_status_success(
    mock_get_status: AsyncMock,
    client: TestClient,
    mock_salesforce_auth_instance: AsyncMock
):
    job_id = "some_job_id_123"
    mock_status_data = {
        "job_info": {"id": job_id, "state": "JobComplete", "operation": "insert", "object": "Account"},
        "results_data": {"successful_records_csv": "Id,Name\nacc1,Test1", "failed_records_csv": ""}
    }
    mock_get_status.return_value = mock_status_data

    response = client.get(f"{settings.API_V1_STR}/bulk/job/{job_id}/status?is_query_job=false&include_results_data=true")

    assert response.status_code == 200
    json_response = response.json()
    assert json_response["job_id"] == job_id
    assert json_response["state"] == "JobComplete"
    # Check if results are parsed (if include_results_data was true and parsing logic is complete)
    assert len(json_response["results_data"]["successful_records"]) == 1
    assert json_response["results_data"]["successful_records"][0]["Name"] == "Test1"

    mock_get_status.assert_called_once_with(
        auth=mock_salesforce_auth_instance, job_id=job_id, is_query_job=False, fetch_results_if_complete=True
    )


# More tests needed for:
# - Authentication failures (mock get_salesforce_auth_instance to raise errors)
# - Validation errors for various payloads (e.g., missing object_name, invalid operation types)
# - Specific error conditions from Salesforce being mapped correctly by operations layer.
# - Edge cases in data handling for file uploads (e.g., malformed CSV/JSON).
# - Detailed testing of the perform_bulk_operation and perform_bulk_query logic itself (in a separate test file for operations.py).
