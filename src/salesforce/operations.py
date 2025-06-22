# src/salesforce/operations.py
import logging
import csv
import io
from typing import Any, Dict, List, Optional, Tuple

from fastapi import HTTPException, status, UploadFile
from src.core.config import settings
from src.core.schemas import SalesforceBulkOperationPayload, BulkOperationResultDetail
from src.salesforce.client import SalesforceApiClient # Assuming SalesforceApiClient is defined
from src.salesforce.auth import SalesforceAuth # For type hinting, actual instance via dependency

logger = logging.getLogger(settings.APP_NAME)

# --- Standard SObject Operations ---

async def describe_sobject(auth: SalesforceAuth, object_name: str) -> Dict[str, Any]:
    """
    Retrieves the metadata (describe result) for a given SObject.
    """
    client = SalesforceApiClient(auth) # In a real setup, client would be injected or passed
    try:
        logger.info(f"Describing SObject: {object_name}")
        description = await client.get_sobject_describe(object_name)
        logger.info(f"Successfully described SObject: {object_name}")
        return description
    except HTTPException as he:
        # Re-raise HTTPException to be caught by FastAPI error handlers
        raise he
    except Exception as e:
        logger.error(f"Unexpected error describing SObject {object_name}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to describe SObject {object_name}: {str(e)}"
        )

async def create_record(auth: SalesforceAuth, object_name: str, data: Dict[str, Any]) -> str:
    """
    Creates a new record in the specified SObject.
    Returns the ID of the newly created record.
    """
    client = SalesforceApiClient(auth)
    try:
        logger.info(f"Creating record in {object_name} with data: {data}")
        response = await client.create_sobject_record(object_name, data)
        if response.get("success"):
            record_id = response.get("id")
            logger.info(f"Successfully created record in {object_name} with ID: {record_id}")
            return record_id
        else:
            errors = response.get("errors", "Unknown error")
            logger.error(f"Failed to create record in {object_name}. Errors: {errors}")
            # Attempt to provide more specific error if possible
            error_message = "Failed to create record."
            if isinstance(errors, list) and errors:
                error_message = errors[0].get("message", error_message)
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=error_message)
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Unexpected error creating record in {object_name}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create record in {object_name}: {str(e)}"
        )

async def get_record(
    auth: SalesforceAuth, object_name: str, record_id: str, fields: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Retrieves a specific record by its ID from the specified SObject.
    """
    client = SalesforceApiClient(auth)
    try:
        logger.info(f"Retrieving record {record_id} from {object_name} with fields: {fields}")
        record_data = await client.get_sobject_record(object_name, record_id, fields)
        # Salesforce returns attributes, remove if not needed by API consumer
        record_data.pop("attributes", None)
        logger.info(f"Successfully retrieved record {record_id} from {object_name}")
        return record_data
    except HTTPException as he:
        if he.status_code == status.HTTP_404_NOT_FOUND:
            logger.warning(f"Record {record_id} not found in {object_name}.")
        raise he
    except Exception as e:
        logger.error(f"Unexpected error retrieving record {record_id} from {object_name}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve record {object_name}/{record_id}: {str(e)}"
        )

async def update_record(
    auth: SalesforceAuth, object_name: str, record_id: str, data: Dict[str, Any]
) -> None:
    """
    Updates an existing record in the specified SObject.
    Salesforce returns HTTP 204 No Content on successful update.
    """
    client = SalesforceApiClient(auth)
    try:
        logger.info(f"Updating record {record_id} in {object_name} with data: {data}")
        success = await client.update_sobject_record(object_name, record_id, data)
        if success:
            logger.info(f"Successfully updated record {record_id} in {object_name}")
        else:
            # This case should ideally be caught by HTTPStatusError in the client
            logger.error(f"Update operation for {record_id} in {object_name} did not return success (204). This indicates an issue.")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Update operation failed to confirm success.")
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Unexpected error updating record {record_id} in {object_name}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update record {object_name}/{record_id}: {str(e)}"
        )

async def delete_record(auth: SalesforceAuth, object_name: str, record_id: str) -> None:
    """
    Deletes a specific record by its ID from the specified SObject.
    Salesforce returns HTTP 204 No Content on successful deletion.
    """
    client = SalesforceApiClient(auth)
    try:
        logger.info(f"Deleting record {record_id} from {object_name}")
        success = await client.delete_sobject_record(object_name, record_id)
        if success:
            logger.info(f"Successfully deleted record {record_id} from {object_name}")
        else:
            # This case should ideally be caught by HTTPStatusError in the client
            logger.error(f"Delete operation for {record_id} in {object_name} did not return success (204). This indicates an issue.")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Delete operation failed to confirm success.")
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Unexpected error deleting record {record_id} from {object_name}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete record {object_name}/{record_id}: {str(e)}"
        )

async def upsert_record(
    auth: SalesforceAuth, object_name: str, external_id_field: str, external_id_value: str, data: Dict[str, Any]
) -> Tuple[str, bool]: # (record_id, created_flag)
    """
    Upserts a record based on an external ID.
    Returns the record ID and a boolean indicating if the record was created.
    """
    client = SalesforceApiClient(auth)
    try:
        logger.info(f"Upserting record in {object_name} via {external_id_field}={external_id_value} with data: {data}")
        response = await client.upsert_sobject_record(object_name, external_id_field, external_id_value, data)
        # Response for PATCH upsert:
        # - 201 Created: {"id": "...", "success": true, "errors": [], "created": true} (if 'created' field is available)
        # - 200 OK (Updated): {"id": "...", "success": true, "errors": [], "created": false}
        # - 204 No Content (Updated, if configured or no body returned by SF)
        # For simplicity, we check 'created' if available, otherwise assume update if not 201.
        # The client.upsert_sobject_record should ideally normalize this.
        # Let's assume client returns a dict with 'id' and 'created' (bool)

        record_id = response.get("id")
        created = response.get("created", False) # Default to False if 'created' not in response

        if response.get("success") is False : # Check if SF reported success:false
            errors = response.get("errors", "Unknown upsert error")
            logger.error(f"Upsert operation failed for {object_name}/{external_id_field}={external_id_value}. Errors: {errors}")
            error_message = "Upsert operation failed."
            if isinstance(errors, list) and errors:
                error_message = errors[0].get("message", error_message)
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=error_message)

        if not record_id and response.get("status") == "updated_no_content": # From client workaround
            record_id = external_id_value # If it was an update by extId and no body, Id won't be in response.
                                        # This is not ideal, Salesforce should return ID.
                                        # For custom ext ID fields, the extID value is NOT the Salesforce ID.
                                        # This part needs careful testing with actual SF responses.
                                        # Best to query by externalId after upsert if ID is critical and not returned.
            logger.warning(f"Upsert (update) for {object_name}/{external_id_field}={external_id_value} returned 204. Record ID might not be the external ID.")
            # For now, we will rely on the client's response format or what SF actually returns.
            # If SF returns 204, it implies an update. It won't return the ID in the body.
            # The `id` field in the response from `client.upsert_sobject_record` should be correct.

        if not record_id: # If still no record_id
             logger.error(f"Upsert for {object_name}/{external_id_field}={external_id_value} did not return a record ID.")
             raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Upsert successful but record ID not found in response.")


        status_str = "created" if created else "updated"
        logger.info(f"Successfully {status_str} record in {object_name} via {external_id_field}={external_id_value}. Record ID: {record_id}")
        return record_id, created

    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Unexpected error upserting record in {object_name} via {external_id_field}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to upsert record in {object_name}: {str(e)}"
        )


# --- Bulk Operations (using Bulk API 2.0) ---

# Removed _prepare_csv_data as it's now in data_handler.py (convert_records_to_csv_string)
from src.utils.data_handler import convert_records_to_csv_string

async def perform_bulk_operation(
    auth: SalesforceAuth,
    payload: SalesforceBulkOperationPayload
) -> Tuple[str, List[BulkOperationResultDetail]]: # Returns (job_id, results)
    """
    Performs a bulk operation (insert, update, upsert, delete) using Bulk API 2.0.
    This is a simplified version; real bulk processing involves polling job status.
    For this example, we'll assume a small enough batch that might complete quickly
    or we just return the job ID for client to poll.

    This function will:
    1. Create a bulk job.
    2. Upload data (if DML).
    3. Close the job.
    4. (Optionally, if quick) Poll for completion and get results.
       For now, we'll just return job_id. Results fetching needs separate endpoints/logic.
    """
    client = SalesforceApiClient(auth)
    job_id = None
    try:
        logger.info(f"Starting Bulk API 2.0 operation: {payload.operation} on {payload.object_name}")

        # 1. Create Bulk Job
        job_info = await client.create_bulk_ingest_job(
            object_name=payload.object_name,
            operation=payload.operation,
            external_id_field=payload.external_id_field if payload.operation == "upsert" else None
        )
        job_id = job_info.get("id")
        content_url = job_info.get("contentUrl") # Relative URL for data upload

        if not job_id or not content_url:
            logger.error(f"Failed to create bulk job. Response: {job_info}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to initialize bulk job with Salesforce.")

        logger.info(f"Bulk job created. ID: {job_id}, Operation: {payload.operation}, Object: {payload.object_name}")

        # 2. Upload Data (if DML operation)
        if payload.operation in ['insert', 'update', 'upsert', 'delete', 'hardDelete']:
            if not payload.records:
                await client.update_bulk_job_state(job_id, "Aborted") # Abort if no records
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No records provided for bulk DML operation.")

            # Determine field order for CSV - use keys from first record if available, or let convert_records_to_csv_string handle it
            field_order = list(payload.records[0].keys()) if payload.records else None
            csv_data = convert_records_to_csv_string(payload.records, field_order=field_order)

            if not csv_data.strip() or len(csv_data.strip().split('\n')) < 2: # Check if empty or only header
                 await client.update_bulk_job_state(job_id, "Aborted")
                 raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="CSV data is empty or contains only headers.")

            logger.info(f"Uploading {len(payload.records)} records to bulk job {job_id}...")
            upload_success = await client.upload_bulk_job_data(content_url, csv_data)
            if not upload_success:
                # Attempt to abort the job if upload fails
                await client.update_bulk_job_state(job_id, "Aborted")
                logger.error(f"Failed to upload data for bulk job {job_id}.")
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to upload data to Salesforce bulk job.")
            logger.info(f"Data uploaded successfully for bulk job {job_id}.")

        # 3. Close the Job (transitions state to UploadComplete, SF starts processing)
        logger.info(f"Closing bulk job {job_id} to start processing.")
        closed_job_info = await client.update_bulk_job_state(job_id, "UploadComplete")
        logger.info(f"Bulk job {job_id} closed. Current state: {closed_job_info.get('state')}")

        # At this point, the job is submitted. Salesforce processes it asynchronously.
        # For a production system, you'd store the job_id and have a separate mechanism
        # (e.g., polling endpoint, webhook if SF supports it for Bulk API 2.0) to get results.
        # For this implementation, we return the job_id.
        # The client/caller is responsible for checking job status and fetching results later.
        # We won't poll here to keep this synchronous endpoint responsive.

        # Placeholder for results - in a real scenario, these are fetched asynchronously.
        results_summary: List[BulkOperationResultDetail] = []

        return job_id, results_summary # results_summary will be empty for now

    except HTTPException as he:
        # If a job was created and an error occurs later, try to abort it.
        if job_id:
            try:
                logger.warning(f"Error during bulk operation for job {job_id}. Attempting to abort job.")
                await client.update_bulk_job_state(job_id, "Aborted")
            except Exception as abort_exc:
                logger.error(f"Failed to abort job {job_id} after error: {abort_exc}", exc_info=True)
        raise he
    except Exception as e:
        if job_id: # Try to abort if job was created
            try:
                await client.update_bulk_job_state(job_id, "Aborted")
            except Exception as abort_exc:
                logger.error(f"Failed to abort job {job_id} after unexpected error: {abort_exc}", exc_info=True)
        logger.error(f"Unexpected error during bulk {payload.operation} for {payload.object_name}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to perform bulk operation: {str(e)}"
        )

# --- Bulk Query Operations ---
async def perform_bulk_query(
    auth: SalesforceAuth,
    object_name: str, # Not directly used by Bulk API Query job creation, but good for context
    soql_query: str,
    operation: str = "query" # "query" or "queryAll"
) -> Tuple[str, List[Dict[str, Any]]]: # Returns (job_id, initial_results_if_any_or_empty_list)
    """
    Submits a Bulk API 2.0 query job.
    Returns the job ID. Results need to be fetched separately using the job ID.
    """
    client = SalesforceApiClient(auth)
    job_id = None
    try:
        logger.info(f"Starting Bulk API 2.0 query job for object: {object_name} with SOQL: {soql_query[:100]}...")

        job_config = {
            "operation": operation, # "query" or "queryAll"
            "query": soql_query,
            # "contentType": "CSV", # Default, can be JSON, XML for Bulk API 1.0, but 2.0 query results are CSV
            # "columnDelimiter": "COMMA", # Default
            # "lineEnding": "LF" # Default
        }
        # Bulk API 2.0 query jobs are slightly different, they are under /jobs/query
        # The client method `_request` with `is_bulk_api=True` should handle the base path.
        # The endpoint for creating a query job is simply "/jobs/query"
        response = await client._request("POST", "/jobs/query", json_data=job_config, is_bulk_api=True)
        job_info = response.json()

        job_id = job_info.get("id")
        if not job_id:
            logger.error(f"Failed to create bulk query job. Response: {job_info}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to initialize bulk query job with Salesforce.")

        logger.info(f"Bulk query job created. ID: {job_id}. State: {job_info.get('state')}")
        # Query jobs don't need data upload or explicit closing (state becomes JobComplete when done).
        # Caller needs to poll for status and then retrieve results.
        return job_id, [] # Return job_id and empty list for initial results

    except HTTPException as he:
        if job_id: # If job was created, try to abort (though query jobs might not need explicit abort like ingest)
             try:
                # Aborting a query job: PATCH /services/data/vXX.X/jobs/query/{jobId} with {"state": "Aborted"}
                await client._request("PATCH", f"/jobs/query/{job_id}", json_data={"state": "Aborted"}, is_bulk_api=True)
             except Exception as abort_exc:
                logger.error(f"Failed to abort bulk query job {job_id} after error: {abort_exc}", exc_info=True)
        raise he
    except Exception as e:
        if job_id:
             try:
                await client._request("PATCH", f"/jobs/query/{job_id}", json_data={"state": "Aborted"}, is_bulk_api=True)
             except Exception as abort_exc:
                logger.error(f"Failed to abort bulk query job {job_id} after unexpected error: {abort_exc}", exc_info=True)
        logger.error(f"Unexpected error during bulk query for {object_name}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to perform bulk query: {str(e)}"
        )

async def get_bulk_job_status_and_results(
    auth: SalesforceAuth,
    job_id: str,
    is_query_job: bool = False
) -> Dict[str, Any]:
    """
    Checks the status of a bulk job (ingest or query) and fetches results if completed.
    This is a simplified polling mechanism.
    """
    client = SalesforceApiClient(auth)
    job_endpoint_base = "/jobs/query" if is_query_job else "/jobs/ingest"

    try:
        logger.info(f"Getting status for bulk job ID: {job_id} (Query job: {is_query_job})")
        job_info = await client._request("GET", f"{job_endpoint_base}/{job_id}", is_bulk_api=True)
        job_info = job_info.json()

        results_data = None
        if job_info.get("state") == "JobComplete":
            logger.info(f"Bulk job {job_id} is complete. Fetching results...")
            if is_query_job:
                # Query results are fetched using a GET to /jobs/query/{jobId}/results
                # The response includes Sforce-Locator. Use ' nächsten Ergebnisse abrufen.
                # For initial batch:
                results_response = await client._request("GET", f"{job_endpoint_base}/{job_id}/results", is_bulk_api=True, params={"maxRecords": 10000}) # Limit for demo
                # This response is CSV. Need to parse it.
                # The 'Sforce-Locator' header indicates if there are more results.
                # For simplicity, we'll just return the first batch as text.
                # A robust solution would handle pagination and CSV parsing.
                results_data = results_response.text # Raw CSV data
                job_info["sforce_locator"] = results_response.headers.get("Sforce-Locator")

            else: # Ingest job
                successful_csv = await client.get_bulk_job_successful_results(job_id)
                failed_csv = await client.get_bulk_job_failed_results(job_id)
                results_data = {
                    "successful_records_csv": successful_csv,
                    "failed_records_csv": failed_csv
                }
        elif job_info.get("state") == "Failed":
            logger.error(f"Bulk job {job_id} failed. Error: {job_info.get('errorMessage')}")
            # Optionally fetch failed records if it's an ingest job and they are available

        return {"job_info": job_info, "results_data": results_data}

    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Unexpected error getting status/results for bulk job {job_id}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get bulk job status/results: {str(e)}"
        )
