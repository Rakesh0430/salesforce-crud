# shared/src/salesforce/operations.py
import logging
from typing import Any, Dict, List, Optional, Tuple
import httpx

from fastapi import HTTPException, status
from core.schemas import SalesforceBulkOperationPayload, BulkOperationResultDetail
from salesforce.client import SalesforceApiClient
from utils.data_handler import convert_records_to_csv_string
from core.config import settings

logger = logging.getLogger(settings.APP_NAME)

async def describe_sobject(client: SalesforceApiClient, object_name: str) -> Dict[str, Any]:
    try:
        logger.info(f"Describing SObject: {object_name}")
        description = await client.get_sobject_describe(object_name)
        logger.info(f"Successfully described SObject: {object_name}")
        return description
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text) from e
    except Exception as e:
        logger.error(f"Unexpected error describing SObject {object_name}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to describe SObject {object_name}: {str(e)}")

async def create_record(client: SalesforceApiClient, object_name: str, data: Dict[str, Any]) -> str:
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
            error_message = "Failed to create record."
            if isinstance(errors, list) and errors:
                error_message = errors[0].get("message", error_message)
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=error_message)
    except HTTPException as he:
        raise he
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text) from e
    except Exception as e:
        logger.error(f"Unexpected error creating record in {object_name}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to create record in {object_name}: {str(e)}")

async def get_record(client: SalesforceApiClient, object_name: str, record_id: str, fields: Optional[List[str]] = None) -> Dict[str, Any]:
    try:
        logger.info(f"Retrieving record {record_id} from {object_name} with fields: {fields}")
        record_data = await client.get_sobject_record(object_name, record_id, fields)
        record_data.pop("attributes", None)
        logger.info(f"Successfully retrieved record {record_id} from {object_name}")
        return record_data
    except httpx.HTTPStatusError as e:
        if e.response.status_code == status.HTTP_404_NOT_FOUND:
            logger.warning(f"Record {record_id} not found in {object_name}.")
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text) from e
    except Exception as e:
        logger.error(f"Unexpected error retrieving record {record_id} from {object_name}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to retrieve record {object_name}/{record_id}: {str(e)}")

async def update_record(client: SalesforceApiClient, object_name: str, record_id: str, data: Dict[str, Any]) -> None:
    try:
        logger.info(f"Updating record {record_id} in {object_name} with data: {data}")
        success = await client.update_sobject_record(object_name, record_id, data)
        if not success:
            logger.error(f"Update operation for {record_id} in {object_name} did not return success (204).")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Update operation failed.")
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text) from e
    except Exception as e:
        logger.error(f"Unexpected error updating record {record_id} in {object_name}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to update record {object_name}/{record_id}: {str(e)}")

async def delete_record(client: SalesforceApiClient, object_name: str, record_id: str) -> None:
    try:
        logger.info(f"Deleting record {record_id} from {object_name}")
        success = await client.delete_sobject_record(object_name, record_id)
        if not success:
            logger.error(f"Delete operation for {record_id} in {object_name} did not return success (204).")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Delete operation failed.")
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text) from e
    except Exception as e:
        logger.error(f"Unexpected error deleting record {record_id} from {object_name}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to delete record {object_name}/{record_id}: {str(e)}")

async def upsert_record(client: SalesforceApiClient, object_name: str, external_id_field: str, external_id_value: str, data: Dict[str, Any]) -> Tuple[str, bool]:
    try:
        logger.info(f"Upserting record in {object_name} via {external_id_field}={external_id_value}")
        response = await client.upsert_sobject_record(object_name, external_id_field, external_id_value, data)
        record_id = response.get("id")
        created = response.get("created", False)
        if not record_id:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Upsert successful but record ID not found.")
        return record_id, created
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text) from e
    except Exception as e:
        logger.error(f"Unexpected error upserting record: {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to upsert record: {str(e)}")

async def perform_bulk_operation(client: SalesforceApiClient, payload: SalesforceBulkOperationPayload) -> Tuple[str, List[BulkOperationResultDetail]]:
    job_id = None
    try:
        logger.info(f"Starting Bulk API 2.0 operation: {payload.operation} on {payload.object_name}")
        job_info = await client.create_bulk_ingest_job(object_name=payload.object_name, operation=payload.operation, external_id_field=payload.external_id_field)
        job_id = job_info.get("id")
        content_url = job_info.get("contentUrl")
        if not job_id or not content_url:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to initialize bulk job.")

        csv_data = convert_records_to_csv_string(payload.records)
        if not csv_data.strip():
            await client.update_bulk_job_state(job_id, "Aborted")
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No records to process.")

        await client.upload_bulk_job_data(content_url, csv_data)
        await client.update_bulk_job_state(job_id, "UploadComplete")
        return job_id, []
    except httpx.HTTPStatusError as e:
        if job_id:
            await client.update_bulk_job_state(job_id, "Aborted")
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text) from e
    except Exception as e:
        if job_id:
            await client.update_bulk_job_state(job_id, "Aborted")
        logger.error(f"Error during bulk operation for job {job_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

async def perform_bulk_query(client: SalesforceApiClient, soql_query: str, operation: str) -> Tuple[str, List[Dict[str, Any]]]:
    job_id = None
    try:
        job_config = {"operation": operation, "query": soql_query}
        response = await client._request("POST", "/jobs/query", json_data=job_config, is_bulk_api=True)
        job_info = response.json()
        job_id = job_info.get("id")
        if not job_id:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to initialize bulk query job.")
        return job_id, []
    except httpx.HTTPStatusError as e:
        if job_id:
            await client._request("PATCH", f"/jobs/query/{job_id}", json_data={"state": "Aborted"}, is_bulk_api=True)
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text) from e
    except Exception as e:
        if job_id:
            await client._request("PATCH", f"/jobs/query/{job_id}", json_data={"state": "Aborted"}, is_bulk_api=True)
        logger.error(f"Error during bulk query: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

async def get_bulk_job_status_and_results(client: SalesforceApiClient, job_id: str, is_query_job: bool) -> Dict[str, Any]:
    job_endpoint_base = "/jobs/query" if is_query_job else "/jobs/ingest"
    try:
        job_info_response = await client._request("GET", f"{job_endpoint_base}/{job_id}", is_bulk_api=True)
        job_info = await job_info_response.json()
        results_data = None
        if job_info.get("state") == "JobComplete":
            if is_query_job:
                results_response = await client._request("GET", f"{job_endpoint_base}/{job_id}/results", is_bulk_api=True)
                results_data = results_response.text
            else:
                successful_csv = await client.get_bulk_job_successful_results(job_id)
                failed_csv = await client.get_bulk_job_failed_results(job_id)
                results_data = {"successful_records_csv": successful_csv, "failed_records_csv": failed_csv}
        return {"job_info": job_info, "results_data": results_data}
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text) from e
    except Exception as e:
        logger.error(f"Error getting bulk job status for job {job_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
