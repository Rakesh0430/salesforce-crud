# bulk-service/src/app/routers/bulk.py
from fastapi import APIRouter, HTTPException, Depends, status, UploadFile, File, Query
from typing import List, Optional
import logging

from core.schemas import (
    SalesforceBulkOperationPayload, FileProcessingPayload,
    BulkOperationResponse, BulkJobStatusResponse,
    BulkQueryJobSubmitPayload, BulkDMLJobSubmitPayload,
    BatchRecordProcessingPayload, BulkOperationResultDetail
)
from salesforce.client import SalesforceApiClient, get_salesforce_api_client
from salesforce.operations import (
    perform_bulk_operation, get_bulk_job_status_and_results, perform_bulk_query,
    create_record, update_record, upsert_record, delete_record
)
from utils.data_handler import read_file_data_for_bulk, read_data_from_local_file, parse_csv_string_to_records
from core.config import settings

logger = logging.getLogger(settings.APP_NAME)
router = APIRouter()

@router.post(
    "/bulk/dml-file-upload",
    response_model=BulkOperationResponse,
    summary="Submit Bulk DML Job from Uploaded File",
    description="Submits a bulk DML operation using data from an uploaded file."
)
async def handle_bulk_dml_from_file_upload(
    object_name: str = Query(..., description="Salesforce SObject API name."),
    operation_type: str = Query(..., description="DML Operation: insert, update, upsert, delete, hardDelete."),
    file: UploadFile = File(..., description="File (CSV, JSON, or XML) with records."),
    external_id_field: Optional[str] = Query(None, description="External ID for upsert."),
    client: SalesforceApiClient = Depends(get_salesforce_api_client)
):
    valid_ops = ["insert", "update", "upsert", "delete", "harddelete"]
    op_type_lower = operation_type.lower()
    if op_type_lower not in valid_ops:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid operation_type. Must be one of {valid_ops}.")
    if op_type_lower == "upsert" and not external_id_field:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="external_id_field is required for upsert.")

    try:
        records = await read_file_data_for_bulk(file)
        if not records:
            return BulkOperationResponse(success=True, message="File is empty. No job submitted.", job_id=None)

        bulk_payload = SalesforceBulkOperationPayload(
            object_name=object_name,
            operation=op_type_lower,
            records=records,
            external_id_field=external_id_field
        )
        job_id, _ = await perform_bulk_operation(client=client, payload=bulk_payload)

        return BulkOperationResponse(
            success=True,
            message=f"Bulk {op_type_lower} job submitted for {object_name}. Job ID: {job_id}.",
            job_id=job_id
        )
    except HTTPException as he:
        logger.error(f"HTTPException in bulk DML (file upload): {he.detail}", exc_info=settings.DEBUG_MODE)
        raise he
    except Exception as e:
        logger.error(f"Error in bulk DML (file upload): {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@router.post(
    "/bulk/local-file-process",
    response_model=BulkOperationResponse,
    summary="Process Records from Local File Path",
    description="Processes records from a server-side file path, using Bulk API or iterative REST."
)
async def handle_process_records_from_local_file(
    payload: FileProcessingPayload,
    client: SalesforceApiClient = Depends(get_salesforce_api_client)
):
    try:
        records = await read_data_from_local_file(payload.file_path)
        if not records:
            return BulkOperationResponse(success=True, message=f"No records in {payload.file_path}.", job_id=None)

        if payload.use_bulk_api:
            bulk_req_payload = SalesforceBulkOperationPayload(
                object_name=payload.object_name,
                operation=payload.operation_type,
                records=records,
                external_id_field=payload.external_id_field
            )
            job_id, _ = await perform_bulk_operation(client=client, payload=bulk_req_payload)
            return BulkOperationResponse(
                success=True,
                message=f"Bulk {payload.operation_type} job submitted from local file. Job ID: {job_id}.",
                job_id=job_id
            )
        else:
            results_summary = []
            success_count = 0
            failed_count = 0
            for i, record_data in enumerate(records):
                single_op_result = {"record_index": i, "success": False, "id": None, "errors": None}
                try:
                    op_type = payload.operation_type
                    if op_type == "create":
                        created_id = await create_record(client, payload.object_name, record_data)
                        single_op_result.update({"id": created_id, "success": True})
                    success_count += 1
                except Exception as e_single:
                    single_op_result["errors"] = [{"message": str(e_single)}]
                    failed_count += 1
                results_summary.append(single_op_result)
            return BulkOperationResponse(
                success=(failed_count == 0),
                message=f"Processed {len(records)} records via REST API. {success_count} successful, {failed_count} failed.",
                job_id=None,
                results=[BulkOperationResultDetail(**res) for res in results_summary]
            )
    except Exception as e:
        logger.error(f"Error processing local file {payload.file_path}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post(
    "/bulk/dml-direct-payload",
    response_model=BulkOperationResponse,
    summary="Submit Bulk DML Job with Direct Payload",
    description="Submits a bulk DML operation with a list of records in the payload."
)
async def handle_bulk_dml_direct_payload(
    payload: BulkDMLJobSubmitPayload,
    client: SalesforceApiClient = Depends(get_salesforce_api_client)
):
    if payload.operation.lower() == "upsert" and not payload.external_id_field:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="external_id_field is required for upsert.")
    if not payload.records:
         raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Records list cannot be empty.")

    try:
        internal_payload = SalesforceBulkOperationPayload(
            object_name=payload.object_name,
            operation=payload.operation,
            records=payload.records,
            external_id_field=payload.external_id_field
        )
        job_id, _ = await perform_bulk_operation(client=client, payload=internal_payload)
        return BulkOperationResponse(
            success=True,
            message=f"Bulk {payload.operation} job submitted. Job ID: {job_id}.",
            job_id=job_id
        )
    except Exception as e:
        logger.error(f"Error in bulk DML (direct payload): {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post(
    "/bulk/query-submit",
    response_model=BulkOperationResponse,
    summary="Submit a Bulk SOQL Query Job",
    description="Submits an asynchronous SOQL query via Bulk API 2.0."
)
async def handle_submit_bulk_query_job(
    payload: BulkQueryJobSubmitPayload,
    client: SalesforceApiClient = Depends(get_salesforce_api_client)
):
    try:
        job_id, _ = await perform_bulk_query(
            client=client,
            soql_query=payload.soql_query,
            operation=payload.operation
        )
        return BulkOperationResponse(
            success=True,
            message=f"Bulk {payload.operation} job submitted. Job ID: {job_id}.",
            job_id=job_id
        )
    except Exception as e:
        logger.error(f"Error submitting bulk query: {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get(
    "/bulk/job/{job_id}/status",
    response_model=BulkJobStatusResponse,
    summary="Get Bulk Job Status and Results",
    description="Retrieves the status of a Bulk API 2.0 job. If complete, can include results."
)
async def handle_get_bulk_job_status_and_results_endpoint(
    job_id: str,
    is_query_job: bool = Query(False, description="Set to true for query jobs."),
    client: SalesforceApiClient = Depends(get_salesforce_api_client)
):
    try:
        status_and_results = await get_bulk_job_status_and_results(
            client=client,
            job_id=job_id,
            is_query_job=is_query_job
        )
        job_info = status_and_results.get("job_info", {})
        results_data = status_and_results.get("results_data")
        parsed_results = None
        if results_data:
            if is_query_job and isinstance(results_data, str):
                parsed_results = parse_csv_string_to_records(results_data)
            elif not is_query_job and isinstance(results_data, dict):
                parsed_results = {
                    "successful_records": parse_csv_string_to_records(results_data.get("successful_records_csv","")),
                    "failed_records": parse_csv_string_to_records(results_data.get("failed_records_csv","")),
                    "unprocessed_records": parse_csv_string_to_records(results_data.get("unprocessed_records_csv",""))
                }

        job_info_data = status_and_results.get("job_info", {})
        return BulkJobStatusResponse(
            job_id=job_info_data.get("id", job_id),
            state=job_info_data.get("state"),
            operation=job_info_data.get("operation"),
            object_name=job_info_data.get("object"),
            error_message=job_info_data.get("errorMessage"),
            records_processed=job_info_data.get("numberRecordsProcessed"),
            records_failed=job_info_data.get("numberRecordsFailed"),
            job_info_details=job_info_data,
            results_data=parsed_results,
            raw_results_csv=results_data
        )
    except Exception as e:
        logger.error(f"Error getting status for bulk job {job_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@router.post(
    "/records/batch",
    response_model=BulkOperationResponse,
    summary="Process a Batch of Records (Bulk or REST)",
    description="Processes a list of records using Bulk API or iterative REST calls."
)
async def handle_batch_record_processing(
    payload: BatchRecordProcessingPayload,
    client: SalesforceApiClient = Depends(get_salesforce_api_client)
):
    try:
        if payload.use_bulk_api:
            bulk_req_payload = SalesforceBulkOperationPayload(
                object_name=payload.object_name,
                operation=payload.operation_type,
                records=payload.records,
                external_id_field=payload.external_id_field
            )
            job_id, _ = await perform_bulk_operation(client=client, payload=bulk_req_payload)
            return BulkOperationResponse(
                success=True,
                message=f"Bulk {payload.operation_type} job submitted for {len(payload.records)} records. Job ID: {job_id}.",
                job_id=job_id
            )
        else:
            results_summary = []
            success_count = 0
            failed_count = 0
            for i, record_data in enumerate(payload.records):
                single_op_result = {"record_index": i, "success": False, "id": None, "errors": None}
                try:
                    op_type = payload.operation_type
                    if op_type == "create":
                        created_id = await create_record(client, payload.object_name, record_data)
                        single_op_result.update({"id": created_id, "success": True})
                    success_count += 1
                except Exception as e_single:
                    single_op_result["errors"] = [{"message": str(e_single)}]
                    failed_count += 1
                results_summary.append(single_op_result)

            return BulkOperationResponse(
                success=(failed_count == 0),
                message=f"Processed {len(payload.records)} records via REST API. {success_count} successful, {failed_count} failed.",
                job_id=None,
                results=[BulkOperationResultDetail(**res) for res in results_summary]
            )
    except Exception as e:
        logger.error(f"Error processing batch for {payload.object_name}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
