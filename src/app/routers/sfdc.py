# src/app/routers/sfdc.py
from fastapi import APIRouter, HTTPException, Depends, Body, status, UploadFile, File, Query
from typing import Any, Dict, List, Optional
import logging

from src.core.schemas import (
    SalesforceOperationPayload, SalesforceBulkOperationPayload, FileProcessingPayload,
    OperationResponse, BulkOperationResponse, DescribeResponse, BulkJobStatusResponse,
    BulkQueryJobSubmitPayload, BulkDMLJobSubmitPayload # More specific payload types
)
from src.salesforce.auth import get_salesforce_auth_instance
from src.salesforce.operations import (
    create_record, get_record, update_record, delete_record, upsert_record,
    perform_bulk_operation, describe_sobject,
    get_bulk_job_status_and_results, perform_bulk_query
)
from src.utils.data_handler import read_file_data_for_bulk, read_data_from_local_file, parse_csv_string_to_records
from src.core.config import settings
from src.salesforce.client import SalesforceApiClient # For type hinting

logger = logging.getLogger(settings.APP_NAME)
router = APIRouter()

# Dependency for SalesforceAuth instance
# This ensures that get_salesforce_auth_instance is called for each request needing auth,
# and the same SalesforceAuth object (and thus its token cache) is reused.
async def get_sfdc_auth() -> SalesforceAuth:
    return await get_salesforce_auth_instance()


@router.post(
    "/records/create", # Path changed for clarity (records vs sobjects)
    response_model=OperationResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a Salesforce Record",
    description="Creates a new record for the specified SObject using REST API."
)
async def handle_create_record_endpoint(
    payload: SalesforceOperationPayload = Body(...),
    auth: SalesforceAuth = Depends(get_sfdc_auth) # Use the simplified dependency
):
    try:
        if not payload.data:
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Field 'data' is required for creating a record.")
        record_id = await create_record(
            auth=auth,
            object_name=payload.object_name,
            data=payload.data
        )
        return OperationResponse(
            success=True,
            message=f"Record created successfully in {payload.object_name}.",
            record_id=record_id
        )
    except HTTPException as he:
        logger.error(f"HTTPException creating record in {payload.object_name}: {he.detail}", exc_info=settings.DEBUG_MODE)
        raise he
    except Exception as e:
        logger.error(f"Error creating record in {payload.object_name}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {str(e)}")

@router.get(
    "/records/{object_name}/{record_id}",
    response_model=OperationResponse,
    summary="Retrieve a Salesforce Record",
    description="Retrieves a specific record by its ID from the specified SObject using REST API."
)
async def handle_get_record_endpoint(
    object_name: str,
    record_id: str,
    fields: Optional[str] = Query(None, description="Comma-separated list of fields to retrieve."),
    auth: SalesforceAuth = Depends(get_sfdc_auth)
):
    try:
        field_list = fields.split(',') if fields else None
        record_data = await get_record(
            auth=auth,
            object_name=object_name,
            record_id=record_id,
            fields=field_list
        )
        # get_record operation itself should raise HTTPException(404) if not found
        return OperationResponse(
            success=True,
            message="Record retrieved successfully.",
            data=record_data
        )
    except HTTPException as he:
        logger.error(f"HTTPException retrieving {object_name}/{record_id}: {he.detail}", exc_info=settings.DEBUG_MODE)
        raise he
    except Exception as e:
        logger.error(f"Error retrieving record {object_name}/{record_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {str(e)}")

@router.patch(
    "/records/update",
    response_model=OperationResponse,
    summary="Update a Salesforce Record by ID",
    description="Updates an existing record in the specified SObject using REST API. Requires object_name, record_id, and data in payload."
)
async def handle_update_record_endpoint(
    payload: SalesforceOperationPayload = Body(...),
    auth: SalesforceAuth = Depends(get_sfdc_auth)
):
    if not payload.record_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Field 'record_id' is required for update operations.")
    if not payload.data:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Field 'data' is required for update operations.")
    try:
        await update_record(
            auth=auth,
            object_name=payload.object_name,
            record_id=payload.record_id,
            data=payload.data
        )
        return OperationResponse(
            success=True,
            message=f"Record {payload.record_id} in {payload.object_name} updated successfully."
        )
    except HTTPException as he:
        logger.error(f"HTTPException updating {payload.object_name}/{payload.record_id}: {he.detail}", exc_info=settings.DEBUG_MODE)
        raise he
    except Exception as e:
        logger.error(f"Error updating record {payload.object_name}/{payload.record_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {str(e)}")

@router.post(
    "/records/upsert",
    response_model=OperationResponse,
    summary="Upsert a Salesforce Record by External ID",
    description="Upserts a record in the specified SObject using an external ID field. Requires object_name, external_id_field, external_id_value (in record_id field of payload for now, or dedicated field), and data."
)
async def handle_upsert_record_endpoint(
    payload: SalesforceOperationPayload = Body(...),
    auth: SalesforceAuth = Depends(get_sfdc_auth)
):
    # Using payload.record_id to hold the external_id_value for this endpoint
    if not payload.external_id_field or not payload.record_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Fields 'external_id_field' and 'record_id' (as external_id_value) are required for upsert.")
    if not payload.data:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Field 'data' is required for upsert operation.")

    try:
        new_record_id, created = await upsert_record(
            auth=auth,
            object_name=payload.object_name,
            external_id_field=payload.external_id_field,
            external_id_value=payload.record_id, # record_id field holds the external ID value
            data=payload.data
        )
        status_message = "created" if created else "updated"
        return OperationResponse(
            success=True,
            message=f"Record {status_message} successfully in {payload.object_name} via external ID {payload.external_id_field}. Salesforce ID: {new_record_id}",
            record_id=new_record_id, # This is the actual Salesforce Record ID
            data={"created": created, "external_id_value": payload.record_id}
        )
    except HTTPException as he:
        logger.error(f"HTTPException upserting {payload.object_name}/{payload.external_id_field}={payload.record_id}: {he.detail}", exc_info=settings.DEBUG_MODE)
        raise he
    except Exception as e:
        logger.error(f"Error upserting record {payload.object_name}/{payload.external_id_field}={payload.record_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {str(e)}")

@router.delete(
    "/records/{object_name}/{record_id}",
    response_model=OperationResponse,
    summary="Delete a Salesforce Record by ID",
    description="Deletes a specific record by its ID from the specified SObject using REST API."
)
async def handle_delete_record_endpoint(
    object_name: str,
    record_id: str,
    auth: SalesforceAuth = Depends(get_sfdc_auth)
):
    try:
        await delete_record(
            auth=auth,
            object_name=object_name,
            record_id=record_id
        )
        return OperationResponse(
            success=True,
            message=f"Record {record_id} in {object_name} deleted successfully."
        )
    except HTTPException as he:
        logger.error(f"HTTPException deleting {object_name}/{record_id}: {he.detail}", exc_info=settings.DEBUG_MODE)
        raise he
    except Exception as e:
        logger.error(f"Error deleting record {object_name}/{record_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {str(e)}")

@router.get(
    "/sobjects/{object_name}/describe",
    response_model=DescribeResponse,
    summary="Describe a Salesforce SObject",
    description="Retrieves metadata (fields, child relationships, etc.) for a specified SObject."
)
async def handle_describe_sobject_endpoint(
    object_name: str,
    auth: SalesforceAuth = Depends(get_sfdc_auth)
):
    try:
        description = await describe_sobject(auth=auth, object_name=object_name)
        return DescribeResponse(
            success=True,
            message=f"Successfully described SObject {object_name}.",
            data=description
        )
    except HTTPException as he:
        logger.error(f"HTTPException describing SObject {object_name}: {he.detail}", exc_info=settings.DEBUG_MODE)
        raise he
    except Exception as e:
        logger.error(f"Error describing SObject {object_name}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {str(e)}")


# --- Bulk Operations ---

@router.post(
    "/bulk/dml-file-upload",
    response_model=BulkOperationResponse,
    summary="Submit Bulk DML Job from Uploaded File",
    description="Submits a bulk DML operation (insert, update, upsert, delete, hardDelete) using data from an uploaded file (CSV, JSON, or XML)."
)
async def handle_bulk_dml_from_file_upload(
    object_name: str = Query(..., description="Salesforce SObject API name."),
    operation_type: str = Query(..., description="DML Operation: insert, update, upsert, delete, hardDelete."),
    file: UploadFile = File(..., description="File (CSV, JSON, or XML) containing records for the bulk operation."),
    external_id_field: Optional[str] = Query(None, description="External ID field API name, required for upsert operation."),
    auth: SalesforceAuth = Depends(get_sfdc_auth)
):
    valid_ops = ["insert", "update", "upsert", "delete", "harddelete"] # Salesforce uses 'hardDelete'
    op_type_lower = operation_type.lower()
    if op_type_lower not in valid_ops:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid operation_type. Must be one of {valid_ops}.")
    if op_type_lower == "upsert" and not external_id_field:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="external_id_field query parameter is required for upsert operation.")

    try:
        records = await read_file_data_for_bulk(file) # This function now resides in data_handler
        if not records:
            return BulkOperationResponse(success=True, message="File is empty or contains no processable records. No bulk job submitted.", job_id=None)

        bulk_payload = SalesforceBulkOperationPayload(
            object_name=object_name,
            operation=op_type_lower,
            records=records,
            external_id_field=external_id_field
        )
        job_id, _ = await perform_bulk_operation(auth=auth, payload=bulk_payload)

        return BulkOperationResponse(
            success=True,
            message=f"Bulk {op_type_lower} job submitted for {object_name} from file {file.filename}. Job ID: {job_id}. Check job status for results.",
            job_id=job_id,
        )
    except HTTPException as he:
        logger.error(f"HTTPException in bulk DML (file upload) for {object_name}: {he.detail}", exc_info=settings.DEBUG_MODE)
        raise he
    except Exception as e:
        logger.error(f"Error in bulk DML (file upload) for {object_name}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {str(e)}")


@router.post(
    "/bulk/local-file-process",
    response_model=BulkOperationResponse, # Can also return OperationResponse for non-bulk part
    summary="Process Records from Local File Path (Bulk or REST)",
    description="Processes records from a file specified by a local path in the payload. Uses Bulk API if specified, otherwise iterates using REST API."
)
async def handle_process_records_from_local_file( # Renamed for clarity
    payload: FileProcessingPayload, # Defined in schemas.py
    auth: SalesforceAuth = Depends(get_sfdc_auth)
):
    try:
        logger.info(f"Processing local file: {payload.file_path} for {payload.object_name}, op: {payload.operation_type}, bulk: {payload.use_bulk_api}")
        records = await read_data_from_local_file(payload.file_path)

        if not records:
            return BulkOperationResponse(success=True, message=f"No records found in file {payload.file_path}. Nothing to process.", job_id=None) # Use job_id=None

        if payload.use_bulk_api:
            bulk_req_payload = SalesforceBulkOperationPayload(
                object_name=payload.object_name,
                operation=payload.operation_type, # Already validated by FileProcessingPayload
                records=records,
                external_id_field=payload.external_id_field
            )
            job_id, _ = await perform_bulk_operation(auth=auth, payload=bulk_req_payload)
            return BulkOperationResponse(
                success=True,
                message=f"Bulk {payload.operation_type} job submitted for {payload.object_name} from local file {payload.file_path}. Job ID: {job_id}. Check job status.",
                job_id=job_id
            )
        else: # Non-bulk processing using REST API calls iteratively
            results_summary = []
            success_count = 0
            failed_count = 0
            for i, record_data in enumerate(records):
                single_op_result = {"record_index": i, "success": False, "id": None, "errors": None}
                try:
                    record_id_in_data = record_data.get("Id")
                    ext_id_val_in_data = record_data.get(payload.external_id_field) if payload.external_id_field else None

                    if payload.operation_type == "create":
                        op_data = {k: v for k, v in record_data.items() if k.lower() != 'id'}
                        created_id = await create_record(auth, payload.object_name, op_data)
                        single_op_result.update({"id": created_id, "success": True})
                        success_count += 1
                    elif payload.operation_type == "update":
                        if not record_id_in_data:
                            single_op_result["errors"] = [{"message": "Missing Id for update."}]
                            failed_count += 1
                        else:
                            op_data = {k: v for k, v in record_data.items() if k.lower() != 'id'}
                            await update_record(auth, payload.object_name, record_id_in_data, op_data)
                            single_op_result.update({"id": record_id_in_data, "success": True})
                            success_count += 1
                    elif payload.operation_type == "upsert":
                        if not payload.external_id_field or not ext_id_val_in_data:
                            single_op_result["errors"] = [{"message": f"Missing external ID field '{payload.external_id_field}' or its value for upsert."}]
                            failed_count += 1
                        else:
                            op_data = record_data.copy()
                            new_id, created_flag = await upsert_record(auth, payload.object_name, payload.external_id_field, ext_id_val_in_data, op_data)
                            single_op_result.update({"id": new_id, "created": created_flag, "success": True})
                            success_count += 1
                    elif payload.operation_type == "delete":
                        if not record_id_in_data:
                            single_op_result["errors"] = [{"message": "Missing Id for delete."}]
                            failed_count += 1
                        else:
                            await delete_record(auth, payload.object_name, record_id_in_data)
                            single_op_result.update({"id": record_id_in_data, "success": True})
                            success_count += 1
                except HTTPException as he_single:
                    single_op_result["errors"] = [{"message": he_single.detail, "errorCode": str(he_single.status_code)}]
                    failed_count += 1
                except Exception as e_single:
                    single_op_result["errors"] = [{"message": str(e_single)}]
                    failed_count += 1
                results_summary.append(single_op_result)
            # Using BulkOperationResponse for consistency, though not a bulk job
            return BulkOperationResponse(
                success=(failed_count == 0),
                message=f"Processed {len(records)} records via REST API from {payload.file_path}. {success_count} successful, {failed_count} failed.",
                job_id=None, # Not a bulk job
                results=[BulkOperationResultDetail(**res) for res in results_summary]
            )
    except HTTPException as he:
        logger.error(f"HTTPException processing local file {payload.file_path}: {he.detail}", exc_info=settings.DEBUG_MODE)
        raise he
    except Exception as e:
        logger.error(f"Error processing local file {payload.file_path}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error processing local file: {str(e)}")


@router.post(
    "/bulk/dml-direct-payload",
    response_model=BulkOperationResponse,
    summary="Submit Bulk DML Job with Direct Record List",
    description="Submits a bulk DML operation using a list of records provided directly in the payload."
)
async def handle_bulk_dml_direct_payload(
    payload: BulkDMLJobSubmitPayload, # Specific schema for DML via direct payload
    auth: SalesforceAuth = Depends(get_sfdc_auth)
):
    if payload.operation.lower() == "upsert" and not payload.external_id_field:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="external_id_field is required for upsert operation.")
    if not payload.records:
         raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Records list is required for DML bulk operations.")

    try:
        # Adapt to SalesforceBulkOperationPayload expected by perform_bulk_operation
        internal_payload = SalesforceBulkOperationPayload(
            object_name=payload.object_name,
            operation=payload.operation,
            records=payload.records,
            external_id_field=payload.external_id_field
        )
        job_id, _ = await perform_bulk_operation(auth=auth, payload=internal_payload)
        return BulkOperationResponse(
            success=True,
            message=f"Bulk {payload.operation} job submitted for {payload.object_name} with direct payload. Job ID: {job_id}. Check job status.",
            job_id=job_id
        )
    except HTTPException as he:
        logger.error(f"HTTPException in bulk DML (direct payload) for {payload.object_name}: {he.detail}", exc_info=settings.DEBUG_MODE)
        raise he
    except Exception as e:
        logger.error(f"Error in bulk DML (direct payload) for {payload.object_name}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {str(e)}")


@router.post(
    "/bulk/query-submit",
    response_model=BulkOperationResponse,
    summary="Submit a Bulk SOQL Query Job",
    description="Submits a SOQL query to be executed asynchronously via Bulk API 2.0."
)
async def handle_submit_bulk_query_job( # Renamed
    payload: BulkQueryJobSubmitPayload, # Specific schema for query
    auth: SalesforceAuth = Depends(get_sfdc_auth)
):
    try:
        job_id, _ = await perform_bulk_query(
            auth=auth,
            object_name=payload.object_name, # For logging/context, not strictly for SF query job API
            soql_query=payload.soql_query,
            operation=payload.operation
        )
        return BulkOperationResponse(
            success=True,
            message=f"Bulk {payload.operation} job submitted for query on {payload.object_name or 'objects'}. Job ID: {job_id}. Check job status.",
            job_id=job_id
        )
    except HTTPException as he:
        logger.error(f"HTTPException submitting bulk query for {payload.object_name or 'query'}: {he.detail}", exc_info=settings.DEBUG_MODE)
        raise he
    except Exception as e:
        logger.error(f"Error submitting bulk query for {payload.object_name or 'query'}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {str(e)}")


@router.get(
    "/bulk/job/{job_id}/status",
    response_model=BulkJobStatusResponse, # Updated response model
    summary="Get Bulk Job Status and Optionally Results",
    description="Retrieves the status of a Bulk API 2.0 job (ingest or query). If completed, results (or links to them) can be included."
)
async def handle_get_bulk_job_status_and_results_endpoint( # Renamed
    job_id: str,
    is_query_job: bool = Query(False, description="Set to true if the job ID is for a query job, false for DML/ingest job."),
    include_results_data: bool = Query(False, description="If true and job is complete, attempts to fetch and include results data (can be large)."),
    auth: SalesforceAuth = Depends(get_sfdc_auth)
):
    try:
        status_and_results_dict = await get_bulk_job_status_and_results(
            auth=auth,
            job_id=job_id,
            is_query_job=is_query_job,
            fetch_results_if_complete=include_results_data # Pass this down
        )
        # The operation now returns a dict; adapt to BulkJobStatusResponse
        # Example adaptation:
        job_info = status_and_results_dict.get("job_info", {})
        results_data = status_and_results_dict.get("results_data") # This could be CSV string or dict of CSV strings

        # Parse CSV results if they are strings and requested
        parsed_results = None
        if include_results_data and results_data:
            if is_query_job and isinstance(results_data, str): # Query job returns single CSV string
                parsed_results = parse_csv_string_to_records(results_data)
            elif not is_query_job and isinstance(results_data, dict): # DML job returns dict of CSV strings
                parsed_results = {
                    "successful_records": parse_csv_string_to_records(results_data.get("successful_records_csv","")),
                    "failed_records": parse_csv_string_to_records(results_data.get("failed_records_csv","")),
                    "unprocessed_records": parse_csv_string_to_records(results_data.get("unprocessed_records_csv",""))
                }

        return BulkJobStatusResponse(
            job_id=job_info.get("id", job_id),
            state=job_info.get("state"),
            operation=job_info.get("operation"),
            object_name=job_info.get("object"),
            error_message=job_info.get("errorMessage"),
            records_processed=job_info.get("numberRecordsProcessed"),
            records_failed=job_info.get("numberRecordsFailed"),
            job_info_details=job_info, # Full job info from SF
            results_data=parsed_results if include_results_data else None, # Parsed records or None
            raw_results_csv=results_data if include_results_data and isinstance(results_data, (str, dict)) else None # Raw CSV if needed
        )

    except HTTPException as he:
        logger.error(f"HTTPException getting status for bulk job {job_id}: {he.detail}", exc_info=settings.DEBUG_MODE)
        raise he
    except Exception as e:
        logger.error(f"Error getting status for bulk job {job_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {str(e)}")


@router.post(
    "/records/batch",
    response_model=BulkOperationResponse, # Reusing for consistency, job_id will be null if not bulk
    summary="Process a Batch of Records (Bulk or REST)",
    description="Processes a list of records provided in the payload. Uses Bulk API if use_bulk_api is true, otherwise iterates using REST API."
)
async def handle_batch_record_processing(
    payload: BatchRecordProcessingPayload, # Defined in schemas.py
    auth: SalesforceAuth = Depends(get_sfdc_auth)
):
    try:
        logger.info(f"Processing batch for {payload.object_name}, op: {payload.operation_type}, count: {len(payload.records)}, bulk: {payload.use_bulk_api}")

        if payload.use_bulk_api:
            bulk_req_payload = SalesforceBulkOperationPayload(
                object_name=payload.object_name,
                operation=payload.operation_type,
                records=payload.records,
                external_id_field=payload.external_id_field
            )
            job_id, _ = await perform_bulk_operation(auth=auth, payload=bulk_req_payload)
            return BulkOperationResponse(
                success=True,
                message=f"Bulk {payload.operation_type} job submitted for {len(payload.records)} records on {payload.object_name}. Job ID: {job_id}. Check job status.",
                job_id=job_id
            )
        else: # Non-bulk processing using REST API calls iteratively
            results_summary = []
            success_count = 0
            failed_count = 0
            for i, record_data in enumerate(payload.records):
                single_op_result = {"record_index": i, "success": False, "id": None, "errors": None}
                try:
                    record_id_in_data = record_data.get("Id")
                    ext_id_val_in_data = record_data.get(payload.external_id_field) if payload.external_id_field else None

                    if payload.operation_type == "create":
                        op_data = {k: v for k, v in record_data.items() if k.lower() != 'id'}
                        created_id = await create_record(auth, payload.object_name, op_data)
                        single_op_result.update({"id": created_id, "success": True})
                        success_count += 1
                    elif payload.operation_type == "update":
                        if not record_id_in_data:
                            single_op_result["errors"] = [{"message": "Missing Id for update."}]
                            failed_count += 1
                        else:
                            op_data = {k: v for k, v in record_data.items() if k.lower() != 'id'}
                            await update_record(auth, payload.object_name, record_id_in_data, op_data)
                            single_op_result.update({"id": record_id_in_data, "success": True})
                            success_count += 1
                    elif payload.operation_type == "upsert":
                        if not payload.external_id_field or not ext_id_val_in_data: # Check if ext_id_val_in_data has a value
                            single_op_result["errors"] = [{"message": f"Missing external ID field '{payload.external_id_field}' or its value in record data for upsert."}]
                            failed_count += 1
                        else:
                            op_data = record_data.copy()
                            new_id, created_flag = await upsert_record(auth, payload.object_name, payload.external_id_field, ext_id_val_in_data, op_data)
                            single_op_result.update({"id": new_id, "created": created_flag, "success": True})
                            success_count += 1
                    elif payload.operation_type == "delete":
                        if not record_id_in_data:
                            single_op_result["errors"] = [{"message": "Missing Id for delete."}]
                            failed_count += 1
                        else:
                            await delete_record(auth, payload.object_name, record_id_in_data)
                            single_op_result.update({"id": record_id_in_data, "success": True})
                            success_count += 1
                except HTTPException as he_single:
                    single_op_result["errors"] = [{"message": he_single.detail, "errorCode": str(he_single.status_code)}]
                    failed_count += 1
                except Exception as e_single:
                    single_op_result["errors"] = [{"message": str(e_single)}]
                    failed_count += 1
                results_summary.append(single_op_result)

            return BulkOperationResponse(
                success=(failed_count == 0),
                message=f"Processed {len(payload.records)} records via REST API for {payload.object_name}. {success_count} successful, {failed_count} failed.",
                job_id=None,
                results=[BulkOperationResultDetail(**res) for res in results_summary]
            )
    except HTTPException as he:
        logger.error(f"HTTPException processing batch for {payload.object_name}: {he.detail}", exc_info=settings.DEBUG_MODE)
        raise he
    except Exception as e:
        logger.error(f"Error processing batch for {payload.object_name}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error processing batch: {str(e)}")
