# record-service/src/app/routers/records.py
from fastapi import APIRouter, HTTPException, Depends, Body, status, Query
from typing import Optional
import logging

from core.schemas import (
    SalesforceOperationPayload,
    OperationResponse,
    DescribeResponse
)
from salesforce.client import SalesforceApiClient, get_salesforce_api_client
from salesforce.operations import (
    create_record, get_record, update_record, delete_record, upsert_record,
    describe_sobject
)
from core.config import settings

logger = logging.getLogger(settings.APP_NAME)
router = APIRouter()

@router.post(
    "/records/create",
    response_model=OperationResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a Salesforce Record",
    description="Creates a new record for the specified SObject using REST API."
)
async def handle_create_record_endpoint(
    payload: SalesforceOperationPayload = Body(...),
    client: SalesforceApiClient = Depends(get_salesforce_api_client)
):
    try:
        if not payload.data:
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Field 'data' is required for creating a record.")
        record_id = await create_record(
            client=client,
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
    client: SalesforceApiClient = Depends(get_salesforce_api_client)
):
    try:
        field_list = fields.split(',') if fields else None
        record_data = await get_record(
            client=client,
            object_name=object_name,
            record_id=record_id,
            fields=field_list
        )
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
    client: SalesforceApiClient = Depends(get_salesforce_api_client)
):
    if not payload.record_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Field 'record_id' is required for update operations.")
    if not payload.data:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Field 'data' is required for update operations.")
    try:
        await update_record(
            client=client,
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
    description="Upserts a record in the specified SObject using an external ID field."
)
async def handle_upsert_record_endpoint(
    payload: SalesforceOperationPayload = Body(...),
    client: SalesforceApiClient = Depends(get_salesforce_api_client)
):
    if not payload.external_id_field or not payload.record_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Fields 'external_id_field' and 'record_id' (as external_id_value) are required for upsert.")
    if not payload.data:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Field 'data' is required for upsert operation.")

    try:
        new_record_id, created = await upsert_record(
            client=client,
            object_name=payload.object_name,
            external_id_field=payload.external_id_field,
            external_id_value=payload.record_id,
            data=payload.data
        )
        status_message = "created" if created else "updated"
        return OperationResponse(
            success=True,
            message=f"Record {status_message} successfully in {payload.object_name} via external ID {payload.external_id_field}. Salesforce ID: {new_record_id}",
            record_id=new_record_id,
            data={"created": created, "external_id_value": payload.record_id}
        )
    except HTTPException as he:
        logger.error(f"HTTPException upserting record: {he.detail}", exc_info=settings.DEBUG_MODE)
        raise he
    except Exception as e:
        logger.error(f"Error upserting record: {str(e)}", exc_info=True)
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
    client: SalesforceApiClient = Depends(get_salesforce_api_client)
):
    try:
        await delete_record(
            client=client,
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
    description="Retrieves metadata for a specified SObject."
)
async def handle_describe_sobject_endpoint(
    object_name: str,
    client: SalesforceApiClient = Depends(get_salesforce_api_client)
):
    try:
        description = await describe_sobject(client=client, object_name=object_name)
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
