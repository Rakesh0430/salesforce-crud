# src/core/schemas.py
from pydantic import BaseModel, Field, validator
from typing import Any, Dict, List, Optional, Union

# --- Request Schemas ---

class SalesforceOperationPayload(BaseModel):
    object_name: str = Field(..., description="The API name of the Salesforce SObject (e.g., Account, MyCustomObject__c).")
    record_id: Optional[str] = Field(None, description="The ID of the record, required for update and delete operations if not in URL.")
    external_id_field: Optional[str] = Field(None, description="The API name of the external ID field, used for upsert operations.")
    data: Optional[Dict[str, Any]] = Field(None, description="A dictionary of field API names and their values for create/update operations.")
    fields: Optional[List[str]] = Field(None, description="A list of field API names to retrieve for get operations.")
    # use_bulk_api: bool = Field(False, description="Set to true to use Bulk API for this operation if applicable (e.g., for a single record, REST API is usually better). This flag is more relevant for collection-based endpoints.")

    @validator('object_name')
    def object_name_must_be_valid(cls, v):
        if not v or not isinstance(v, str):
            raise ValueError('object_name must be a non-empty string')
        # Add more validation if needed (e.g., regex for SObject names)
        return v

class SalesforceBulkOperationPayload(BaseModel):
    object_name: str = Field(..., description="The API name of the Salesforce SObject.")
    operation: str = Field(..., description="The bulk operation to perform (e.g., 'insert', 'update', 'upsert', 'delete', 'hardDelete', 'query', 'queryAll').")
    records: Optional[List[Dict[str, Any]]] = Field(None, description="A list of records (dictionaries of field API names and values) for DML operations.")
    soql_query: Optional[str] = Field(None, description="SOQL query string for 'query' or 'queryAll' operations.")
    external_id_field: Optional[str] = Field(None, description="The API name of the external ID field, used for 'upsert' operations.")
    # assignment_rule_id: Optional[str] = Field(None, description="ID of an assignment rule to run for Account, Case, or Lead.") # Example for Bulk API 1.0 header
    # Other Bulk API options can be added here

    @validator('operation')
    def operation_must_be_valid(cls, v):
        valid_ops = {'insert', 'update', 'upsert', 'delete', 'hardDelete', 'query', 'queryAll'}
        if v not in valid_ops:
            raise ValueError(f'Invalid operation. Must be one of {valid_ops}')
        return v

    @validator('records', always=True)
    def records_or_query_must_be_present(cls, v, values):
        operation = values.get('operation')
        if operation in {'insert', 'update', 'upsert', 'delete', 'hardDelete'}:
            if not v:
                raise ValueError('records must be provided for DML operations')
        return v

    @validator('soql_query', always=True)
    def query_must_be_present_for_query_ops(cls, v, values):
        operation = values.get('operation')
        if operation in {'query', 'queryAll'}:
            if not v:
                raise ValueError('soql_query must be provided for query operations')
        return v

    @validator('external_id_field', always=True)
    def external_id_field_must_be_present_for_upsert(cls, v, values):
        operation = values.get('operation')
        if operation == 'upsert' and not v:
            raise ValueError('external_id_field must be provided for upsert operation')
        return v


# --- Response Schemas ---

class OperationResponse(BaseModel):
    success: bool
    message: str
    record_id: Optional[str] = None
    data: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None # For get or query results
    errors: Optional[List[Any]] = None # For detailed errors

class BulkOperationResultDetail(BaseModel):
    success: bool
    created: Optional[bool] = None
    id: Optional[str] = None
    errors: Optional[List[Dict[str, Any]]] = Field(None, description="List of errors for this specific record from Salesforce.")

class BulkOperationResponse(BaseModel):
    success: bool
    message: str
    job_id: Optional[str] = None # Bulk API Job ID
    results: Optional[List[BulkOperationResultDetail]] = None # Results for each record if processed synchronously or fetched after job completion
    # May also include links to retrieve results for asynchronous jobs

class DescribeFieldSchema(BaseModel):
    name: str
    label: str
    type: str
    length: Optional[int] = None
    custom: bool
    nillable: bool
    filterable: bool
    sortable: bool
    createable: bool
    updateable: bool
    picklistValues: Optional[List[Dict[str, Any]]] = None
    referenceTo: Optional[List[str]] = None
    # Add other relevant field properties from describe response

class DescribeResponse(BaseModel):
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = Field(None, description="Full describe information for the SObject.")
    # Or a more structured model:
    # name: Optional[str] = None
    # label: Optional[str] = None
    # fields: Optional[List[DescribeFieldSchema]] = None
    # childRelationships: Optional[List[Dict[str, Any]]] = None
    # ... other describe properties

# --- Specific Bulk Job Payloads ---
class BulkDMLJobSubmitPayload(BaseModel):
    object_name: str = Field(..., description="The API name of the Salesforce SObject for DML operation.")
    operation: str = Field(..., description="DML Operation: 'insert', 'update', 'upsert', 'delete', 'hardDelete'.")
    records: List[Dict[str, Any]] = Field(..., description="List of records (dictionaries) to process.")
    external_id_field: Optional[str] = Field(None, description="External ID field API name, required for 'upsert' operation.")

    @validator('operation')
    def dml_operation_must_be_valid(cls, v):
        valid_ops = {'insert', 'update', 'upsert', 'delete', 'harddelete'} # Salesforce uses hardDelete
        op_lower = v.lower()
        if op_lower not in valid_ops:
            raise ValueError(f'Invalid DML operation. Must be one of {valid_ops}')
        return op_lower

    @validator('external_id_field', always=True)
    def dml_external_id_field_for_upsert(cls, v, values):
        # Pydantic v2: `values` is a dict. For v1, it was `values.data`.
        operation = values.get('operation')
        if operation == 'upsert' and not v:
            raise ValueError('external_id_field is required for DML upsert operation.')
        return v

class BulkQueryJobSubmitPayload(BaseModel):
    object_name: Optional[str] = Field(None, description="The primary Salesforce SObject API name involved in the query (for context/logging).")
    soql_query: str = Field(..., description="The SOQL query string to execute.")
    operation: str = Field(default="query", description="Query Operation: 'query' or 'queryAll'.")

    @validator('operation')
    def query_operation_must_be_valid(cls, v):
        valid_ops = {'query', 'queryall'}
        op_lower = v.lower()
        if op_lower not in valid_ops:
            raise ValueError(f'Invalid query operation. Must be "query" or "queryAll".')
        return op_lower

# --- Specific Bulk Job Response Schemas ---
class BulkJobStatusResponse(BaseModel):
    job_id: str
    state: Optional[str] = None
    operation: Optional[str] = None
    object_name: Optional[str] = None
    error_message: Optional[str] = None
    records_processed: Optional[int] = None
    records_failed: Optional[int] = None
    job_info_details: Optional[Dict[str, Any]] = Field(None, description="Full job information from Salesforce.")
    results_data: Optional[Union[List[Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]] = Field(None, description="Parsed results if job is complete and results were fetched. For DML, can be a dict with 'successful_records', 'failed_records'. For Query, a list of records.")
    raw_results_csv: Optional[Union[str, Dict[str,str]]] = Field(None, description="Raw CSV results if fetched. For DML, can be dict with 'successful_records_csv', etc.")


class BatchRecordProcessingPayload(BaseModel):
    object_name: str = Field(..., description="The API name of the Salesforce SObject.")
    operation_type: str = Field(..., description="Operation to perform: 'create', 'update', 'upsert', 'delete'.")
    records: List[Dict[str, Any]] = Field(..., description="List of records to process.")
    use_bulk_api: bool = Field(False, description="If true, attempts to use Bulk API. If false or for small batches, uses iterative REST API calls.")
    external_id_field: Optional[str] = Field(None, description="External ID field for 'upsert' operations.")
    # Consider adding a batch_size_for_rest_api: Optional[int] = Field(200, description="If not using bulk, how many records per REST API batch (e.g. SObject Collections). For future use.")


    @validator('operation_type')
    def batch_op_type_must_be_valid(cls, v):
        valid_ops = {'create', 'update', 'upsert', 'delete'}
        op_lower = v.lower()
        if op_lower not in valid_ops:
            raise ValueError(f'Invalid operation_type. Must be one of {valid_ops}')
        return op_lower

    @validator('external_id_field', always=True)
    def batch_external_id_field_for_upsert(cls, v, values):
        operation = values.get('operation_type')
        if operation == 'upsert' and not v:
            raise ValueError('external_id_field is required for batch upsert operation.')
        return v

    @validator('records')
    def records_must_not_be_empty(cls, v):
        if not v:
            raise ValueError('Records list cannot be empty.')
        return v


# Generic payload for the entrypoint as described in the problem
class FileProcessingPayload(BaseModel):
    object_name: str = Field(..., description="The API name of the Salesforce SObject.")
    use_bulk_api: bool = Field(True, description="Whether to use Bulk API for the operation.")
    file_path: str = Field(..., description="Path to the data file (e.g., CSV) for processing.")
    # Assuming 'update_objects' implies an upsert or update operation.
    # For more clarity, an 'operation_type' field would be better (e.g., 'create', 'update', 'upsert').
    # For this example, we'll assume file_path implies records to be processed,
    # and 'update_objects' might mean 'upsert' or 'update'.
    # Let's make it more explicit:
    operation_type: str = Field(..., description="Operation to perform: 'create', 'update', 'upsert', 'delete'.")
    external_id_field: Optional[str] = Field(None, description="External ID field for upsert operations.")

    @validator('operation_type')
    def operation_type_must_be_valid(cls, v):
        valid_ops = {'create', 'update', 'upsert', 'delete'}
        if v.lower() not in valid_ops:
            raise ValueError(f'Invalid operation_type. Must be one of {valid_ops}')
        return v.lower()

    @validator('external_id_field', always=True)
    def external_id_field_for_upsert(cls, v, values):
        if values.get('operation_type') == 'upsert' and not v:
            raise ValueError('external_id_field is required for upsert operation with file.')
        return v
