# shared/src/core/schemas.py
from pydantic import BaseModel, Field, field_validator
from typing import Any, Dict, List, Optional, Union

class SalesforceOperationPayload(BaseModel):
    object_name: str = Field(..., description="The API name of the Salesforce SObject.")
    record_id: Optional[str] = Field(None, description="The ID of the record.")
    external_id_field: Optional[str] = Field(None, description="The API name of the external ID field.")
    data: Optional[Dict[str, Any]] = Field(None, description="A dictionary of field data.")
    fields: Optional[List[str]] = Field(None, description="A list of fields to retrieve.")

    @field_validator('object_name')
    def object_name_must_be_valid(cls, v):
        if not v or not isinstance(v, str):
            raise ValueError('object_name must be a non-empty string')
        return v

class SalesforceBulkOperationPayload(BaseModel):
    object_name: str = Field(..., description="The API name of the Salesforce SObject.")
    operation: str = Field(..., description="The bulk operation to perform.")
    records: Optional[List[Dict[str, Any]]] = Field(None, description="A list of records for DML operations.")
    soql_query: Optional[str] = Field(None, description="SOQL query string.")
    external_id_field: Optional[str] = Field(None, description="The external ID field for upsert.")

    @field_validator('operation')
    def operation_must_be_valid(cls, v):
        valid_ops = {'insert', 'update', 'upsert', 'delete', 'hardDelete', 'query', 'queryAll'}
        if v not in valid_ops:
            raise ValueError(f'Invalid operation. Must be one of {valid_ops}')
        return v

    @field_validator('records', mode='before')
    def records_or_query_must_be_present(cls, v, values):
        if 'operation' in values.data and values.data['operation'] in {'insert', 'update', 'upsert', 'delete', 'hardDelete'}:
            if not v:
                raise ValueError('records must be provided for DML operations')
        return v

    @field_validator('soql_query', mode='before')
    def query_must_be_present_for_query_ops(cls, v, values):
        if 'operation' in values.data and values.data['operation'] in {'query', 'queryAll'}:
            if not v:
                raise ValueError('soql_query must be provided for query operations')
        return v

    @field_validator('external_id_field', mode='before')
    def external_id_field_must_be_present_for_upsert(cls, v, values):
        if 'operation' in values.data and values.data['operation'] == 'upsert' and not v:
            raise ValueError('external_id_field must be provided for upsert operation')
        return v

class OperationResponse(BaseModel):
    success: bool
    message: str
    record_id: Optional[str] = None
    data: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None
    errors: Optional[List[Any]] = None

class BulkOperationResultDetail(BaseModel):
    success: bool
    created: Optional[bool] = None
    id: Optional[str] = None
    errors: Optional[List[Dict[str, Any]]] = None

class BulkOperationResponse(BaseModel):
    success: bool
    message: str
    job_id: Optional[str] = None
    results: Optional[List[BulkOperationResultDetail]] = None

class DescribeResponse(BaseModel):
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None

class BulkDMLJobSubmitPayload(BaseModel):
    object_name: str
    operation: str
    records: List[Dict[str, Any]]
    external_id_field: Optional[str] = None

    @field_validator('operation')
    def dml_operation_must_be_valid(cls, v):
        valid_ops = {'insert', 'update', 'upsert', 'delete', 'harddelete'}
        op_lower = v.lower()
        if op_lower not in valid_ops:
            raise ValueError(f'Invalid DML operation. Must be one of {valid_ops}')
        return op_lower

    @field_validator('external_id_field', mode='before')
    def dml_external_id_field_for_upsert(cls, v, values):
        if 'operation' in values.data and values.data['operation'] == 'upsert' and not v:
            raise ValueError('external_id_field is required for DML upsert operation.')
        return v

class BulkQueryJobSubmitPayload(BaseModel):
    object_name: Optional[str] = None
    soql_query: str
    operation: str = "query"

    @field_validator('operation')
    def query_operation_must_be_valid(cls, v):
        valid_ops = {'query', 'queryall'}
        op_lower = v.lower()
        if op_lower not in valid_ops:
            raise ValueError(f'Invalid query operation. Must be "query" or "queryAll".')
        return op_lower

class BulkJobStatusResponse(BaseModel):
    job_id: str
    state: Optional[str] = None
    operation: Optional[str] = None
    object_name: Optional[str] = None
    error_message: Optional[str] = None
    records_processed: Optional[int] = None
    records_failed: Optional[int] = None
    job_info_details: Optional[Dict[str, Any]] = None
    results_data: Optional[Union[List[Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]] = None
    raw_results_csv: Optional[Union[str, Dict[str,str]]] = None

class BatchRecordProcessingPayload(BaseModel):
    object_name: str
    operation_type: str
    records: List[Dict[str, Any]]
    use_bulk_api: bool = False
    external_id_field: Optional[str] = None

    @field_validator('operation_type')
    def batch_op_type_must_be_valid(cls, v):
        valid_ops = {'create', 'update', 'upsert', 'delete', 'insert'}
        op_lower = v.lower()
        if op_lower not in valid_ops:
            raise ValueError(f"Invalid operation_type. Must be one of {valid_ops}")
        return op_lower

    @field_validator('external_id_field', mode='before')
    def batch_external_id_field_for_upsert(cls, v, values):
        if 'operation_type' in values.data and values.data['operation_type'] == 'upsert' and not v:
            raise ValueError('external_id_field is required for batch upsert operation.')
        return v

    @field_validator('records')
    def records_must_not_be_empty(cls, v):
        if not v:
            raise ValueError('Records list cannot be empty.')
        return v

class FileProcessingPayload(BaseModel):
    object_name: str
    use_bulk_api: bool = True
    file_path: str
    operation_type: str
    external_id_field: Optional[str] = None

    @field_validator('operation_type')
    def operation_type_must_be_valid(cls, v):
        valid_ops = {'create', 'update', 'upsert', 'delete', 'insert'}
        if v.lower() not in valid_ops:
            raise ValueError(f'Invalid operation_type. Must be one of {valid_ops}')
        return v.lower()

    @field_validator('external_id_field', mode='before')
    def external_id_field_for_upsert(cls, v, values):
        if 'operation_type' in values.data and values.data['operation_type'] == 'upsert' and not v:
            raise ValueError('external_id_field is required for upsert operation with file.')
        return v
