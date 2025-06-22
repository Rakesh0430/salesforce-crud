# src/salesforce/client.py
import httpx
import json
import logging
from typing import Any, Dict, List, Optional, Union, Tuple

from fastapi import HTTPException, status
from src.core.config import settings
from src.salesforce.auth import SalesforceAuth # Assuming SalesforceAuth class is in auth.py

logger = logging.getLogger(settings.APP_NAME)

# Maximum number of retries for a request if it fails (e.g. due to 401 or network issues)
MAX_RETRIES = 1 # Total attempts = 1 (original) + MAX_RETRIES

class SalesforceApiClient:
    """
    An asynchronous client for interacting with the Salesforce REST API.
    Handles request authentication, common API call patterns, and error handling.
    """

    def __init__(self, auth_instance: SalesforceAuth):
        self.auth = auth_instance
        self.base_url_template = "{instance_url}/services/data/{api_version}"

    async def _get_base_url(self) -> str:
        _, instance_url = await self.auth.get_auth_details()
        return self.base_url_template.format(
            instance_url=instance_url.rstrip('/'),
            api_version=settings.SALESFORCE_API_VERSION
        )

    async def _get_headers(self) -> Dict[str, str]:
        access_token, _ = await self.auth.get_auth_details()
        return {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Sforce-Call-Options": f"client={settings.APP_NAME}/{settings.APP_VERSION}"
        }

    async def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Any] = None,
        data: Optional[Any] = None, # For form data, not typically used with SF REST JSON API
        is_bulk_api: bool = False # Flag to adjust base URL for Bulk API if needed
    ) -> httpx.Response:
        """
        Makes an HTTP request to the Salesforce API with retry logic for 401 errors.
        """
        base_url = await self._get_base_url()
        url = f"{base_url}{endpoint}"

        # Adjust URL for Bulk API 2.0 if needed (it uses /services/data/vXX.X/jobs/ingest)
        # This client is primarily for REST API; Bulk might have its own client or methods.
        # For now, this is a simple flag.
        if is_bulk_api:
             _, instance_url = await self.auth.get_auth_details()
             # Example for Bulk API 2.0 ingest jobs
             if endpoint.startswith("/jobs/ingest") or endpoint.startswith("/jobs/query"):
                url = f"{instance_url.rstrip('/')}/services/data/{settings.SALESFORCE_API_VERSION}{endpoint}"


        async with httpx.AsyncClient(timeout=60.0) as client: # Increased timeout for potentially long SF operations
            for attempt in range(MAX_RETRIES + 1):
                headers = await self._get_headers()
                try:
                    logger.debug(f"Salesforce API Request: {method} {url} | Params: {params} | Body: {json.dumps(json_data) if json_data else None}")

                    response = await client.request(
                        method,
                        url,
                        headers=headers,
                        params=params,
                        json=json_data,
                        data=data
                    )
                    logger.debug(f"Salesforce API Response: {response.status_code} {response.text[:500]}")

                    if response.status_code == status.HTTP_401_UNAUTHORIZED:
                        if attempt < MAX_RETRIES:
                            logger.warning(f"401 Unauthorized from Salesforce. Attempt {attempt + 1}/{MAX_RETRIES + 1}. Refreshing token...")
                            await self.auth.handle_401_unauthorized() # Force token refresh
                            # Headers will be refetched in the next iteration with the new token
                            continue # Retry the request
                        else:
                            logger.error(f"401 Unauthorized from Salesforce after {MAX_RETRIES + 1} attempts. Giving up.")
                            # Let it fall through to raise_for_status or be handled by caller

                    response.raise_for_status() # Raises HTTPStatusError for 4xx/5xx client/server responses
                    return response

                except httpx.HTTPStatusError as e:
                    # Log detailed Salesforce error if available
                    error_detail = e.response.text
                    try:
                        sfdc_error = e.response.json()
                        if isinstance(sfdc_error, list) and sfdc_error: # Standard SF error format
                            error_detail = json.dumps(sfdc_error)
                        elif isinstance(sfdc_error, dict) and ("message" in sfdc_error or "error_description" in sfdc_error):
                            error_detail = json.dumps(sfdc_error)
                    except ValueError: # Not a JSON response
                        pass
                    logger.error(f"Salesforce API HTTPStatusError: {e.response.status_code} on {method} {url}. Detail: {error_detail}", exc_info=False) # exc_info=False to avoid redundant stack trace for HTTPStatusError
                    raise HTTPException(
                        status_code=e.response.status_code,
                        detail=f"Salesforce API Error: {error_detail}"
                    ) from e
                except httpx.RequestError as e: # Covers network errors, timeouts, etc.
                    logger.error(f"Salesforce API RequestError: {e.__class__.__name__} on {method} {url}. Detail: {str(e)}", exc_info=True)
                    if attempt < MAX_RETRIES:
                        logger.warning(f"Network error for Salesforce request. Attempt {attempt + 1}/{MAX_RETRIES + 1}. Retrying...")
                        await asyncio.sleep(1 * (attempt + 1)) # Exponential backoff could be added
                        continue
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail=f"Salesforce API communication error: {e.__class__.__name__}"
                    ) from e
            # Should not be reached if logic is correct, but as a fallback:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to execute Salesforce request after retries.")


    # --- Standard SObject Methods ---
    async def get_sobject_describe(self, object_name: str) -> Dict[str, Any]:
        """Describes the specified SObject."""
        endpoint = f"/sobjects/{object_name}/describe"
        response = await self._request("GET", endpoint)
        return response.json()

    async def create_sobject_record(self, object_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Creates a new record for the specified SObject."""
        endpoint = f"/sobjects/{object_name}"
        response = await self._request("POST", endpoint, json_data=data)
        return response.json() # Should contain { "id": "...", "success": true, "errors": [] }

    async def get_sobject_record(self, object_name: str, record_id: str, fields: Optional[List[str]] = None) -> Dict[str, Any]:
        """Retrieves a record by its ID."""
        endpoint = f"/sobjects/{object_name}/{record_id}"
        params = {"fields": ",".join(fields)} if fields else None
        response = await self._request("GET", endpoint, params=params)
        return response.json()

    async def update_sobject_record(self, object_name: str, record_id: str, data: Dict[str, Any]) -> bool:
        """Updates an existing record. Returns True on success (204 No Content)."""
        endpoint = f"/sobjects/{object_name}/{record_id}"
        response = await self._request("PATCH", endpoint, json_data=data)
        return response.status_code == status.HTTP_204_NO_CONTENT

    async def delete_sobject_record(self, object_name: str, record_id: str) -> bool:
        """Deletes a record. Returns True on success (204 No Content)."""
        endpoint = f"/sobjects/{object_name}/{record_id}"
        response = await self._request("DELETE", endpoint)
        return response.status_code == status.HTTP_204_NO_CONTENT

    async def upsert_sobject_record(
        self, object_name: str, external_id_field: str, external_id: str, data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Upserts a record based on an external ID field."""
        # Ensure the data being upserted does not contain the external ID field name itself,
        # as it's part of the URL, not the payload for PATCH.
        payload = data.copy()
        if external_id_field in payload: # Salesforce doesn't expect the ext ID field in the body for PATCH upsert
            del payload[external_id_field]

        endpoint = f"/sobjects/{object_name}/{external_id_field}/{external_id}"
        response = await self._request("PATCH", endpoint, json_data=payload)
        # Successful upsert can be 200 (updated), 201 (created), or 204 (updated, no content if configured)
        # We'll return the JSON body which usually has id, success, errors.
        if response.status_code == status.HTTP_204_NO_CONTENT: # Update with no content
             return {"id": record_id if 'record_id' in locals() else external_id, "success": True, "created": False, "errors": [], "status": "updated_no_content"} # record_id might not be available
        return response.json()


    # --- SOQL Query ---
    async def execute_soql_query(self, query: str) -> Dict[str, Any]:
        """Executes a SOQL query."""
        endpoint = "/query"
        params = {"q": query}
        response = await self._request("GET", endpoint, params=params)
        return response.json() # Returns records, totalSize, done, nextRecordsUrl

    async def get_next_query_results(self, next_records_url: str) -> Dict[str, Any]:
        """Retrieves the next batch of query results using the nextRecordsUrl."""
        # next_records_url is relative to instance_url, e.g., /services/data/vXX.X/query/01g...
        # So we pass it directly as the endpoint.
        # We need to strip the instance_url part if it's absolute, or ensure _request handles full URLs if needed.
        # For simplicity, assuming next_records_url is the path part.
        if next_records_url.startswith(await self._get_base_url()):
            endpoint = next_records_url[len(await self._get_base_url()):]
        elif next_records_url.startswith("/services/data"): # Common pattern for nextRecordsUrl
             endpoint = next_records_url[len("/services/data") - len("/services/data") - len(settings.SALESFORCE_API_VERSION) -1:] # risky slicing
             # A safer way:
             parts = next_records_url.split(f"/services/data/{settings.SALESFORCE_API_VERSION}")
             if len(parts) > 1:
                 endpoint = parts[1]
             else: # Fallback if URL structure is unexpected
                 endpoint = next_records_url

        else: # if it's just the query locator part
            endpoint = f"/query/{next_records_url.split('/')[-1]}"


        response = await self._request("GET", endpoint)
        return response.json()


    # --- Bulk API 2.0 Methods (Placeholders/Examples) ---
    # These would typically involve creating a job, uploading data (CSV), closing job, checking status, getting results.

    async def create_bulk_ingest_job(self, object_name: str, operation: str, external_id_field: Optional[str] = None, line_ending: str = "LF") -> Dict[str, Any]:
        """Creates a Bulk API 2.0 ingest job."""
        endpoint = "/jobs/ingest"
        job_config: Dict[str, Any] = {
            "object": object_name,
            "operation": operation, # 'insert', 'update', 'upsert', 'delete', 'hardDelete'
            "contentType": "CSV", # Only CSV supported for Bulk API 2.0
            "lineEnding": line_ending # "LF" or "CRLF"
        }
        if operation == "upsert" and external_id_field:
            job_config["externalIdFieldName"] = external_id_field

        response = await self._request("POST", endpoint, json_data=job_config, is_bulk_api=True)
        return response.json() # Returns job info: id, state, contentUrl etc.

    async def upload_bulk_job_data(self, content_url: str, csv_data: Union[str, bytes]) -> bool:
        """Uploads CSV data to a Bulk API 2.0 job. content_url is from job creation response."""
        # content_url is usually like: "services/data/vXX.X/jobs/ingest/jobID/batches"
        # The _request method needs to handle this. It's not a typical JSON request.
        # It's a PUT request with text/csv content.

        # We need to get the full URL for the upload. content_url is relative.
        _, instance_url = await self.auth.get_auth_details()
        upload_url = f"{instance_url.rstrip('/')}/{content_url.lstrip('/')}"

        access_token, _ = await self.auth.get_auth_details() # Re-fetch token in case it refreshed
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "text/csv",
            "Accept": "application/json" # For the response from SF
        }

        async with httpx.AsyncClient(timeout=300.0) as client: # Longer timeout for data upload
            try:
                logger.debug(f"Bulk Uploading data to: {upload_url}")
                # Data should be bytes if it's pre-encoded, or string
                data_to_upload = csv_data.encode('utf-8') if isinstance(csv_data, str) else csv_data

                response = await client.put(upload_url, content=data_to_upload, headers=headers)
                response.raise_for_status()
                logger.info(f"Bulk data uploaded successfully to job via {upload_url}. Status: {response.status_code}")
                return response.status_code == status.HTTP_201_CREATED # Successful upload
            except httpx.HTTPStatusError as e:
                logger.error(f"Bulk data upload HTTPStatusError: {e.response.status_code} on PUT {upload_url}. Detail: {e.response.text}", exc_info=False)
                raise HTTPException(
                    status_code=e.response.status_code,
                    detail=f"Salesforce Bulk API Upload Error: {e.response.text}"
                ) from e
            except httpx.RequestError as e:
                logger.error(f"Bulk data upload RequestError: {e.__class__.__name__} on PUT {upload_url}. Detail: {str(e)}", exc_info=True)
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail=f"Salesforce Bulk API Upload communication error: {e.__class__.__name__}"
                ) from e

    async def update_bulk_job_state(self, job_id: str, new_state: str) -> Dict[str, Any]:
        """Updates the state of a Bulk API 2.0 job (e.g., to 'UploadComplete' or 'Aborted')."""
        endpoint = f"/jobs/ingest/{job_id}"
        payload = {"state": new_state}
        response = await self._request("PATCH", endpoint, json_data=payload, is_bulk_api=True)
        return response.json() # Returns updated job info

    async def get_bulk_job_info(self, job_id: str) -> Dict[str, Any]:
        """Retrieves information about a specific Bulk API 2.0 ingest job."""
        endpoint = f"/jobs/ingest/{job_id}"
        response = await self._request("GET", endpoint, is_bulk_api=True)
        return response.json()

    async def get_bulk_job_successful_results(self, job_id: str) -> str: # Returns CSV data as string
        """Retrieves successful record results for a completed Bulk API 2.0 job."""
        endpoint = f"/jobs/ingest/{job_id}/successfulResults/"
        # This request needs to handle CSV response, not JSON
        headers = await self._get_headers()
        headers["Accept"] = "text/csv" # Override accept for CSV
        base_url = await self._get_base_url() # SF Bulk API uses same base path
        url = f"{base_url}{endpoint}"

        async with httpx.AsyncClient(timeout=180.0) as client:
            try:
                response = await client.get(url, headers=headers)
                response.raise_for_status()
                return response.text # CSV content
            except httpx.HTTPStatusError as e:
                 logger.error(f"Failed to get bulk job successful results: {e.response.status_code} - {e.response.text}")
                 raise HTTPException(status_code=e.response.status_code, detail=f"SF Bulk API Error: {e.response.text}")
            except httpx.RequestError as e:
                logger.error(f"Network error getting bulk job successful results: {e}")
                raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Network error fetching bulk results.")


    async def get_bulk_job_failed_results(self, job_id: str) -> str: # Returns CSV data as string
        """Retrieves failed record results for a completed Bulk API 2.0 job."""
        endpoint = f"/jobs/ingest/{job_id}/failedResults/"
        headers = await self._get_headers()
        headers["Accept"] = "text/csv"
        base_url = await self._get_base_url()
        url = f"{base_url}{endpoint}"

        async with httpx.AsyncClient(timeout=180.0) as client:
            try:
                response = await client.get(url, headers=headers)
                response.raise_for_status()
                return response.text # CSV content
            except httpx.HTTPStatusError as e:
                 logger.error(f"Failed to get bulk job failed results: {e.response.status_code} - {e.response.text}")
                 raise HTTPException(status_code=e.response.status_code, detail=f"SF Bulk API Error: {e.response.text}")
            except httpx.RequestError as e:
                logger.error(f"Network error getting bulk job failed results: {e}")
                raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Network error fetching bulk results.")

    async def get_bulk_job_unprocessed_records(self, job_id: str) -> str: # Returns CSV data as string
        """Retrieves unprocessed record results for a Bulk API 2.0 job."""
        endpoint = f"/jobs/ingest/{job_id}/unprocessedrecords/"
        headers = await self._get_headers()
        headers["Accept"] = "text/csv"
        base_url = await self._get_base_url()
        url = f"{base_url}{endpoint}"

        async with httpx.AsyncClient(timeout=180.0) as client:
            try:
                response = await client.get(url, headers=headers)
                response.raise_for_status()
                return response.text # CSV content
            except httpx.HTTPStatusError as e:
                 logger.error(f"Failed to get bulk job unprocessed records: {e.response.status_code} - {e.response.text}")
                 raise HTTPException(status_code=e.response.status_code, detail=f"SF Bulk API Error: {e.response.text}")
            except httpx.RequestError as e:
                logger.error(f"Network error getting bulk job unprocessed records: {e}")
                raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Network error fetching unprocessed bulk records.")


# Dependency for FastAPI
async def get_sfdc_client(auth: SalesforceAuth = Depends(SalesforceAuth)) -> SalesforceApiClient:
    # This function was intended to use Depends(get_salesforce_auth_instance)
    # Corrected version:
    # async def get_sfdc_client(auth_instance: SalesforceAuth = Depends(get_salesforce_auth_instance)) -> SalesforceApiClient:
    #   return SalesforceApiClient(auth_instance)
    # For now, this will work if SalesforceAuth itself is made a dependency that resolves to the singleton.
    # However, the explicit get_salesforce_auth_instance is preferred for clarity.
    # This will be fixed when integrating. For now, assume `auth` is correctly injected.
    return SalesforceApiClient(auth)

# Corrected dependency injector function
async def get_salesforce_api_client(
    auth_instance: SalesforceAuth = Depends(SalesforceAuth) # This will use the __call__ of SalesforceAuth if it's a class dependency
                                                            # Or, more explicitly:
                                                            # auth_instance: SalesforceAuth = Depends(get_salesforce_auth_instance)
) -> SalesforceApiClient:
    """FastAPI dependency to get an instance of SalesforceApiClient."""
    # If SalesforceAuth is a class that can be directly depended on (e.g. if it has __call__)
    # or if get_salesforce_auth_instance is used:
    # from .auth import get_salesforce_auth_instance
    # auth_instance: SalesforceAuth = Depends(get_salesforce_auth_instance)
    return SalesforceApiClient(auth_instance)
