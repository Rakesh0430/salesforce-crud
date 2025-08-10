# src/salesforce/auth.py
import httpx
import asyncio
import logging
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Optional, Tuple, Any

from fastapi import HTTPException, status, Depends
from core.config import settings

logger = logging.getLogger(settings.APP_NAME)

# Lock to ensure concurrency safety when refreshing tokens
token_lock = asyncio.Lock()

class SalesforceAuth:
    """
    Manages Salesforce authentication by retrieving and caching an access token.
    Automatically re-authenticates if the token is missing, expired, or invalid.
    Supports refreshing the token close to expiry or upon 401 errors.
    Designed to be used as a FastAPI dependency.
    """
    _access_token: Optional[str] = None
    _instance_url: Optional[str] = None
    _token_expiry: Optional[datetime] = None
    _issued_at: Optional[int] = None # Store as Unix timestamp (milliseconds)

    async def _is_token_expired(self) -> bool:
        """
        Determines whether the token is close to expiration or already expired.
        Applies a configurable buffer to avoid mid-request expiry.
        """
        if not self._token_expiry or not self._access_token:
            logger.info("Token or expiry not set, considering token expired.")
            return True

        # Ensure _token_expiry is offset-aware if settings.SALESFORCE_TOKEN_REFRESH_BUFFER is significant
        # For simplicity, assuming UTC for now.
        now_utc = datetime.utcnow()
        # logger.debug(f"Current UTC time: {now_utc}, Token expiry: {self._token_expiry}, Buffer: {settings.SALESFORCE_TOKEN_REFRESH_BUFFER}s")

        if now_utc >= (self._token_expiry - timedelta(seconds=settings.SALESFORCE_TOKEN_REFRESH_BUFFER)):
            logger.info("Token is close to or past expiry threshold; will attempt refresh.")
            return True
        return False

    async def authenticate(self) -> None:
        """
        Performs an authentication request against Salesforce
        using the OAuth 2.0 Password flow.
        On success, caches the access token, instance URL,
        and expiry time for future requests.
        Raises an HTTPException if authentication fails.
        """
        payload = {
            'grant_type': 'password',
            'client_id': settings.SALESFORCE_CLIENT_ID,
            'client_secret': settings.SALESFORCE_CLIENT_SECRET,
            'username': settings.SALESFORCE_USERNAME,
            'password': settings.SALESFORCE_PASSWORD
        }

        logger.info("Attempting to authenticate with Salesforce...")
        async with httpx.AsyncClient(timeout=30.0) as client: # Added timeout
            try:
                response = await client.post(str(settings.SALESFORCE_TOKEN_URL), data=payload) # Ensure URL is string
                response.raise_for_status() # Raises HTTPStatusError for 4xx/5xx responses
            except httpx.RequestError as e:
                logger.error(f"Salesforce authentication request failed (network issue): {str(e)}")
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail=f"Failed to authenticate with Salesforce (network issue): {e.__class__.__name__}"
                )
            except httpx.HTTPStatusError as e:
                logger.error(
                    f"Salesforce authentication failed with status {e.response.status_code}: {e.response.text}"
                )
                detail_msg = f"Failed to authenticate with Salesforce (HTTP error {e.response.status_code})"
                try:
                    err_json = e.response.json()
                    if 'error_description' in err_json:
                        detail_msg += f": {err_json['error_description']}"
                    elif 'error' in err_json:
                        detail_msg += f": {err_json['error']}"
                except ValueError: # Not JSON
                    pass # Use default detail_msg
                raise HTTPException(
                    status_code=e.response.status_code,
                    detail=detail_msg
                )

        auth_response = response.json()
        SalesforceAuth._access_token = auth_response.get('access_token')
        SalesforceAuth._instance_url = auth_response.get('instance_url')
        SalesforceAuth._issued_at = int(auth_response.get("issued_at")) # In milliseconds from epoch

        if not SalesforceAuth._access_token or not SalesforceAuth._instance_url:
            logger.error(f"Authentication response missing access_token or instance_url. Response: {auth_response}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Salesforce authentication response missing critical data."
            )

        # Calculate expiry time. 'issued_at' is Unix timestamp in milliseconds.
        # Salesforce typically doesn't return 'expires_in', so we assume a standard duration (e.g., 2 hours)
        # or rely on a fixed session timeout configured in Salesforce.
        # For robustness, let's assume a default validity if not provided by SF, e.g. 2 hours.
        # However, the 'issued_at' and refresh buffer logic is more about proactive refresh.
        # The actual session duration is managed by Salesforce.
        # Let's assume a default session duration of 2 hours (7200 seconds) for calculation if not available.
        # Note: Salesforce OAuth Password Flow tokens are often tied to session settings.
        # A better approach for server-to-server is JWT Bearer Flow or Client Credentials Flow if possible.

        # Using 'issued_at' (milliseconds) and assuming a session duration (e.g. 2 hours for this example)
        # This is a simplification. Real token lifetime is managed by Salesforce policies.
        # The refresh buffer helps us re-auth proactively.
        issued_at_seconds = SalesforceAuth._issued_at / 1000.0
        # Let's assume a typical session duration, e.g., 2 hours, if not specified by SF.
        # This is primarily for the _is_token_expired check with buffer.
        # The token will be invalidated by SF based on its policies.
        assumed_validity_seconds = 2 * 60 * 60 # 2 hours
        SalesforceAuth._token_expiry = datetime.utcfromtimestamp(issued_at_seconds + assumed_validity_seconds)

        logger.info(
            f"Authentication successful. Instance URL: {SalesforceAuth._instance_url}. Token will be proactively refreshed. Estimated expiry based on issued_at: {SalesforceAuth._token_expiry} UTC"
        )

    async def get_auth_details(self) -> Tuple[str, str]:
        """
        Retrieves the currently cached access token and instance URL.
        Re-authenticates if necessary or if the token is close to expiry.

        Returns:
            Tuple[str, str]: (access_token, instance_url)
        Raises:
            HTTPException: If authentication fails.
        """
        async with token_lock: # Ensure only one coroutine tries to authenticate/refresh at a time
            if await self._is_token_expired():
                logger.info("Token expired or needs refresh. Re-authenticating...")
                await self.authenticate()
            elif not SalesforceAuth._access_token or not SalesforceAuth._instance_url:
                logger.info("Token or instance URL not available. Re-authenticating...")
                await self.authenticate()

        if not SalesforceAuth._access_token or not SalesforceAuth._instance_url:
            logger.error("Authentication failed to produce a token or instance URL after attempt.")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to obtain Salesforce authentication details."
            )
        return SalesforceAuth._access_token, SalesforceAuth._instance_url

    async def handle_401_unauthorized(self):
        """
        Forces a token refresh when a 401 Unauthorized error is encountered
        from Salesforce. This method should be called by the client making the API call.
        """
        logger.warning("Received 401 Unauthorized from Salesforce. Forcing token refresh.")
        async with token_lock:
            # Invalidate current token details to ensure re-authentication
            SalesforceAuth._access_token = None
            SalesforceAuth._token_expiry = None
            await self.authenticate()
        logger.info("Token refreshed after 401.")


# Singleton instance of SalesforceAuth
# This instance will be shared across requests if get_salesforce_auth_instance is used as a dependency.
_auth_instance = None

async def get_salesforce_auth_instance() -> SalesforceAuth:
    """
    FastAPI dependency to get a SalesforceAuth instance.
    It ensures that the token is fetched and valid.
    """
    global _auth_instance
    if _auth_instance is None:
        _auth_instance = SalesforceAuth()

    # This call will trigger authentication if needed
    await _auth_instance.get_auth_details()
    return _auth_instance

# Example of how it might be used as a dependency in a router:
# from fastapi import Depends
# @router.get("/some-sfdc-data")
# async def get_some_data(auth: SalesforceAuth = Depends(get_salesforce_auth_instance)):
#     access_token, instance_url = await auth.get_auth_details()
#     # ... use token and url to make Salesforce API call ...
#     pass
