import aiohttp
from typing import Optional, List, Dict, Any, TypedDict, cast
import time

class PipedreamAuthError(Exception):
    """Custom exception for Pipedream authentication errors."""
    pass

class PipedreamApiError(Exception):
    """Custom exception for Pipedream API errors."""
    pass

class ConnectTokenResponse(TypedDict):
    token: str
    expires_at: str
    connect_link_url: str

class AppInfo(TypedDict):
    id: str
    name: str
    # Add other potential fields from the example if needed, marking as Optional
    name_slug: Optional[str]
    auth_type: Optional[str]
    description: Optional[str]
    img_src: Optional[str]
    custom_fields_json: Optional[str] # Assuming string representation of JSON array
    categories: Optional[List[str]]

class Account(TypedDict):
    id: str
    name: Optional[str]
    external_id: str
    healthy: bool
    dead: Optional[bool] # Can be null in example
    app: AppInfo
    created_at: str
    updated_at: str
    credentials: Optional[Dict[str, Any]] # Changed from AccountCredentials
    expires_at: Optional[str]
    error: Optional[Any] # Structure unknown
    last_refreshed_at: Optional[str]
    next_refresh_at: Optional[str]

class PageInfo(TypedDict):
    total_count: int
    count: int
    start_cursor: Optional[str]
    end_cursor: Optional[str]

class GetAccountsResponse(TypedDict):
    page_info: PageInfo
    data: List[Account] # The docs show data: { accounts: [...]}, adjusting to list directly for simplicity

class GetAccountByIdResponse(TypedDict):
    data: Account

class ComponentSummary(TypedDict):
    name: str
    version: str
    key: str
    # Add other fields if needed from a more detailed example
    description: Optional[str] # Added based on V1Component
    component_type: Optional[str] # Added based on V1Component

class GetComponentsResponse(TypedDict):
    page_info: PageInfo
    data: List[ComponentSummary]

# Represents a single property definition within a component
class ComponentProp(TypedDict):
    name: str
    type: str
    label: Optional[str]
    description: Optional[str]
    app: Optional[str] # If type is 'app'
    optional: Optional[bool]
    default: Optional[Any]
    remoteOptions: Optional[bool] # Deprecated? Use options/asyncOptions?
    options: Optional[List[Any]] # Or List[Dict[str, Any]] for label/value pairs
    customResponse: Optional[bool] # For http interface
    useQuery: Optional[bool]
    reloadProps: Optional[bool]
    static: Optional[Dict[str, Any]] # For timer interface
    # Add other potential prop fields as needed

class ComponentDetail(TypedDict):
    name: str
    version: str
    key: str
    configurable_props: List[ComponentProp]
    # Add other potential fields like 'description', 'app', etc. if available
    description: Optional[str] # Added based on V1Component
    component_type: Optional[str] # Added based on V1Component

class GetComponentResponse(TypedDict):
    data: ComponentDetail

class Timings(TypedDict):
    timings: Optional[Dict[str, float]]

# Represents the dynamic props info returned by reload_component_props
class DynamicPropsInfo(TypedDict):
    id: str
    configurableProps: List[ComponentProp]

class ReloadComponentPropsResponse(TypedDict):
    dynamicProps: DynamicPropsInfo
    errors: List[str]
    observations: Optional[List[Any]]

# Represents the response from running an action
class RunActionResponse(TypedDict):
    exports: Dict[str, Any] # Based on example, could be Any if structure varies
    os: List[Any] # Observations/logs
    ret: Any # The direct return value of the action

# Represents a deployed trigger instance (V1DeployedComponent in TS)
class DeployedComponent(TypedDict):
    id: str # The deployed component ID (e.g., dc_xxxx)
    owner_id: str # Pipedream internal user ID (e.g., exu_xxxx)
    component_id: str # Source component ID (e.g., sc_xxxx)
    configurable_props: List[ComponentProp]
    configured_props: Dict[str, Any]
    active: bool
    created_at: int # Unix timestamp (seconds)
    updated_at: int # Unix timestamp (seconds)
    name: str
    name_slug: str
    # callback_observations: Optional[Any] # Field from TS, not in mdx example

# Response for deploying a trigger
class DeployTriggerResponse(TypedDict):
    data: DeployedComponent

# Response for listing deployed triggers
class GetDeployedTriggersResponse(TypedDict):
    page_info: PageInfo
    data: List[DeployedComponent]

# Response for getting a single deployed trigger
class GetDeployedTriggerResponse(TypedDict):
    data: DeployedComponent

# Represents an emitted event (V1EmittedEvent in TS)
class EmittedEvent(TypedDict):
    e: Dict[str, Any] # The event payload
    k: str # Event type (e.g., "emit")
    ts: int # Timestamp in milliseconds
    id: str # Event ID

# Response for getting trigger events
class GetDeployedTriggerEventsResponse(TypedDict):
    data: List[EmittedEvent]

# Response for getting/updating trigger webhooks
class GetDeployedTriggerWebhooksResponse(TypedDict):
    webhook_urls: List[str]

class UpdateDeployedTriggerWebhooksResponse(TypedDict):
    webhook_urls: List[str]

# Response for getting/updating trigger workflows
class GetDeployedTriggerWorkflowsResponse(TypedDict):
    workflow_ids: List[str]

class UpdateDeployedTriggerWorkflowsResponse(TypedDict):
    workflow_ids: List[str]

# Response for creating a rate limit
class CreateRateLimitResponse(TypedDict):
    token: str

class PipedreamClient:
    """Asynchronous client for the Pipedream Connect API."""

    BASE_URL = "https://api.pipedream.com/v1"

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        project_id: str,
        environment: Optional[str] = None,
        session: Optional[aiohttp.ClientSession] = None,
    ):
        """
        Initializes the Pipedream async client.

        Args:
            client_id: Your Pipedream OAuth client ID.
            client_secret: Your Pipedream OAuth client secret.
            project_id: Your Pipedream project ID.
            environment: Optional environment ('development' or 'production').
            session: Optional external aiohttp.ClientSession.
        """
        if not client_id or not client_secret or not project_id:
            raise ValueError("client_id, client_secret, and project_id are required.")

        self._client_id = client_id
        self._client_secret = client_secret
        self._project_id = project_id
        self._environment = environment
        self._session = session or aiohttp.ClientSession()
        self._close_session = session is None  # Flag to close session only if created internally

        self._access_token: Optional[str] = None
        self._token_expires_at: float = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        """Closes the underlying aiohttp session if it was created internally."""
        if self._close_session and self._session and not self._session.closed:
            await self._session.close()

    async def _get_access_token(self) -> str:
        """
        Retrieves an OAuth access token using client credentials.
        Handles token caching and expiration.
        """
        if self._access_token and time.time() < self._token_expires_at:
            return self._access_token

        token_url = f"{self.BASE_URL}/oauth/token"
        payload = {
            "grant_type": "client_credentials",
            "client_id": self._client_id,
            "client_secret": self._client_secret,
        }
        headers = {"Content-Type": "application/json"}

        try:
            async with self._session.post(token_url, json=payload, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    self._access_token = data.get("access_token")
                    # Set expiry slightly before actual expiry (e.g., 60 seconds buffer)
                    expires_in = data.get("expires_in", 3600) # Default to 1 hour if not provided
                    self._token_expires_at = time.time() + expires_in - 60
                    if not self._access_token:
                         raise PipedreamAuthError("Failed to retrieve access token: 'access_token' missing in response.")
                    return self._access_token
                else:
                    error_text = await response.text()
                    raise PipedreamAuthError(
                        f"Failed to retrieve access token: {response.status} - {error_text}"
                    )
        except aiohttp.ClientError as e:
            raise PipedreamAuthError(f"Network error during token retrieval: {e}")

    async def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        requires_auth: bool = True,
    ) -> Dict[str, Any]:
        """Makes an authenticated request to the Pipedream API."""
        url = f"{self.BASE_URL}/{path.lstrip('/')}"
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        if requires_auth:
            access_token = await self._get_access_token()
            headers["Authorization"] = f"Bearer {access_token}"

        if self._environment:
            headers["X-PD-Environment"] = self._environment

        try:
            async with self._session.request(
                method, url, params=params, json=json_data, headers=headers
            ) as response:
                response_data = await response.json() # Assume JSON response for simplicity
                if 200 <= response.status < 300:
                    return response_data
                else:
                    raise PipedreamApiError(
                        f"API request failed: {response.status} - {response_data.get('error', {}).get('message', response_data)}"
                    )
        except aiohttp.ClientError as e:
            raise PipedreamApiError(f"Network error during API request: {e}")
        except ValueError: # Catches JSONDecodeError
             error_text = await response.text()
             raise PipedreamApiError(f"Failed to decode JSON response: {response.status} - {error_text}")


    async def create_connect_token(
        self,
        external_user_id: str,
        allowed_origins: Optional[List[str]] = None,
        success_redirect_uri: Optional[str] = None,
        error_redirect_uri: Optional[str] = None,
        webhook_uri: Optional[str] = None,
    ) -> ConnectTokenResponse:
        """
        Creates a short-lived token for initiating account connections.

        Args:
            external_user_id: The ID of your end user in your system.
            allowed_origins: List of URLs allowed to make requests with the token (required for client-side).
            success_redirect_uri: Optional redirect URI on successful auth (Connect Link).
            error_redirect_uri: Optional redirect URI on auth error (Connect Link).
            webhook_uri: Optional webhook URL for auth events.

        Returns:
            A dictionary containing the token, expiration time, and connect link URL.

        Raises:
            PipedreamAuthError: If authentication fails.
            PipedreamApiError: If the API request fails.
            ValueError: If required parameters are missing.
        """
        if not external_user_id:
            raise ValueError("external_user_id is required.")

        path = f"connect/{self._project_id}/tokens"
        payload: Dict[str, Any] = {"external_user_id": external_user_id}

        if allowed_origins:
            payload["allowed_origins"] = allowed_origins
        if success_redirect_uri:
            payload["success_redirect_uri"] = success_redirect_uri
        if error_redirect_uri:
            payload["error_redirect_uri"] = error_redirect_uri
        if webhook_uri:
            payload["webhook_uri"] = webhook_uri

        response_data = await self._request("POST", path, json_data=payload)

        # Validate expected keys are in the response
        if not all(k in response_data for k in ["token", "expires_at", "connect_link_url"]):
             raise PipedreamApiError(f"Unexpected response format for create_connect_token: {response_data}")

        return ConnectTokenResponse(
            token=response_data["token"],
            expires_at=response_data["expires_at"],
            connect_link_url=response_data["connect_link_url"]
        )

    async def get_accounts(
        self,
        app_filter: Optional[str] = None,
        oauth_app_id: Optional[str] = None,
        external_user_id: Optional[str] = None,
        include_credentials: bool = False,
        limit: Optional[int] = None, # Added standard pagination param
        cursor: Optional[str] = None, # Added standard pagination param
    ) -> GetAccountsResponse:
        """
        Lists connected accounts within the project.

        Args:
            app_filter: Optional app ID or name slug to filter by (e.g., 'slack' or 'app_OkrhR1').
            oauth_app_id: Optional OAuth app ID to filter accounts for.
            external_user_id: Optional external user ID to filter accounts for.
            include_credentials: If True, includes account credentials in the response (use with caution).
            limit: Optional maximum number of accounts to return.
            cursor: Optional pagination cursor for fetching the next page.

        Returns:
            A dictionary containing pagination info and a list of accounts.

        Raises:
            PipedreamAuthError: If authentication fails.
            PipedreamApiError: If the API request fails.
        """
        path = f"connect/{self._project_id}/accounts"
        params: Dict[str, Any] = {}
        if app_filter:
            params["app"] = app_filter
        if oauth_app_id:
            params["oauth_app_id"] = oauth_app_id
        if external_user_id:
            params["external_user_id"] = external_user_id
        if include_credentials:
            # API uses string 'true', ensure correct type
            params["include_credentials"] = str(include_credentials).lower()
        if limit:
            params["limit"] = limit
        if cursor:
            params["cursor"] = cursor # Assuming API uses 'cursor' for pagination

        response_data = await self._request("GET", path, params=params)

        # Basic validation of response structure
        if "page_info" not in response_data or "data" not in response_data:
             raise PipedreamApiError(f"Unexpected response format for get_accounts: {response_data}")

        # The API response has { data: { accounts: [...] } }, this method adapts it.
        # The TypedDict expects the final structure: { page_info: ..., data: List[Account] }
        accounts_data = response_data["data"]
        if isinstance(accounts_data, dict) and "accounts" in accounts_data:
            account_list = accounts_data["accounts"]
        elif isinstance(accounts_data, list):
             # Handle cases where the API might return just the list directly under data
             account_list = accounts_data
        else:
             raise PipedreamApiError(f"Unexpected 'data' structure in get_accounts response: {accounts_data}")

        return GetAccountsResponse(
            page_info=response_data["page_info"],
            data=account_list # Ensure this matches the TypedDict
        )

    async def get_account_by_id(
        self,
        account_id: str,
        include_credentials: bool = False,
    ) -> GetAccountByIdResponse:
        """
        Retrieves details for a specific connected account.

        Args:
            account_id: The ID of the account to retrieve (e.g., 'apn_WYhMlrz').
            include_credentials: If True, includes account credentials in the response.

        Returns:
            A dictionary containing the account details under the 'data' key.

        Raises:
            PipedreamAuthError: If authentication fails.
            PipedreamApiError: If the API request fails or the response format is unexpected.
            ValueError: If account_id is not provided.
        """
        if not account_id:
            raise ValueError("account_id is required.")

        path = f"connect/{self._project_id}/accounts/{account_id}"
        params: Dict[str, Any] = {}
        if include_credentials:
            params["include_credentials"] = str(include_credentials).lower()

        response_data = await self._request("GET", path, params=params)

        # Basic validation of response structure
        if "data" not in response_data or not isinstance(response_data["data"], dict):
             raise PipedreamApiError(f"Unexpected response format for get_account_by_id: {response_data}")
        if "id" not in response_data["data"]: # Check for essential key within data
             raise PipedreamApiError(f"Missing 'id' in account data for get_account_by_id: {response_data['data']}")

        # The type checker expects the structure defined in GetAccountByIdResponse
        # We assume the structure matches the Account TypedDict under the 'data' key
        # Cast to assure the type checker
        return GetAccountByIdResponse(data=cast(Account, response_data["data"]))

    async def delete_account(self, account_id: str) -> None:
        """
        Deletes a specific connected account.

        Args:
            account_id: The ID of the account to delete.

        Returns:
            None

        Raises:
            PipedreamAuthError: If authentication fails.
            PipedreamApiError: If the API request fails (e.g., account not found, permission error).
            ValueError: If account_id is not provided.
        """
        if not account_id:
            raise ValueError("account_id is required.")

        path = f"connect/{self._project_id}/accounts/{account_id}"

        # Use _request but expect no JSON body on success (204)
        # Modify _request or handle 204 specifically here
        url = f"{self.BASE_URL}/{path.lstrip('/')}"
        headers = {"Accept": "application/json"} # Still accept JSON for potential errors

        access_token = await self._get_access_token()
        headers["Authorization"] = f"Bearer {access_token}"

        if self._environment:
            headers["X-PD-Environment"] = self._environment

        try:
            async with self._session.delete(url, headers=headers) as response:
                if response.status == 204:
                    return # Success
                else:
                    # Try to get error details if available
                    try:
                        error_data = await response.json()
                        error_message = error_data.get('error', {}).get('message', str(error_data))
                    except (aiohttp.ContentTypeError, ValueError): # Handle non-JSON or decode error
                        error_message = await response.text()
                    raise PipedreamApiError(
                        f"API request failed: {response.status} - {error_message}"
                    )
        except aiohttp.ClientError as e:
            raise PipedreamApiError(f"Network error during API request: {e}")

    async def delete_accounts_by_app(self, app_id: str) -> None:
        """
        Deletes all connected accounts for a specific app within the project.

        Args:
            app_id: The app ID (e.g., 'app_OkrhR1') or name slug (e.g., 'slack')
                    for which to delete all connected accounts.

        Returns:
            None

        Raises:
            PipedreamAuthError: If authentication fails.
            PipedreamApiError: If the API request fails.
            ValueError: If app_id is not provided.
        """
        if not app_id:
            raise ValueError("app_id is required.")

        path = f"connect/{self._project_id}/apps/{app_id}/accounts"

        # Similar to delete_account, expect 204 on success
        url = f"{self.BASE_URL}/{path.lstrip('/')}"
        headers = {"Accept": "application/json"}

        access_token = await self._get_access_token()
        headers["Authorization"] = f"Bearer {access_token}"

        if self._environment:
            headers["X-PD-Environment"] = self._environment

        try:
            async with self._session.delete(url, headers=headers) as response:
                if response.status == 204:
                    return # Success
                else:
                    try:
                        error_data = await response.json()
                        error_message = error_data.get('error', {}).get('message', str(error_data))
                    except (aiohttp.ContentTypeError, ValueError):
                        error_message = await response.text()
                    raise PipedreamApiError(
                        f"API request failed: {response.status} - {error_message}"
                    )
        except aiohttp.ClientError as e:
            raise PipedreamApiError(f"Network error during API request: {e}")

    async def delete_external_user(self, external_user_id: str) -> None:
        """
        Deletes an end user, all their connected accounts, and any deployed triggers.

        Args:
            external_user_id: The external user ID in your system.

        Returns:
            None

        Raises:
            PipedreamAuthError: If authentication fails.
            PipedreamApiError: If the API request fails.
            ValueError: If external_user_id is not provided.
        """
        if not external_user_id:
            raise ValueError("external_user_id is required.")

        path = f"connect/{self._project_id}/users/{external_user_id}"

        # Expect 204 on success
        url = f"{self.BASE_URL}/{path.lstrip('/')}"
        headers = {"Accept": "application/json"}

        access_token = await self._get_access_token()
        headers["Authorization"] = f"Bearer {access_token}"

        if self._environment:
            headers["X-PD-Environment"] = self._environment

        try:
            async with self._session.delete(url, headers=headers) as response:
                if response.status == 204:
                    return # Success
                else:
                    try:
                        error_data = await response.json()
                        error_message = error_data.get('error', {}).get('message', str(error_data))
                    except (aiohttp.ContentTypeError, ValueError):
                        error_message = await response.text()
                    raise PipedreamApiError(
                        f"API request failed: {response.status} - {error_message}"
                    )
        except aiohttp.ClientError as e:
            raise PipedreamApiError(f"Network error during API request: {e}")

    # --- Components --- #

    async def get_components(
        self,
        component_type: str, # 'triggers', 'actions', or 'components'
        app_filter: Optional[str] = None,
        search_query: Optional[str] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
    ) -> GetComponentsResponse:
        """
        Lists components (triggers, actions, or general components) in the registry.

        Args:
            component_type: The type of component to list ('triggers', 'actions', 'components').
            app_filter: Optional app ID or name slug to filter by.
            search_query: Optional search query ('q') to filter components by key/name.
            limit: Optional maximum number of components to return.
            cursor: Optional pagination cursor.

        Returns:
            A dictionary containing pagination info and a list of component summaries.

        Raises:
            PipedreamAuthError: If authentication fails.
            PipedreamApiError: If the API request fails.
            ValueError: If component_type is invalid.
        """
        valid_types = ["triggers", "actions", "components"]
        if component_type not in valid_types:
            raise ValueError(f"Invalid component_type: '{component_type}'. Must be one of {valid_types}")

        # Note: The API path seems to be /v1/{component_type} based on docs, not /v1/connect/{project_id}/{component_type}
        # Adjusting path based on GET /{component_type} example
        # However, auth/project scoping might still be needed implicitly or via headers.
        # Let's assume the connect base path IS needed for consistency unless proven otherwise.
        path = f"connect/{self._project_id}/{component_type}"
        # If the above fails, the alternative path structure from docs is simply: path = component_type

        params: Dict[str, Any] = {}
        if app_filter:
            params["app"] = app_filter
        if search_query:
            params["q"] = search_query
        if limit:
            params["limit"] = limit
        if cursor:
            params["cursor"] = cursor

        response_data = await self._request("GET", path, params=params)

        if "page_info" not in response_data or "data" not in response_data or not isinstance(response_data["data"], list):
            raise PipedreamApiError(f"Unexpected response format for get_components: {response_data}")

        return GetComponentsResponse(
            page_info=response_data["page_info"],
            data=response_data["data"]
        )

    async def get_component(
        self,
        component_type: str,
        component_key: str,
    ) -> GetComponentResponse:
        """
        Retrieves details for a specific component from the registry.

        Args:
            component_type: The type of component ('triggers', 'actions', 'components').
            component_key: The key identifying the component (e.g., 'gitlab-new-issue').

        Returns:
            A dictionary containing the component details under the 'data' key.

        Raises:
            PipedreamAuthError: If authentication fails.
            PipedreamApiError: If the API request fails.
            ValueError: If component_type is invalid or component_key is missing.
        """
        valid_types = ["triggers", "actions", "components"]
        if component_type not in valid_types:
            raise ValueError(f"Invalid component_type: '{component_type}'. Must be one of {valid_types}")
        if not component_key:
            raise ValueError("component_key is required.")

        # Assuming consistent path structure with /connect/{project_id}/
        path = f"connect/{self._project_id}/{component_type}/{component_key}"
        # Alternative path if needed: path = f"{component_type}/{component_key}"

        response_data = await self._request("GET", path)

        if "data" not in response_data or not isinstance(response_data["data"], dict):
             raise PipedreamApiError(f"Unexpected response format for get_component: {response_data}")
        if "key" not in response_data["data"] or "configurable_props" not in response_data["data"]:
             raise PipedreamApiError(f"Missing essential keys in component data for get_component: {response_data['data']}")

        # Cast needed because _request returns Dict[str, Any]
        return GetComponentResponse(data=cast(ComponentDetail, response_data["data"]))

    async def reload_component_props(
        self,
        component_type: str,
        component_key: str,
        external_user_id: str,
        configured_props: Dict[str, Any],
        dynamic_props_id: Optional[str] = None,
    ) -> ReloadComponentPropsResponse:
        """
        Reloads a component's props, typically after setting a dynamic prop.

        Args:
            component_type: The type of component ('triggers', 'actions', 'components').
            component_key: The key identifying the component.
            external_user_id: The external user ID in your system.
            configured_props: Dictionary of props already configured for the component.
            dynamic_props_id: Optional ID from a previous prop reconfiguration.

        Returns:
            A dictionary containing the potentially updated dynamic props structure.

        Raises:
            PipedreamAuthError: If authentication fails.
            PipedreamApiError: If the API request fails.
            ValueError: If required arguments are missing or component_type is invalid.
        """
        valid_types = ["triggers", "actions", "components"]
        if component_type not in valid_types:
            raise ValueError(f"Invalid component_type: '{component_type}'. Must be one of {valid_types}")
        if not all([component_key, external_user_id]):
            raise ValueError("component_key and external_user_id are required.")

        # Path structure based on docs: /{component_type}/props
        # Assuming /connect/{project_id} prefix is needed
        path = f"connect/{self._project_id}/{component_type}/props"

        payload = {
            "id": component_key,
            "external_user_id": external_user_id,
            "configured_props": configured_props,
        }
        if dynamic_props_id:
            payload["dynamic_props_id"] = dynamic_props_id

        response_data = await self._request("POST", path, json_data=payload)

        # Basic validation
        if "dynamicProps" not in response_data or not isinstance(response_data["dynamicProps"], dict):
             raise PipedreamApiError(f"Missing or invalid 'dynamicProps' in response for reload_component_props: {response_data}")
        if "id" not in response_data["dynamicProps"] or "configurableProps" not in response_data["dynamicProps"]:
             raise PipedreamApiError(f"Missing keys within 'dynamicProps' in response for reload_component_props: {response_data['dynamicProps']}")

        # Cast needed due to _request return type
        dynamic_props_data = cast(DynamicPropsInfo, response_data["dynamicProps"])

        return ReloadComponentPropsResponse(
            dynamicProps=dynamic_props_data,
            errors=response_data.get("errors", []),
            observations=response_data.get("observations")
        )

    async def run_action(
        self,
        action_key: str,
        external_user_id: str,
        configured_props: Dict[str, Any],
        dynamic_props_id: Optional[str] = None,
    ) -> RunActionResponse:
        """
        Invokes an action component for a Pipedream Connect user.

        Args:
            action_key: The key identifying the action component (e.g., 'gitlab-list-commits').
            external_user_id: The external user ID in your system.
            configured_props: Dictionary of props configured for the action.
            dynamic_props_id: Optional ID from a previous prop reconfiguration.

        Returns:
            A dictionary containing the action's exports, observations (logs), and return value.

        Raises:
            PipedreamAuthError: If authentication fails.
            PipedreamApiError: If the API request fails.
            ValueError: If required arguments are missing.
        """
        if not all([action_key, external_user_id]):
             raise ValueError("action_key and external_user_id are required.")

        # Path: /actions/run - Note: component_type is 'actions' implicitly
        path = f"connect/{self._project_id}/actions/run"

        payload = {
            "id": action_key,
            "external_user_id": external_user_id,
            "configured_props": configured_props,
        }
        if dynamic_props_id:
            payload["dynamic_props_id"] = dynamic_props_id

        response_data = await self._request("POST", path, json_data=payload)

        # Basic validation based on example response keys
        if not all(k in response_data for k in ["exports", "os", "ret"]):
            raise PipedreamApiError(f"Unexpected response format for run_action: {response_data}")

        # Using Any for ret, os elements, and exports values for flexibility
        return RunActionResponse(
            exports=response_data["exports"],
            os=response_data["os"],
            ret=response_data["ret"]
        )

    async def deploy_trigger(
        self,
        trigger_key: str,
        external_user_id: str,
        configured_props: Dict[str, Any],
        webhook_url: Optional[str] = None,
        workflow_id: Optional[str] = None,
        dynamic_props_id: Optional[str] = None,
    ) -> DeployTriggerResponse:
        """
        Deploys a trigger component for a Pipedream Connect user.

        Args:
            trigger_key: The key identifying the trigger component (e.g., 'gitlab-new-issue').
            external_user_id: The external user ID in your system.
            configured_props: Dictionary of props configured for the trigger.
            webhook_url: Optional URL to which the trigger will send events.
            workflow_id: Optional Pipedream workflow ID (p_...) to which events are sent.
            dynamic_props_id: Optional ID from a previous prop reconfiguration.

        Returns:
            A dictionary containing the details of the deployed trigger under the 'data' key.

        Raises:
            PipedreamAuthError: If authentication fails.
            PipedreamApiError: If the API request fails.
            ValueError: If required arguments are missing or webhook/workflow conflict.
        """
        if not all([trigger_key, external_user_id]):
             raise ValueError("trigger_key and external_user_id are required.")
        if webhook_url and workflow_id:
             raise ValueError("Provide either webhook_url or workflow_id, not both.")
        # Note: The API might allow deploying without either webhook_url or workflow_id,
        # depending on the trigger type (e.g., if it uses $.interface.timer internally).
        # We won't enforce having one here, letting the API decide.

        # Path: /triggers/deploy
        path = f"connect/{self._project_id}/triggers/deploy"

        payload = {
            "id": trigger_key, # Maps to triggerId in TS opts
            "external_user_id": external_user_id,
            "configured_props": configured_props,
        }
        if webhook_url:
            payload["webhook_url"] = webhook_url # Key based on .mdx
        if workflow_id:
            payload["workflowId"] = workflow_id # Key based on TS DeployTriggerOpts
        if dynamic_props_id:
            payload["dynamic_props_id"] = dynamic_props_id

        response_data = await self._request("POST", path, json_data=payload)

        # Basic validation based on example response structure
        if "data" not in response_data or not isinstance(response_data["data"], dict):
            raise PipedreamApiError(f"Unexpected response format for deploy_trigger: {response_data}")
        if "id" not in response_data["data"] or "component_id" not in response_data["data"]:
             raise PipedreamApiError(f"Missing essential keys in deploy_trigger data: {response_data['data']}")

        # Cast needed because _request returns Dict[str, Any]
        return DeployTriggerResponse(data=cast(DeployedComponent, response_data["data"]))

    # --- Deployed Triggers --- #

    async def get_deployed_triggers(
        self,
        external_user_id: str,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
    ) -> GetDeployedTriggersResponse:
        """
        Lists deployed triggers for a specific external user.

        Args:
            external_user_id: The external user ID in your system.
            limit: Optional maximum number of triggers to return.
            cursor: Optional pagination cursor.

        Returns:
            A dictionary containing pagination info and a list of deployed triggers.

        Raises:
            PipedreamAuthError: If authentication fails.
            PipedreamApiError: If the API request fails.
            ValueError: If external_user_id is missing.
        """
        if not external_user_id:
            raise ValueError("external_user_id is required.")

        path = f"connect/{self._project_id}/deployed-triggers"
        params: Dict[str, Any] = {"external_user_id": external_user_id}
        if limit:
            params["limit"] = limit
        if cursor:
            # Assuming cursor param is named 'cursor', aligned with get_accounts
            # TS uses 'after'/'before', might need adjustment if API differs
            params["cursor"] = cursor

        response_data = await self._request("GET", path, params=params)

        if not all(k in response_data for k in ["page_info", "data"]) or not isinstance(response_data["data"], list):
            raise PipedreamApiError(f"Unexpected response format for get_deployed_triggers: {response_data}")

        return GetDeployedTriggersResponse(
            page_info=response_data["page_info"],
            data=response_data["data"] # Assuming data is List[DeployedComponent]
        )

    async def get_deployed_trigger(
        self,
        deployed_component_id: str,
        external_user_id: str,
    ) -> GetDeployedTriggerResponse:
        """
        Retrieves details for a specific deployed trigger.

        Args:
            deployed_component_id: The ID of the deployed trigger (e.g., 'dc_xxxxxxx').
            external_user_id: The external user ID associated with the trigger.

        Returns:
            A dictionary containing the deployed trigger details under the 'data' key.

        Raises:
            PipedreamAuthError: If authentication fails.
            PipedreamApiError: If the API request fails.
            ValueError: If required arguments are missing.
        """
        if not all([deployed_component_id, external_user_id]):
            raise ValueError("deployed_component_id and external_user_id are required.")

        path = f"connect/{self._project_id}/deployed-triggers/{deployed_component_id}"
        params = {"external_user_id": external_user_id}

        response_data = await self._request("GET", path, params=params)

        if "data" not in response_data or not isinstance(response_data["data"], dict):
             raise PipedreamApiError(f"Unexpected response format for get_deployed_trigger: {response_data}")
        if "id" not in response_data["data"]: # Check essential key
             raise PipedreamApiError(f"Missing 'id' in deployed trigger data: {response_data['data']}")

        return GetDeployedTriggerResponse(data=cast(DeployedComponent, response_data["data"]))

    async def delete_deployed_trigger(
        self,
        deployed_component_id: str,
        external_user_id: str,
        ignore_hook_errors: bool = False,
    ) -> None:
        """
        Deletes a specific deployed trigger.

        Args:
            deployed_component_id: The ID of the deployed trigger to delete.
            external_user_id: The external user ID associated with the trigger.
            ignore_hook_errors: If True, ignores errors during the trigger's deactivation hook.

        Returns: None

        Raises:
            PipedreamAuthError: If authentication fails.
            PipedreamApiError: If the API request fails.
            ValueError: If required arguments are missing.
        """
        if not all([deployed_component_id, external_user_id]):
            raise ValueError("deployed_component_id and external_user_id are required.")

        path = f"connect/{self._project_id}/deployed-triggers/{deployed_component_id}"
        url = f"{self.BASE_URL}/{path.lstrip('/')}"

        # Parameters are sent in query string for DELETE according to docs/TS
        params: Dict[str, Any] = {"external_user_id": external_user_id}
        if ignore_hook_errors:
            params["ignoreHookErrors"] = str(ignore_hook_errors).lower()

        headers = {"Accept": "application/json"}
        access_token = await self._get_access_token()
        headers["Authorization"] = f"Bearer {access_token}"
        if self._environment:
            headers["X-PD-Environment"] = self._environment

        try:
            async with self._session.delete(url, params=params, headers=headers) as response:
                if response.status == 204:
                    return # Success
                else:
                    try:
                        error_data = await response.json()
                        error_message = error_data.get('error', {}).get('message', str(error_data))
                    except (aiohttp.ContentTypeError, ValueError):
                        error_message = await response.text()
                    raise PipedreamApiError(f"API request failed: {response.status} - {error_message}")
        except aiohttp.ClientError as e:
            raise PipedreamApiError(f"Network error during API request: {e}")

    async def get_deployed_trigger_events(
        self,
        deployed_component_id: str,
        external_user_id: str,
        limit: Optional[int] = None, # .mdx uses 'n', TS uses 'limit'. Assuming 'limit'.
    ) -> GetDeployedTriggerEventsResponse:
        """
        Retrieves recent events emitted by a deployed trigger.

        Args:
            deployed_component_id: The ID of the deployed trigger.
            external_user_id: The external user ID associated with the trigger.
            limit: Optional maximum number of events to retrieve (default 10, max 100 in docs).

        Returns:
            A dictionary containing a list of emitted events under the 'data' key.

        Raises:
            PipedreamAuthError: If authentication fails.
            PipedreamApiError: If the API request fails.
            ValueError: If required arguments are missing.
        """
        if not all([deployed_component_id, external_user_id]):
            raise ValueError("deployed_component_id and external_user_id are required.")

        path = f"connect/{self._project_id}/deployed-triggers/{deployed_component_id}/events"
        params: Dict[str, Any] = {"external_user_id": external_user_id}
        if limit is not None:
            params["limit"] = limit # Using 'limit' based on TS GetTriggerEventsOpts

        response_data = await self._request("GET", path, params=params)

        if "data" not in response_data or not isinstance(response_data["data"], list):
             raise PipedreamApiError(f"Unexpected response format for get_deployed_trigger_events: {response_data}")

        return GetDeployedTriggerEventsResponse(data=response_data["data"])

    async def get_deployed_trigger_webhooks(
        self,
        deployed_component_id: str,
        external_user_id: str,
    ) -> GetDeployedTriggerWebhooksResponse:
        """
        Retrieves the list of webhook URLs listening to a deployed trigger.

        Args:
            deployed_component_id: The ID of the deployed trigger.
            external_user_id: The external user ID associated with the trigger.

        Returns:
            A dictionary containing a list of webhook URLs.

        Raises:
            PipedreamAuthError: If authentication fails.
            PipedreamApiError: If the API request fails.
            ValueError: If required arguments are missing.
        """
        if not all([deployed_component_id, external_user_id]):
            raise ValueError("deployed_component_id and external_user_id are required.")

        path = f"connect/{self._project_id}/deployed-triggers/{deployed_component_id}/webhooks"
        params = {"external_user_id": external_user_id}

        response_data = await self._request("GET", path, params=params)

        if "webhook_urls" not in response_data or not isinstance(response_data["webhook_urls"], list):
             raise PipedreamApiError(f"Unexpected response format for get_deployed_trigger_webhooks: {response_data}")

        return GetDeployedTriggerWebhooksResponse(webhook_urls=response_data["webhook_urls"])

    async def update_deployed_trigger_webhooks(
        self,
        deployed_component_id: str,
        external_user_id: str,
        webhook_urls: List[str],
    ) -> UpdateDeployedTriggerWebhooksResponse:
        """
        Updates the list of webhook URLs listening to a deployed trigger.

        Args:
            deployed_component_id: The ID of the deployed trigger.
            external_user_id: The external user ID associated with the trigger.
            webhook_urls: The new list of webhook URLs.

        Returns:
            A dictionary containing the confirmed list of webhook URLs.

        Raises:
            PipedreamAuthError: If authentication fails.
            PipedreamApiError: If the API request fails.
            ValueError: If required arguments are missing.
        """
        if not all([deployed_component_id, external_user_id]):
            raise ValueError("deployed_component_id and external_user_id are required.")
        # webhook_urls can be an empty list

        path = f"connect/{self._project_id}/deployed-triggers/{deployed_component_id}/webhooks"
        # PUT request requires external_user_id in query params according to .mdx curl example
        params = {"external_user_id": external_user_id}
        payload = {"webhook_urls": webhook_urls}

        response_data = await self._request("PUT", path, params=params, json_data=payload)

        if "webhook_urls" not in response_data or not isinstance(response_data["webhook_urls"], list):
             raise PipedreamApiError(f"Unexpected response format for update_deployed_trigger_webhooks: {response_data}")

        return UpdateDeployedTriggerWebhooksResponse(webhook_urls=response_data["webhook_urls"])

    async def get_deployed_trigger_workflows(
        self,
        deployed_component_id: str,
        external_user_id: str,
    ) -> GetDeployedTriggerWorkflowsResponse:
        """
        Retrieves the list of workflow IDs listening to a deployed trigger.

        Args:
            deployed_component_id: The ID of the deployed trigger.
            external_user_id: The external user ID associated with the trigger.

        Returns:
            A dictionary containing a list of workflow IDs.

        Raises:
            PipedreamAuthError: If authentication fails.
            PipedreamApiError: If the API request fails.
            ValueError: If required arguments are missing.
        """
        if not all([deployed_component_id, external_user_id]):
            raise ValueError("deployed_component_id and external_user_id are required.")

        path = f"connect/{self._project_id}/deployed-triggers/{deployed_component_id}/workflows"
        params = {"external_user_id": external_user_id}

        response_data = await self._request("GET", path, params=params)

        if "workflow_ids" not in response_data or not isinstance(response_data["workflow_ids"], list):
             raise PipedreamApiError(f"Unexpected response format for get_deployed_trigger_workflows: {response_data}")

        return GetDeployedTriggerWorkflowsResponse(workflow_ids=response_data["workflow_ids"])

    async def update_deployed_trigger_workflows(
        self,
        deployed_component_id: str,
        external_user_id: str,
        workflow_ids: List[str],
    ) -> UpdateDeployedTriggerWorkflowsResponse:
        """
        Updates the list of workflow IDs listening to a deployed trigger.

        Args:
            deployed_component_id: The ID of the deployed trigger.
            external_user_id: The external user ID associated with the trigger.
            workflow_ids: The new list of workflow IDs.

        Returns:
            A dictionary containing the confirmed list of workflow IDs.

        Raises:
            PipedreamAuthError: If authentication fails.
            PipedreamApiError: If the API request fails.
            ValueError: If required arguments are missing.
        """
        if not all([deployed_component_id, external_user_id]):
            raise ValueError("deployed_component_id and external_user_id are required.")
        # workflow_ids can be an empty list

        path = f"connect/{self._project_id}/deployed-triggers/{deployed_component_id}/workflows"
        # PUT request requires external_user_id in query params according to .mdx curl example
        params = {"external_user_id": external_user_id}
        # TS uses workflowIds key, .mdx example uses workflow_ids. Use TS key for consistency?
        # Let's stick to pythonic snake_case for payload keys where possible, matching .mdx
        payload = {"workflow_ids": workflow_ids}

        response_data = await self._request("PUT", path, params=params, json_data=payload)

        if "workflow_ids" not in response_data or not isinstance(response_data["workflow_ids"], list):
             raise PipedreamApiError(f"Unexpected response format for update_deployed_trigger_workflows: {response_data}")

        return UpdateDeployedTriggerWorkflowsResponse(workflow_ids=response_data["workflow_ids"])

    # --- Rate Limits --- #

    async def create_rate_limit(
        self,
        window_size_seconds: int,
        requests_per_window: int,
    ) -> CreateRateLimitResponse:
        """
        Defines rate limits for users and retrieves a rate limit token.

        Args:
            window_size_seconds: The size of the time window in seconds.
            requests_per_window: The number of requests allowed per window.

        Returns:
            A dictionary containing the rate limit token.

        Raises:
            PipedreamAuthError: If authentication fails.
            PipedreamApiError: If the API request fails.
            ValueError: If required arguments are invalid.
        """
        if not window_size_seconds or window_size_seconds <= 0:
            raise ValueError("window_size_seconds must be a positive integer.")
        if not requests_per_window or requests_per_window <= 0:
            raise ValueError("requests_per_window must be a positive integer.")

        # Path based on docs: /v1/connect/rate_limits (no project_id in path)
        # Assuming auth is still handled via Bearer token
        path = "connect/rate_limits"

        payload = {
            "window_size_seconds": window_size_seconds,
            "requests_per_window": requests_per_window,
        }

        response_data = await self._request("POST", path, json_data=payload)

        if "token" not in response_data or not isinstance(response_data["token"], str):
            raise PipedreamApiError(f"Unexpected response format for create_rate_limit: {response_data}")

        return CreateRateLimitResponse(token=response_data["token"])
