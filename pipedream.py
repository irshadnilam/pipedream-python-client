import aiohttp
import asyncio
import base64
import urllib.parse
from typing import Optional, List, Dict, Any, TypedDict, cast, Literal, Union
import time
import json

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

# Corrected Literal Type Definition
AppAuthType = Literal["oauth", "keys", "none"] # type: ignore

class App(TypedDict):
    id: Optional[str]
    name_slug: str
    name: str
    auth_type: AppAuthType
    img_src: str
    description: Optional[str]
    custom_fields_json: Optional[str]
    categories: Optional[List[str]]

class Account(TypedDict):
    id: str
    name: Optional[str]
    external_id: str
    healthy: bool
    dead: Optional[bool]
    app: App
    created_at: str
    updated_at: str
    credentials: Optional[Dict[str, Any]]
    expires_at: Optional[str]
    error: Optional[Any]
    last_refreshed_at: Optional[str]
    next_refresh_at: Optional[str]

class PageInfo(TypedDict):
    total_count: int
    count: int
    start_cursor: Optional[str]
    end_cursor: Optional[str]

class GetAccountsResponse(TypedDict):
    page_info: PageInfo
    data: List[Account]

class GetAccountByIdResponse(TypedDict):
    data: Account

class ComponentSummary(TypedDict):
    name: str
    version: str
    key: str
    description: Optional[str]
    component_type: Optional[str]

class GetComponentsResponse(TypedDict):
    page_info: PageInfo
    data: List[ComponentSummary]

class ComponentProp(TypedDict, total=False):
    name: str
    type: str
    label: str
    description: str
    app: str
    optional: bool
    default: Any
    remoteOptions: bool
    options: List[Any]
    customResponse: bool
    useQuery: bool
    reloadProps: bool
    static: Dict[str, Any]
    secret: bool
    min: int
    max: int
    alertType: Literal["info", "neutral", "warning", "error"]
    content: str

class ComponentDetail(TypedDict):
    name: str
    version: str
    key: str
    configurable_props: List[ComponentProp]
    description: Optional[str]
    component_type: Optional[str]

class GetComponentResponse(TypedDict):
    data: ComponentDetail

class PropOption(TypedDict):
    label: str
    value: Any

class ConfigureComponentResponse(TypedDict):
    options: Optional[List[PropOption]]
    stringOptions: Optional[List[str]]
    errors: List[str]
    observations: Optional[List[Any]]
    context: Optional[Any]
    timings: Optional[Dict[str, float]]

class DynamicPropsInfo(TypedDict):
    id: str
    configurableProps: List[ComponentProp]

class ReloadComponentPropsResponse(TypedDict):
    dynamicProps: DynamicPropsInfo
    errors: List[str]
    observations: Optional[List[Any]]

class RunActionResponse(TypedDict):
    exports: Any
    os: List[Any]
    ret: Any

class DeployedComponent(TypedDict):
    id: str
    owner_id: str
    component_id: str
    configurable_props: List[ComponentProp]
    configured_props: Dict[str, Any]
    active: bool
    created_at: int
    updated_at: int
    name: str
    name_slug: str
    callback_observations: Optional[Any]

class DeployTriggerResponse(TypedDict):
    data: DeployedComponent

class GetDeployedTriggersResponse(TypedDict):
    page_info: PageInfo
    data: List[DeployedComponent]

class GetDeployedTriggerResponse(TypedDict):
    data: DeployedComponent

class EmittedEvent(TypedDict):
    e: Dict[str, Any]
    k: str
    ts: int
    id: str

class GetDeployedTriggerEventsResponse(TypedDict):
    data: List[EmittedEvent]

class GetDeployedTriggerWebhooksResponse(TypedDict):
    webhook_urls: List[str]

class UpdateDeployedTriggerWebhooksResponse(TypedDict):
    webhook_urls: List[str]

class GetDeployedTriggerWorkflowsResponse(TypedDict):
    workflow_ids: List[str]

class UpdateDeployedTriggerWorkflowsResponse(TypedDict):
    workflow_ids: List[str]

class CreateRateLimitResponse(TypedDict):
    token: str

class ProjectInfoResponse(TypedDict):
    apps: List[App]

class GetAppsResponse(TypedDict):
    page_info: PageInfo
    data: List[App]

class GetAppResponse(TypedDict):
    data: App

class ProxyApiOpts(TypedDict):
    searchParams: Dict[str, str]

class ProxyTargetApiOpts(TypedDict):
    method: Literal["GET", "POST", "PUT", "DELETE", "PATCH"]
    headers: Optional[Dict[str, str]]
    body: Optional[str]

class ProxyTargetApiRequest(TypedDict):
    url: str
    options: ProxyTargetApiOpts

# Corrected Literal Type Definition
HTTPAuthType = Literal["none", "static_bearer_token", "oauth"] # type: ignore

class UpdateTriggerOpts(TypedDict, total=False):
    active: bool
    name: str

class PipedreamClient:
    """Asynchronous client for the Pipedream Connect API."""

    BASE_URL = "https://api.pipedream.com/v1"

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        project_id: str,
        environment: Literal["development", "production"] = "production",
        session: Optional[aiohttp.ClientSession] = None,
        api_host: str = "api.pipedream.com",
        workflow_domain: str = "m.pipedream.net"
    ):
        """
        Initializes the Pipedream async client.

        Args:
            client_id: Your Pipedream OAuth client ID.
            client_secret: Your Pipedream OAuth client secret.
            project_id: Your Pipedream project ID.
            environment: Environment ('development' or 'production'). Defaults to 'production'.
            session: Optional external aiohttp.ClientSession.
            api_host: Pipedream API host. Defaults to 'api.pipedream.com'.
            workflow_domain: Base domain for workflows. Defaults to 'm.pipedream.net'.
        """
        if not client_id or not client_secret or not project_id:
            raise ValueError("client_id, client_secret, and project_id are required.")
        if environment not in ["development", "production"]:
             raise ValueError("environment must be 'development' or 'production'.")

        self._client_id = client_id
        self._client_secret = client_secret
        self._project_id = project_id
        self._environment = environment
        self._session = session or aiohttp.ClientSession()
        self._close_session = session is None

        self._api_host = api_host
        self._workflow_domain = workflow_domain
        self._base_api_url = f"https://{self._api_host}/v1"

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
        Handles token caching and expiration. Uses configured API host.
        """
        if self._access_token and time.time() < self._token_expires_at:
            return self._access_token

        token_url = f"https://{self._api_host}/v1/oauth/token"
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
                    expires_in = data.get("expires_in", 3600)
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
        data: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
        requires_auth: bool = True,
        base_url_override: Optional[str] = None,
        include_pd_headers: bool = True,
        expected_status: Union[int, List[int]] = 200,
    ) -> Dict[str, Any]:
        """Makes a request to the Pipedream API or other URLs."""
        url = f"{base_url_override or self._base_api_url}/{path.lstrip('/')}"

        query_params: Dict[str, str] = {}
        if params:
            for key, value in params.items():
                if value is not None:
                    if isinstance(value, bool):
                         query_params[key] = str(value).lower()
                    else:
                         query_params[key] = str(value)

        request_headers = {
            "Accept": "application/json",
        }

        if headers:
             request_headers.update(headers)

        if include_pd_headers:
             request_headers.setdefault("X-PD-SDK-Version", "pipedream-python-sdk-dev")
             request_headers.setdefault("X-PD-Environment", self._environment)

        request_body: Optional[Any] = None
        if json_data is not None:
            request_headers.setdefault("Content-Type", "application/json")
            request_body = json.dumps(json_data)
        elif data is not None:
            request_body = data

        if requires_auth:
            access_token = await self._get_access_token()
            request_headers.setdefault("Authorization", f"Bearer {access_token}")

        try:
            async with self._session.request(
                method, url, params=query_params, data=request_body, headers=request_headers
            ) as response:

                expected_statuses = [expected_status] if isinstance(expected_status, int) else expected_status
                if response.status in expected_statuses:
                     if response.status == 204:
                         return {}

                     content_type = response.headers.get("Content-Type", "")
                     if "application/json" in content_type:
                         try:
                             return await response.json()
                         except json.JSONDecodeError as e:
                              raise PipedreamApiError(f"Failed to decode JSON response: {e} - Status: {response.status}")
                     else:
                         return {"raw_response": await response.text()}
                else:
                    try:
                        error_data = await response.json()
                        error_message = error_data.get('error', {}).get('message', str(error_data))
                    except (aiohttp.ContentTypeError, ValueError, json.JSONDecodeError):
                        error_message = await response.text()
                    raise PipedreamApiError(
                        f"API request failed: {response.status} - {error_message}"
                    )
        except aiohttp.ClientError as e:
            raise PipedreamApiError(f"Network error during API request: {e}")

    async def _connect_request(self, path: str, **kwargs):
         """Helper for requests prefixed with /connect/{project_id}."""
         full_path = f"connect/{self._project_id}/{path.lstrip('/')}"
         return await self._request(path=full_path, **kwargs)

    async def create_connect_token(
        self,
        external_user_id: str,
        allowed_origins: Optional[List[str]] = None,
        success_redirect_uri: Optional[str] = None,
        error_redirect_uri: Optional[str] = None,
        webhook_uri: Optional[str] = None,
    ) -> ConnectTokenResponse:
        """Creates a short-lived token for initiating account connections."""
        if not external_user_id:
            raise ValueError("external_user_id is required.")

        path = "tokens"
        payload: Dict[str, Any] = {
            "external_user_id": external_user_id,
            "external_id": external_user_id,
        }
        if allowed_origins: payload["allowed_origins"] = allowed_origins
        if success_redirect_uri: payload["success_redirect_uri"] = success_redirect_uri
        if error_redirect_uri: payload["error_redirect_uri"] = error_redirect_uri
        if webhook_uri: payload["webhook_uri"] = webhook_uri

        response_data = await self._connect_request(path, method="POST", json_data=payload)

        return cast(ConnectTokenResponse, response_data)

    async def get_accounts(
        self,
        external_user_id: Optional[str] = None,
        app_filter: Optional[str] = None,
        oauth_app_id: Optional[str] = None,
        include_credentials: bool = False,
        limit: Optional[int] = None,
        after: Optional[str] = None,
        before: Optional[str] = None,
    ) -> GetAccountsResponse:
        """Lists connected accounts."""
        path = "accounts"
        params: Dict[str, Any] = {
            "external_user_id": external_user_id,
            "app": app_filter,
            "oauth_app_id": oauth_app_id,
            "include_credentials": include_credentials,
            "limit": limit,
            "after": after,
            "before": before,
        }
        response_data = await self._connect_request(path, method="GET", params=params)
        return cast(GetAccountsResponse, response_data)

    async def get_account_by_id(
        self,
        account_id: str,
        external_user_id: Optional[str] = None,
        include_credentials: bool = False,
    ) -> GetAccountByIdResponse:
        """Retrieves details for a specific connected account."""
        if not account_id: raise ValueError("account_id is required.")
        path = f"accounts/{account_id}"
        params = {
            "include_credentials": include_credentials,
             "external_user_id": external_user_id
        }
        response_data = await self._connect_request(path, method="GET", params=params)
        return cast(GetAccountByIdResponse, response_data)

    async def delete_account(self, account_id: str, external_user_id: Optional[str] = None) -> None:
        """Deletes a specific connected account."""
        if not account_id: raise ValueError("account_id is required.")
        path = f"accounts/{account_id}"
        params = {"external_user_id": external_user_id}
        await self._connect_request(path, method="DELETE", params=params, expected_status=204)

    async def delete_accounts_by_app(self, app_id: str, external_user_id: Optional[str] = None) -> None:
        """Deletes all connected accounts for a specific app."""
        if not app_id: raise ValueError("app_id is required.")
        path = f"accounts/app/{app_id}"
        params = {"external_user_id": external_user_id}
        await self._connect_request(path, method="DELETE", params=params, expected_status=204)

    async def delete_external_user(self, external_user_id: str) -> None:
        """Deletes an end user and all associated data."""
        if not external_user_id: raise ValueError("external_user_id is required.")
        path = f"users/{external_user_id}"
        await self._connect_request(path, method="DELETE", expected_status=204)

    async def get_project_info(self) -> ProjectInfoResponse:
        """Retrieves the project's information (e.g., linked apps)."""
        path = "projects/info"
        response_data = await self._connect_request(path, method="GET")
        return cast(ProjectInfoResponse, response_data)

    async def get_apps(
        self,
        q: Optional[str] = None,
        has_actions: Optional[bool] = None,
        has_components: Optional[bool] = None,
        has_triggers: Optional[bool] = None,
        limit: Optional[int] = None,
        after: Optional[str] = None,
        before: Optional[str] = None,
    ) -> GetAppsResponse:
        """Retrieves the list of apps available in Pipedream."""
        path = "apps"
        params = {
            "q": q,
            "has_actions": has_actions,
            "has_components": has_components,
            "has_triggers": has_triggers,
            "limit": limit,
            "after": after,
            "before": before,
        }
        response_data = await self._request(path=path, method="GET", params=params)
        return cast(GetAppsResponse, response_data)

    async def get_app(self, app_id_or_slug: str) -> GetAppResponse:
        """Retrieves metadata for a specific app."""
        if not app_id_or_slug: raise ValueError("app_id_or_slug is required.")
        path = f"apps/{app_id_or_slug}"
        response_data = await self._request(path=path, method="GET")
        return cast(GetAppResponse, response_data)

    async def get_components(
        self,
        component_type: Literal["triggers", "actions", "components"],
        app_filter: Optional[str] = None,
        search_query: Optional[str] = None,
        limit: Optional[int] = None,
        after: Optional[str] = None,
        before: Optional[str] = None,
    ) -> GetComponentsResponse:
        """Lists components (triggers, actions, or general components)."""
        if component_type not in ["triggers", "actions", "components"]:
             raise ValueError("Invalid component_type.")
        path = component_type
        params = {
             "app": app_filter,
             "q": search_query,
             "limit": limit,
             "after": after,
             "before": before,
        }
        response_data = await self._connect_request(path=path, method="GET", params=params)
        return cast(GetComponentsResponse, response_data)

    async def get_component(
        self,
        component_type: Literal["triggers", "actions", "components"],
        component_key: str,
    ) -> GetComponentResponse:
        """Retrieves details for a specific component."""
        if component_type not in ["triggers", "actions", "components"]:
             raise ValueError("Invalid component_type.")
        if not component_key: raise ValueError("component_key is required.")
        path = f"{component_type}/{component_key}"
        response_data = await self._connect_request(path=path, method="GET")
        return cast(GetComponentResponse, response_data)

    async def configure_component(
        self,
        component_type: Literal["triggers", "actions", "components"],
        component_key: str,
        prop_name: str,
        external_user_id: str,
        configured_props: Dict[str, Any],
        dynamic_props_id: Optional[str] = None,
        page: Optional[int] = None,
        prev_context: Optional[Any] = None,
        query: Optional[str] = None,
    ) -> ConfigureComponentResponse:
        """Configures a component's prop, retrieving dynamic options."""
        if component_type not in ["triggers", "actions", "components"]:
             raise ValueError("Invalid component_type.")
        if not all([component_key, prop_name, external_user_id]):
            raise ValueError("component_key, prop_name, and external_user_id are required.")
        path = "components/configure"
        payload = {
            "id": component_key,
            "prop_name": prop_name,
            "external_user_id": external_user_id,
            "configured_props": configured_props,
            "dynamic_props_id": dynamic_props_id,
            "page": page,
            "prev_context": prev_context,
            "query": query,
        }
        filtered_payload = {k: v for k, v in payload.items() if v is not None}
        response_data = await self._connect_request(path=path, method="POST", json_data=filtered_payload)
        return cast(ConfigureComponentResponse, response_data)

    async def reload_component_props(
        self,
        component_type: Literal["triggers", "actions", "components"],
        component_key: str,
        external_user_id: str,
        configured_props: Dict[str, Any],
        dynamic_props_id: Optional[str] = None,
    ) -> ReloadComponentPropsResponse:
        """Reloads a component's props, typically after setting a dynamic prop."""
        if component_type not in ["triggers", "actions", "components"]:
             raise ValueError("Invalid component_type.")
        if not all([component_key, external_user_id]):
            raise ValueError("component_key and external_user_id are required.")
        path = "components/props"
        payload = {
            "id": component_key,
            "external_user_id": external_user_id,
            "configured_props": configured_props,
            "dynamic_props_id": dynamic_props_id,
        }
        filtered_payload = {k: v for k, v in payload.items() if v is not None}
        response_data = await self._connect_request(path=path, method="POST", json_data=filtered_payload)
        return cast(ReloadComponentPropsResponse, response_data)

    async def run_action(
        self,
        action_key: str,
        external_user_id: str,
        configured_props: Dict[str, Any],
        dynamic_props_id: Optional[str] = None,
    ) -> RunActionResponse:
        """Invokes an action component."""
        if not all([action_key, external_user_id]):
             raise ValueError("action_key and external_user_id are required.")
        path = "actions/run"
        payload = {
            "id": action_key,
            "external_user_id": external_user_id,
            "configured_props": configured_props,
            "dynamic_props_id": dynamic_props_id,
        }
        filtered_payload = {k: v for k, v in payload.items() if v is not None}
        response_data = await self._connect_request(path=path, method="POST", json_data=filtered_payload)
        return cast(RunActionResponse, response_data)

    async def deploy_trigger(
        self,
        trigger_key: str,
        external_user_id: str,
        configured_props: Dict[str, Any],
        webhook_url: Optional[str] = None,
        workflow_id: Optional[str] = None,
        dynamic_props_id: Optional[str] = None,
    ) -> DeployTriggerResponse:
        """Deploys a trigger component."""
        if not all([trigger_key, external_user_id]):
             raise ValueError("trigger_key and external_user_id are required.")
        if webhook_url and workflow_id:
             raise ValueError("Provide either webhook_url or workflow_id, not both.")
        path = "triggers/deploy"
        payload: Dict[str, Any] = {
            "id": trigger_key,
            "external_user_id": external_user_id,
            "configured_props": configured_props,
            "dynamic_props_id": dynamic_props_id,
            "webhookUrl": webhook_url,
            "workflowId": workflow_id,
        }

        filtered_payload = {k: v for k, v in payload.items() if v is not None}
        response_data = await self._connect_request(path=path, method="POST", json_data=filtered_payload)
        return cast(DeployTriggerResponse, response_data)

    async def get_deployed_triggers(
        self,
        external_user_id: str,
        limit: Optional[int] = None,
        after: Optional[str] = None,
        before: Optional[str] = None,
    ) -> GetDeployedTriggersResponse:
        """Lists deployed triggers for a specific external user."""
        if not external_user_id: raise ValueError("external_user_id is required.")
        path = "deployed-triggers"
        params = {
            "external_user_id": external_user_id,
            "limit": limit,
            "after": after,
            "before": before,
        }
        response_data = await self._connect_request(path, method="GET", params=params)
        return cast(GetDeployedTriggersResponse, response_data)

    async def get_deployed_trigger(
        self,
        deployed_component_id: str,
        external_user_id: str,
    ) -> GetDeployedTriggerResponse:
        """Retrieves details for a specific deployed trigger."""
        if not all([deployed_component_id, external_user_id]):
            raise ValueError("deployed_component_id and external_user_id are required.")
        path = f"deployed-triggers/{deployed_component_id}"
        params = {"external_user_id": external_user_id}
        response_data = await self._connect_request(path, method="GET", params=params)
        return cast(GetDeployedTriggerResponse, response_data)

    async def delete_deployed_trigger(
        self,
        deployed_component_id: str,
        external_user_id: str,
        ignore_hook_errors: bool = False,
    ) -> None:
        """Deletes a specific deployed trigger."""
        if not all([deployed_component_id, external_user_id]):
            raise ValueError("deployed_component_id and external_user_id are required.")
        path = f"deployed-triggers/{deployed_component_id}"
        params = {
            "external_user_id": external_user_id,
             "ignore_hook_errors": ignore_hook_errors,
        }
        await self._connect_request(path, method="DELETE", params=params, expected_status=204)

    async def update_deployed_trigger(
        self,
        deployed_component_id: str,
        external_user_id: str,
        active: Optional[bool] = None,
        name: Optional[str] = None,
    ) -> GetDeployedTriggerResponse:
        """Updates a specific deployed trigger (name or active status)."""
        if not all([deployed_component_id, external_user_id]):
            raise ValueError("deployed_component_id and external_user_id are required.")
        if active is None and name is None:
             raise ValueError("Either active or name must be provided to update.")

        path = f"deployed-triggers/{deployed_component_id}"
        params = {"external_user_id": external_user_id}
        payload: UpdateTriggerOpts = {}
        if active is not None: payload["active"] = active
        if name is not None: payload["name"] = name

        response_data = await self._connect_request(path, method="PUT", params=params, json_data=payload)
        return GetDeployedTriggerResponse(data=cast(DeployedComponent, response_data))

    async def get_deployed_trigger_events(
        self,
        deployed_component_id: str,
        external_user_id: str,
        limit: Optional[int] = None,
    ) -> GetDeployedTriggerEventsResponse:
        """Retrieves recent events emitted by a deployed trigger."""
        if not all([deployed_component_id, external_user_id]):
            raise ValueError("deployed_component_id and external_user_id are required.")
        path = f"deployed-triggers/{deployed_component_id}/events"
        params = {
            "external_user_id": external_user_id,
             "n": limit
        }
        response_data = await self._connect_request(path, method="GET", params=params)
        return cast(GetDeployedTriggerEventsResponse, response_data)

    async def get_deployed_trigger_webhooks(
        self,
        deployed_component_id: str,
        external_user_id: str,
    ) -> GetDeployedTriggerWebhooksResponse:
        """Retrieves webhook URLs listening to a deployed trigger."""
        if not all([deployed_component_id, external_user_id]):
            raise ValueError("deployed_component_id and external_user_id are required.")
        path = f"deployed-triggers/{deployed_component_id}/webhooks"
        params = {"external_user_id": external_user_id}
        response_data = await self._connect_request(path, method="GET", params=params)
        return cast(GetDeployedTriggerWebhooksResponse, response_data)

    async def update_deployed_trigger_webhooks(
        self,
        deployed_component_id: str,
        external_user_id: str,
        webhook_urls: List[str],
    ) -> UpdateDeployedTriggerWebhooksResponse:
        """Updates webhook URLs listening to a deployed trigger."""
        if not all([deployed_component_id, external_user_id]):
            raise ValueError("deployed_component_id and external_user_id are required.")
        path = f"deployed-triggers/{deployed_component_id}/webhooks"
        params = {"external_user_id": external_user_id}
        payload = {"webhookUrls": webhook_urls}
        response_data = await self._connect_request(path, method="PUT", params=params, json_data=payload)
        if "webhook_urls" not in response_data:
             raise PipedreamApiError(f"Unexpected response format for update_deployed_trigger_webhooks: {response_data}")
        return cast(UpdateDeployedTriggerWebhooksResponse, response_data)

    async def get_deployed_trigger_workflows(
        self,
        deployed_component_id: str,
        external_user_id: str,
    ) -> GetDeployedTriggerWorkflowsResponse:
        """Retrieves workflow IDs listening to a deployed trigger."""
        if not all([deployed_component_id, external_user_id]):
            raise ValueError("deployed_component_id and external_user_id are required.")
        path = f"deployed-triggers/{deployed_component_id}/pipelines"
        params = {"external_user_id": external_user_id}
        response_data = await self._connect_request(path, method="GET", params=params)
        if "workflow_ids" not in response_data:
              raise PipedreamApiError(f"Unexpected response format for get_deployed_trigger_workflows: {response_data}")
        return cast(GetDeployedTriggerWorkflowsResponse, response_data)

    async def update_deployed_trigger_workflows(
        self,
        deployed_component_id: str,
        external_user_id: str,
        workflow_ids: List[str],
    ) -> UpdateDeployedTriggerWorkflowsResponse:
        """Updates workflow IDs listening to a deployed trigger."""
        if not all([deployed_component_id, external_user_id]):
            raise ValueError("deployed_component_id and external_user_id are required.")
        path = f"deployed-triggers/{deployed_component_id}/pipelines"
        params = {"external_user_id": external_user_id}
        payload = {"workflowIds": workflow_ids}
        response_data = await self._connect_request(path, method="PUT", params=params, json_data=payload)
        if "workflow_ids" not in response_data:
             raise PipedreamApiError(f"Unexpected response format for update_deployed_trigger_workflows: {response_data}")
        return cast(UpdateDeployedTriggerWorkflowsResponse, response_data)

    async def create_rate_limit(
        self,
        window_size_seconds: int,
        requests_per_window: int,
    ) -> CreateRateLimitResponse:
        """Defines rate limits and retrieves a token. (Not found in provided TS SDK files)."""
        if not window_size_seconds or window_size_seconds <= 0:
            raise ValueError("window_size_seconds must be a positive integer.")
        if not requests_per_window or requests_per_window <= 0:
            raise ValueError("requests_per_window must be a positive integer.")
        path = "connect/rate_limits"
        payload = {
            "window_size_seconds": window_size_seconds,
            "requests_per_window": requests_per_window,
        }
        response_data = await self._request(path=path, method="POST", json_data=payload)
        return cast(CreateRateLimitResponse, response_data)

    def _build_workflow_url(self, url_or_endpoint_id: str) -> str:
         """Builds a full workflow URL (Internal helper)."""
         if not url_or_endpoint_id: raise ValueError("URL or endpoint ID required.")
         inp = url_or_endpoint_id.strip().lower()
         if "." in inp or inp.startswith("http"):
             url = inp if inp.startswith("http") else f"https://{inp}"
             parsed = urllib.parse.urlparse(url)
             if not parsed.hostname or not parsed.hostname.endswith(self._workflow_domain):
                 raise ValueError(f"Invalid workflow domain. Must end with {self._workflow_domain}")
             return url
         else:
             if not inp.startswith("en") and not inp.startswith("eo"):
                  raise ValueError("Invalid endpoint ID format.")
             return f"https://{inp}.{self._workflow_domain}"

    async def invoke_workflow(
        self,
        url_or_endpoint_id: str,
        method: str = "POST",
        headers: Optional[Dict[str, str]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        data: Optional[Any] = None,
        auth_type: HTTPAuthType = "none",
    ) -> Any:
         """Invokes a workflow by its HTTP endpoint URL or ID."""
         workflow_url = self._build_workflow_url(url_or_endpoint_id)
         parsed_url = urllib.parse.urlparse(workflow_url)
         base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
         path = parsed_url.path

         req_headers = headers or {}
         requires_auth = False
         if auth_type == "oauth":
              requires_auth = True

         return await self._request(
             method=method,
             path=path,
             headers=req_headers,
             json_data=json_data,
             data=data,
             requires_auth=requires_auth,
             base_url_override=base_url,
             include_pd_headers=False
         )

    async def invoke_workflow_for_external_user(
        self,
        url_or_endpoint_id: str,
        external_user_id: str,
        method: str = "POST",
        headers: Optional[Dict[str, str]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        data: Optional[Any] = None,
    ) -> Any:
        """Invokes a workflow for a specific external user."""
        if not external_user_id: raise ValueError("external_user_id is required.")

        req_headers = headers or {}
        req_headers["X-PD-External-User-ID"] = external_user_id

        return await self.invoke_workflow(
             url_or_endpoint_id=url_or_endpoint_id,
             method=method,
             headers=req_headers,
             json_data=json_data,
             data=data,
             auth_type="oauth"
        )
