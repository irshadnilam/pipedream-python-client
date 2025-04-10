# Pipedream Connect Python Client

This directory contains an asynchronous Python client library for interacting with the Pipedream Connect API, based on the documentation provided in `api.mdx` and `server.d.ts`.

## Installation

Requires `aiohttp`:
```bash
pip install aiohttp
```

## Usage

Import the client and instantiate it with your credentials:

```python
import asyncio
import os
from .pipedream import PipedreamClient, PipedreamAuthError, PipedreamApiError

async def run():
    client_id = os.environ.get("PD_CLIENT_ID")
    client_secret = os.environ.get("PD_CLIENT_SECRET")
    project_id = os.environ.get("PD_PROJECT_ID")
    external_user_id = "some-unique-user-id"

    if not all([client_id, client_secret, project_id]):
        print("Set PD_CLIENT_ID, PD_CLIENT_SECRET, PD_PROJECT_ID env vars")
        return

    try:
        # Note: environment parameter is required
        async with PipedreamClient(client_id, client_secret, project_id, environment="development") as client:
            # Call API methods
            token_info = await client.create_connect_token(external_user_id=external_user_id)
            print(f"Token: {token_info['token']}")

            accounts = await client.get_accounts(external_user_id=external_user_id, limit=10)
            print(f"Found {len(accounts['data'])} accounts.")

            # ... other method calls

    except PipedreamAuthError as e:
        print(f"Authentication Error: {e}")
    except PipedreamApiError as e:
        print(f"API Error: {e}")

if __name__ == "__main__":
    asyncio.run(run())
```

See `example.py` for more detailed usage examples of various methods. Remember to replace placeholders and handle potentially destructive operations (like DELETE methods) with care.

## Available Methods

The `PipedreamClient` class provides methods corresponding to the documented API endpoints:

**Project**
*   `get_project_info()`: Retrieves project info (e.g., linked apps).

**Apps**
*   `get_apps(q=None, has_actions=None, has_components=None, has_triggers=None, limit=None, after=None, before=None)`: Retrieves the list of available apps.
*   `get_app(app_id_or_slug)`: Retrieves metadata for a specific app.

**Tokens**
*   `create_connect_token(external_user_id, allowed_origins=None, success_redirect_uri=None, error_redirect_uri=None, webhook_uri=None)`: Creates a short-lived connect token.

**Accounts**
*   `get_accounts(external_user_id=None, app_filter=None, oauth_app_id=None, include_credentials=False, limit=None, after=None, before=None)`: Lists connected accounts.
*   `get_account_by_id(account_id, external_user_id=None, include_credentials=False)`: Retrieves details for a specific account.
*   `delete_account(account_id, external_user_id=None)`: Deletes a specific connected account.
*   `delete_accounts_by_app(app_id, external_user_id=None)`: Deletes all accounts for a specific app.
*   `delete_external_user(external_user_id)`: Deletes an end user and all their associated data.

**Components**
*   `get_components(component_type, app_filter=None, search_query=None, limit=None, after=None, before=None)`: Lists components ('triggers', 'actions', 'components').
*   `get_component(component_type, component_key)`: Retrieves details for a specific component.
*   `configure_component(component_type, component_key, prop_name, external_user_id, configured_props, dynamic_props_id=None, page=None, prev_context=None, query=None)`: Retrieves dynamic options for a component prop.
*   `reload_component_props(component_type, component_key, external_user_id, configured_props, dynamic_props_id=None)`: Reloads component props after configuring a dynamic prop.

**Actions**
*   `run_action(action_key, external_user_id, configured_props, dynamic_props_id=None)`: Executes an action component.

**Triggers**
*   `deploy_trigger(trigger_key, external_user_id, configured_props, webhookUrl=None, workflowId=None, dynamic_props_id=None)`: Deploys a trigger component.

**Deployed Triggers**
*   `get_deployed_triggers(external_user_id, limit=None, after=None, before=None)`: Lists deployed triggers for a user.
*   `get_deployed_trigger(deployed_component_id, external_user_id)`: Retrieves details of a specific deployed trigger.
*   `delete_deployed_trigger(deployed_component_id, external_user_id, ignore_hook_errors=False)`: Deletes a specific deployed trigger.
*   `update_deployed_trigger(deployed_component_id, external_user_id, active=None, name=None)`: Updates the name or active status of a deployed trigger.
*   `get_deployed_trigger_events(deployed_component_id, external_user_id, n=None)`: Retrieves recent events for a deployed trigger (using `n` for limit).
*   `get_deployed_trigger_webhooks(deployed_component_id, external_user_id)`: Gets webhook listeners for a deployed trigger.
*   `update_deployed_trigger_webhooks(deployed_component_id, external_user_id, webhookUrls)`: Updates webhook listeners for a deployed trigger.
*   `get_deployed_trigger_workflows(deployed_component_id, external_user_id)`: Gets workflow listeners for a deployed trigger.
*   `update_deployed_trigger_workflows(deployed_component_id, external_user_id, workflowIds)`: Updates workflow listeners for a deployed trigger.

**Rate Limits**
*   `create_rate_limit(window_size_seconds, requests_per_window)`: Defines rate limits and gets a token. *(Note: Not present in provided TS SDK)*.

**Proxy**
*   `make_proxy_request(proxy_opts, target_request)`: Makes a proxy request to a target app API.

**Workflow Invocation**
*   `invoke_workflow(url_or_endpoint_id, method='POST', headers=None, json_data=None, data=None, auth_type='none')`: Invokes a workflow by its HTTP endpoint URL or ID.
*   `invoke_workflow_for_external_user(url_or_endpoint_id, external_user_id, method='POST', headers=None, json_data=None, data=None)`: Invokes a workflow for a specific external user.

## Error Handling

The client raises `PipedreamAuthError` for authentication issues and `PipedreamApiError` for other API-related errors (invalid requests, server errors, unexpected responses). Standard `ValueError` may be raised for invalid input arguments. 