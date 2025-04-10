# Example Usage (requires actual credentials and running async context)
import asyncio
import os
from .pipedream import PipedreamClient, PipedreamAuthError, PipedreamApiError

async def main():
    # Replace with your actual credentials and project ID
    # Ensure environment variables are set or replace placeholders directly
    client_id = os.environ.get("PD_CLIENT_ID")
    client_secret = os.environ.get("PD_CLIENT_SECRET")
    project_id = os.environ.get("PD_PROJECT_ID")
    external_user_id = "test-user-123" # Replace with a relevant user ID

    if not client_id or not client_secret or not project_id:
        print("Please set PD_CLIENT_ID, PD_CLIENT_SECRET, and PD_PROJECT_ID environment variables.")
        return

    try:
        async with PipedreamClient(client_id, client_secret, project_id, environment="development") as client:
            print("Client created. Attempting to create connect token...")
            token_info = await client.create_connect_token(
                external_user_id=external_user_id,
                allowed_origins=["http://localhost:3000"] # Example allowed origin
            )
            print("Successfully created connect token:")
            print(f"  Token: {token_info['token'][:10]}...") # Print only start of token
            print(f"  Expires At: {token_info['expires_at']}")
            print(f"  Connect Link URL: {token_info['connect_link_url']}")

            print("\nAttempting to list accounts...")
            accounts_response = await client.get_accounts(limit=5) # Get first 5 accounts
            print(f"Successfully retrieved {len(accounts_response['data'])} accounts (out of {accounts_response['page_info']['total_count']} total):")
            first_account_id = None
            for account in accounts_response['data']:
                print(f"  - Account ID: {account['id']}, App: {account['app']['name']}, External User: {account['external_id']}")
                if first_account_id is None:
                    first_account_id = account['id']

            if first_account_id:
                print(f"\nAttempting to retrieve details for account ID: {first_account_id}...")
                try:
                    account_details_response = await client.get_account_by_id(first_account_id, include_credentials=False)
                    account_details = account_details_response['data']
                    print("Successfully retrieved account details:")
                    print(f"  ID: {account_details['id']}")
                    print(f"  App Name: {account_details['app']['name']}")
                    print(f"  External ID: {account_details['external_id']}")
                    print(f"  Healthy: {account_details['healthy']}")
                    # Optionally show credentials if include_credentials was True (but be careful!)
                    # if 'credentials' in account_details and account_details['credentials']:
                    #     print(f"  Credentials Present: Yes (not shown)")
                except PipedreamApiError as e:
                    print(f"Could not retrieve details for account {first_account_id}: {e}")
            else:
                print("\nSkipping get_account_by_id example as no accounts were listed.")

            # Example for delete (Use with extreme caution! Maybe create a dummy account first)
            # account_to_delete = "apn_..." # Replace with an actual ID you want to delete
            # if account_to_delete:
            #     print(f"\nAttempting to delete account ID: {account_to_delete}...")
            #     try:
            #         await client.delete_account(account_to_delete)
            #         print(f"Successfully deleted account {account_to_delete}.")
            #     except PipedreamApiError as e:
            #         print(f"Failed to delete account {account_to_delete}: {e}")
            # else:
            #      print("\nSkipping delete_account example as no ID was specified.")

            # Example for delete_accounts_by_app (Use with extreme caution!)
            # app_to_clear = "slack" # Replace with an actual app ID or slug
            # if app_to_clear:
            #     print(f"\nAttempting to delete all accounts for app: {app_to_clear}...")
            #     try:
            #         await client.delete_accounts_by_app(app_to_clear)
            #         print(f"Successfully deleted all accounts for app {app_to_clear}.")
            #     except PipedreamApiError as e:
            #         print(f"Failed to delete accounts for app {app_to_clear}: {e}")
            # else:
            #      print("\nSkipping delete_accounts_by_app example as no app was specified.")

            # Example for delete_external_user (Use with extreme caution!)
            # user_to_delete = "test-user-123" # Replace with an actual external user ID
            # if user_to_delete:
            #     print(f"\nAttempting to delete external user: {user_to_delete}...")
            #     try:
            #         await client.delete_external_user(user_to_delete)
            #         print(f"Successfully deleted external user {user_to_delete}.")
            #     except PipedreamApiError as e:
            #         print(f"Failed to delete external user {user_to_delete}: {e}")
            # else:
            #      print("\nSkipping delete_external_user example as no user was specified.")

            print("\nAttempting to list components (actions for 'gitlab')...")
            try:
                # Change component_type to 'triggers' if you want to test trigger deployment
                component_type_to_list = "actions"
                components_response = await client.get_components(
                    component_type=component_type_to_list,
                    app_filter="gitlab", # Example: filter for gitlab actions
                    search_query="issue", # Example: search for 'issue'
                    limit=5
                )
                print(f"Successfully listed {len(components_response['data'])} components:")
                first_component_key = None
                component_type_used = component_type_to_list # Use the same type for get_component
                for comp in components_response['data']:
                    print(f"  - Key: {comp['key']}, Name: {comp['name']}, Version: {comp['version']}")
                    if first_component_key is None:
                        first_component_key = comp['key']

                if first_component_key:
                    print(f"\nAttempting to retrieve details for component: {first_component_key}...")
                    try:
                        component_details_resp = await client.get_component(component_type_used, first_component_key)
                        details = component_details_resp['data']
                        print("Successfully retrieved component details:")
                        print(f"  Key: {details['key']}")
                        print(f"  Name: {details['name']}")
                        print(f"  Version: {details['version']}")
                        print(f"  Configurable Props ({len(details['configurable_props'])}):")
                        for prop in details['configurable_props']:
                            print(f"    - {prop['name']} ({prop['type']})")

                        # --- Example for reload_component_props --- #
                        # This uses google_sheets-add-single-row as an example component with dynamic props
                        # It requires a valid googleSheets authProvisionId and a sheetId to trigger reload
                        if component_type_used == "actions" and first_component_key == "google_sheets-add-single-row": # Example
                             print(f"\nAttempting to reload props for component {first_component_key}...")
                             # Replace with real values
                             example_external_user_sheets = external_user_id
                             example_sheets_apn = "apn_..." # <--- Replace with REAL Google Sheets auth ID
                             example_sheet_id = "your_sheet_id_here" # <--- Replace with REAL Sheet ID

                             if example_sheets_apn != "apn_..." and example_sheet_id != "your_sheet_id_here":
                                 try:
                                     reload_resp = await client.reload_component_props(
                                         component_type=component_type_used,
                                         component_key=first_component_key,
                                         external_user_id=example_external_user_sheets,
                                         configured_props={
                                             "googleSheets": {"authProvisionId": example_sheets_apn},
                                             "sheetId": example_sheet_id
                                         }
                                     )
                                     print(f"Successfully reloaded props (dynamic ID: {reload_resp['dynamicProps']['id']}):")
                                     print(f"  New Configurable Props ({len(reload_resp['dynamicProps']['configurableProps'])}):")
                                     for prop in reload_resp['dynamicProps']['configurableProps']:
                                         print(f"    - {prop['name']} ({prop['type']})")
                                     if reload_resp.get('errors'):
                                         print(f"  Errors: {reload_resp['errors']}")

                                 except PipedreamApiError as e:
                                     print(f"Failed to reload component props: {e}")
                             else:
                                 print("Skipping reload_component_props example: Replace placeholders for Sheets auth and Sheet ID.")
                        # --- End reload_component_props Example --- #

                        # --- Example for run_action --- #
                        # This uses gitlab-list-commits as an example
                        # Requires valid Gitlab auth ID (apn) and project ID prop
                        if component_type_used == "actions" and first_component_key == "gitlab-list-commits":
                            print(f"\nAttempting to run action {first_component_key}...")
                            # Use previously defined example values or replace
                            example_gitlab_apn = "apn_..." # <--- Replace with REAL Gitlab auth ID
                            example_gitlab_project_id = 45672541 # <--- Replace with REAL Project ID if needed
                            example_ref_name = "main" # Example branch/ref

                            if example_gitlab_apn != "apn_..." and example_gitlab_project_id:
                                try:
                                    run_resp = await client.run_action(
                                        action_key=first_component_key,
                                        external_user_id=external_user_id, # Use relevant user ID (Corrected variable name)
                                        configured_props={
                                            "gitlab": {"authProvisionId": example_gitlab_apn},
                                            "projectId": example_gitlab_project_id,
                                            "refName": example_ref_name
                                        }
                                        # dynamic_props_id=... # Add if needed
                                    )
                                    print(f"Successfully ran action {first_component_key}:")
                                    print(f"  Return Value (type {type(run_resp['ret'])}): {str(run_resp['ret'])[:100]}...") # Show preview
                                    print(f"  Exports: {run_resp['exports']}")
                                    print(f"  Observations/Logs: {len(run_resp['os'])} entries")

                                except PipedreamApiError as e:
                                    print(f"Failed to run action: {e}")
                            else:
                                print("Skipping run_action example: Replace placeholders for Gitlab auth and project ID.")
                        # --- End run_action Example --- #

                        # --- Example for deploy_trigger --- #
                        # This uses gitlab-new-issue as an example
                        # Requires valid Gitlab auth ID (apn) and project ID prop
                        # Also requires a destination (webhook_url or workflow_id)
                        # NOTE: This example assumes get_components listed TRIGGERS with key 'gitlab-new-issue'
                        # Adjust the get_components call above if needed.
                        if component_type_used == "triggers" and first_component_key == "gitlab-new-issue":
                            print(f"\nAttempting to deploy trigger {first_component_key}...")
                            # Use previously defined example values or replace
                            example_gitlab_apn_trigger = "apn_..." # <--- Replace with REAL Gitlab auth ID
                            example_gitlab_project_id_trigger = 45672541 # <--- Replace with REAL Project ID
                            example_destination_webhook = "https://your.webhook.url/example" # <--- Replace
                            deployed_trigger_id_to_manage = None # Reset for this example run

                            if example_gitlab_apn_trigger != "apn_..." and example_gitlab_project_id_trigger and example_destination_webhook:
                                try:
                                    deploy_resp = await client.deploy_trigger(
                                        trigger_key=first_component_key,
                                        external_user_id=external_user_id,
                                        configured_props={
                                            "gitlab": {"authProvisionId": example_gitlab_apn_trigger},
                                            "projectId": example_gitlab_project_id_trigger,
                                            # Other props might be needed depending on the trigger
                                        },
                                        webhook_url=example_destination_webhook
                                        # workflow_id="p_..." # Or use workflow_id
                                        # dynamic_props_id=... # Add if needed
                                    )
                                    deployed_data = deploy_resp['data']
                                    print(f"Successfully deployed trigger:")
                                    print(f"  Deployed ID: {deployed_data['id']}")
                                    print(f"  Name: {deployed_data['name']}")
                                    print(f"  Active: {deployed_data['active']}")
                                    print(f"  Owner ID: {deployed_data['owner_id']}")
                                    deployed_trigger_id_to_manage = deployed_data['id'] # Store ID for later examples

                                except PipedreamApiError as e:
                                    print(f"Failed to deploy trigger: {e}")
                            else:
                                print("Skipping deploy_trigger example: Replace placeholders for Gitlab auth, project ID, and webhook URL.")
                        # --- End deploy_trigger Example --- #

                        # --- Start Deployed Trigger Examples --- #
                        # Attempt to get ID from deployment, otherwise list and pick first
                        temp_trigger_id = deployed_trigger_id_to_manage

                        # --- get_deployed_triggers --- #
                        print("\nAttempting to list deployed triggers...")
                        try:
                            list_triggers_resp = await client.get_deployed_triggers(external_user_id, limit=5)
                            print(f"Successfully listed {len(list_triggers_resp['data'])} deployed triggers:")
                            for trigger in list_triggers_resp['data']:
                                print(f"  - ID: {trigger['id']}, Name: {trigger['name']}, Active: {trigger['active']}")
                                # Use the first listed trigger for subsequent examples if not populated from deploy
                                if temp_trigger_id is None:
                                     temp_trigger_id = trigger['id']
                        except PipedreamApiError as e:
                            print(f"Failed to list deployed triggers: {e}")

                        if temp_trigger_id:
                            # --- get_deployed_trigger --- #
                            print(f"\nAttempting to get deployed trigger {temp_trigger_id}...")
                            try:
                                get_trigger_resp = await client.get_deployed_trigger(temp_trigger_id, external_user_id)
                                print(f"Successfully got trigger {get_trigger_resp['data']['id']}")
                            except PipedreamApiError as e:
                                print(f"Failed to get deployed trigger: {e}")

                            # --- get_deployed_trigger_events --- #
                            print(f"\nAttempting to get events for trigger {temp_trigger_id}...")
                            try:
                                events_resp = await client.get_deployed_trigger_events(temp_trigger_id, external_user_id, limit=5)
                                print(f"Successfully got {len(events_resp['data'])} events:")
                                for event in events_resp['data']:
                                     print(f"  - Event ID: {event['id']}, Timestamp: {event['ts']}")
                            except PipedreamApiError as e:
                                print(f"Failed to get trigger events: {e}")

                            # --- get/update webhooks --- # (Example assumes it had a webhook)
                            print(f"\nAttempting to get webhooks for trigger {temp_trigger_id}...")
                            try:
                                webhooks_resp = await client.get_deployed_trigger_webhooks(temp_trigger_id, external_user_id)
                                print(f"Current webhooks: {webhooks_resp['webhook_urls']}")
                                # Example Update (Add a dummy URL)
                                # current_urls = webhooks_resp['webhook_urls']
                                # new_urls = current_urls + ["https://new.dummy.url/test"]
                                # print(f"Attempting to update webhooks to: {new_urls}")
                                # update_webhook_resp = await client.update_deployed_trigger_webhooks(temp_trigger_id, external_user_id, new_urls)
                                # print(f"Confirmed webhooks: {update_webhook_resp['webhook_urls']}")
                            except PipedreamApiError as e:
                                print(f"Failed to get/update trigger webhooks: {e}")

                            # --- get/update workflows --- # (Example assumes it didn't have one)
                            print(f"\nAttempting to get workflows for trigger {temp_trigger_id}...")
                            try:
                                workflows_resp = await client.get_deployed_trigger_workflows(temp_trigger_id, external_user_id)
                                print(f"Current workflows: {workflows_resp['workflow_ids']}")
                                # Example Update (Add a dummy workflow ID)
                                # current_wf_ids = workflows_resp['workflow_ids']
                                # new_wf_ids = current_wf_ids + ["p_dummy123"]
                                # print(f"Attempting to update workflows to: {new_wf_ids}")
                                # update_wf_resp = await client.update_deployed_trigger_workflows(temp_trigger_id, external_user_id, new_wf_ids)
                                # print(f"Confirmed workflows: {update_wf_resp['workflow_ids']}")
                            except PipedreamApiError as e:
                                print(f"Failed to get/update trigger workflows: {e}")

                            # --- delete_deployed_trigger --- # (Use with caution!)
                            # print(f"\nAttempting to delete trigger {temp_trigger_id}...")
                            # try:
                            #     await client.delete_deployed_trigger(temp_trigger_id, external_user_id)
                            #     print(f"Successfully deleted trigger {temp_trigger_id}.")
                            # except PipedreamApiError as e:
                            #     print(f"Failed to delete trigger: {e}")

                        else:
                            print("\nSkipping deployed trigger management examples as no ID was found/provided.")
                        # --- End Deployed Trigger Examples --- #

                        # --- create_rate_limit Example --- #
                        print("\nAttempting to create a rate limit token...")
                        try:
                            rate_limit_resp = await client.create_rate_limit(window_size_seconds=10, requests_per_window=1000)
                            print(f"Successfully created rate limit token: {rate_limit_resp['token'][:10]}...")
                        except PipedreamApiError as e:
                            print(f"Failed to create rate limit token: {e}")
                        # --- End create_rate_limit Example --- #

                    except PipedreamApiError as e:
                        print(f"Failed to retrieve component {first_component_key}: {e}")
                else:
                    print("\nSkipping get_component example as no components were listed.")

            except PipedreamApiError as e:
                print(f"Failed to list components: {e}")

    except (PipedreamAuthError, PipedreamApiError, ValueError) as e:
        print(f"An error occurred: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
     # To run this example:
     # 1. Install aiohttp: pip install aiohttp
     # 2. Set environment variables:
     #    export PD_CLIENT_ID='your_client_id'
     #    export PD_CLIENT_SECRET='your_client_secret'
     #    export PD_PROJECT_ID='your_project_id'
     # 3. Run the script: python example.py
     # Note: Running top-level await requires Python 3.8+ in REPL or use asyncio.run()
     asyncio.run(main()) 