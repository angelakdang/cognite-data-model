from cognite.client import ClientConfig, CogniteClient, global_config
from cognite.client.credentials import OAuthClientCredentials
from dotenv import dotenv_values


def get_cognite_client() -> CogniteClient:
    env_vars = dotenv_values(".env")

    cluster = env_vars["CDF_CLUSTER"]
    base_url = f"https://{cluster}.cognitedata.com"
    tenant_id = env_vars["AZURE_TENANT_ID"]
    client_id = env_vars["AZURE_CLIENT_ID"]
    client_secret = env_vars["AZURE_CLIENT_SECRET"]
    creds = OAuthClientCredentials(
        token_url=f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=[f"{base_url}/.default"],
    )

    cnf = ClientConfig(
        client_name=env_vars["CDF_CLIENT_NAME"],
        base_url=base_url,
        project=env_vars["CDF_PROJECT_NAME"],
        credentials=creds,
    )

    global_config.default_client_config = cnf
    return CogniteClient()


# Check to see if client works
# client = get_cognite_client()
# print(client.config)
