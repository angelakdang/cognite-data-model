from cognite.client import CogniteClient
from cognite.client.config import ClientConfig
from cognite.client.credentials import OAuthClientCredentials
from dotenv import dotenv_values


def get_cognite_client() -> CogniteClient:
    env_vars = dotenv_values(".env")

    oauth_provider = OAuthClientCredentials(
        token_url=f"https://login.microsoftonline.com/{env_vars['AZURE_TENANT_ID']}/oauth2/v2.0/token",
        client_id=env_vars["AZURE_CLIENT_ID"],
        client_secret=env_vars["AZURE_CLIENT_SECRET"],
        scopes=["https://greenfield.cognitedata.com/.default"],
    )

    config = ClientConfig(
        client_name=env_vars["CDF_CLIENT_NAME"],
        project=env_vars["CDF_PROJECT_NAME"],
        credentials=oauth_provider,
    )

    return CogniteClient(config=config)
