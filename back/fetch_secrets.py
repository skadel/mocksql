import os

from google.cloud import secretmanager

PROJECT_ID = os.environ["PROJECT_ID"]
VERSION_ID = "latest"

secret_id = os.getenv("SECRET_MANAGER_SECRET_ID", "mocksql-env")


def access_secret_version():
    """
    Access the payload for the given secret version if one exists.
    """
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/{VERSION_ID}"

    # Access the secret version.
    response = client.access_secret_version(name=name)

    # Return the decoded payload.
    payload = response.payload.data.decode("UTF-8")
    return payload


# Récupérer les secrets
secret_payload = access_secret_version()

# Écrire les secrets dans un fichier .env
with open(".env", "w") as f:
    f.write(secret_payload)
