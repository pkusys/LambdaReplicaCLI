import os
import json
import shutil

CONFIG_DIR = os.path.join(os.path.expanduser("~"), ".LambdaReplica")
LOCAL_METADATA_FILE = os.path.join(CONFIG_DIR, "metadata.json")
LOCAL_GOOGLE_METADATA_FILE = os.path.join(CONFIG_DIR, "google_metadata.json")
CLOUD_METADATA_FILE = "./credentials/metadata.json"
CLOUD_GOOGLE_METADATA_FILE = "./credentials/google_metadata.json"

def configure_credentials(aws_access_key, aws_secret_key, aws_role_arn,
                          azure_subscription_id, azure_client_id, azure_tenant_id, azure_client_secret,
                          azure_resource_group, gcp_credentials_path):
    with open(LOCAL_METADATA_FILE, "r+") as f:
        content = f.read().strip()
        current_metadata = json.loads(content) if content else {}
        current_metadata["aws_access_key"] = aws_access_key
        current_metadata["aws_secret_key"] = aws_secret_key
        current_metadata["aws_role_arn"] = aws_role_arn
        current_metadata["azure_subscription_id"] = azure_subscription_id
        current_metadata["azure_client_id"] = azure_client_id
        current_metadata["azure_tenant_id"] = azure_tenant_id
        current_metadata["azure_client_secret"] = azure_client_secret
        current_metadata["azure_resource_group"] = azure_resource_group
        shutil.copy(gcp_credentials_path, LOCAL_GOOGLE_METADATA_FILE)
        f.seek(0)
        json.dump(current_metadata, f, indent=4)
        f.truncate()

def update_credentials(new_metadata):
    with open(LOCAL_METADATA_FILE, "w") as f:
        if 'gcp_credentials_path' in new_metadata:
            del new_metadata['gcp_credentials_path']
        json.dump(new_metadata, f, indent=4)

def read_credentials(is_cloud=True):
    metadata_file_path = CLOUD_METADATA_FILE if is_cloud else LOCAL_METADATA_FILE
    try:
        with open(metadata_file_path, "r") as f:
            content = f.read().strip()
            current_metadata = json.loads(content) if content else {}
            current_metadata["gcp_credentials_path"] = CLOUD_GOOGLE_METADATA_FILE if is_cloud else LOCAL_GOOGLE_METADATA_FILE
            return current_metadata
    except IOError:
        return {}
