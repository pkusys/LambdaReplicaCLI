import argparse
import sys
import os

from lambdareplica.setup.setup_credentials import configure_credentials

CONFIG_DIR = os.path.join(os.path.expanduser("~"), ".LambdaReplica")
METADATA_FILE = os.path.join(CONFIG_DIR, "metadata.json")


def configure_lambdareplica():
    print("Configuring LambdaReplica...")

    os.makedirs(CONFIG_DIR, exist_ok=True)

    aws_access_key = input("Please enter your AWS Access Key: ")
    aws_secret_key = input("Please enter your AWS Secret Key: ")
    aws_role_arn = input("Please enter your AWS Role ARN: ")
    azure_subscription_id = input("Please enter your Azure Subscription ID: ")
    azure_client_id = input("Please enter your Azure Client ID: ")
    azure_tenant_id = input("Please enter your Azure Tenant ID: ")
    azure_client_secret = input("Please enter your Azure Client Secret: ")
    azure_resource_group = input("Please enter your Azure Resource Group: ")
    gcp_credentials_path = input("Please enter the path to your GCP Credentials: ")

    try:
        configure_credentials(aws_access_key, aws_secret_key, aws_role_arn,
                              azure_subscription_id, azure_client_id, azure_tenant_id, azure_client_secret,
                              azure_resource_group, gcp_credentials_path)
        print(f"\nConfiguration saved successfully to: {METADATA_FILE}")
    except IOError as e:
        print(f"\nError: Could not write configuration file. {e}", file=sys.stderr)
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(
        description="The configuration tool for LambdaReplica."
    )
    parser.parse_args()
    configure_lambdareplica()

if __name__ == "__main__":
    main()
