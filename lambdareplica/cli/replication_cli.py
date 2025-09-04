#!/usr/bin/env python3

import argparse
import json
import math
import os
import sys
from typing import Tuple

from lambdareplica.integration import AWSClient, AzureClient, GcpClient
from lambdareplica.integration.aws_client import read_credentials
from lambdareplica.setup.setup_resources import setup_aws, setup_azure, setup_gcp

CONFIG_DIR = os.path.join(os.path.expanduser("~"), ".LambdaReplica")
REGION_STATUS_FILE = os.path.join(CONFIG_DIR, "status.json")
MAXIMUM_PARALLELISM = 128
MINIMUM_MB_PER_FUNCTION = 32

aws_client = AWSClient(is_cloud=False)
azure_client = AzureClient(is_cloud=False)
gcp_client = GcpClient(is_cloud=False)

def choose_execute_region(source_cloud: str,
                          source_region: str,
                          destination_cloud: str,
                          destination_region: str) -> Tuple[str, str]:
    if destination_cloud == 'AWS':
        return destination_cloud, destination_region
    else:
        return source_cloud, source_region

def calculate_parallelism(parallelism: int, size: int) -> int:
    if parallelism > 0:
        return parallelism
    else:
        parallelism = math.ceil(size / (MINIMUM_MB_PER_FUNCTION * 1024 * 1024))
        if parallelism > MAXIMUM_PARALLELISM:
            parallelism = MAXIMUM_PARALLELISM
        return parallelism

def replicate(
        source_cloud: str,
        source_bucket: str,
        object_key: str,
        destination_cloud: str,
        destination_bucket: str,
        parallelism: int,
        chunk_size_mb: int
) -> None:

    print("--- Starting LambdaReplica ---")
    print(f"Source: {source_cloud}://{source_bucket}/{object_key}")
    print(f"Destination: {destination_cloud}://{destination_bucket}/{object_key}")

    if source_cloud == 'AWS':
        source_region = aws_client.get_bucket_region(source_bucket)
        obj_md = aws_client.get_object_metadata(source_bucket, object_key)
        object_size = obj_md["ContentLength"]
    elif source_cloud == 'Azure':
        source_region, source_bucket = source_bucket.split("/", 1)
        obj_md = azure_client.get_object_metadata(container_name=source_bucket, object_key=object_key,
                                                  account=source_region)
        object_size = obj_md.size
    elif source_cloud == 'GCP':
        source_region = gcp_client.get_bucket_region(source_bucket)
        obj_md = gcp_client.get_object_metadata(source_bucket, object_key)
        object_size = obj_md.size
    else:
        print("Source cloud is not supported.")
        sys.exit(1)

    if destination_cloud == 'AWS':
        destination_region = aws_client.get_bucket_region(destination_bucket)
    elif destination_cloud == 'Azure':
        destination_region, destination_bucket = destination_bucket.split("/", 1)
    elif destination_cloud == 'GCP':
        destination_region = gcp_client.get_bucket_region(destination_bucket)
    else:
        print("Destination cloud is not supported.")
        sys.exit(1)

    parallelism = calculate_parallelism(parallelism, object_size)
    print(f"Configuration: Parallelism={parallelism}, Chunk Size={chunk_size_mb}MB")

    execute_cloud, execute_region = choose_execute_region(source_cloud, source_region,
                                                          destination_cloud, destination_region)
    if execute_cloud == 'Azure':
        execute_region = azure_client.get_storage_account_location(execute_region)
    print(f"Functions will be executed at: {execute_cloud}/{execute_region}")

    if not os.path.exists(CONFIG_DIR):
        print(f"lambda-replica-config must be run before using lambda-replica")
        sys.exit(1)

    with open(REGION_STATUS_FILE, "r+") as f:
        content = f.read().strip()
        region_status = json.loads(content) if content else {"AWS": [], "Azure": [], "GCP": []}
        if execute_region not in region_status[execute_cloud]:
            print(f"Functions are being deployed to: {execute_cloud}/{execute_region}")
            status = False
            if execute_cloud == 'AWS':
                status = setup_aws(execute_region)
            elif execute_cloud == 'Azure':
                status = setup_azure(execute_region)
            elif execute_cloud == 'GCP':
                status = setup_gcp(execute_region)
            if status:
                print(f"Functions have been deployed to: {execute_cloud}/{execute_region}")
                region_status[execute_cloud].append(execute_region)
                f.seek(0)
                json.dump(region_status, f, indent=4)
                f.truncate()
            else:
                print(f"Error: unable to deploy LambdaReplica to: {execute_cloud}/{execute_region}.")
                sys.exit(1)
        else:
            print(f"Functions are ready in: {execute_cloud}/{execute_region}")

    payload = {
        "src_cloud": source_cloud,
        "src_region": source_region,
        "src_bucket": source_bucket,
        "key": object_key,
        "dst_cloud": destination_cloud,
        "dst_region": destination_region,
        "dst_bucket": destination_bucket,
        "parallelism": parallelism,
        "chunk_size": chunk_size_mb * 1024 * 1024,
        "size": object_size
    }

    print(f"Start replicating the object.")

    if execute_cloud == 'AWS':
        response = aws_client.invoke_lambda_sync(f'lr_main_{execute_region}',
                                                 payload, region=execute_region)
        if response['StatusCode'] == 200:
            result = json.loads(response['Payload'].read())
            duration = result['time']
        else:
            print("Error: unable to replicate the object.")
            sys.exit(1)
    elif execute_cloud == 'Azure':
        response = azure_client.invoke_function_http(
            function_url=f'https://lambdafuncapp{execute_region}.azurewebsites.net/api/http_trigger?',
            payload=payload)
        if response.status_code == 200:
            result = response.json()
            duration = result['time']
        else:
            print("Error: unable to replicate the object.")
            sys.exit(1)
    else:
        credentials = read_credentials(False)
        with open(credentials['gcp_credentials_path'], 'r') as f:
            gcp_credentials = json.load(f)
            project_id = gcp_credentials['project_id']
        response = gcp_client.invoke_function_http(
            function_url=f'https://{execute_region}-{project_id}.cloudfunctions.net/lambda-{execute_region}',
            payload=payload,
            region=execute_region)
        if response.status_code == 200:
            result = response.json()
            duration = result['time']
        else:
            print("Error: unable to replicate the object.")
            sys.exit(1)

    print(f"Successfully replicated the object in {duration} seconds.")

def main():
    parser = argparse.ArgumentParser(
        description="Replicate an object from one cloud storage to another in a serverless manner.",
        formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument('--source-cloud',
                        choices=['AWS', 'GCP', 'Azure'],
                        required=True,
                        help="Source cloud provider.")
    parser.add_argument('--source-bucket',
                        type=str,
                        required=True,
                        help="Name of the source bucket.")
    parser.add_argument('--object-key',
                        type=str,
                        required=True,
                        help="The key (path/name) of the object to replicate.")
    parser.add_argument('--destination-cloud',
                        choices=['AWS', 'GCP', 'Azure'],
                        required=True,
                        help="Destination cloud provider.")
    parser.add_argument('--destination-bucket',
                        type=str,
                        required=True,
                        help="Name of the destination bucket.")

    parser.add_argument('--parallelism',
                        type=int,
                        default=-1,
                        help="Number of parallel functions to use.")
    parser.add_argument('--chunk-size',
                        type=int,
                        default=8,
                        help="The size of each chunk for multipart uploads in MB. (Default: 8)")

    args = parser.parse_args()

    replicate(
        source_cloud=args.source_cloud,
        source_bucket=args.source_bucket,
        object_key=args.object_key,
        destination_cloud=args.destination_cloud,
        destination_bucket=args.destination_bucket,
        parallelism=args.parallelism,
        chunk_size_mb=args.chunk_size
    )

if __name__ == '__main__':
    main()