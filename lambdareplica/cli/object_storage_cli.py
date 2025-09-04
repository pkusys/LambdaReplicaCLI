#!/usr/bin/env python3

import argparse
import sys
import hashlib
import os
from lambdareplica.integration import AWSClient, AzureClient, GcpClient

aws_client = AWSClient(is_cloud=False)
azure_client = AzureClient(is_cloud=False)
gcp_client = GcpClient(is_cloud=False)

def calculate_checksum(file_path: str) -> str:
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def handle_upload(args: argparse.Namespace) -> None:
    print("--- Starting File Upload ---")
    print(f"Source: Local file '{args.local_path}'")
    print(f"Destination: {args.cloud}://{args.bucket}/{args.object_key}")

    try:
        if not os.path.exists(args.local_path):
            raise FileNotFoundError(f"Local file not found: {args.local_path}")

        print(f"INFO: Uploading '{args.local_path}' to '{args.bucket}'...")
        if args.cloud == 'AWS':
            region = aws_client.get_bucket_region(bucket_name=args.bucket)
            aws_client.upload_object(bucket_name=args.bucket, object_key=args.object_key,
                                     save_path=args.local_path, region=region)
        elif args.cloud == 'Azure':
            account, container = args.bucket.split("/", 1)
            azure_client.upload_object(container_name=container, object_key=args.object_key,
                                       save_path=args.local_path, account=account)
        elif args.cloud == 'GCP':
            gcp_client.upload_object(bucket_name=args.bucket, object_key=args.object_key, save_path=args.local_path)
        else:
            raise NotImplementedError
        print("\n--- Upload Successful ---")
        print(f"File '{args.local_path}' uploaded to '{args.object_key}'.")
    except (ValueError, FileNotFoundError) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: An unexpected error occurred during upload: {e}", file=sys.stderr)
        sys.exit(1)

def handle_download(args: argparse.Namespace) -> None:
    print("--- Starting File Download ---")
    print(f"Source: {args.cloud}://{args.bucket}/{args.object_key}")
    print(f"Destination: Local file '{args.local_path}'")

    try:
        print(f"INFO: Downloading '{args.object_key}' from '{args.bucket}'...")
        if args.cloud == 'AWS':
            region = aws_client.get_bucket_region(bucket_name=args.bucket)
            aws_client.download_object(bucket_name=args.bucket, object_key=args.object_key,
                                       save_path=args.local_path, region=region)
        elif args.cloud == 'Azure':
            account, container = args.bucket.split("/", 1)
            azure_client.download_object(container_name=container, object_key=args.object_key,
                                         save_path=args.local_path, account=account)
        elif args.cloud == 'GCP':
            gcp_client.download_object(bucket_name=args.bucket, object_key=args.object_key, save_path=args.local_path)
        else:
            raise NotImplementedError
        print("\n--- Download Successful ---")
        print(f"Object '{args.object_key}' downloaded to '{args.local_path}'.")
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: An unexpected error occurred during download: {e}", file=sys.stderr)
        sys.exit(1)

def _download_for_compare(cloud: str, bucket: str, key: str, index: int) -> str:
    print(f"INFO: Downloading file {index} ({cloud}://{bucket}/{key}) for comparison...")
    temp_dir = os.path.join(os.path.expanduser("~"), ".lambda-replica-cache")
    os.makedirs(temp_dir, exist_ok=True)
    temp_path = os.path.join(temp_dir, f"compare_file_{index}_{os.path.basename(key)}")

    if cloud == 'AWS':
        region = aws_client.get_bucket_region(bucket_name=bucket)
        aws_client.download_object(bucket_name=bucket, object_key=key, save_path=temp_path, region=region)
    elif cloud == 'Azure':
        account, container = bucket.split("/", 1)
        azure_client.download_object(container_name=container, object_key=key, save_path=temp_path, account=account)
    elif cloud == 'GCP':
        gcp_client.download_object(bucket_name=bucket, object_key=key, save_path=temp_path)
    else:
        raise NotImplementedError
    print(f"INFO: File {index} downloaded to temporary path '{temp_path}'.")
    return temp_path


def handle_compare(args: argparse.Namespace) -> None:
    print("--- Starting File Comparison ---")
    print(f"File 1: {args.cloud1}://{args.bucket1}/{args.key1}")
    print(f"File 2: {args.cloud2}://{args.bucket2}/{args.key2}")

    temp_file1_path, temp_file2_path = "", ""

    try:
        temp_file1_path = _download_for_compare(args.cloud1, args.bucket1, args.key1, 1)
        temp_file2_path = _download_for_compare(args.cloud2, args.bucket2, args.key2, 2)

        print("INFO: Calculating checksums...")
        checksum1 = calculate_checksum(temp_file1_path)
        checksum2 = calculate_checksum(temp_file2_path)

        print(f"Checksum 1 (SHA256): {checksum1}")
        print(f"Checksum 2 (SHA256): {checksum2}")

        print("\n--- Comparison Result ---")
        if checksum1 == checksum2:
            print("✅ The files are IDENTICAL.")
        else:
            print("❌ The files are DIFFERENT.")
    except (ValueError, FileNotFoundError) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: An unexpected error occurred during comparison: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        for path in [temp_file1_path, temp_file2_path]:
            if path and os.path.exists(path):
                os.remove(path)
                print(f"INFO: Cleaned up temporary file: {path}")

def handle_delete(args: argparse.Namespace) -> None:
    print("--- Starting Object Deletion ---")
    print(f"Target: {args.cloud}://{args.bucket}/{args.object_key}")

    try:
        print(f"INFO: Deleting '{args.object_key}' from '{args.bucket}'...")
        if args.cloud == 'AWS':
            aws_client.delete_object(bucket_name=args.bucket, object_key=args.object_key)
        elif args.cloud == 'Azure':
            account, container = args.bucket.split("/", 1)
            azure_client.delete_object(container_name=container, object_key=args.object_key, account=account)
        elif args.cloud == 'GCP':
            gcp_client.delete_object(bucket_name=args.bucket, object_key=args.object_key)
        else:
            raise NotImplementedError
        print("\n--- Deletion Successful ---")
        print(f"Object '{args.object_key}' was deleted from '{args.bucket}'.")
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: An unexpected error occurred during deletion: {e}", file=sys.stderr)
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(
        description="A CLI tool for basic cloud storage operations.",
        formatter_class=argparse.RawTextHelpFormatter
    )

    subparsers = parser.add_subparsers(dest='command', required=True, help='Available commands')

    upload_parser = subparsers.add_parser('upload', help='Upload a local file to a cloud bucket.')
    upload_parser.add_argument('--local-path', type=str, required=True, help='Path to the local file to upload.')
    upload_parser.add_argument('--cloud', choices=['AWS', 'GCP', 'Azure'], required=True, help='Target cloud provider.')
    upload_parser.add_argument('--bucket', type=str, required=True, help='Name of the target cloud bucket.')
    upload_parser.add_argument('--object-key', type=str, required=True,
                               help='The key (path/name) for the object in the bucket.')
    upload_parser.set_defaults(func=handle_upload)

    download_parser = subparsers.add_parser('download', help='Download an object from a cloud bucket to a local file.')
    download_parser.add_argument('--cloud', choices=['AWS', 'GCP', 'Azure'], required=True,
                                 help='Source cloud provider.')
    download_parser.add_argument('--bucket', type=str, required=True, help='Name of the source cloud bucket.')
    download_parser.add_argument('--object-key', type=str, required=True,
                                 help='The key (path/name) of the object to download.')
    download_parser.add_argument('--local-path', type=str, required=True,
                                 help='Path to save the downloaded file locally.')
    download_parser.set_defaults(func=handle_download)

    compare_parser = subparsers.add_parser('compare', help='Compare two cloud objects by their checksums.')
    compare_parser.add_argument('--cloud1', choices=['AWS', 'GCP', 'Azure'], required=True, help='Cloud provider for the first file.')
    compare_parser.add_argument('--bucket1', type=str, required=True, help='Bucket for the first file.')
    compare_parser.add_argument('--key1', type=str, required=True, help='Object key for the first file.')
    compare_parser.add_argument('--cloud2', choices=['AWS', 'GCP', 'Azure'], required=True, help='Cloud provider for the second file.')
    compare_parser.add_argument('--bucket2', type=str, required=True, help='Bucket for the second file.')
    compare_parser.add_argument('--key2', type=str, required=True, help='Object key for the second file.')
    compare_parser.set_defaults(func=handle_compare)

    delete_parser = subparsers.add_parser('delete', help='Delete an object from a cloud bucket.')
    delete_parser.add_argument('--cloud', choices=['AWS', 'GCP', 'Azure'], required=True,
                               help='Cloud provider of the object to delete.')
    delete_parser.add_argument('--bucket', type=str, required=True, help='Name of the cloud bucket.')
    delete_parser.add_argument('--object-key', type=str, required=True,
                               help='The key (path/name) of the object to delete.')
    delete_parser.set_defaults(func=handle_delete)

    args = parser.parse_args()
    if hasattr(args, 'func'):
        args.func(args)
    else:
        parser.print_help()

if __name__ == '__main__':
    main()
