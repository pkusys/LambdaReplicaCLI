import base64
import json
import logging
import os
import subprocess
import requests
from functools import lru_cache
from azure.core.exceptions import ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.mgmt.cosmosdb import CosmosDBManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.web import WebSiteManagementClient
from azure.storage.blob import BlobServiceClient, ContentSettings, BlobBlock
from azure.core import MatchConditions
from azure.storage.queue import QueueClient, TextBase64EncodePolicy
from azure.cosmos.cosmos_client import CosmosClient

CONFIG_DIR = os.path.join(os.path.expanduser("~"), ".LambdaReplica")
LOCAL_METADATA_FILE = os.path.join(CONFIG_DIR, "metadata.json")
LOCAL_GOOGLE_METADATA_FILE = os.path.join(CONFIG_DIR, "google_metadata.json")
CLOUD_METADATA_FILE = "./credentials/metadata.json"
CLOUD_GOOGLE_METADATA_FILE = "./credentials/google_metadata.json"

logger = logging.getLogger("azure")
logger.setLevel(logging.ERROR)

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

class AzureClient:

    @classmethod
    @lru_cache
    def get_instance(cls, is_cloud=True):
        return cls(is_cloud)

    def __init__(self, is_cloud=True):
        self.is_cloud = is_cloud
        credentials = read_credentials(self.is_cloud)
        os.environ["AZURE_SUBSCRIPTION_ID"] = credentials["azure_subscription_id"]
        os.environ["AZURE_CLIENT_ID"] = credentials["azure_client_id"]
        os.environ["AZURE_TENANT_ID"] = credentials["azure_tenant_id"]
        os.environ["AZURE_CLIENT_SECRET"] = credentials["azure_client_secret"]
        self.credential = DefaultAzureCredential()
        self.cosmos_keys = {}
        if 'azure_cosmos_keys' in credentials:
            cosmos_metadata = credentials['azure_cosmos_keys']
            for region, metadata in cosmos_metadata.items():
                self.cosmos_keys[metadata['account']] = metadata['key']
        self.blob_clients = dict()
        self.queue_clients = dict()
        self.cosmos_clients = dict()

    def get_blob_client(self, account):
        if account in self.blob_clients:
            return self.blob_clients[account]
        else:
            blob_client = BlobServiceClient(f'https://{account}.blob.core.windows.net',
                                            credential=self.credential)
            self.blob_clients[account] = blob_client
            return blob_client

    def get_queue_client(self, topic, account):
        key = f'{account}-{topic}'
        if key in self.queue_clients:
            return self.queue_clients[key]
        else:
            queue_client = QueueClient(f'https://{account}.queue.core.windows.net',
                                       queue_name=topic, credential=self.credential,
                                       message_encode_policy=TextBase64EncodePolicy())
            self.queue_clients[key] = queue_client
            return queue_client

    def get_cosmos_client(self, account):
        if account in self.cosmos_clients:
            return self.cosmos_clients[account]
        else:
            cosmos_client = CosmosClient(f'https://{account}.documents.azure.com:443/',
                                         {'masterKey': self.cosmos_keys[account]})
            self.cosmos_clients[account] = cosmos_client
            return cosmos_client

    def get_storage_account_location(self, account_name):
        client = StorageManagementClient(self.credential, os.environ["AZURE_SUBSCRIPTION_ID"])
        credentials = read_credentials(self.is_cloud)
        storage_account = client.storage_accounts.get_properties(
            credentials['azure_resource_group'],
            account_name
        )
        return storage_account.location

    def get_object_metadata(self, container_name, object_key, account):
        blob_service_client = self.get_blob_client(account)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=object_key)
        return blob_client.get_blob_properties()

    def download_object(self, container_name, object_key, save_path, account, etag=None, offset=-1, size=-1,
                        write_block_size=2 ** 17, max_concurrency=1):
        blob_service_client = self.get_blob_client(account)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=object_key)

        if offset < 0:
            download_stream = blob_client.download_blob(
                max_concurrency=max_concurrency,
                **(dict(etag=etag, match_condition=MatchConditions.IfNotModified) if etag else dict())
            )
        else:
            download_stream = blob_client.download_blob(offset=offset, length=size,
                                                        max_concurrency=max_concurrency,
                                                        **(dict(etag=etag,
                                                                match_condition=MatchConditions.IfNotModified)
                                                           if etag else dict())
                                                        )
        with open(save_path, "wb") as f:
            b = download_stream.read(write_block_size)
            while b:
                f.write(b)
                b = download_stream.read(write_block_size)

    def upload_object(self, container_name, object_key, save_path, account,
                      mime_type=None, metadata=None, max_concurrency=1):
        blob_service_client = self.get_blob_client(account)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=object_key)

        with open(save_path, "rb") as f:
            blob_client.upload_blob(
                data=f,
                length=os.path.getsize(save_path),
                overwrite=True,
                max_concurrency=max_concurrency,
                **(dict(metadata=metadata) if metadata else dict()),
                **(dict(content_settings=ContentSettings(content_type=mime_type)) if mime_type else dict())
            )

    def delete_object(self, container_name, object_key, account):
        blob_service_client = self.get_blob_client(account)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=object_key)
        blob_client.delete_blob()

    def multipart_upload(self, container_name, object_key, save_path, part_num, account):
        blob_service_client = self.get_blob_client(account)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=object_key)

        with open(save_path, "rb") as f:
            block_id = self.id_to_base64_encoding(part_number=part_num, object_key=object_key)
            blob_client.stage_block(block_id=block_id, data=f, length=os.path.getsize(save_path))

    def multipart_upload_inmemory(self, container_name, object_key, data, size, part_num, account):
        blob_service_client = self.get_blob_client(account)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=object_key)
        block_id = self.id_to_base64_encoding(part_number=part_num, object_key=object_key)
        blob_client.stage_block(block_id=block_id, data=data, length=size)

    def complete_multipart_upload(self, container_name, object_key, num_parts, account, mime_type=None,
                                  metadata=None):
        blob_service_client = self.get_blob_client(account)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=object_key)
        block_list = list()
        for i in range(num_parts):
            block_id = self.id_to_base64_encoding(i, object_key)
            block_list.append(BlobBlock(block_id=block_id))
        blob_client.commit_block_list(block_list=block_list,
                                      **(dict(metadata=metadata) if metadata else dict()),
                                      **(dict(content_settings=ContentSettings(
                                          content_type=mime_type)) if mime_type else dict()))

    @staticmethod
    def id_to_base64_encoding(part_number, object_key):
        max_length = 5 + len(object_key)
        block_id = f"{part_number}{object_key}"
        block_id = block_id.ljust(max_length, "0")
        block_id = block_id.encode("utf-8")
        block_id = base64.b64encode(block_id).decode("utf-8")
        return block_id

    def deploy_functionapp(self, function_name, package_path, instance_memory_mb, maximum_instance_count,
                           per_instance_concurrency, resource_group, region):
        storage_client = StorageManagementClient(self.credential, os.environ["AZURE_SUBSCRIPTION_ID"])
        web_client = WebSiteManagementClient(self.credential, os.environ["AZURE_SUBSCRIPTION_ID"])

        storage_account_name = f"lrfuncappstorage{region}"
        try:
            storage_account = storage_client.storage_accounts.get_properties(
                resource_group, storage_account_name
            )
        except ResourceNotFoundError:
            poller = storage_client.storage_accounts.begin_create(
                resource_group,
                storage_account_name,
                {
                    "sku": {"name": "Standard_LRS"},
                    "kind": "StorageV2",
                    "location": region,
                },
            )
            storage_account = poller.result()

        keys = storage_client.storage_accounts.list_keys(
            resource_group, storage_account_name
        )
        storage_connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={keys.keys[0].value};EndpointSuffix=core.windows.net"

        blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
        try:
            container_client = blob_service_client.create_container(
                name=f'{function_name}-releases',
            )
        except Exception as e:
            if "ContainerAlreadyExists" not in str(e):
                print(str(e))
                return False

        app_service_plan_name = f"{function_name}plan"
        plan_poller = web_client.app_service_plans.begin_create_or_update(
            resource_group,
            app_service_plan_name,
            {
                "location": region,
                "sku": {"name": "FC1", "tier": "FlexConsumption"},
                "kind": "functionapp",
                "reserved": True
            },
        )
        app_service_plan = plan_poller.result()

        function_app_poller = web_client.web_apps.begin_create_or_update(
            resource_group,
            function_name,
            {
                "location": region,
                "kind": "functionapp,linux",
                "server_farm_id": app_service_plan.id,
                "site_config": {
                    "app_settings": [
                        {"name": "AzureWebJobsStorage", "value": storage_connection_string},
                        {"name": "FUNCTIONS_EXTENSION_VERSION", "value": "~4"},
                        {"name": "REGION", "value": region}
                    ],
                },
                "function_app_config": {
                    "scaleAndConcurrency": {
                        "instanceMemoryMB": instance_memory_mb,
                        "maximumInstanceCount": maximum_instance_count,
                        "triggers": {
                            "http": { "perInstanceConcurrency": per_instance_concurrency }
                        }
                    },
                    "deployment": {
                        "storage": {
                            "type": "BlobContainer",
                            "value": f"https://{storage_account_name}.blob.core.windows.net/{function_name}-releases",
                            "authentication": {
                                "type": "StorageAccountConnectionString",
                                "storageAccountConnectionStringName": "AzureWebJobsStorage"
                            }
                        }
                    },
                    "runtime": {
                        "name": "python",
                        "version": "3.11"
                    }
                },
                "https_only": True,
            },
        )
        function_app = function_app_poller.result()

        command = [
            "az", "functionapp", "deployment", "source", "config-zip",
            "--resource-group", resource_group,
            "-n", function_name,
            "--src", package_path,
            "--timeout", "900"
        ]

        try:
            result = subprocess.run(
                command,
                check=True,
                capture_output=True,
                text=True
            )
        except FileNotFoundError:
            return False
        except subprocess.CalledProcessError as e:
            return False

        return True

    def invoke_function_queue(self, topic, payload, account):
        queue_client = self.get_queue_client(topic, account)
        message = json.dumps(payload)
        queue_client.send_message(message)

    def invoke_function_http(self, function_url, payload):
        headers = {
            'Content-Type': 'application/json'
        }
        response = requests.post(function_url, json=payload, headers=headers)
        return response

    def create_cosmosdb_table(self, account, database, container, resource_group, region):

        try:
            cosmosdb_client = CosmosDBManagementClient(self.credential, os.environ["AZURE_SUBSCRIPTION_ID"])

            poller = cosmosdb_client.database_accounts.begin_create_or_update(
                resource_group,
                account,
                {
                    "location": region,
                    "kind": "GlobalDocumentDB",
                    "capabilities": [{"name": "EnableServerless"}],
                    "locations": [
                        {
                            "location_name": region,
                            "failover_priority": 0,
                        }
                    ],
                    "database_account_offer_type": "Standard",
                    "backup_policy": {
                        "type": "Periodic",
                        "periodic_mode_properties": {
                            "backup_interval_in_minutes": 1440,
                            "backup_retention_interval_in_hours": 8,
                            "backup_storage_redundancy": "Local"
                        }
                    }
                },
            )
            account_result = poller.result()

            db_poller = cosmosdb_client.sql_resources.begin_create_update_sql_database(
                resource_group,
                account,
                database,
                {
                    "resource": {"id": database},
                    "options": {}
                }
            )
            db_result = db_poller.result()

            container_poller = cosmosdb_client.sql_resources.begin_create_update_sql_container(
                resource_group,
                account,
                database,
                container,
                {
                    "resource": {
                        "id": container,
                        "partition_key": {
                            "paths": ['/id'],
                            "kind": "Hash"
                        }
                    },
                    "options": {}
                }
            )
            container_result = container_poller.result()

            keys = cosmosdb_client.database_accounts.list_keys(
                resource_group, account
            )

            return {"endpoint": account_result.document_endpoint,
                    "key": keys.primary_master_key}

        except Exception as ex:
            print(ex)
            return None

    def create_item(self, database_id, container_id, item, account):
        cosmos_client = self.get_cosmos_client(account)
        db = cosmos_client.get_database_client(database_id)
        container = db.get_container_client(container_id)
        response = container.create_item(body=item)
        return response

    def replace_item(self, database_id, container_id, key, item, account):
        cosmos_client = self.get_cosmos_client(account)
        db = cosmos_client.get_database_client(database_id)
        container = db.get_container_client(container_id)
        response = container.replace_item(item=key, body=item)
        return response

    def read_item(self, database_id, container_id, key, account, partition_key=None):
        cosmos_client = self.get_cosmos_client(account)
        db = cosmos_client.get_database_client(database_id)
        container = db.get_container_client(container_id)
        if partition_key is None:
            partition_key = key
        response = container.read_item(item=key, partition_key=partition_key)
        return response

    def patch_item(self, database_id, container_id, key, operations, account, partition_key=None):
        cosmos_client = self.get_cosmos_client(account)
        db = cosmos_client.get_database_client(database_id)
        container = db.get_container_client(container_id)
        if partition_key is None:
            partition_key = key
        response = container.patch_item(item=key, partition_key=partition_key,
                                        patch_operations=operations)
        return response

    def delete_item(self, database_id, container_id, key, account, partition_key=None):
        cosmos_client = self.get_cosmos_client(account)
        db = cosmos_client.get_database_client(database_id)
        container = db.get_container_client(container_id)
        if partition_key is None:
            partition_key = key
        response = container.delete_item(item=key, partition_key=partition_key)
        return response
