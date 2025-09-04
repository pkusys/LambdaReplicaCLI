import datetime
import json
import uuid
from functools import lru_cache
import requests
from xml.etree import ElementTree

from google.api_core import exceptions
from google.api_core.exceptions import NotFound
from google.cloud import storage
from google.cloud import pubsub_v1
from google.cloud import firestore
from google.cloud import functions_v2
from google.cloud import run_v2
import google.oauth2.id_token
import google.auth.transport.requests
import logging
import os

from google.cloud.firestore_admin_v1 import FirestoreAdminClient
from google.cloud.firestore_admin_v1.types import Database
from google.iam.v1 import policy_pb2, iam_policy_pb2
from google.protobuf import field_mask_pb2

CONFIG_DIR = os.path.join(os.path.expanduser("~"), ".LambdaReplica")
LOCAL_METADATA_FILE = os.path.join(CONFIG_DIR, "metadata.json")
LOCAL_GOOGLE_METADATA_FILE = os.path.join(CONFIG_DIR, "google_metadata.json")
CLOUD_METADATA_FILE = "./credentials/metadata.json"
CLOUD_GOOGLE_METADATA_FILE = "./credentials/google_metadata.json"

logger = logging.getLogger()
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

class GcpClient:

    @classmethod
    @lru_cache
    def get_instance(cls, region="us-east1", is_cloud=True):
        return cls(region, is_cloud)

    def __init__(self, region="us-east1", is_cloud=True):
        credentials = read_credentials(is_cloud)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials['gcp_credentials_path']
        self._requests_session = requests.Session()
        self.is_cloud = is_cloud
        self.region = region
        self.storage_client = None
        self.queue_client = None
        self.firestore_clients = dict()

    def get_storage_client(self):
        if self.storage_client is None:
            self.storage_client = storage.Client()
        return self.storage_client

    def get_queue_client(self):
        if self.queue_client is None:
            self.queue_client = pubsub_v1.PublisherClient()
        return self.queue_client

    def get_firestore_client(self, database):
        if database in self.firestore_clients:
            return self.firestore_clients[database]
        else:
            firestore_client = firestore.Client(database=database)
            self.firestore_clients[database] = firestore_client
            return firestore_client

    def get_bucket_region(self, bucket_name):
        storage_client = self.get_storage_client()
        bucket = storage_client.get_bucket(bucket_name)
        return bucket.location.lower()

    def get_object_metadata(self, bucket_name, object_key, region=None):
        storage_client = self.get_storage_client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.get_blob(object_key)
        return blob

    def download_object(self, bucket_name, object_key, save_path, etag=None, offset=-1, size=-1, region=None,
                        write_block_size=2 ** 17):
        storage_client = self.get_storage_client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(object_key)
        if offset < 0:
            blob.download_to_filename(filename=save_path, **(dict(if_etag_match=etag) if etag else dict()))
        else:
            blob.download_to_filename(filename=save_path, start=offset, end=offset + size - 1,
                                      **(dict(if_etag_match=etag) if etag else dict()))

    def upload_object(self, bucket_name, object_key, save_path, mime_type=None, metadata=None, region=None):
        storage_client = self.get_storage_client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(object_key)
        blob.upload_from_filename(save_path, **(dict(content_type=mime_type) if mime_type else dict()))

    def delete_object(self, bucket_name, object_key, region=None):
        storage_client = self.get_storage_client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(object_key)
        blob.delete()

    def send_xml_request(self, bucket_name, object_key, params, method, headers=None,
                         expiration=datetime.timedelta(minutes=15), data=None, content_type="application/octet-stream"):
        storage_client = self.get_storage_client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(object_key)

        headers = headers or {}
        headers["Content-Type"] = content_type

        # generate signed URL
        url = blob.generate_signed_url(
            version="v4", expiration=expiration, method=method,
            **(dict(content_type=content_type) if content_type else dict()),
            query_parameters=params, headers=headers
        )

        # prepare request
        if data:
            req = requests.Request(method, url, headers=headers, data=data)
        else:
            req = requests.Request(method, url, headers=headers)

        prepared = req.prepare()
        response = self._requests_session.send(prepared)

        if not response.ok:
            raise ValueError(f"Invalid status code {response.status_code}: {response.text}")

        return response

    def init_multipart_upload(self, bucket_name, object_key, mime_type=None, region=None):
        response = self.send_xml_request(bucket_name, object_key, {"uploads": None}, "POST",
                                         **(dict(content_type=mime_type) if mime_type else dict()))
        return ElementTree.fromstring(response.content)[2].text

    def multipart_upload(self, bucket_name, object_key, save_path, upload_id, part_num, region=None):
        self.send_xml_request(bucket_name, object_key,
                              {"uploadId": upload_id, "partNumber": part_num},
                              "PUT", data=open(save_path, "rb"))
    
    def multipart_upload_inmemoty(self, bucket_name, object_key, data, upload_id, part_num, region=None):
        self.send_xml_request(bucket_name, object_key,
                              {"uploadId": upload_id, "partNumber": part_num},
                              "PUT", data=data)

    def complete_multipart_upload(self, bucket_name, object_key, upload_id, region=None):
        # get parts
        xml_data = ElementTree.Element("CompleteMultipartUpload")
        next_part_number_marker = None

        # Parts in the list are ordered sequentially, and the XML API does not return lists longer than 1000 parts.
        while True:
            if next_part_number_marker is None:
                response = self.send_xml_request(bucket_name, object_key, {"uploadId": upload_id}, "GET")
            else:
                response = self.send_xml_request(
                    bucket_name, object_key,
                    {"uploadId": upload_id, "part-number-marker": next_part_number_marker}, "GET")

            # build request xml tree
            tree = ElementTree.fromstring(response.content)
            ns = {"ns": tree.tag.split("}")[0][1:]}
            for part in tree.findall("ns:Part", ns):
                part_xml = ElementTree.Element("Part")
                etag_match = part.find("ns:ETag", ns)
                assert etag_match is not None
                etag = etag_match.text
                part_num_match = part.find("ns:PartNumber", ns)
                assert part_num_match is not None
                part_num = part_num_match.text
                ElementTree.SubElement(part_xml, "PartNumber").text = part_num
                ElementTree.SubElement(part_xml, "ETag").text = etag
                xml_data.append(part_xml)

            is_truncated = tree.findall("ns:IsTruncated", ns)[0].text
            if is_truncated == "false":
                break
            else:
                next_part_number_marker = tree.findall("ns:NextPartNumberMarker", ns)[0].text

        xml_data = ElementTree.tostring(xml_data, encoding="utf-8", method="xml")
        xml_data = xml_data.replace(b"ns0:", b"")

        self.send_xml_request(
            bucket_name, object_key, {"uploadId": upload_id}, "POST",
            data=xml_data, content_type="application/xml")

    def abort_multipart_upload(self, bucket_name, object_key, upload_id, region=None):
        response = self.send_xml_request(bucket_name, object_key, {"uploadId": upload_id}, "DELETE")
        return ElementTree.fromstring(response.content)[2].text

    def deploy_cloud_run(self, function_name, code_path, max_instances, timeout, concurrency,
        cpu, memory, region):

        credentials = read_credentials(self.is_cloud)
        with open(credentials['gcp_credentials_path'], 'r') as f:
            gcp_credentials = json.load(f)
            project_id = gcp_credentials['project_id']

        storage_client = storage.Client()
        code_bucket = f"gcs-source-bucket-{project_id}"

        try:
            bucket = storage_client.get_bucket(code_bucket)
        except exceptions.NotFound:
            try:
                bucket = storage_client.create_bucket(code_bucket)
            except exceptions.Conflict as e:
                return False

        code_object = f'package-{uuid.uuid4().hex}.zip'
        blob = bucket.blob(code_object)
        blob.upload_from_filename(code_path)

        func_service_client = functions_v2.FunctionServiceClient()

        parent = f"projects/{project_id}/locations/{region}"
        function_path = f"{parent}/functions/{function_name}"

        try:
            func_service_client.get_function(name=function_path)
            is_existing = True
        except exceptions.NotFound:
            is_existing = False
        except Exception as e:
            return False

        env_variables = {"REGION": region}

        function_obj = functions_v2.Function(
            name=function_path,
            build_config=functions_v2.BuildConfig(
                runtime='python312',
                entry_point='handle_http',
                source=functions_v2.Source(
                    storage_source=functions_v2.StorageSource(
                        bucket=code_bucket,
                        object=code_object,
                    )
                ),
            ),
            service_config=functions_v2.ServiceConfig(
                max_instance_count=max_instances,
                timeout_seconds=timeout,
                max_instance_request_concurrency=concurrency,
                ingress_settings=functions_v2.ServiceConfig.IngressSettings.ALLOW_ALL,
                environment_variables=env_variables,
                available_cpu=cpu,
                available_memory=memory
            ),
        )

        if is_existing:
            update_mask = field_mask_pb2.FieldMask(
                paths=[
                    "build_config",
                    "service_config",
                ]
            )

            try:
                operation = func_service_client.update_function(
                    function=function_obj,
                    update_mask=update_mask,
                )
                operation.result()

            except Exception as e:
                return False
        else:
            try:
                operation = func_service_client.create_function(
                    parent=parent,
                    function=function_obj,
                    function_id=function_name,
                )
                response = operation.result()
            except Exception as e:
                return False

        try:
            updated_function = func_service_client.get_function(name=function_path)
            cloud_run_service = updated_function.service_config.service
            run_client = run_v2.ServicesClient()
            request = iam_policy_pb2.GetIamPolicyRequest()
            request.resource = cloud_run_service
            policy = run_client.get_iam_policy(request)
            binding_exists = False
            for binding in policy.bindings:
                if (binding.role == "roles/run.invoker" and
                        "allUsers" in binding.members):
                    binding_exists = True
                    break
            if not binding_exists:
                binding = policy_pb2.Binding(
                    role="roles/run.invoker",
                    members=["allUsers"],
                )
                policy.bindings.append(binding)

                request = iam_policy_pb2.SetIamPolicyRequest()
                request.resource = cloud_run_service
                request.policy.CopyFrom(policy)
                policy = run_client.set_iam_policy(request)
        except Exception as e:
            return False

        return True

    def invoke_function_async(self, topic, payload, region=None):
        if region is None:
            region = self.region
        queue_client = self.get_queue_client()
        topic_path = queue_client.topic_path('skyblock-412614', f'{topic}-{region}')
        message = json.dumps(payload)
        message_bytes = message.encode('utf-8')
        future = queue_client.publish(topic_path, message_bytes)
        return future

    def invoke_function_http(self, function_url, payload, region=None):
        request = google.auth.transport.requests.Request()
        token = google.oauth2.id_token.fetch_id_token(request, function_url)
        response = requests.post(
            function_url,
            headers={'Authorization': f"Bearer {token}", "Content-Type": "application/json"},
            json=payload)
        return response

    def create_firestore_database(self, database_name, region):
        try:
            admin_client = FirestoreAdminClient()
            credentials = read_credentials(self.is_cloud)
            with open(credentials['gcp_credentials_path'], 'r') as f:
                gcp_credentials = json.load(f)
                project_id = gcp_credentials['project_id']
                parent = f"projects/{project_id}"

            try:
                existing_db = admin_client.get_database(name=f'{parent}/databases/{database_name}')
                return True
            except NotFound:
                pass

            database = Database(
                location_id=region,
                type=Database.DatabaseType.FIRESTORE_NATIVE
            )

            operation = admin_client.create_database(
                parent=parent,
                database_id=database_name,
                database=database,
            )

            created_database = operation.result()
            return True

        except Exception as e:
            print(e)
            return False

    def create_item(self, collection_name, document_id, item, database):
        client = self.get_firestore_client(database)
        doc_ref = client.collection(collection_name).document(document_id)
        response = doc_ref.create(item)
        return response

    def put_item(self, collection_name, document_id, item, database):
        client = self.get_firestore_client(database)
        doc_ref = client.collection(collection_name).document(document_id)
        response = doc_ref.set(item)
        return response

    def get_item(self, collection_name, document_id, database):
        client = self.get_firestore_client(database)
        doc_ref = client.collection(collection_name).document(document_id)
        response = doc_ref.get()
        if response.exists:
            return response
        else:
            return None

    def update_item(self, collection_name, document_id, updates, database):
        # https://cloud.google.com/firestore/docs/samples/firestore-data-set-numeric-increment#firestore_data_set_numeric_increment-python
        client = self.get_firestore_client(database)
        doc_ref = client.collection(collection_name).document(document_id)
        response = doc_ref.update(updates)
        return response

    def delete_item(self, collection_name, document_id, database):
        client = self.get_firestore_client(database)
        doc_ref = client.collection(collection_name).document(document_id)
        response = doc_ref.delete()
        return response
