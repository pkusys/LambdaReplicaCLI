import json
import logging
import os
from functools import lru_cache
from dataclasses import dataclass, field
from typing import ClassVar
import boto3
from botocore.exceptions import ClientError
import functools
import hashlib
import math
import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional, TypeVar

CONFIG_DIR = os.path.join(os.path.expanduser("~"), ".LambdaReplica")
LOCAL_METADATA_FILE = os.path.join(CONFIG_DIR, "metadata.json")
LOCAL_GOOGLE_METADATA_FILE = os.path.join(CONFIG_DIR, "google_metadata.json")
CLOUD_METADATA_FILE = "./credentials/metadata.json"
CLOUD_GOOGLE_METADATA_FILE = "./credentials/google_metadata.json"

logger = logging.getLogger("lambdareplica")
logger.setLevel(logging.INFO)

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

def log_event(event_type: str, payload: dict, level: int = logging.INFO):
    now_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
    event = {"event_type": event_type, "event_payload": payload, "event_timestamp": now_ts}
    logger.log(level, f"{event_type}: {payload}", extra=event)

def unmarshal_value(node):
    if type(node) is not dict:
        return node

    for key, value in node.items():
        # S – String - return string
        # N – Number - return int or float (if includes '.')
        # B – Binary - not handled
        # BOOL – Boolean - return Bool
        # NULL – Null - return None
        # M – Map - return a dict
        # L – List - return a list
        # SS – String Set - not handled
        # NN – Number Set - not handled
        # BB – Binary Set - not handled
        key = key.lower()
        if key == "bool":
            return value
        if key == "null":
            return None
        if key == "s":
            return value
        if key == "n":
            if "." in str(value):
                return float(value)
            return int(value)
        if key in ["m", "l"]:
            if key == "m":
                data = {}
                for key1, value1 in value.items():
                    if key1.lower() == "l":
                        data = [unmarshal_value(n) for n in value1]
                    else:
                        if type(value1) is not dict:
                            return unmarshal_value(value)
                        data[key1] = unmarshal_value(value1)
                return data
            data = []
            for item in value:
                data.append(unmarshal_value(item))
            return data


def get_number_of_functions(num_dest, obj_size, size_threshold):
    return math.ceil(obj_size * num_dest / size_threshold)


def calculate_s3_etag(file_path, chunk_size):
    md5s = []

    with open(file_path, "rb") as fp:
        while True:
            data = fp.read(chunk_size)
            if not data:
                break
            md5s.append(hashlib.md5(data))

    if len(md5s) < 1:
        return '"{}"'.format(hashlib.md5().hexdigest())

    if len(md5s) == 1:
        return '"{}"'.format(md5s[0].hexdigest())

    digests = b"".join(m.digest() for m in md5s)
    digests_md5 = hashlib.md5(digests)
    return '"{}-{}"'.format(digests_md5.hexdigest(), len(md5s))

T = TypeVar("T")
def repeat_until(
    func: Callable[[], T],
    condition: Callable[[T], bool],
    max_retries: int = 20,
    interval: float = 0.3,
    callback: Optional[Callable[[T], Any]] = None,
) -> T:
    iter = 0
    while True:
        result = func()
        if callback:
            callback(result)
        if condition(result):
            return result
        time.sleep(interval)
        iter += 1
        if iter >= max_retries:
            raise TimeoutError("Max retries reached")
        continue

def cached(func: Callable) -> Callable:
    cache = {}

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        key = (args, frozenset(kwargs.items()))
        if key not in cache:
            cache[key] = func(*args, **kwargs)
        return cache[key]

    return wrapper

def now():
    return datetime.now(timezone.utc)

def to_timestamp(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)

def now_timestamp():
    return to_timestamp(now())

def allocate_part_nums(num_worker, parts_per_worker):
    part_nums = []
    for i in range(num_worker):
        part_nums.append(list(range(1 + i * parts_per_worker, 1 + (i+1) * parts_per_worker)))
    return part_nums

@dataclass
class ChangeLog:
    TABLE_NAME: ClassVar[str] = "changelog"

    @classmethod
    def make_key(cls, bucket_name: str, object_key: str, etag: str) -> dict:
        return {
            "BucketName_ObjectKey": {"S": f"{bucket_name}/{object_key}"},
            "ETag": {"S": etag},
        }

    def to_dynamodb_record(self) -> dict:
        value = {
            "BucketName_ObjectKey": {"S": self.BucketName_ObjectKey},
            "ETag": {"S": self.ETag},
            "NumTasks": {"N": str(self.NumTasks)} if self.NumTasks else None,
            "ChangeLog": {"L": [item.marshal() for item in self.ChangeLog]},
        }
        return { k: v for k, v in value.items() if v is not None }

    BucketName_ObjectKey: str
    ETag: str
    NumTasks: Optional[int] = field(default=None)
    ChangeLog: list["ChangeLogItem"] = field(default_factory=list)

@dataclass
class ChangeLogItem:
    key: str
    etag: str
    offset: int = field(default=-1)
    size: int = field(default=-1)

    def marshal(self) -> dict:
        return {
            "M": {
                "key": {"S": self.key},
                "etag": {"S": self.etag},
                "offset": {"N": str(self.offset)},
                "size": {"N": str(self.size)},
            }
        }

@dataclass
class ObjectLock:
    TABLE_NAME: ClassVar[str] = "object_lock"

    @classmethod
    def make_key(cls, bucket_name: str, object_key: str) -> dict:
        return {"BucketName": {"S": bucket_name}, "ObjectKey": {"S": object_key}}

    @classmethod
    def from_dynamodb_record(cls, record: dict) -> "ObjectLock":
        object_lock: dict = unmarshal_value({"M": record})
        return ObjectLock(
            bucket_name=object_lock["BucketName"],
            object_key=object_lock["ObjectKey"],
            sequencer=object_lock["Sequencer"],
            locked=object_lock["Locked"],
        )

    def to_dynamodb_record(self) -> dict:
        value = {
            "BucketName": {"S": self.bucket_name},
            "ObjectKey": {"S": self.object_key},
            "Sequencer": {"N": str(self.sequencer)},
            "Locked": {"BOOL": self.locked},
        }
        return { k: v for k, v in value.items() if v is not None }

    bucket_name: str
    object_key: str
    sequencer: int
    locked: bool

class AWSClient:

    @classmethod
    @lru_cache
    def get_instance(cls, region="us-east-1", config=None, is_cloud=True):
        return cls(region, config, is_cloud)

    def __init__(self, region="us-east-1", config=None, is_cloud=True):
        logger.info(f"Creating AWSClient with region: {region}, config: {config}")
        self.region = region
        self.s3_clients = dict()
        self.lambda_clients = dict()
        self.dynamodb_clients = dict()
        self.event_clients = dict()
        self.scheduler_clients = dict()
        self.stepfunction_clients = dict()
        self.config = config
        self.is_cloud = is_cloud

    def get_s3_client(self, region=None):
        if region is None:
            region = self.region
        if region in self.s3_clients:
            return self.s3_clients[region]
        else:
            credentials = read_credentials(self.is_cloud)
            s3_client = boto3.client("s3", region_name=region, config=self.config,
                                     aws_access_key_id=credentials['aws_access_key'],
                                     aws_secret_access_key=credentials['aws_secret_key'])
            self.s3_clients[region] = s3_client
            return s3_client

    def get_lambda_client(self, region=None):
        if region is None:
            region = self.region
        if region in self.lambda_clients:
            return self.lambda_clients[region]
        else:
            credentials = read_credentials(self.is_cloud)
            lambda_client = boto3.client("lambda", region_name=region, config=self.config,
                                         aws_access_key_id=credentials['aws_access_key'],
                                         aws_secret_access_key=credentials['aws_secret_key']
                                         )
            self.lambda_clients[region] = lambda_client
            return lambda_client

    def get_dynamodb_client(self, region=None):
        if region is None:
            region = self.region
        if region in self.dynamodb_clients:
            return self.dynamodb_clients[region]
        else:
            credentials = read_credentials(self.is_cloud)
            dynamodb_client = boto3.client("dynamodb", region_name=region, config=self.config,
                                           aws_access_key_id=credentials['aws_access_key'],
                                           aws_secret_access_key=credentials['aws_secret_key']
                                           )
            self.dynamodb_clients[region] = dynamodb_client
            return dynamodb_client

    def get_event_client(self, region=None):
        if region is None:
            region = self.region
        if region in self.event_clients:
            return self.event_clients[region]
        else:
            credentials = read_credentials(self.is_cloud)
            event_client = boto3.client("events", region_name=region, config=self.config,
                                        aws_access_key_id=credentials['aws_access_key'],
                                        aws_secret_access_key=credentials['aws_secret_key']
                                        )
            self.event_clients[region] = event_client
            return event_client

    def get_bucket_region(self, bucket_name):
        s3_client = self.get_s3_client('us-east-1')

        response = s3_client.get_bucket_location(Bucket=bucket_name)
        region = response["LocationConstraint"]

        if region is None:
            region = "us-east-1"
        if region == 'EU':
            region = "eu-west-1"

        return region

    def get_object_metadata(self, bucket_name, object_key, version_id=None, region=None):
        s3_client = self.get_s3_client(region)
        metadata = s3_client.head_object(
            Bucket=bucket_name, Key=object_key, **(dict(VersionId=version_id) if version_id else dict())
        )
        return metadata

    def copy_object(self, src_bucket, src_key, tgt_bucket, tgt_key, region=None):
        s3_client = self.get_s3_client(region)
        response = s3_client.copy_object(
            Bucket=tgt_bucket, CopySource={"Bucket": src_bucket, "Key": src_key}, Key=tgt_key
        )
        return response

    def get_object(
            self, bucket_name, object_key, etag=None, version_id=None, offset=-1, size=-1, region=None
    ):
        s3_client = self.get_s3_client(region)
        if offset < 0:
            response = s3_client.get_object(
                Bucket=bucket_name,
                Key=object_key,
                **(dict(IfMatch=etag) if etag else dict()),
                **(dict(VersionId=version_id) if version_id else dict()),
            )
        else:
            response = s3_client.get_object(
                Bucket=bucket_name,
                Key=object_key,
                **(dict(IfMatch=etag) if etag else dict()),
                **(dict(VersionId=version_id) if version_id else dict()),
                Range=f"bytes={offset}-{offset + size - 1}",
            )
        return response

    def download_object(
            self,
            bucket_name,
            object_key,
            save_path,
            etag=None,
            version_id=None,
            offset=-1,
            size=-1,
            region=None,
            write_block_size=2 ** 17,
    ):
        s3_client = self.get_s3_client(region)
        if offset < 0:
            response = s3_client.get_object(
                Bucket=bucket_name,
                Key=object_key,
                **(dict(IfMatch=etag) if etag else dict()),
                **(dict(VersionId=version_id) if version_id else dict()),
            )
        else:
            response = s3_client.get_object(
                Bucket=bucket_name,
                Key=object_key,
                **(dict(IfMatch=etag) if etag else dict()),
                **(dict(VersionId=version_id) if version_id else dict()),
                Range=f"bytes={offset}-{offset + size - 1}",
            )
        with open(save_path, "wb") as f:
            b = response["Body"].read(write_block_size)
            while b:
                f.write(b)
                b = response["Body"].read(write_block_size)
        response["Body"].close()
        return response

    def upload_object(self, bucket_name, object_key, save_path, mime_type=None, metadata=None, region=None):
        s3_client = self.get_s3_client(region)
        with open(save_path, "rb") as f:
            response = s3_client.put_object(
                Body=f,
                Key=object_key,
                Bucket=bucket_name,
                **(dict(Metadata=metadata) if metadata else dict()),
                **(dict(ContentType=mime_type) if mime_type else dict()),
            )

        return response

    def upload_object_inmemory(self, bucket_name, object_key, data, mime_type=None, metadata=None, region=None):
        s3_client = self.get_s3_client(region)
        response = s3_client.put_object(
            Body=data,
            Key=object_key,
            Bucket=bucket_name,
            **(dict(Metadata=metadata) if metadata else dict()),
            **(dict(ContentType=mime_type) if mime_type else dict()),
        )

        return response

    def delete_object(self, bucket_name, object_key, version_id=None, region=None):
        s3_client = self.get_s3_client(region)
        response = s3_client.delete_object(
            Bucket=bucket_name, Key=object_key, **(dict(VersionId=version_id) if version_id else dict())
        )
        return response

    def init_multipart_upload(self, bucket_name, object_key, mime_type=None, metadata=None, region=None, get_timestamp=False):
        s3_client = self.get_s3_client(region)
        response = s3_client.create_multipart_upload(
            Bucket=bucket_name,
            Key=object_key,
            **(dict(Metadata=metadata) if metadata else dict()),
            **(dict(ContentType=mime_type) if mime_type else dict()),
        )
        if get_timestamp:
            date_str = response["ResponseMetadata"]["HTTPHeaders"]["date"]
            date = datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S %Z')
            return response["UploadId"], date
        return response["UploadId"]

    def multipart_upload(self, bucket_name, object_key, save_path, upload_id, part_num, region=None):
        s3_client = self.get_s3_client(region)
        with open(save_path, "rb") as f:
            response = s3_client.upload_part(
                Body=f,
                Key=object_key,
                Bucket=bucket_name,
                PartNumber=part_num,
                UploadId=upload_id.strip(),
            )
        return response

    def multipart_upload_inmemory(self, bucket_name, object_key, data, upload_id, part_num, region=None):
        s3_client = self.get_s3_client(region)
        response = s3_client.upload_part(
            Body=data, Key=object_key, Bucket=bucket_name, PartNumber=part_num, UploadId=upload_id.strip()
        )
        return response

    def multipart_upload_copy(
            self,
            bucket_name,
            object_key,
            src_bucket,
            src_obj,
            upload_id,
            part_num,
            src_offset=None,
            src_size=None,
            region=None,
    ):
        s3_client = self.get_s3_client(region)
        copy_source_range = f"bytes={src_offset}-{src_offset + src_size - 1}" if src_offset else None
        print(f"Copying {src_bucket}/{src_obj} to {bucket_name}/{object_key}, range: {copy_source_range}")
        response = s3_client.upload_part_copy(
            Key=object_key,
            Bucket=bucket_name,
            PartNumber=part_num,
            UploadId=upload_id.strip(),
            CopySource={"Bucket": src_bucket, "Key": src_obj},
            **(dict(CopySourceRange=copy_source_range) if src_offset else dict()),
        )
        return response

    def complete_multipart_upload(self, bucket_name, object_key, upload_id, max_parts=100, region=None):
        s3_client = self.get_s3_client(region)
        all_parts = []
        part_number_marker = None
        while True:
            response = s3_client.list_parts(
                Bucket=bucket_name,
                Key=object_key,
                MaxParts=max_parts,
                UploadId=upload_id,
                **({"PartNumberMarker": part_number_marker} if part_number_marker else {}),
            )
            if "NextPartNumberMarker" in response:
                part_number_marker = response["NextPartNumberMarker"]
            if "Parts" not in response or len(response["Parts"]) == 0:
                break
            else:
                all_parts += response["Parts"]
        all_parts = sorted(all_parts, key=lambda d: d["PartNumber"])
        response = s3_client.complete_multipart_upload(
            UploadId=upload_id,
            Bucket=bucket_name,
            Key=object_key,
            MultipartUpload={
                "Parts": [{"PartNumber": p["PartNumber"], "ETag": p["ETag"]} for p in all_parts]
            },
        )
        return response

    def abort_multipart_upload(self, bucket_name, object_key, upload_id, region=None):
        s3_client = self.get_s3_client(region)
        try:
            response = s3_client.abort_multipart_upload(
                Bucket=bucket_name, Key=object_key, UploadId=upload_id
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchUpload":
                return None

    def deploy_lambda(self, function_name, package_path, handler,
                      timeout=900, mem_size=1024, storage_size=2048, region=None):
        lambda_client = self.get_lambda_client(region)

        try:
            lambda_client.delete_function(FunctionName=function_name)
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceNotFoundException':
               return False
        try:
            with open(package_path, 'rb') as f:
                zipped_code = f.read()
        except FileNotFoundError:
            print(f"Error: Deployment package '{package_path}' not found.")
            return False
        credentials = read_credentials(self.is_cloud)

        try:
            response = lambda_client.create_function(
                FunctionName=function_name,
                Runtime='python3.10',
                Role=credentials['aws_role_arn'],
                Handler=handler,
                Code={'ZipFile': zipped_code},
                Publish=True,
                Timeout=timeout,
                MemorySize=mem_size,
                EphemeralStorage={
                    'Size': storage_size
                },
                Architectures=['x86_64']
            )
        except ClientError as e:
            return False

        return True

    def invoke_lambda_async(self, function_name, payload, region=None):
        lambda_client = self.get_lambda_client(region)
        response = lambda_client.invoke(
            FunctionName=function_name, InvocationType="Event", Payload=json.dumps(payload)
        )
        return 200 <= response["StatusCode"] < 300

    def invoke_lambda_sync(self, function_name, payload, region=None):
        lambda_client = self.get_lambda_client(region)
        response = lambda_client.invoke(
            FunctionName=function_name, InvocationType="RequestResponse", Payload=json.dumps(payload)
        )
        return response

    def create_dynamodb_table(self, table_name, key_schema, attribute_definitions, region=None):
        dynamodb_client = self.get_dynamodb_client(region)
        try:
            dynamodb_client.create_table(
                TableName=table_name,
                KeySchema=key_schema,
                AttributeDefinitions=attribute_definitions,
                BillingMode='PAY_PER_REQUEST'
            )

            waiter = dynamodb_client.get_waiter('table_exists')
            waiter.wait(TableName=table_name)
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceInUseException':
                return None
        table_description = dynamodb_client.describe_table(TableName=table_name)
        return table_description

    def put_dynamodb_entry(
        self, table, entry, attr_vals=None, attr_names=None, cond_expr=None, ret_val=None, region=None
    ):
        dynamodb_client = self.get_dynamodb_client(region)
        try:
            response = dynamodb_client.put_item(
                TableName=table,
                Item=entry,
                **(dict(ExpressionAttributeValues=attr_vals) if attr_vals else dict()),
                **(dict(ExpressionAttributeNames=attr_names)) if attr_names else dict(),
                **(dict(ConditionExpression=cond_expr) if cond_expr else dict()),
                **(dict(ReturnValues=ret_val) if ret_val else dict()),
            )
            return response
        except dynamodb_client.exceptions.ConditionalCheckFailedException:
            return None

    def read_dynamodb_entry(self, table, key, region=None, **kwargs):
        dynamodb_client = self.get_dynamodb_client(region)
        response = dynamodb_client.get_item(TableName=table, Key=key, **kwargs)
        item = response.get("Item")
        return item

    def update_dynamodb_entry(
            self, table, key, update_expr, attr_vals=None, cond_expr=None, ret_val=None, region=None
    ):
        dynamodb_client = self.get_dynamodb_client(region)
        try:
            response = dynamodb_client.update_item(
                TableName=table,
                Key=key,
                UpdateExpression=update_expr,
                **(dict(ExpressionAttributeValues=attr_vals) if attr_vals else dict()),
                **(dict(ConditionExpression=cond_expr) if cond_expr else dict()),
                ReturnValues=ret_val
            )
            return response
        except dynamodb_client.exceptions.ConditionalCheckFailedException:
            return None

    def delete_dynamodb_entry(self, table, key, attr_vals=None, cond_expr=None, ret_val=None, region=None):
        dynamodb_client = self.get_dynamodb_client(region)
        try:
            response = dynamodb_client.delete_item(
                TableName=table,
                Key=key,
                **(dict(ExpressionAttributeValues=attr_vals) if attr_vals else dict()),
                **(dict(ConditionExpression=cond_expr) if cond_expr else dict()),
                **(dict(ReturnValues=ret_val) if ret_val else dict()),
            )
            return response
        except dynamodb_client.exceptions.ConditionalCheckFailedException:
            return None

    def query_dynamodb(
            self, table, key_expr, expr_attr_vals=None, expr_attr_names=None, region=None, callback=None, **kwargs
    ) -> list[dict]:
        dynamodb_client = self.get_dynamodb_client(region)
        items = []
        last_evaluated_key = None

        while True:
            query_kwargs = {
                "TableName": table,
                "KeyConditionExpression": key_expr,
                **(dict(ExpressionAttributeValues=expr_attr_vals) if expr_attr_vals else dict()),
                **(dict(ExpressionAttributeNames=expr_attr_names) if expr_attr_names else dict()),
                **(dict(ExclusiveStartKey=last_evaluated_key) if last_evaluated_key else dict()),
                **kwargs,
            }
            response = dynamodb_client.query(**query_kwargs)
            if callback:
                callback(response)
            items.extend(response.get("Items", []))
            last_evaluated_key = response.get("LastEvaluatedKey", None)

            if not last_evaluated_key:
                break

        return items

    def bulk_delete_dynamodb_entries(self, table, keys, region=None):
        dynamodb_client = self.get_dynamodb_client(region)
        delete_requests = [{"DeleteRequest": {"Key": key}} for key in keys]

        for i in range(0, len(delete_requests), 25):
            dynamodb_client.batch_write_item(RequestItems={table: delete_requests[i: i + 25]})


    def get_changelog(self, bucket_name, object_key, etag, region=None):
        item = self.read_dynamodb_entry(
            table="changelog",
            key=ChangeLog.make_key(bucket_name, object_key, etag),
            region=region,
        )
        if item:
            return unmarshal_value(item["ChangeLog"])
        else:
            return None

    def update_changelog(self, bucket_name, object_key, etag, num_tasks, region=None):
        self.update_dynamodb_entry(
            table="changelog",
            key=ChangeLog.make_key(bucket_name, object_key, etag),
            update_expr="SET NumTasks = :val1",
            attr_vals={":val1": {"N": str(num_tasks)}},
            ret_val="UPDATED_NEW",
            region=region,
        )

    def delete_changelog(self, bucket_name, object_key, etag, region=None):
        self.delete_dynamodb_entry(
            table="changelog",
            key=ChangeLog.make_key(bucket_name, object_key, etag),
            region=region,
        )

    def lock_object(self, bucket_name: str, object_key: str, sequencer: int, region=None):
        lock = ObjectLock(bucket_name, object_key, sequencer, True)
        resp = self.put_dynamodb_entry(
            table=ObjectLock.TABLE_NAME,
            entry=lock.to_dynamodb_record(),
            cond_expr="(attribute_not_exists(BucketName) AND attribute_not_exists(ObjectKey))"
                        "OR (Sequencer < :val1)",
            attr_vals={":val1": {"N": str(sequencer)}},
            region=region,
            ret_val="ALL_OLD",
        )
        if resp is None:  # lock exists, with larger sequencer, do nothing
            logger.info(f"lock exists, with equal or larger sequencer (>{sequencer}), do nothing")
            return False
        elif "Attributes" not in resp:  # lock not exists
            logger.info(f"lock not exists, successfully locked object, sequencer={sequencer}")
            return True
        else:  # lock exists, with smaller sequencer, need to trigger phase 2
            old_lock = ObjectLock.from_dynamodb_record(resp["Attributes"])
            if not old_lock.locked:
                logger.info(f"lock exists, but not locked, locking it, sequencer={sequencer}")
                return True
            else:
                assert old_lock.sequencer < sequencer, "corrupted lock sequencer"
                logger.info(f"lock exists, with smaller sequencer ({old_lock.sequencer}), update its sequencer to {sequencer}")
                return False

    def update_lock(self, bucket_name: str, object_key: str, sequencer: int, region=None):
        lock = ObjectLock(bucket_name, object_key, sequencer, True)
        logger.info(f"update lock with sequencer={sequencer}")
        resp = self.put_dynamodb_entry(
            table=ObjectLock.TABLE_NAME,
            entry=lock.to_dynamodb_record(),
            cond_expr="Sequencer < :val1",
            attr_vals={":val1": {"N": str(sequencer)}},
            region=region,
            ret_val="ALL_OLD",
        )
        if resp is None:
            logger.info(f"lock update failed, the lock has been locked with a larger sequencer (>{sequencer})")
            return False
        else:
            old_lock = ObjectLock.from_dynamodb_record(resp["Attributes"])
            logger.info(f"lock updated, old_sequencer={old_lock.sequencer}, new_sequencer={sequencer}")

    def unlock_object(self, bucket_name: str, object_key: str, sequencer: int, region=None):
        logger.info(f"unlocking object {bucket_name}/{object_key}, sequencer={sequencer}")
        dynamodb_client = self.get_dynamodb_client(region)
        cond = True
        try:
            resp = dynamodb_client.update_item(
                TableName=ObjectLock.TABLE_NAME,
                Key=ObjectLock.make_key(bucket_name, object_key),
                UpdateExpression="SET Locked = :val1",
                ConditionExpression="Sequencer = :val2",
                ExpressionAttributeValues={":val1": {"BOOL": False}, ":val2": {"N": str(sequencer)}},
                ReturnValuesOnConditionCheckFailure="ALL_OLD",
                ReturnValues="ALL_OLD",
            )
        except dynamodb_client.exceptions.ConditionalCheckFailedException as e:
            cond = False
            resp = e.response
        if cond:
            logger.info(f"lock has not been updated, unlock object {bucket_name}/{object_key} successfully, sequencer={sequencer}")
        else:
            assert resp and "Item" in resp, "Lock does not exist"
            cur_lock = ObjectLock.from_dynamodb_record(resp["Item"])
            logger.info(f"lock has been updated, cur_sequencer={cur_lock.sequencer}, trigger unlock")
            assert cur_lock.sequencer > sequencer, "corrupted lock sequencer"
            unlock_payload = {
                "bucket": bucket_name,
                "key": object_key,
                "sequencer": cur_lock.sequencer,
            }
            logger.info(f"invoke s3_event_gateway_unlock with payload: {unlock_payload}")
            self.invoke_lambda_async("s3_event_gateway_unlock", unlock_payload, region)
