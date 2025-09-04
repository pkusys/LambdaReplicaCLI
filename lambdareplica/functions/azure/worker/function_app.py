import os
import azure.functions as func
from lambdareplica.integration.azure_client import AzureClient
from lambdareplica.integration.aws_client import AWSClient
from lambdareplica.integration.gcp_client import GcpClient
import time
import json

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

current_region = os.environ.get("REGION")
aws_client = AWSClient()
azure_client = AzureClient()
gcp_client = GcpClient()


@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    start_time = time.time()
    obj_get = 0
    obj_put = 0
    db_put = 0
    tasks = 0
    src_cloud = req.params.get('src_cloud')
    if not src_cloud:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            src_cloud = req_body.get('src_cloud')
    src_region = req.params.get('src_region')
    if not src_region:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            src_region = req_body.get('src_region')
    src_bucket = req.params.get('src_bucket')
    if not src_bucket:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            src_bucket = req_body.get('src_bucket')
    key = req.params.get('key')
    if not key:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            key = req_body.get('key')
    dst_cloud = req.params.get('dst_cloud')
    if not dst_cloud:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            dst_cloud = req_body.get('dst_cloud')
    dst_region = req.params.get('dst_region')
    if not dst_region:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            dst_region = req_body.get('dst_region')
    dst_bucket = req.params.get('dst_bucket')
    if not dst_bucket:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            dst_bucket = req_body.get('dst_bucket')
    chunk_size = req.params.get('chunk_size')
    if not chunk_size:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            chunk_size = req_body.get('chunk_size')
    num_parts = req.params.get('num_parts')
    if not num_parts:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            num_parts = req_body.get('num_parts')

    save_path = '/tmp/' + key

    while True:
        response = azure_client.patch_item(database_id='LambdaReplica', container_id='multipart',
                                           key=f'{dst_region}-{dst_bucket}-{key}',
                                           operations=[{"op": "incr", "path": "/current_offset", "value": 1}],
                                           account=f'lambda-cosmos-{current_region}')
        part_num = response["current_offset"] - 1
        db_put += 1
        if part_num >= num_parts:
            total_time = time.time() - start_time
            ret_json = {
                "time": total_time,
                "tasks": tasks,
                "db_put": db_put,
                "obj_get": obj_get,
                "obj_put": obj_put
            }
            return func.HttpResponse(json.dumps(ret_json))
        upload_id = response["upload_id"]
        if src_cloud == 'AWS':
            aws_client.download_object(bucket_name=src_bucket, object_key=key, save_path=save_path,
                                       offset=part_num * chunk_size, size=chunk_size,
                                       region=src_region)
            obj_get += 1
        elif src_cloud == 'Azure':
            azure_client.download_object(container_name=src_bucket, object_key=key, save_path=save_path,
                                         offset=part_num * chunk_size, size=chunk_size, max_concurrency=8,
                                         account=src_region)
            obj_get += 1
        else:
            gcp_client.download_object(bucket_name=src_bucket, object_key=key, save_path=save_path,
                                       offset=part_num * chunk_size, size=chunk_size, region=src_region)
            obj_get += 1
        if dst_cloud == 'AWS':
            aws_client.multipart_upload(bucket_name=dst_bucket, object_key=key, save_path=save_path,
                                        upload_id=upload_id, part_num=part_num+1, region=dst_region)
            obj_put += 1
        elif dst_cloud == 'Azure':
            azure_client.multipart_upload(container_name=dst_bucket, object_key=key, save_path=save_path,
                                          part_num=part_num, account=dst_region)
            obj_put += 1
        else:
            gcp_client.multipart_upload(bucket_name=dst_bucket, object_key=key, save_path=save_path,
                                        upload_id=upload_id, part_num=part_num+1, region=dst_region)
            obj_put += 1
        os.remove(save_path)
        tasks += 1
