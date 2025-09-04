import os

import functions_framework
from lambdareplica.integration.azure_client import AzureClient
from lambdareplica.integration.aws_client import AWSClient
from lambdareplica.integration.gcp_client import GcpClient
from google.cloud import firestore
import time
import json


current_region = os.environ['REGION']
aws_client = AWSClient()
azure_client = AzureClient()
gcp_client = GcpClient()


@functions_framework.http
def handle_http(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    start_time = time.time()
    obj_get = 0
    obj_put = 0
    db_put = 0
    tasks = 0
    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json and 'src_cloud' in request_json:
        src_cloud = request_json['src_cloud']
    elif request_args and 'src_cloud' in request_args:
        src_cloud = request_args['src_cloud']
    if request_json and 'src_region' in request_json:
        src_region = request_json['src_region']
    elif request_args and 'src_region' in request_args:
        src_region = request_args['src_region']
    if request_json and 'src_bucket' in request_json:
        src_bucket = request_json['src_bucket']
    elif request_args and 'src_bucket' in request_args:
        src_bucket = request_args['src_bucket']
    if request_json and 'key' in request_json:
        key = request_json['key']
    elif request_args and 'key' in request_args:
        key = request_args['key']
    if request_json and 'dst_cloud' in request_json:
        dst_cloud = request_json['dst_cloud']
    elif request_args and 'dst_cloud' in request_args:
        dst_cloud = request_args['dst_cloud']
    if request_json and 'dst_region' in request_json:
        dst_region = request_json['dst_region']
    elif request_args and 'dst_region' in request_args:
        dst_region = request_args['dst_region']
    if request_json and 'dst_bucket' in request_json:
        dst_bucket = request_json['dst_bucket']
    elif request_args and 'dst_bucket' in request_args:
        dst_bucket = request_args['dst_bucket']
    if request_json and 'chunk_size' in request_json:
        chunk_size = request_json['chunk_size']
    elif request_args and 'chunk_size' in request_args:
        chunk_size = request_args['chunk_size']
    if request_json and 'num_parts' in request_json:
        num_parts = request_json['num_parts']
    elif request_args and 'num_parts' in request_args:
        num_parts = request_args['num_parts']
    if request_json and 'upload_id' in request_json:
        upload_id = request_json['upload_id']
    elif request_args and 'upload_id' in request_args:
        upload_id = request_args['upload_id']

    save_path = '/tmp/' + key

    while True:
        response = gcp_client.update_item(collection_name=f'multipart',
                                          document_id=f'{dst_region}-{dst_bucket}-{key}',
                                          updates={"current_offset": firestore.Increment(1)},
                                          database=f'lambdareplica-{current_region}')
        part_num = response.transform_results[0].integer_value - 1
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
            return json.dumps(ret_json)
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
