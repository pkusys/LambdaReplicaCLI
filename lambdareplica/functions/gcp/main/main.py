import math
import os

import functions_framework
from lambdareplica.integration.azure_client import AzureClient
from lambdareplica.integration.aws_client import AWSClient
from lambdareplica.integration.gcp_client import GcpClient, read_credentials
import time
import json
import aiohttp
import asyncio
from aiohttp import ClientTimeout

current_region = os.environ['REGION']
aws_client = AWSClient()
azure_client = AzureClient()
gcp_client = GcpClient()

credentials = read_credentials(True)
with open(credentials['gcp_credentials_path'], 'r') as f:
    gcp_credentials = json.load(f)
    project_id = gcp_credentials['project_id']

async def fetch(url, header, body):
    timeout = ClientTimeout(total=600)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(url, headers=header, json=body) as response:
            return await response.text()


async def fetch_all(jobs):
    tasks = []
    for job in jobs:
        task = asyncio.ensure_future(fetch(job['url'], job['header'], job['body']))
        tasks.append(task)
    responses = await asyncio.gather(*tasks)
    return responses


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
    obj_get = {
        'src': 0,
        'dst': 0
    }
    obj_put = {
        'src': 0,
        'dst': 0
    }
    db_put = 0
    db_get = 0
    fn_invoc = 1
    err = ''
    worker_time = list()
    worker_tasks = list()

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
    if request_json and 'parallelism' in request_json:
        parallelism = request_json['parallelism']
    elif request_args and 'parallelism' in request_args:
        parallelism = request_args['parallelism']
    if request_json and 'chunk_size' in request_json:
        chunk_size = request_json['chunk_size']
    elif request_args and 'chunk_size' in request_args:
        chunk_size = request_args['chunk_size']
    if request_json and 'size' in request_json:
        size = request_json['size']
    elif request_args and 'size' in request_args:
        size = request_args['size']

    if parallelism == 1:
        # process locally
        save_path = '/tmp/' + key
        if src_cloud == 'AWS':
            download_response = aws_client.download_object(src_bucket, key, save_path, region=src_region)
            mime_type = download_response['ContentType']
            obj_get['src'] += 1
        elif src_cloud == 'Azure':
            azure_client.download_object(container_name=src_bucket, object_key=key, save_path=save_path,
                                         max_concurrency=8,
                                         account=src_region)
            metadata = azure_client.get_object_metadata(container_name=src_bucket, object_key=key, account=src_region)
            mime_type = metadata.content_settings.content_type
            obj_get['src'] += 2
        else:
            gcp_client.download_object(bucket_name=src_bucket, object_key=key, save_path=save_path, region=src_region)
            metadata = gcp_client.get_object_metadata(bucket_name=src_bucket, object_key=key, region=src_region)
            mime_type = metadata.content_type
            obj_get['src'] += 2
        if dst_cloud == 'AWS':
            aws_client.upload_object(dst_bucket, key, save_path, mime_type=mime_type, region=dst_region)
            obj_put['dst'] += 1
        elif dst_cloud == 'Azure':
            azure_client.upload_object(container_name=dst_bucket, object_key=key, save_path=save_path,
                                       account=dst_region,
                                       max_concurrency=8)
            obj_put['dst'] += 1
        else:
            gcp_client.upload_object(bucket_name=dst_bucket, object_key=key, save_path=save_path, region=src_region)
            obj_put['dst'] += 1
    else:
        if src_cloud == 'AWS':
            metadata = aws_client.get_object_metadata(bucket_name=src_bucket, object_key=key, region=src_region)
            mime_type = metadata['ContentType']
            obj_get['src'] += 1
        elif src_cloud == 'Azure':
            metadata = azure_client.get_object_metadata(container_name=src_bucket, object_key=key, account=src_region)
            mime_type = metadata.content_settings.content_type
            obj_get['src'] += 1
        else:
            metadata = gcp_client.get_object_metadata(bucket_name=src_bucket, object_key=key, region=src_region)
            mime_type = metadata.content_type
            obj_get['src'] += 1
        # process with additional functions
        if dst_cloud == "AWS":
            upload_id = aws_client.init_multipart_upload(bucket_name=dst_bucket, object_key=key,
                                                         mime_type=mime_type, region=dst_region)
            obj_put['dst'] += 1
        elif dst_cloud == "Azure":
            upload_id = ""
        else:
            upload_id = gcp_client.init_multipart_upload(bucket_name=dst_bucket, object_key=key,
                                                         mime_type=mime_type, region=dst_region)
            obj_put['dst'] += 1
        gcp_client.put_item(collection_name=f'multipart', document_id=f'{dst_region}-{dst_bucket}-{key}',
                            item={'current_offset': 0, 'upload_id': upload_id}, database=f'lambdareplica-{current_region}')
        db_put += 1
        num_parts = math.ceil(size / chunk_size)
        job_json = {
            'header': {'Content-Type': 'application/json'},
            'body': {
                "src_cloud": src_cloud,
                "src_region": src_region,
                "src_bucket": src_bucket,
                "key": key,
                "dst_cloud": dst_cloud,
                "dst_region": dst_region,
                "dst_bucket": dst_bucket,
                "chunk_size": chunk_size,
                "num_parts": num_parts,
                "upload_id": upload_id
            },
            'url': f'https://{current_region}-{project_id}.cloudfunctions.net/lambda-worker-{current_region}'
        }
        jobs = [job_json] * parallelism
        text_responses = asyncio.run(fetch_all(jobs))
        fn_invoc += parallelism
        for text_response in text_responses:
            try:
                response = json.loads(text_response)
            except ValueError:
                err += text_response
                continue
            else:
                worker_time.append(response['time'])
                worker_tasks.append(response['tasks'])
                db_put += response['db_put']
                obj_get['src'] += response['obj_get']
                obj_put['dst'] += response['obj_put']

        if dst_cloud == "AWS":
            aws_client.complete_multipart_upload(bucket_name=dst_bucket, object_key=key,
                                                 upload_id=upload_id, region=dst_region)
            obj_put['dst'] += 1
            obj_get['dst'] += math.ceil(parallelism / 100)
        elif dst_cloud == "Azure":
            azure_client.complete_multipart_upload(container_name=dst_bucket, object_key=key, num_parts=num_parts,
                                                   mime_type=mime_type, account=dst_region)
            obj_put['dst'] += 1
        else:
            gcp_client.complete_multipart_upload(bucket_name=dst_bucket, object_key=key, upload_id=upload_id,
                                                 region=dst_region)
            obj_put['dst'] += 1
            obj_get['dst'] += math.ceil(parallelism / 1000)
        gcp_client.delete_item(collection_name=f'multipart',
                               document_id=f'{dst_region}-{dst_bucket}-{key}',
                               database=f'lambdareplica-{current_region}')
        db_put += 1

    total_time = time.time() - start_time
    ret_json = {
        "time": total_time,
        "obj_get": obj_get,
        "obj_put": obj_put,
        "db_put": db_put,
        "db_get": db_get,
        "fn_invoc": fn_invoc,
        "worker_time": worker_time,
        "worker_tasks": worker_tasks,
        "err": err
    }

    return json.dumps(ret_json)
