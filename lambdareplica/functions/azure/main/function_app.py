import os
import aiohttp
import asyncio
import azure.functions as func
from lambdareplica.integration.azure_client import AzureClient
from lambdareplica.integration.aws_client import AWSClient
from lambdareplica.integration.gcp_client import GcpClient
import time
import json
from aiohttp import ClientTimeout
import math

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

current_region = os.environ.get("REGION")
aws_client = AWSClient()
azure_client = AzureClient()
gcp_client = GcpClient()


async def fetch(session, url, header, body):
    async with session.post(url, headers=header, json=body) as response:
        return await response.text()


async def fetch_all(jobs):
    timeout = ClientTimeout(total=600)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = []
        for job in jobs:
            task = asyncio.ensure_future(fetch(session, job['url'], job['header'], job['body']))
            tasks.append(task)
        responses = await asyncio.gather(*tasks)
        return responses


@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    print(current_region)
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
    parallelism = req.params.get('parallelism')
    if not parallelism:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            parallelism = req_body.get('parallelism')
    chunk_size = req.params.get('chunk_size')
    if not chunk_size:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            chunk_size = req_body.get('chunk_size')
    size = req.params.get('size')
    if not size:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            size = req_body.get('size')

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
        azure_client.create_item(database_id='LambdaReplica', container_id='multipart',
                                 item={'id': f'{dst_region}-{dst_bucket}-{key}',
                                       'current_offset': 0,
                                       'upload_id': upload_id}, account=f'lambda-cosmos-{current_region}')
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
            },
            'url': f'https://lambdaworkerfuncapp{current_region}.azurewebsites.net/api/http_trigger?'
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

        azure_client.delete_item(database_id='LambdaReplica', container_id='multipart',
                                 key=f'{dst_region}-{dst_bucket}-{key}', account=f'lambda-cosmos-{current_region}')
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

    return func.HttpResponse(json.dumps(ret_json))
