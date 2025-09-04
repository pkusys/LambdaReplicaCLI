import json
import math
import os
import time
from lambdareplica.integration import AWSClient, AzureClient, GcpClient
from lambdareplica.integration.aws_client import unmarshal_value

current_region = os.environ.get('AWS_REGION')
aws_client = AWSClient(region=current_region)
azure_client = AzureClient()
gcp_client = GcpClient()


def lambda_handler(event, context):
    src_cloud = event['src_cloud']
    src_region = event['src_region']
    src_bucket = event['src_bucket']
    key = event['key']
    dst_cloud = event['dst_cloud']
    dst_region = event['dst_region']
    dst_bucket = event['dst_bucket']
    parallelism = event['parallelism']
    chunk_size = event['chunk_size']
    size = event['size']

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
    worker_time = list()
    worker_tasks = list()

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
        aws_client.put_dynamodb_entry(table='multipart',
                                      entry={
                                          'Key': {'S': f'{dst_region}-{dst_bucket}-{key}'},
                                          'CurrentOffset': {'N': '0'},
                                          'UploadId': {'S': upload_id},
                                          'Results': {'L': list()}
                                      },
                                      region=current_region)
        db_put += 1
        num_parts = math.ceil(size / chunk_size)
        payload = {
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
        }
        for i in range(parallelism):
            aws_client.invoke_lambda_async(f'lr_worker_{current_region}',
                                           payload, current_region)
        fn_invoc += parallelism
        while True:
            item = aws_client.read_dynamodb_entry(table='multipart',
                                                  key={'Key': {'S': f'{dst_region}-{dst_bucket}-{key}'}},
                                                  region=current_region)
            results = unmarshal_value(item['Results'])
            if len(results) == parallelism:
                for result in results:
                    json_result = json.loads(result)
                    worker_time.append(json_result['time'])
                    worker_tasks.append(json_result['tasks'])
                    db_put += json_result['db_put']
                    obj_get['src'] += json_result['obj_get']
                    obj_put['dst'] += json_result['obj_put']
                break

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
        aws_client.delete_dynamodb_entry(table='multipart',
                                         key={'Key': {'S': f'{dst_region}-{dst_bucket}-{key}'}},
                                         region=current_region)
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
        "worker_tasks": worker_tasks
    }

    return ret_json
