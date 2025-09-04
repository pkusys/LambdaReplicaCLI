import json
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
    chunk_size = event['chunk_size']
    num_parts = event['num_parts']
    upload_id = event['upload_id']

    start_time = time.time()
    obj_get = 0
    obj_put = 0
    db_put = 0
    tasks = 0

    save_path = '/tmp/' + key

    while True:
        response = aws_client.update_dynamodb_entry(
            table='multipart',
            key={'Key': {'S': f'{dst_region}-{dst_bucket}-{key}'}},
            update_expr="SET CurrentOffset = CurrentOffset + :val1",
            attr_vals={":val1": {"N": "1"}},
            ret_val="UPDATED_OLD",
        )
        part_num = unmarshal_value(response['Attributes']['CurrentOffset'])
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
            aws_client.update_dynamodb_entry(
                table='multipart',
                key={'Key': {'S': f'{dst_region}-{dst_bucket}-{key}'}},
                update_expr='SET Results = list_append(Results, :val1)',
                attr_vals={':val1': {"L": [{"S": json.dumps(ret_json)}]}},
                ret_val="UPDATED_NEW",
                region=current_region
            )
            return ret_json
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
