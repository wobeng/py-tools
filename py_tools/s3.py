import os
from collections import OrderedDict
from operator import itemgetter
from uuid import uuid4

import boto3

from py_tools.format import loads

s3 = boto3.client('s3')


def get_object(bucket, key, **kwargs):
    response = s3.get_object(Bucket=bucket, Key=key, **kwargs)
    response = response['Body'].read().decode('utf-8')
    return response


def get_object_head(bucket, key):
    try:
        response = s3.head_object(Bucket=bucket, Key=key)
        response['Key'] = key
        response['name'] = response['Key'].split('/')[-1]
        response['ext'] = response['name'].split('.')[-1]
        return response
    except BaseException:
        pass


def list_folder(bucket, prefix):
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [f['Key'] for f in resp['Contents']] if 'Contents' in resp else []


def delete_files(bucket, prefix):
    keys = list_folder(bucket, prefix)
    if keys:
        s3.delete_objects(
            Bucket=bucket,
            Delete=dict(Objects=[dict(Key=f) for f in keys], Quiet=True)
        )


def generate_url(bucket, key, media_size):
    conditions = list()
    conditions.append({'key': key})
    conditions.append(['content-length-range', 1, media_size])
    conditions.append(['starts-with', '$Content-Type', ''])
    conditions.append({'success_action_status': '201'})

    s3_url = s3.generate_presigned_post(
        bucket,
        key,
        Fields=None,
        Conditions=conditions,
        ExpiresIn=3600
    )
    s3_url['fields']['key'] = key
    s3_url['fields']['Content-Type'] = ''
    s3_url['fields']['success_action_status'] = 201

    return s3_url


def create_presigned_url(bucket_name, object_name, expiration=3600):
    response = s3.generate_presigned_url('get_object',
                                         Params={'Bucket': bucket_name, 'Key': object_name},
                                         ExpiresIn=expiration
                                         )
    return response


def get_json_object(bucket, key, sort=False, **kwargs):
    response = get_object(bucket, key, **kwargs)
    response = loads(response)
    if sort:
        response = OrderedDict(sorted(response.items(), key=itemgetter(1)))
    return response


def download_object(bucket, key, save_dir, rename=False):
    filename = key.split('/')[-1]
    if rename:
        filename = str(uuid4()) + '.' + filename.split('.')[-1]
    output = os.path.join(save_dir, filename)
    s3.download_file(bucket, key, output)
    return output


def upload_file(file_name, bucket, object_name=None):
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)
    s3.upload_file(file_name, bucket, object_name)
