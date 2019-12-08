import boto3

s3 = boto3.client('s3')


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
