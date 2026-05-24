import boto3
import pyarrow.fs as pafs
from botocore.config import Config
from feat_stream.config import settings

def client():
    return boto3.client('s3',
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name=settings.s3_region,
        config=Config(s3={'addressing_style': 'path'}),
    )

def fs():
    return pafs.S3FileSystem(
        access_key=settings.s3_access_key,
        secret_key=settings.s3_secret_key,
        region=settings.s3_region,
        endpoint_override=settings.s3_endpoint_url,
        scheme='http',
    )

def list_parquet_keys(bucket, prefix):
    c = client()
    keys = []
    token = None
    while True:
        kwargs = {'Bucket': bucket, 'Prefix': prefix}
        if token:
            kwargs['ContinuationToken'] = token
        resp = c.list_objects_v2(**kwargs)
        for o in resp.get('Contents', []):
            k = o['Key']
            if k.endswith('.parquet'):
                keys.append(k)
        if not resp.get('IsTruncated'):
            break
        token = resp.get('NextContinuationToken')
    return keys
