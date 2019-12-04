#!/usr/bin/env python
# https://gist.github.com/sladkovm/d12381f9e4a2a0f50a5e30f4aff3eef5

from pathlib import Path
from itertools import chain
import boto3
import os
from dotenv import load_dotenv

load_dotenv(verbose=True)

data_path = Path(os.getenv("LASERCHRON_UPLOADER_PATH"))

file_list = chain(data_path.glob("**/*.xls"), data_path.glob("**/*.xls[xm]"))
region = os.getenv('DO_SPACES_REGION')

client = boto3.client('s3',
    region_name=os.getenv('DO_SPACES_REGION'),
    endpoint_url=f"https://{region}.digitaloceanspaces.com",
    aws_access_key_id=os.getenv('DO_SPACES_KEY'),
    aws_secret_access_key=os.getenv('DO_SPACES_SECRET'))

def upload_file(f_name, **kwargs):
    """Upload json file to the S3 bucket
    :parame client: boto3 client object pointing to s3
    :param f_name: path to json file to upload
    """
    with open(f_name, 'rb') as f:
        _bucket = kwargs.get('Bucket', os.getenv('DO_BUCKET'))
        _key = kwargs.get('Key', str(f_name.relative_to(data_path)))
        client.upload_fileobj(f, Bucket=_bucket, Key=_key)

for fn in file_list:
    print(fn.relative_to(data_path))
    upload_file(fn)
