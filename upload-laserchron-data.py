#!/usr/bin/env python
# https://gist.github.com/sladkovm/d12381f9e4a2a0f50a5e30f4aff3eef5

import boto3
from pathlib import Path
from itertools import chain
from os import getenv
from dotenv import load_dotenv

load_dotenv(verbose=True)

data_path = Path(getenv("LASERCHRON_UPLOADER_PATH"))

file_list = chain(data_path.glob("**/*.xls"), data_path.glob("**/*.xls[xm]"))
region = getenv('DO_SPACES_REGION')

client = boto3.client('s3',
    region_name=getenv('DO_SPACES_REGION'),
    endpoint_url=f"https://{region}.digitaloceanspaces.com",
    aws_access_key_id=getenv('DO_SPACES_KEY'),
    aws_secret_access_key=getenv('DO_SPACES_SECRET'))

def upload_file(f_name, **kwargs):
    """Upload json file to the S3 bucket
    :param f_name: path to json file to upload
    """
    with open(f_name, 'rb') as f:
        _bucket = kwargs.get('Bucket', getenv('DO_BUCKET'))
        _key = kwargs.get('Key', str(f_name.relative_to(data_path)))
        client.upload_fileobj(f, Bucket=_bucket, Key=_key)

for fn in file_list:
    print(fn.relative_to(data_path))
    upload_file(fn)
