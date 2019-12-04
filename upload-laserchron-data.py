#!/usr/bin/env python
# https://gist.github.com/sladkovm/d12381f9e4a2a0f50a5e30f4aff3eef5

import boto3
from pathlib import Path
from itertools import chain
from os import getenv
from dotenv import load_dotenv
from hashlib import md5
from click import echo, secho
from botocore.exceptions import ClientError

# https://blogs.msdn.microsoft.com/vsofficedeveloper/2008/05/08/office-2007-file-format-mime-types-for-http-content-streaming-2/

def md5hash(fname):
    """
    Compute the md5 hash of a file-like object
    """
    hash = md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash.update(chunk)
    return hash.hexdigest()

def key_from_filename(f_name):
    return str(f_name.relative_to(data_path))

def get_content_type(f_name):
    if f_name.suffix == '.xls':
        return 'application/vnd.ms-excel'
    if f_name.suffix == '.xlsx':
        return 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    if f_name.suffix == '.xlsm':
        return 'application/vnd.ms-excel.sheet.macroEnabled.12'
    return 'binary/octet-stream'

load_dotenv(verbose=True)

region = getenv('DO_SPACES_REGION')
_bucket = getenv('DO_BUCKET')
data_path = Path(getenv("LASERCHRON_UPLOADER_PATH"))

client = boto3.client('s3',
    region_name=getenv('DO_SPACES_REGION'),
    endpoint_url=f"https://{region}.digitaloceanspaces.com",
    aws_access_key_id=getenv('DO_SPACES_KEY'),
    aws_secret_access_key=getenv('DO_SPACES_SECRET'))

def upload_file(f_name, content_hash):
    """Upload json file to the S3 bucket
    :param f_name: path to json file to upload

    See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Bucket.put_object
    """
    with open(f_name, 'rb') as f:
        client.upload_fileobj(f,
            Bucket=_bucket,
            Key=key_from_filename(f_name),
            ExtraArgs=dict(
                ContentType=get_content_type(f_name),
                Metadata={'Content-MD5': content_hash}))


file_list = chain(data_path.glob("**/*.xls[xm]"), data_path.glob("**/*.xls"))

def status(msg, **kwargs):
    secho("...", nl=False, dim=True)
    secho(msg, **kwargs)

for fn in file_list:
    # Try to get file head object
    echo(key_from_filename(fn))
    content_hash=md5hash(fn)
    try:
        head = client.head_object(Bucket=_bucket, Key=key_from_filename(fn))
        # Check if the file has changes
        assert head['Metadata']['content-md5'] == content_hash
        status("aready exists", fg='green')
    except ClientError:
        upload_file(fn, content_hash)
        status("created", fg='cyan')
    except AssertionError:
        status("exists with changes", fg='yellow')
        upload_file(fn, content_hash)
        status("overwritten with new version", fg='cyan')
