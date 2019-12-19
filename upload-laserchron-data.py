#!/usr/bin/env python
# https://gist.github.com/sladkovm/d12381f9e4a2a0f50a5e30f4aff3eef5

import boto3
import click
from pathlib import Path
from itertools import chain
from os import getenv
from dotenv import load_dotenv
from hashlib import md5
from click import echo, secho
from botocore.exceptions import ClientError

# https://blogs.msdn.microsoft.com/vsofficedeveloper/2008/05/08/office-2007-file-format-mime-types-for-http-content-streaming-2/

load_dotenv(verbose=True)

def md5hash(fname):
    """
    Compute the md5 hash of a file-like object
    """
    hash = md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash.update(chunk)
    return hash.hexdigest()

def get_content_type(f_name):
    if f_name.suffix == '.xls':
        return 'application/vnd.ms-excel'
    if f_name.suffix == '.xlsx':
        return 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    if f_name.suffix == '.xlsm':
        return 'application/vnd.ms-excel.sheet.macroEnabled.12'
    return 'binary/octet-stream'

def status(msg, **kwargs):
    secho("...", nl=False, dim=True)
    secho(msg, **kwargs)

_bucket = getenv('DO_BUCKET')
region = getenv('DO_SPACES_REGION')

client = boto3.client('s3',
    region_name=getenv('DO_SPACES_REGION'),
    endpoint_url=f"https://{region}.digitaloceanspaces.com",
    aws_access_key_id=getenv('DO_SPACES_KEY'),
    aws_secret_access_key=getenv('DO_SPACES_SECRET'))


class FolderImporter(object):
    def __init__(self, base_path, bucket=None, dry_run=False):
        self.base_path = Path(base_path)
        self.dry_run = dry_run

    def key_for_filename(self, f_name):
        """
        Get prefixed key for AWS storage service
        """
        return str(f_name.relative_to(self.base_path).as_posix())

    def get_head(self, fn):
        key = self.key_for_filename(fn)
        try:
            head = client.head_object(Bucket=_bucket, Key=key)
        except ClientError as err:
            return None
        return head

    def process_file(self, fn):
        # Try to get file head object
        s3key = self.key_for_filename(fn)
        echo(s3key)
        content_hash=md5hash(fn)
        # Unfortunately, we can only get buckets by their key.
        # We have to have another process to deduplicate data
        # We will probably build this into the Sparrow importer.
        head = self.get_head(fn)

        if head is None:
            self.upload_file(fn, content_hash)
            return

        if head['Metadata']['content-md5'] == content_hash:
            status("already exists", fg='green')
        else:
            status("exists with changes", fg='yellow')
            self.upload_file(fn, content_hash, is_overwrite=True)

    def upload_file(self, f_name, content_hash, is_overwrite=False):
        """Upload json file to the S3 bucket
        :param f_name: path to json file to upload

        See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Bucket.put_object
        """
        if self.dry_run:
            return

        with open(f_name, 'rb') as f:
            client.upload_fileobj(f,
                Bucket=_bucket,
                Key=self.key_for_filename(f_name),
                ExtraArgs=dict(
                    ContentType=get_content_type(f_name),
                    Metadata={'Content-MD5': content_hash}))
        if is_overwrite:
            status("updated", fg='cyan')
        else:
            status("created", fg='cyan')

    def process_files(self):
        if self.dry_run:
            echo("Starting dry run")

        EXTENSIONS = {'.xls', '.xlsx', '.xlsm'}

        all_files = self.base_path.glob("**/*.*")
        excel_files = (f for f in all_files if f.is_file()
                                and f.suffix in EXTENSIONS)

        for fn in excel_files:
            self.process_file(fn)

folder_arg = click.Path(file_okay=False, dir_okay=True, resolve_path=True, exists=True)

@click.command(name="import-laserchron")
@click.argument("paths", type=folder_arg, nargs=-1)
@click.option("--dry-run", is_flag=True, default=False)
def import_laserchron(paths, dry_run=False):
    if len(paths) == 0:
        paths = (getenv("LASERCHRON_UPLOADER_PATH"),)
    print(paths)
    for import_path in paths:
        FolderImporter(import_path, dry_run=dry_run).process_files()


if __name__ == '__main__':
    import_laserchron()
