import boto3
import os

from django_s3_csv_2_sfdc.utils import get_temp

from pathlib import Path


def upload_file(
    local_path: Path, bucket, s3_key: Path = None, public_read: bool = False
):
    """Upload a file to an S3 bucket

    :param local_path: File to upload
    :param bucket: S3 Bucket to upload to
    :param s3_key: S3 object name. If not specified then local_path is used
    :param public_read: permissions
    """

    # If S3 s3_key was not specified, use local_path
    if s3_key is None:
        s3_key = local_path

    # S3 uses posix-like paths
    s3_key = s3_key.as_posix()
    # cast to string to get local filesystem's path
    local_path = str(local_path)

    s3_client = boto3.client("s3")
    if public_read:
        s3_client.upload_file(
            local_path, bucket, s3_key, ExtraArgs={"ACL": "public-read"}
        )
    else:
        s3_client.upload_file(local_path, bucket, s3_key)


def respond_to_s3_event(event, callback, *args, **kwargs):
    """
    Use like this:
        def process_s3_event(s3_object_key, bucket_name):
            print(s3_object_key, bucket_name)

        def handler(event, context):
            respond_to_s3_event(event, process_s3_event)
    """
    records = event["Records"]
    for record in records:
        s3_data = record["s3"]
        bucket = s3_data["bucket"]
        bucket_name = bucket["name"]
        s3_object = s3_data["object"]
        s3_object_key = s3_object["key"]
        callback(s3_object_key, bucket_name, *args, **kwargs)


def s3_to_temp(s3_object_key, bucket_name) -> Path:
    """
    Downloads a file from s3, dropping it in the temp directory
    following the pathing convention from the s3_object_key

        e.g., s3_object_key = archive/a_file.txt
        will drop it in

        %TEMP%/archive/a_file.txt

    TEMP must be defined in your django settings
    """
    tmp = get_temp()

    s3_client = boto3.client("s3")
    download_folder = tmp / os.path.dirname(s3_object_key)
    download_path = tmp / s3_object_key
    # spawn the nested folders without the os complaining
    Path(download_folder).mkdir(parents=True, exist_ok=True)
    s3_client.download_file(bucket_name, s3_object_key, str(download_path))

    return download_path