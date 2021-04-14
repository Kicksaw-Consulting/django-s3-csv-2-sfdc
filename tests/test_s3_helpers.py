import os
import pytest

from django_s3_csv_2_sfdc.utils import get_iso
from django_s3_csv_2_sfdc.s3_helpers import timestamp_s3_key, respond_to_s3_event


@pytest.mark.parametrize(
    "s3_key,keep_folder,expected",
    [
        ("origin/a_file.csv", False, f"a_file-{get_iso()}.csv"),
        ("origin/a_file.csv", True, os.path.join("origin", f"a_file-{get_iso()}.csv")),
    ],
)
def test_timestamp_s3_key(s3_key, keep_folder, expected):
    timestamped_s3_key = timestamp_s3_key(s3_key, keep_folder)
    assert timestamped_s3_key == expected


@pytest.mark.parametrize(
    "s3_key,expected_s3_key,bucket,expected_bucket",
    [
        ("origin/a_file.csv", "origin/a_file.csv", "bucket", "bucket"),
        (
            "origin/a_%7Bfile%7D.csv",
            r"origin/a_{file}.csv",
            "%7Bbucket%7D",
            r"{bucket}",
        ),
        ("Im+a+problem.csv", "Im a problem.csv", "bucket", "bucket"),
    ],
)
def test_respond_to_s3_event(s3_key, expected_s3_key, bucket, expected_bucket):
    def process(s3_object_key, bucket_name):
        assert s3_object_key == expected_s3_key
        assert bucket_name == expected_bucket

    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": bucket},
                    "object": {"key": s3_key},
                },
            }
        ]
    }
    respond_to_s3_event(event, process)