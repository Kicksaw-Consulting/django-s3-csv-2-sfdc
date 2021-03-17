import os
from os import path
import pytest

from django_s3_csv_2_sfdc.utils import get_iso
from django_s3_csv_2_sfdc.s3_helpers import timestamp_s3_key


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
