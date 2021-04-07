import datetime

from django_s3_csv_2_sfdc.utils import get_timestamp_folder


def test_get_timestamp_folder():
    dt = datetime.datetime(year=2021, month=3, day=27)
    folder = get_timestamp_folder(dt)
    assert "2021/03/27" == folder.as_posix()