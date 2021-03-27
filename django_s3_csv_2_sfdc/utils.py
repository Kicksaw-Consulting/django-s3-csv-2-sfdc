import datetime

from django.conf import settings
from pathlib import Path


def get_temp() -> Path:
    assert hasattr(
        settings, "TEMP"
    ), "TEMP must be defined in your django settings; it should be a path to some folder on your local machine"
    return Path(settings.TEMP)


def get_iso() -> str:
    """
    Get an iso timestamp with less precision than .iso() returns, and no special characters
    """
    return datetime.datetime.now().strftime("%Y%m%dT%H%M")


def get_timestamp_folder(datetime_object: datetime.datetime = None) -> Path:
    """
    Returns a Path object that's a timestamped folder path

    e.g., ts_folder = 2021/03/27
        so you can do Path("some/path") / ts_folder / <your_file>.txt

    Defaults to datetime.datetime.now() if nothing's passed in
    """
    if not datetime_object:
        datetime_object = datetime.datetime.now()

    year = datetime_object.strftime("%Y")
    month = datetime_object.strftime("%m")
    day = datetime_object.strftime("%d")

    return Path(year) / month / day
