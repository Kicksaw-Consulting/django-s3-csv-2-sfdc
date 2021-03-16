from django.conf import settings
from pathlib import Path


def get_temp() -> Path:
    assert hasattr(
        settings, "TEMP"
    ), "TEMP must be defined in your django settings; it should be a path to some folder on your local machine"
    return Path(settings.TEMP)
