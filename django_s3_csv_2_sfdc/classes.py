import datetime
import tempfile

from django.conf import settings
from pathlib import Path
from simple_salesforce import Salesforce

from django_s3_csv_2_sfdc.s3_helpers import upload_file


class SfClient(Salesforce):
    def __init__(self):
        config = {
            "username": settings.SFDC_USERNAME,
            "password": settings.SFDC_PASSWORD,
            "security_token": settings.SFDC_SECURITY_TOKEN,
        }
        if settings.SFDC_DOMAIN.lower() != "na":
            config["domain"] = settings.SFDC_DOMAIN

        super().__init__(**config)


class S3ReportManager:
    def __init__(self, file_name: str, subfolder_name: str = None) -> None:
        self.bucket = settings.S3_BUCKET
        self.subfolder_name = subfolder_name if subfolder_name else ""
        self.file_name = Path(file_name)

        # Timestamps
        self.start_time = datetime.now().isoformat()
        # used for memo. is None until it's used.
        self.archive_time = None

    @property
    def folder_archive(self) -> Path:
        return self._subfolder_suffixed("archive")

    @property
    def path_completed(self) -> Path:
        return self._archive_named("completed.csv")

    @property
    def path_errors(self) -> Path:
        return self._archive_named("errors.csv")

    def as_sfdc_report(self, start_time: datetime, errors_count: int) -> dict:
        """
        The hash sent to Salesforce that records this execution
        """
        return {
            "Time_Started__c": self.start_time,
            "Time_Ended__c": self._archive_time,
            "Completed_Path__c": self.path_completed,
            "Errors_Path__c": self.path_errors,
            "Errors_Count__c": errors_count,
            "subfolder_name__c": self.subfolder_name,
        }

    def _root_path(self, path):
        return Path(tempfile.gettempdir()) / path

    @property
    def _archive_time(self) -> str:
        """
        Cached like this so timestamp in filename and timestamp
        in SFDC report object share values
        """
        if not self.archive_time:
            self.archive_time = datetime.now()
        return self.archive_time.isoformat()

    def _archive_named(self, suffix: str):
        iso = self._archive_time
        extension = self.file_name.suffix
        stem = self.file_name.stem
        return self.folder_archive / Path(f"{stem}-{iso}-{suffix}{extension}")

    def _subfolder_suffixed(self, path: str):
        return Path(path) / Path(self.subfolder_name)

    def upload_completed(self, local_path: Path):
        upload_file(local_path, self.bucket, self.path_completed)

    def upload_errors(self, local_path: Path):
        upload_file(local_path, self.bucket, self.path_errors, public_read=True)