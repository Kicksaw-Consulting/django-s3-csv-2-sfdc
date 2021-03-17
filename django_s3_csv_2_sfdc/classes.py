from pathlib import Path

from django.conf import settings
from simple_salesforce import Salesforce

from django_s3_csv_2_sfdc.csv_helpers import create_error_report
from django_s3_csv_2_sfdc.s3_helpers import (
    s3_to_temp,
    upload_file,
    timestamp_s3_key,
)
from django_s3_csv_2_sfdc.sfdc_helpers import parse_bulk_upsert_results


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


class Orchestrator:
    """
    This class can be used to orchestrate the following flow

    __init__
    1. S3 event triggerred
    2. File is downloaded

    (developer must implement this code themselves)
    3. File is serialized into whatever the business requirements are (abstract step)
    4. serialized data is pushed to Salesforce (abstract step)
        developer calls log_batch after every push

    automagically_finish_up
    5. results of the push are parsed for errors
    6. an error report csv is created
    7. an archive of the original file and error report are pushed to S3
    8. a custom SFDC object is created, logging all of the above

    If you don't need/need to change something, subclass it!
    """

    def __init__(
        self,
        s3_object_key,
        bucket_name,
        sf_client: SfClient = None,
        archive_folder: str = None,
        error_folder: str = None,
        execution_object_name: str = None,
    ) -> None:
        self.s3_object_key = s3_object_key
        self.bucket_name = bucket_name

        self.archive_folder = archive_folder
        self.error_folder = error_folder

        self.execution_object_name = execution_object_name

        self.downloaded_file = s3_to_temp(s3_object_key, bucket_name)
        self.sf_client = sf_client

        self.error_report_path: str = None
        self.error_count: int = None

        self.batches = list()

    def set_sf_client(self, sf_client: SfClient):
        self.sf_client = sf_client

    def log_batch(
        self, results: list, data: list, salesforce_object: str, upsert_key: str
    ):
        """
        The intention here is to call this method after making a bulk upsert

        Parameters:
            results: The results from the Salesforce API
            data: The data you pushed
            salesforce_object: The name of the object you upserted to
            upsert_key: The upsert key you used
        """
        self.batches.append((results, data, salesforce_object, upsert_key))

    def automagically_finish_up(self):
        self.generate_error_report()
        self.report()

    def generate_error_report(self):
        error_groups = list()
        for batch in self.batches:
            _, errors = parse_bulk_upsert_results(*batch)
            error_groups.append(errors)

        error_report_path, error_count = create_error_report(error_groups)

        self.error_report_path = error_report_path
        self.error_count = error_count

    def report(self):
        self.upload_archive_file()
        self.upload_error_report()
        self.create_execution_object()

    def upload_archive_file(self):
        upload_file(self.downloaded_file, self.bucket_name, self.archive_file_s3_key)

    def upload_error_report(self):
        assert self.error_report_path, f"error_report_path is not set"
        upload_file(self.error_report_path, self.bucket_name, self.error_file_s3_key)

    @property
    def archive_file_s3_key(self):
        s3_object_key = self.s3_object_key
        archive_folder = self.archive_folder if self.archive_folder else "archive"
        archive_s3_key = timestamp_s3_key(s3_object_key)
        return Path(archive_folder) / archive_s3_key

    @property
    def error_file_s3_key(self):
        error_folder = self.error_folder if self.error_folder else "errors"
        error_report_s3_key = timestamp_s3_key("error-report.csv")
        return Path(error_folder) / error_report_s3_key

    def create_execution_object(self):
        assert self.sf_client, f"sf_client isn't set"
        getattr(self.sf_client, self.execution_object_name).create(
            self.execution_sfdc_hash
        )

    @property
    def execution_sfdc_hash(self):
        """
        The data to record in salesforce for this s3 event

        This function should return something like this

        return {
            "Origin_Path__c": self.self.s3_object_key,
            "Archive_Path__c": self.archive_file_s3_key,
            "Errors_Path__c": self.error_file_s3_key,
            "Errors_Count__c": self.error_count,
        }
        """
        raise NotImplementedError
