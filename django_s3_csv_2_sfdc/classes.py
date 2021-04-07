from pathlib import Path

from django.conf import settings

from simple_salesforce import Salesforce
from simple_salesforce.bulk import (
    SFBulkHandler as BaseSFBulkHandler,
    SFBulkType as BaseSFBulkType,
)
from simple_salesforce.exceptions import SalesforceMalformedRequest

from django_s3_csv_2_sfdc.csv_helpers import create_error_report
from django_s3_csv_2_sfdc.s3_helpers import (
    download_file,
    upload_file,
    timestamp_s3_key,
    move_file,
)
from django_s3_csv_2_sfdc.sfdc_helpers import parse_bulk_upsert_results
from django_s3_csv_2_sfdc.utils import get_iso


class SFBulkType(BaseSFBulkType):
    def _bulk_operation(
        self,
        *args,
        batch_size=10000,
        **kwargs,
    ):
        try:
            return super()._bulk_operation(
                *args,
                batch_size=batch_size,
                **kwargs,
            )
        except SalesforceMalformedRequest as exception:
            if "Exceeded max size limit" in str(exception):
                new_batch_size = batch_size - 1000
                assert new_batch_size > 0, "Batch Size Too Low!"
                print(
                    f"Payload too large. Retrying with a lower batch size. {batch_size} -> {new_batch_size}"
                )
                return super()._bulk_operation(
                    *args,
                    batch_size=new_batch_size,
                    **kwargs,
                )
            raise exception


class SFBulkHandler(BaseSFBulkHandler):
    def __getattr__(self, name):
        return SFBulkType(
            object_name=name,
            bulk_url=self.bulk_url,
            headers=self.headers,
            session=self.session,
        )


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

    def __getattr__(self, name):
        if name == "bulk":
            # Deal with bulk API functions
            return SFBulkHandler(
                self.session_id, self.bulk_url, self.proxies, self.session
            )
        return super().__getattr__(name)


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

        self.download_s3_file()
        self.sf_client = sf_client

        self.error_report_path: str = None
        self.error_count: int = None

        self.batches = list()

        self.timestamp = None

    def download_s3_file(self):
        self.downloaded_file = download_file(self.s3_object_key, self.bucket_name)

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
        self.batches.append((results, list(data), salesforce_object, upsert_key))

    def automagically_finish_up(self):
        self.generate_error_report()
        self.report()

    def parse_sfdc_results(self, *args):
        return parse_bulk_upsert_results(*args)

    def get_error_groups(self):
        error_groups = list()
        for batch in self.batches:
            _, errors = self.parse_sfdc_results(*batch)
            error_groups.append(errors)
        return error_groups

    def create_error_report_file(self, error_groups):
        return create_error_report(error_groups)

    def generate_error_report(self):
        error_groups = self.get_error_groups()
        error_report_path, error_count = self.create_error_report_file(error_groups)

        self.error_report_path = error_report_path
        self.error_count = error_count

    def report(self):
        self.archive_file()
        self.upload_error_report()
        self.create_execution_object()

    def archive_file(self):
        move_file(self.s3_object_key, self.archive_file_s3_key, self.bucket_name)

    def upload_error_report(self):
        assert self.error_report_path, f"error_report_path is not set"
        return upload_file(
            self.error_report_path, self.bucket_name, self.error_file_s3_key
        )

    def set_timestamp(self, timestamp: str = None):
        self.timestamp = timestamp if timestamp else get_iso()

    def get_timestamp(self):
        if self.timestamp:
            return self.timestamp
        return get_iso()

    @property
    def archive_file_s3_key(self):
        s3_object_key = self.s3_object_key
        archive_folder = self.archive_folder if self.archive_folder else "archive"
        archive_s3_key = timestamp_s3_key(s3_object_key, timestamp=self.get_timestamp())
        return (Path(archive_folder) / archive_s3_key).as_posix()

    @property
    def error_file_s3_key(self):
        error_folder = self.error_folder if self.error_folder else "errors"
        error_report_s3_key = timestamp_s3_key(
            "error-report.csv", timestamp=self.get_timestamp()
        )
        return (Path(error_folder) / error_report_s3_key).as_posix()

    def create_execution_object(self):
        assert self.sf_client, f"sf_client isn't set"
        assert self.execution_object_name, f"execution_object_name isn't set"
        return getattr(self.sf_client, self.execution_object_name).create(
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
