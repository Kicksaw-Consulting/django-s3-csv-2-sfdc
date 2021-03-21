# Overview

A set of helper functions for CSV to Salesforce procedures, with reporting in AWS S3, based in a Django project.
The use case is extremely specific, but the helpers should be modular so they can be cherry-picked.

Typical use case:

- Receive an S3 event
- Download the S3 object
- Serialize the file into JSON
- Bulk upsert the JSON data to Salesforce
- Parse the results of the upsert for errors
- Construct a CSV error report
- Move the triggering S3 object to an archive folder
- Push the error report to an error folder in the same bucket
- Push an object to Salesforce that details information about the above execution

# Example

```python
from django_s3_csv_2_sfdc.csv_helpers import create_error_report
from django_s3_csv_2_sfdc.s3_helpers import download_file, respond_to_s3_event, upload_file
from django_s3_csv_2_sfdc.sfdc_helpers import extract_errors_from_results


# handler for listening to s3 events
def handler(event, context):
    respond_to_s3_event(event, download_and_process)


def download_and_process(s3_object_key, bucket_name):
    download_path = download_file(s3_object_key, bucket_name)

    # This function contains your own biz logic; does not come from this library
    results = serialize_and_push_to_sfdc(download_path)

    sucesses, errors = parse_bulk_upsert_results(results)

    report_path, errors_count = create_error_report([errors])

    upload_file(report_path, bucket_name)
```

Just take what'cha need!
