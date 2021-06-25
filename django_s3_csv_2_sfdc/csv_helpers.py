import csv
import os
from pathlib import Path
from typing import List, Tuple

from django_s3_csv_2_sfdc.utils import get_temp


def create_error_report(
    errors: list,
    report_path: Path,
    headers: List[str] = None,
) -> int:
    """
    Takes in the errors from the output of parse_bulk_upsert_results and writes a report

    TEMP must be defined in your django settings
    """
    csv_rows = []
    if not headers:
        headers = [
            "salesforce_object",
            "code",
            "message",
            "upsert_key",
            "upsert_key_value",
            "object_json",
        ]

    if not os.path.isfile(report_path):
        csv_rows.append(headers)

    errors_count = 0
    for error in errors:
        errors_count += 1
        csv_rows.append([error[header] for header in headers])

    with open(report_path, mode="a", newline="") as file:
        writer = csv.writer(
            file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
        )
        for row in csv_rows:
            writer.writerow(row)

    return errors_count