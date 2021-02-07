import csv
import tempfile

from pathlib import Path
from typing import List, Tuple


def create_error_report(
    error_groups: List[list], report_path: Path = None
) -> Tuple[Path, int]:
    """
    Takes in the errors from the output of parse_bulk_upsert_results and writes a report

    # TODO: allow a custom headers
    """
    csv_rows = []
    headers = [
        "salesforce_object",
        "code",
        "message",
        "upsert_key",
        "upsert_key_value",
        "object_json",
    ]

    csv_rows.append(headers)

    errors_count = 0
    for group in error_groups:
        for error in group:
            errors_count += 1
            csv_rows.append([error[header] for header in headers])

    if not report_path:
        report_path = Path(tempfile.gettempdir()) / "error_report.csv"

    with open(report_path, mode="w", newline="") as file:
        writer = csv.writer(
            file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
        )
        for row in csv_rows:
            writer.writerow(row)

    return report_path, errors_count