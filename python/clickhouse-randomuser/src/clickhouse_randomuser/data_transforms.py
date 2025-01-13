import requests
import csv
from logging import Logger
from typing import Iterator
from pathlib import Path


def download_random_users(
    logger: Logger, n_record: int, include_header: bool = False
) -> Iterator[str]:
    url = "https://randomuser.me/api/"
    response = requests.get(url, params={"format": "csv", "results": n_record})
    if response.ok:
        response_list = response.text.split("\n")
        if len(response_list) != n_record + 1:
            logger.error(
                f"GET to {url} has retrieved {len(response_list)} rows; expected {n_record + 1}"
            )
        if response.text.startswith("gender,name.title") and not include_header:
            response_list = response_list[1:]  # drop the 1st row, which is header
        for row in response_list:
            yield row + "\n"
    else:
        logger.error(
            f"GET to {url} failed with {response.status_code}: {response.reason}"
        )


def download_random_users_to_file(logger: Logger, output_path: Path, n_record: int = 3):
    with open(output_path, "wt") as f:
        for line in download_random_users(logger, n_record):
            f.write(line)


def schemaed_csv_rows(logger: Logger, input_generator: Iterator[str]) -> Iterator[tuple[int, str]]:
    for line in input_generator:
        stream_index = 1
        if len(line) < 2000:  # sanity check for crazy long lines
            rows = list(csv.reader([line]))
            if len(rows) == 1:  # check that this yields only one row
                for row in rows:
                    if len(row) == 34:  # check there are 34 columns in the record
                        stream_index = 0
        yield (stream_index, line)


def schemaed_csv_rows_to_file(
        logger: Logger,
        processing_log_path: Path,
        raw_folder: Path,
        valid_output_path: Path,
        invalid_output_path: Path
):
    csv_filenames = raw_folder.glob("**/*csv")
    with open(processing_log_path, "wt") as processing_log_f:
        for csv_filename in csv_filenames:
            subdir = str(csv_filename.parent).split("/")[-1]  # extract the subdirectory under "raw"
            valid_records = []
            invalid_records = []
            with open(csv_filename, "rt") as in_f:
                for stream_index, record in schemaed_csv_rows(logger, in_f):
                    if stream_index == 0:
                        valid_records.append(record)
                    elif stream_index == 1:
                        invalid_records.append(record)
                    else:
                        logger.error(f"encountered unknown stream_index {stream_index}")

            (valid_output_path / subdir).mkdir(parents=True)
            full_valid_path = valid_output_path / subdir / "schemaed-randomusers.csv"
            with open(full_valid_path, "wt") as valid_output_lines:
                valid_output_lines.writelines(valid_records)

            (invalid_output_path / subdir).mkdir(parents=True)
            full_invalid_path = invalid_output_path / subdir / "schemaed-failed-randomusers.csv"
            with open(full_invalid_path, "wt") as invalid_output_lines:
                invalid_output_lines.writelines(invalid_records)

            processing_log_f.write(str(csv_filename))
            csv_filename.unlink()  # delete processed raw file
            csv_filename.parent.rmdir()  # delete folder containing now-removed raw file
