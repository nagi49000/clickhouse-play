import requests
import csv
from logging import Logger
from typing import Iterator
from pathlib import Path
from clickhouse_connect import get_client
from .clickhouse_queries import (
    get_create_database_query,
    get_create_table_query,
    get_insert_into_query,
)


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
        if stream_index != 0:
            logger.warning("Encountered schema-failing record")
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
        processing_log_f.write("landing_filename,number_valid_records,number_invalid_records\n")
        n_processed_file = 0
        for csv_filename in csv_filenames:
            try:
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

                processing_log_f.write(f"{csv_filename},{len(valid_records)},{len(invalid_records)}\n")
                csv_filename.unlink()  # delete processed raw file
                csv_filename.parent.rmdir()  # delete folder containing now-removed raw file
                n_processed_file += 1
            except Exception as exc:
                logger.error(f"schemaed_csv_rows_to_file: on file {csv_filename} encountered {exc}")
        logger.debug(f"schemaed_csv_rows_to_file: processed {n_processed_file} files")


def valid_rows_to_clickhouse(
    logger: Logger,
    processing_log_path: Path,
    valid_rows_path: Path,
    to_clickhouse_fail_path: Path,
    clickhouse_host: str,
    clickhouse_database: str,
    clickhouse_table: str,
    clickhouse_username: str | None = None,
    clickhouse_password: str = ""
):
    # TODO pull this from schema
    columns = (
        "uuid",
        "gender",
        "name_title",
        "name_first",
        "name_last",
        "location_street_number",
        "location_street_name",
        "location_city",
        "location_state",
        "location_country",
        "location_postcode",
        "location_coordinates_latitude",
        "location_coordinates_longitude",
        "location_timezone_offset",
        "location_timezone_description",
        "email",
        "login_uuid",
        "login_username",
        "login_password",
        "login_salt",
        "login_md5",
        "login_sha1",
        "login_sha256",
        "dob_date",
        "dob_age",
        "registered_date",
        "registered_age",
        "phone",
        "cell",
        "id_name",
        "id_value",
        "picture_large",
        "picture_medium",
        "picture_thumbnail",
        "nat",
    )
    client = get_client(
        host=clickhouse_host,
        username=clickhouse_username,
        password=clickhouse_password,
    )
    client.command(get_create_database_query(clickhouse_database, "Simple Database for random users"))
    client.command(get_create_table_query(clickhouse_database, clickhouse_table, "Fat table for random users"))
    csv_filenames = valid_rows_path.glob("**/*csv")
    with open(processing_log_path, "wt") as processing_log_f:
        processing_log_f.write("schemaed_filename,n_rows_to_clickhouse\n")
        n_processed_file = 0
        for csv_filename in csv_filenames:
            try:
                subdir = str(csv_filename.parent).split("/")[-1]  # extract the subdirectory under "schemaed"
                with open(csv_filename, "rt") as file_obj:
                    reader = csv.reader(file_obj)
                    n_rows, insert_into_query = get_insert_into_query(
                        clickhouse_database, clickhouse_table, reader, columns=columns
                    )
                    # print(insert_into_query)
                    query_summary = client.command(insert_into_query)
                    # TODO some qualify query_summary, e.g. query_summary.written_rows

                processing_log_f.write(f"{csv_filename},{n_rows}\n")
                csv_filename.unlink()  # delete processed schemaed file
                csv_filename.parent.rmdir()  # delete folder containing now-removed raw file
                n_processed_file += 1
            except Exception as exc:
                logger.error(f"valid_rows_to_clickhouse: on file {csv_filename} encountered {exc}")
        logger.debug(f"valid_rows_to_clickhouse: processed {n_processed_file} files")
