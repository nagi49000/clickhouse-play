from typing import Iterable


def get_create_database_query(database_name: str, database_comment: str) -> str:
    return f"""
    CREATE DATABASE IF NOT EXISTS {database_name} COMMENT '{database_comment}';
    """


def get_create_table_query(database_name: str, table_name: str, table_comment: str) -> str:
    # TODO extract out schema as an argument
    return f"""
    CREATE TABLE IF NOT EXISTS {database_name}.{table_name}
    (
        uuid UUID,
        gender String,
        name_title String,
        name_first String,
        name_last String,
        location_street_number UInt32,
        location_street_name String,
        location_city String,
        location_state String,
        location_country String,
        location_postcode String,
        location_coordinates_latitude Float64,
        location_coordinates_longitude Float64,
        location_timezone_offset String,
        location_timezone_description String,
        email String,
        login_uuid String,
        login_username String,
        login_password String,
        login_salt String,
        login_md5 String,
        login_sha1 String,
        login_sha256 String,
        dob_date DateTime,
        dob_age UInt16,
        registered_date DateTime,
        registered_age UInt16,
        phone String,
        cell String,
        id_name String,
        id_value String,
        picture_large String,
        picture_medium String,
        picture_thumbnail String,
        nat String,
        ingest_date DateTime DEFAULT now(),
    )
    ENGINE = MergeTree
    ORDER BY uuid
    COMMENT '{table_comment}';
    """


def get_insert_into_query(
    database_name: str,
    table_name: str,
    values: Iterable[tuple],
    columns: tuple[str] | None = None
) -> (int, str):
    columns_as_str = "(*)" if columns is None else str(tuple(columns)).replace("'", "")
    # replace to remove quotes around column names
    query_pt_1 = f"INSERT INTO {database_name}.{table_name} {columns_as_str} VALUES"
    values_as_insert_strs = [str(("generateUUIDv4()", *x)) for x in values]
    query_pt_2 = ", ".join(values_as_insert_strs)
    # replace to remove quotes around clickhouse function
    query = f"{query_pt_1} {query_pt_2} ;".replace("'generateUUIDv4()'", "generateUUIDv4()")
    return len(values_as_insert_strs), query
