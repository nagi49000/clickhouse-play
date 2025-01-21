from typing import Iterable


# each column defined by a 3-tuple of (<column name>, <type>, <other clickhouse commands>)
# so that when the 3-tuple is concatenated (with spaces) the line is a valid line in a CREATE TABLE query
table_schema = [
    ("uuid", "UUID", "DEFAULT generateUUIDv4()"),
    ("gender", "String", ""),
    ("name_title", "String", ""),
    ("name_first", "String", ""),
    ("name_last", "String", ""),
    ("location_street_number", "UInt32", ""),
    ("location_street_name", "String", ""),
    ("location_city", "String", ""),
    ("location_state", "String", ""),
    ("location_country", "String", ""),
    ("location_postcode", "String", ""),
    ("location_coordinates_latitude", "Float64", ""),
    ("location_coordinates_longitude", "Float64", ""),
    ("location_timezone_offset", "String", ""),
    ("location_timezone_description", "String", ""),
    ("email", "String", ""),
    ("login_uuid", "String", ""),
    ("login_username", "String", ""),
    ("login_password", "String", ""),
    ("login_salt", "String", ""),
    ("login_md5", "String", ""),
    ("login_sha1", "String", ""),
    ("login_sha256", "String", ""),
    ("dob_date", "DateTime", ""),
    ("dob_age", "UInt16", ""),
    ("registered_date", "DateTime", ""),
    ("registered_age", "UInt16", ""),
    ("phone", "String", ""),
    ("cell", "String", ""),
    ("id_name", "String", ""),
    ("id_value", "String", ""),
    ("picture_large", "String", ""),
    ("picture_medium", "String", ""),
    ("picture_thumbnail", "String", ""),
    ("nat", "String", ""),
    ("ingest_date", "DateTime", "DEFAULT now()"),
]


def get_create_database_query(
    database_name: str,
    database_comment: str
) -> str:
    return f"""
    CREATE DATABASE IF NOT EXISTS {database_name} COMMENT '{database_comment}';
    """


def get_create_table_query(
    database_name: str,
    table_name: str,
    table_comment: str,
    table_schema: list[tuple[str, str, str]],
    order_by_column: str
) -> str:
    # concat each tuple in the schema with spaces, add a comma,
    # and then make one big string with newlines between each row
    create_table_columns = "\n".join([" ".join(x) + "," for x in table_schema])

    return f"""
    CREATE TABLE IF NOT EXISTS {database_name}.{table_name}
    (
        {create_table_columns}
    )
    ENGINE = MergeTree
    ORDER BY {order_by_column}
    COMMENT '{table_comment}';
    """


def get_insert_into_query(
    database_name: str,
    table_name: str,
    values_iterable: Iterable[tuple],
    columns: tuple[str] | None = None
) -> (int, str):
    # TODO make query async
    # replace to remove quotes around column names
    columns_as_str = "(*)" if columns is None else str(tuple(columns)).replace("'", "")
    query_pt_1 = f"INSERT INTO {database_name}.{table_name} {columns_as_str} VALUES"
    # convert each row into a string ready for query insertion; pre-prend UUID and nasty crow-bar clean on '
    # replace to remove quotes around clickhouse function
    values_as_insert_strs = [str(tuple([x.replace("'", " ") for x in values]))
                             for values in values_iterable]
    query_pt_2 = ", ".join(values_as_insert_strs)
    query = f"{query_pt_1} {query_pt_2} ;"
    return len(values_as_insert_strs), query
