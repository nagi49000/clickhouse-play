def get_create_database_query(database_name: str, database_comment: str) -> str:
    return f"""
    CREATE DATABASE IF NOT EXISTS {database_name} COMMENT '{database_comment}';
    """


def get_create_table_query(database_name: str, table_name: str, table_comment: str) -> str:
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
        nat String
    )
    ENGINE = MergeTree
    ORDER BY uuid
    COMMENT '{table_comment}';
    """
