def get_create_database_query(database_name: str, database_comment: str) -> str:
    return f"""
    CREATE DATABASE IF NOT EXISTS {database_name} COMMENT '{database_comment}';
    """


def get_create_table_query(database_name: str, table_name: str, table_comment: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {database_name}.{table_name}
    (
        gender String
        name.title String
        name.first String
        name.last String
        location.street.number UInt32
        location.street.name String
        location.city String
        location.state String
        location.country String
        location.postcode String
        location.coordinates.latitude Float64
        location.coordinates.longitude Float64
        location.timezone.offset String
        location.timezone.description String
        email String
        login.uuid String
        login.username String
        login.password String
        login.salt String
        login.md5 String
        login.sha1 String
        login.sha256 String
        dob.date DateTime
        dob.age UInt16
        registered.date DateTime
        registered.age UInt16
        phone String
        cell String
        id.name String
        id.value String
        picture.large String
        picture.medium String
        picture.thumbnail String
        nat String
    )
    ENGINE = MergeTree
    COMMENT '{table_comment}'
    """
