import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop tables in the database.

    Args:
        cur (psycopg2.extensions.cursor): The database cursor.
        conn (psycopg2.extensions.connection): The database connection.

    Returns:
        None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create tables in the database.

    Args:
        cur (psycopg2.extensions.cursor): The database cursor.
        conn (psycopg2.extensions.connection): The database connection.

    Returns:
        None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function for executing table creation and deletion.

    Args:
        None

    Returns:
        None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    DWH_ENDPOINT = config.get("DWH", "HOST")
    DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
    DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
    DWH_DB_PORT = config.get("DWH", "DWH_DB_PORT")
    DWH_DB_NAME = config.get("DWH", "DWH_DB_NAME")

    conn = psycopg2.connect(
        host=DWH_ENDPOINT,
        dbname=DWH_DB_NAME,
        user=DWH_DB_USER,
        password=DWH_DB_PASSWORD,
        port=DWH_DB_PORT
    )

    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
