import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data from S3 into staging tables.

    Args:
        cur (psycopg2.extensions.cursor): The database cursor.
        conn (psycopg2.extensions.connection): The database connection.

    Returns:
        None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert data from staging tables into analytics tables.

    Args:
        cur (psycopg2.extensions.cursor): The database cursor.
        conn (psycopg2.extensions.connection): The database connection.

    Returns:
        None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function for loading staging tables and inserting data into analytics tables.

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

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
