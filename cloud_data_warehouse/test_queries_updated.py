import boto3
import json
import pandas as pd
import configparser
import time
from botocore.exceptions import ClientError
import psycopg2
from prettytable import PrettyTable

# Read configuration from file
config = configparser.ConfigParser()
config.read("dwh.cfg")

DWH_ENDPOINT = config.get("DWH", "HOST")
DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
DWH_DB_PORT = config.get("DWH", "DWH_DB_PORT")
DWH_DB_NAME = config.get("DWH", "DWH_DB_NAME")

# Establish connection to Redshift cluster
conn = psycopg2.connect(
    host=DWH_ENDPOINT,
    dbname=DWH_DB_NAME,
    user=DWH_DB_USER,
    password=DWH_DB_PASSWORD,
    port=DWH_DB_PORT,
)

# Define the queries
queries = [
    """SELECT DISTINCT artist_id, location FROM artists LIMIT 5;""",
    """SELECT COUNT(*) AS total_songs FROM songs;""",
    """SELECT user_id, COUNT(*) AS songplays_count 
       FROM songplays 
       GROUP BY user_id 
       ORDER BY songplays_count DESC 
       LIMIT 10;""",
    """SELECT u.gender, AVG(s.duration) AS average_song_length
       FROM songplays sp
       JOIN songs s ON sp.song_id = s.song_id
       JOIN users u ON sp.user_id = u.user_id
       GROUP BY u.gender;""",
    """SELECT AVG(duration) / 60 AS average_duration_minutes FROM songs;""",
]


def execute_queries(connection, query_list):
    """
    Execute a list of queries on the specified database connection.

    Args:
        connection (psycopg2.extensions.connection): The database connection.
        query_list (list): A list of SQL queries to execute.

    Returns:
        None
    """
    for query in query_list:
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()

        # Create a prettytable object
        table = PrettyTable([desc[0] for desc in cursor.description])

        # Add rows to the table
        for row in result:
            table.add_row(row)

        # Print the table
        print(table)


# Execute the queries and display the results
execute_queries(conn, queries)

# Close the connection
conn.close()
