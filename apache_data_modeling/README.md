# Data Modeling with Apache Cassandra

## Purpose
This project aims to create an Apache Cassandra database for a fictitious startup called Sparkify to query song play data. The raw data is in a directory of CSV files, and we will build an ETL pipeline to transform the raw data into the Apache Cassandra database.

## Dataset
There is one dataset called event_data, which is in a directory of CSV files partitioned by date.

## Queries
We design the schema for NoSQL databases based on the queries we know we want to perform. For this project, we have three queries:

1. Find the artist, song title, and song length heard during sessionId=338 and itemInSession=4.
   ```
   SELECT artist, song, length FROM table_1 WHERE sessionId=338 AND itemInSession=4
   ```

2. Find the name of the artist, song (sorted by itemInSession), and user (first and last name) for userid=10 and sessionId=182.
   ```
   SELECT artist, song, firstName, lastName FROM table_2 WHERE userId=10 AND sessionId=182
   ```

3. Find every user name (first and last) who listened to the song 'All Hands Against His Own'.
   ```
   SELECT firstName, lastName WHERE song='All Hands Against His Own'
   ```

## Project Structure
- `event_data` folder: Contains all the necessary data files.
- `Project_1B_Project_Template.ipynb`: The main code file.
- `event_datafile_new.csv`: A smaller event data CSV file used to insert data into the Apache Cassandra tables.
- `Images` folder: Contains a screenshot of what the denormalized data should appear like in the `event_datafile_new.csv`.
- `README.md`: The current file describing the project.

## Build Instructions
1. Run each portion of `Project_1B_Project_Template.ipynb`.
2. Execute the `insert_tables` function:
   ```python
   def insert_tables(cur, conn):
       """
       Executes the insert queries.

       Parameters:
       cur (psycopg2.extensions.cursor): The database cursor.
       conn (psycopg2.extensions.connection): The database connection object.

       Returns:
       None
       """
       for query in insert_table_queries:
           print('Inserting to tables -- QUERY: ', query)
           cur.execute(query)
           conn.commit()
   ```
