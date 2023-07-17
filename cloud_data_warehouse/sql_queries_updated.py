import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg') 

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES

staging_events_table_create= ("""

   CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender VARCHAR,
        itemInSession INTEGER,
        lastName VARCHAR,
        length FLOAT,
        level VARCHAR,
        location TEXT,
        method VARCHAR,
        page VARCHAR,
        registration VARCHAR,
        sessionId INTEGER,
        song VARCHAR,
        status INTEGER,
        ts NUMERIC,
        userAgent TEXT,
        userId INTEGER
    )
""")


staging_songs_table_create = ("""
   CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs INTEGER,
        artist_id VARCHAR NOT NULL,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location TEXT,
        artist_name VARCHAR,
        song_id VARCHAR NOT NULL,
        title VARCHAR,
        duration FLOAT,
        year INTEGER
)
""")

songplay_table_create = ("""
   CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INTEGER  IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        user_id INTEGER NOT NULL,
        level VARCHAR,
        song_id VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        session_id INTEGER NOT NULL,
        location TEXT,
        user_agent TEXT
)
""")

user_table_create = ("""
   CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY NOT NULL,
        first_name VARCHAR,
        last_name VARCHAR,
        gender VARCHAR,
        level VARCHAR
)
""")

song_table_create = ("""
   CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR PRIMARY KEY NOT NULL,
        title VARCHAR,
        artist_id VARCHAR NOT NULL,
        year INTEGER,
        duration FLOAT
)
""")


artist_table_create = ("""
   CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY NOT NULL,
        name VARCHAR,
        location TEXT,
        latitude FLOAT,
        longitude FLOAT
)
""")


time_table_create = ("""
   CREATE TABLE IF NOT EXISTS times (
        start_time TIMESTAMP PRIMARY KEY,
        hour INTEGER,
        day INTEGER,
        week INTEGER,
        month INTEGER,
        year INTEGER,
        weekday VARCHAR
)
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {}
credentials 'aws_iam_role={}' 
format as json {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
copy staging_songs from {}
credentials 'aws_iam_role={}' 
json 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
   INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT
           TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second' as start_time,
           e.userId AS user_id,
           e.level AS level,
           s.song_id AS song_id,
           s.artist_id AS artist_id,
           e.sessionId AS session_id,
           e.location AS location,
           e.userAgent AS user_agent
    FROM staging_events e
    JOIN staging_songs s ON e.song = s.title AND e.artist = s.artist_name
    WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
   INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
           userId AS user_id,
           firstName AS first_name,
           lastName AS last_name,
           gender AS gender,
           level AS level
    FROM staging_events
    WHERE page = 'NextSong'
""")

song_table_insert = ("""
   INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
           song_id AS song_id,
           title AS title,
           artist_id AS artist_id,
           year AS year,
           duration AS duration
    FROM staging_songs
""")

artist_table_insert = ("""
   INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
           artist_id AS artist_id,
           artist_name AS name,
           artist_location AS location,
           artist_latitude AS latitude,
           artist_longitude AS longitude
    FROM staging_songs
""")

time_table_insert = ("""
   INSERT INTO times (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
           TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' as start_time,
           EXTRACT(hour FROM start_time) AS hour,
           EXTRACT(day FROM start_time) AS day,
           EXTRACT(week FROM start_time) AS week,
           EXTRACT(month FROM start_time) AS month,
           EXTRACT(year FROM start_time) AS year,
           EXTRACT(weekday FROM start_time) AS weekday
    FROM staging_events
    WHERE page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
