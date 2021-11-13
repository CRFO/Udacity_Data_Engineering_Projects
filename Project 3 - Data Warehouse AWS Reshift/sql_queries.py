import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs"
songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "DROP table IF EXISTS users"
song_table_drop = "DROP table IF EXISTS songs"
artist_table_drop = "DROP table IF EXISTS artists"
time_table_drop = "DROP table IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR(255),
        auth VARCHAR(255),
        firstName VARCHAR(255),
        gender CHAR,
        intemInSession INT,
        lastName VARCHAR(255), 
        length DECIMAL(18,6),
        level VARCHAR(255),
        location VARCHAR(255),
        method VARCHAR(25),
        page VARCHAR(255),
        registration FLOAT,
        sessionId INT,
        song VARCHAR(255),
        status INT,
        ts BIGINT,
        userAgent VARCHAR(512),
        userId INT )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs INT,
        artist_id VARCHAR(255),
        artist_latitude DECIMAL(18,6),
        artist_longitude DECIMAL(18,6),
        artist_location VARCHAR(255),
        artist_name VARCHAR(255),
        song_id VARCHAR(255),
        title VARCHAR(512),
        duration DECIMAL(18,6),
        year INT )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        start_time TIMESTAMP NOT NULL, 
        user_id INT NOT NULL, 
        level VARCHAR(255), 
        song_id VARCHAR(255), 
        artist_id VARCHAR(255), 
        session_id INT, 
        location VARCHAR(255), 
        user_agent VARCHAR(512), 
        songplay_id INT IDENTITY(0,1) PRIMARY KEY )
    diststyle all;
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY, 
        first_name VARCHAR(255), 
        last_name VARCHAR(255), 
        gender CHAR, 
        level VARCHAR(255) )
    diststyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR(255) PRIMARY KEY, 
        title VARCHAR(512), 
        artist_id VARCHAR(255), 
        year INT, 
        duration DECIMAL(18,6) )
    diststyle all;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR(255) PRIMARY KEY, 
        name VARCHAR(255), 
        location VARCHAR(255), 
        latitude DECIMAL(18,6), 
        longitude DECIMAL(18,6) )
    diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY, 
        hour INT ,
        day INT, 
        week INT, 
        month INT, 
        year INT, 
        weekday INT )
    diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM 's3://udacity-dend/log_data'
    credentials 'aws_iam_role={}'
    format json as 's3://udacity-dend/log_json_path.json'
    region 'us-west-2'
""").format(config.get("IAM_ROLE","ARN"))

staging_songs_copy = ("""
    COPY staging_songs FROM 's3://udacity-dend/song_data'
    credentials 'aws_iam_role={}'
    JSON 'auto'
    region 'us-west-2'
""").format(config.get("IAM_ROLE","ARN"))

# FINAL TABLES

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    ( SELECT DISTINCT userId,firstName,lastName,gender,level 
        FROM staging_events
        WHERE page = 'NextSong' AND userId is not null )
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    ( SELECT DISTINCT song_id, title, artist_id, year, duration 
        FROM staging_songs )
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    ( SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude 
        FROM staging_songs )
""")

time_table_insert = ("""
    INSERT INTO time ( start_time, hour, day, week, month, year, weekday )
    ( SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000*INTERVAL '1 second' AS start_time,
        EXTRACT (HOUR FROM start_time) AS hour,
        EXTRACT (DAY FROM start_time) AS day,
        EXTRACT (WEEK FROM start_time) AS week,
        EXTRACT (MONTH FROM start_time)  AS month,
        EXTRACT (YEAR FROM start_time) AS year,
        EXTRACT (WEEKDAY FROM start_time)  AS weekday
        FROM staging_events se)
""")

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    ( SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000*INTERVAL '1 second' AS start_time, 
        se.userId, se.level, ss.song_id, ss.artist_id, se.sessionId, se.location, se.userAgent
        FROM staging_events se, staging_songs ss
        WHERE se.artist = ss.artist_name AND se.song = ss.title AND se.length = ss.duration 
        AND se.page = 'NextSong' )
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
