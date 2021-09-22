# DROP TABLES

songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "DROP table IF EXISTS users"
song_table_drop = "DROP table IF EXISTS songs"
artist_table_drop = "DROP table IF EXISTS artists"
time_table_drop = "DROP table IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
            CREATE TABLE IF NOT EXISTS songplays 
            (start_time timestamp NOT NULL, user_id int NOT NULL, level varchar, song_id varchar, artist_id varchar, 
            session_id int, location varchar, user_agent varchar, songplay_id SERIAL PRIMARY KEY)
            """)

user_table_create = ("""
            CREATE TABLE IF NOT EXISTS users 
            (user_id int PRIMARY KEY, first_name varchar, 
            last_name varchar, gender varchar, level varchar)
            """)

song_table_create = ("""
            CREATE TABLE IF NOT EXISTS songs 
            (song_id varchar PRIMARY KEY, title varchar, artist_id varchar, 
            year int, duration decimal)
            """)

artist_table_create = ("""
            CREATE TABLE IF NOT EXISTS artists 
            (artist_id varchar PRIMARY KEY, name varchar, location varchar, 
            latitude decimal, longitude decimal)
            """)

time_table_create = ("""
            CREATE TABLE IF NOT EXISTS time 
            (start_time timestamp PRIMARY KEY, hour int , day int, 
            week int, month int, year int, weekday int)
            """)

# INSERT RECORDS

songplay_table_insert = ("INSERT INTO songplays VALUES (%s, %s, %s, %s, %s, %s, %s, %s) \
            ON CONFLICT DO NOTHING")

user_table_insert = ("INSERT INTO users VALUES (%s, %s, %s, %s, %s) \
            ON CONFLICT (user_id) DO UPDATE SET level = excluded.level")

song_table_insert = ("INSERT INTO songs VALUES (%s, %s, %s, %s, %s) \
            ON CONFLICT (song_id) DO NOTHING")

artist_table_insert = ("INSERT INTO artists VALUES (%s, %s, %s, %s, %s) \
            ON CONFLICT (artist_id) DO NOTHING")

time_table_insert = ("INSERT INTO time VALUES (%s, %s, %s, %s, %s, %s, %s) \
            ON CONFLICT (start_time) DO NOTHING")

# FIND SONGS

song_select = ("""
                SELECT songs.song_id, artists.artist_id 
                FROM songs JOIN artists on songs.artist_id = artists.artist_id 
                WHERE songs.title=%s AND artists.name=%s AND songs.duration=%s
            """)

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
