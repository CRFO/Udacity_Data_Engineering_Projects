# Project: Data Modeling with Postgres

# Summary

The project builds an ETL pipeline for the music streaming app Sparkify using Python and SQL transferring data from 2 folders (log_data and song_data) that have JSON files localed in two local directories into 5 tables using a Postgres database: *songplays, users, songs, artists and time*.

# Files Structure

- *data/* folder contains log_data and song_data files in JSON format.
- *sql_queries.py* defines the SQL queries to create, drop and insert into tables.
- *create_tables.py* creates the Sparkify DB and executes creation and deletion of all tables.
- *etl.py* reads and processes the song and log JSON files and inserts them into the Postgres DB.
- *etl.ipynb* and test.ipynb test execution of the Python scripts and SQL statements respectively.

# Star Schema (PostgreSQL relational database)

## Fact Table
- songplays - list of records from log data withy song plays: *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

## Dimension Tables
- users - list of users: *user_id, first_name, last_name, gender, level*
- songs - list of songs: *song_id, title, artist_id, year, duration*
- artists - list of artists: *artist_id, name, location, latitude, longitude*
- time - timestamps of records in song plays:  start_time, hour, day, week, month, year, weekday

The above schema was created and records were inserted from data/log_data and data/song_data JSON files into the tables one by one.

# Load functions

- def process_log_file(cur, filepath)
- def process_song_file(cur, filepath)
- def process_data(cur, conn, filepath, func)

# Scripts to execute in IPython

1. run create_tables.py
2. run etl.py

# Project Steps

1. Create, drop, insert table statements in sql_queries.py
2. Run create_tables.py to create sparkifydb database and tables.
3. Run test.ipynb to check if tables were created correctly. Restart kernel to close DB connection after executing this script.
4. Build ETL processes by following instructions in elt.ipynb to confirm insertions were successful. 
5. Complete etl.py to build ETL pipeline from elt.ipynb to process the entire dataset. Rerun create_tables.py to reset tables then execute etl.py.
6. Run again test.ipynb to confirm all insertions were successful. Restart kernel to close DB connection after executing this script.

# SQL Examples for song play analysis with results


[8]:

# Total of song plays
%sql SELECT count(*) FROM songplays;
 * postgresql://student:***@127.0.0.1/sparkifydb
1 rows affected.
[8]:
count
6820
[9]:

# Check ID values for both song_id and artist_id in fact table: songplays
%sql SELECT * FROM songplays WHERE song_id is NOT NULL and artist_id is NOT NULL;
 * postgresql://student:***@127.0.0.1/sparkifydb
1 rows affected.
[9]:
start_time	user_id	level	song_id	artist_id	session_id	location	user_agent	songplay_id
2018-11-21 21:56:47.796000	15	paid	SOZCTXZ12AB0182364	AR5KOSW1187FB35FF4	818	Chicago-Naperville-Elgin, IL-IN-WI	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/36.0.1985.125 Chrome/36.0.1985.125 Safari/537.36"	4108
[10]:

# List of distinct artist names and titles (up to 5) descending
%sql SELECT distinct artists.name, COUNT(songs.title) FROM artists JOIN songs on songs.artist_id = artists.artist_id group by artists.name order by COUNT(songs.title) DESC LIMIT 5;
 * postgresql://student:***@127.0.0.1/sparkifydb
5 rows affected.
[10]:
name	count
Casual	2
Clp	2
The Box Tops	1
The Dillinger Escape Plan	1
King Curtis	1
[11]:

# list of distinct titles for artist name "Casual" 
%sql SELECT distinct songs.title FROM songs JOIN artists on songs.artist_id = artists.artist_id WHERE artists.name = 'Casual';
 * postgresql://student:***@127.0.0.1/sparkifydb
2 rows affected.
[11]:
title
I Didn't Mean To
OAKtown



