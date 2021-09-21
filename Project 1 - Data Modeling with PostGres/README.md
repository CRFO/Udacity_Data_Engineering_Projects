# Project: Data Modeling with PostGres

# Summary

The project builds an ETL pipeline using Python and SQL transferring data from 2 folders (log_data and song_data) that have JSON files localed in two local directories into 5 tables: *songplays, users, songs, artists and time*.

# Files Structure:

- *data/* folder contains log_data and song_data files in JSON format.
- *sql_queries.py* defines the SQL queries to create, drop and insert into tables.
- *create_tables.py* creates the Sparkify DB and executes creation and deletion of all tables.
- *etl.py* reads and processes the song and log JSON files and inserts them into the PostGres DB.
- *etl.ipynb* and test.ipynb test execution of the Python scripts and SQL statements respectively.

# Star Schema

## Fact Table
### songplays - song plays records from log data: *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

## Dimension Tables
### users - list of users: *user_id, first_name, last_name, gender, level*
### songs - list of songs: *song_id, title, artist_id, year, duration*
### artists - list of artists: *artist_id, name, location, latitude, longitude*
### time - timestamps of records in songplays:  start_time, hour, day, week, month, year, weekday

The above schema was created and records were inserted from data/log_data and data/song_data JSON files then data was loaded into the tables one by one.

# Load functions:

- def process_log_file(cur, filepath)
- def process_song_file(cur, filepath)
- def process_data(cur, conn, filepath, func)

# Scripts to execute in IPython:

1. run create_tables.py
2. run etl.py

# Project Steps:

1. Create, drop, insert table statements in sql_queries.py
2. Run create_tables.py to create DB and tables.
3. Run test.ipynb to check if tables were created correctly. 
4. Build ETL processes by following instructions in elt.ipynb and running test.ipynb to confirm insertions were successful. 
5. Complete etl.py from elt.ipynb to process the entire dataset. 
6. Run again test.ipynb to confirm all insertions were successful.
