# Project: Data Modeling with Postgres

# Summary

The project builds an ETL pipeline using Python and SQL transferring data from 2 folders (log_data and song_data) that have JSON files localed in two local directories into 5 tables: *songplays, users, songs, artists and time*.

# Schema

## Fact Table
### songplays - song plays records from log data: *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

## Dimension Tables
### users - list of users: *user_id, first_name, last_name, gender, level*
### songs - list of songs: *song_id, title, artist_id, year, duration*
### artists - list of artists: *artist_id, name, location, latitude, longitude*
### time* - timestamps of records in songplays:  start_time, hour, day, week, month, year, weekday

The above schema was created and records were inserted across all the 30 files then data was loaded into the tables one by one.

# Load functions:

def process_log_file(cur, filepath)
def process_song_file(cur, filepath)
def process_data(cur, conn, filepath, func)

# Scripts to execute in IPython in the following order:

1. run create_tables.py
2. run etl.py

# Project Steps

1. Create table statements in sql_queries.py
2. Drop table statements in sql_queries.py
3. Run create_tables.py to create DB and tables.
4. Run test.ipynb to check if tables were created correctly. 
5. Build ETL processes by following instructions in elt.ipynb and running test.ipynb to confirm insertions were successful. After this, complete etl.py from elt.ipynb to process the entire dataset. Run again test.ipynb to fonfirm all insertions were successful.