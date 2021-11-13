# Project: Data Warehouse with AWS Redshift

# Summary:

The project builds an ETL pipeline for the music streaming app Sparkify with Python and SQL to transfer JSON files (log and song data) located in S3 to staging tables (staging_events and staging_songs) in AWS Redshift data warehouse and to load staging data into the following analytical tables:

- Fact table: *songplays*
- Dimmension tables: *users*, *songs*, *artists* and *time*

The analytics team will be able to execute queries from the tables above to find insights from the music data. 

# Files Structure:

- *create_tables.py* creates and drops all staging and database tables.
- *dwh.cfg* stores host, database name, user, password and database port credentials.
- *etl.py* loads the staging tables in AWS Redshift from log and song JSON files located in S3 then inserts the data into songplays, users, songs, artists and time tables.
- *sql_queries.py* defines the SQL queries to create, drop, copy and insert data into tables.
- *test.ipynb* confirms that data is inserted into the staging, fact and dimmension tables.

# Tables:

## Staging Tables
- staging_events - copy of log_data JSON files from S3
- staging_songs - copy of song_data JSON files from S3

## Fact Table
- songplays - list of records from staging data with song plays: *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

## Dimension Tables
- users - list of users: *user_id, first_name, last_name, gender, level*
- songs - list of songs: *song_id, title, artist_id, year, duration*
- artists - list of artists: *artist_id, name, location, latitude, longitude*
- time - timestamps of records in song plays:  start_time, hour, day, week, month, year, weekday

# Load functions

- def drop_tables(cur, conn)
- def create_tables(cur, conn)
- def load_staging_tables(cur, conn)
- def insert_tables(cur, conn)

# Scripts to execute in terminal

1. python create_tables.py
2. python etl.py

# Project Steps

1. Create Redshift cluster and update dwh.cfg file with host name, database name, user, password and port.
2. Write create, drop, copy to staging and insert table statements in sql_queries.py.
2. Run create_tables.py to drop and create staging, fact and dimension tables.
3. Execute etl.py to build ETL pipeline to load staging tables from S3 to AWS Redshift and insert staging data into fact and dimension tables. 
5. Run test.ipynb to confirm all tables were created and loaded successfully. 
6. Delete Redshift cluster.

