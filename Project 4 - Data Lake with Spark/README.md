# Project: Data Lake with Spark

# Summary:

The project builds an ETL pipeline for the music streaming app Sparkify using a Spark job to transfer JSON files (song_data and log_data) from S3 to the following table directories with partitioned parquet format in S3:

- Fact table: *songplays*
- Dimmension tables: *users*, *songs*, *artists* and *time*

The analytics team will be able to execute queries from the tables above to find insights from the partitioned parquet files in table directories. Two SQL examples are included at the end of the test.ipynb script.

# Files Structure:

- *dl.cfg* stores AWS access key ID and secret access key. Both keys have been removed for safety reasons.
- *etl.py* loads song_data and log_data JSON files located in S3 then writes the data into songplays, users, songs, artists and time table directories with partitioned parquet format in S3. 
- *test.ipynb* loads song_data and log_data from S3 and writes partitioned parquet files in table directories in S3. The song_data only reads JSON files under the top 2 A folders.
- *README.md* gives an explanation of this project and how to execute it.

# Tables:

## Fact Table
- songplays - list of records from staging data with song plays: *songplay_id, start_time, userId, level, song_id, artist_id, sessionId, location, userAgent, year, month* partiioned by year and month.

## Dimension Tables
- users - list of users: *userId, firstName, lastName, gender, level*
- songs - list of songs: *song_id, title, artist_id, year, duration* partitioned by year and artist_id.
- artists - list of artists: *artist_id, artist_name, artist_location, artist_latitude, artist_longitude*
- time - timestamps of records in song plays:  *start_time, hour, day, week, month, year, weekday* partiioned by year and month.

# Load functions

- def create_spark_session()
- def process_song_data(spark, input_data, output_data)
- def process_log_data(spark, input_data, output_data)

# Steps to execute code

1. Create AWS EMR Cluster with 1 Master node and 2 worker nodes using UI (or CLI command) with installation of Spark.
2. Copy etl.py and dl.cfg to hadoop file system in Cluster (with a command like this: scp -i <key pair>.pem dl.cfg etl.py <hadoop link>:~/)
3. Execute Python etl.py script with command at the /home/hadoop directory: "spark-submit --master yarn etl.py".

# Project Steps

1. Create test.ipynb script with the Spark commands to load log and song JSON files from S3 and create partitioned parquet files in table directories in S3.
2. Run test.ipynb to confirm code was successful. 
3. Update the etl.py with the same commands from test.ipynb.
4. Create AWS EMR Cluster with 1 Master node and 2 worker nodes using UI (or CLI command) with installation of Spark.
5. Execute etl.py with command at the /home/hadoop directory: "spark-submit --master yarn etl.py".
6. Terminate AWS EMR cluster.
