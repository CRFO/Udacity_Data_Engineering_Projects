import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear
import pyspark.sql.functions as F
from pyspark.sql import types as T
from pyspark.sql import Window
from datetime import datetime

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    
    """
    Description: This function creates a Spark Session 
    Arguments: None
    Returns: Spark session object
    """

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3-us-west-2.amazonaws.com") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
     Description: This function reads data from Song JSON files and inserts into songs and artists tables directories with 
                 partitioned parquet format in S3.
     Arguments:
        spark: spark session object
        input_data: source files from S3 folder
        output_data: parquet output files from S3 folder
     Returns: None
    """
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    song_df = spark.read.json(song_data)
    
    # show first record of song_data
    song_df.take(1)
    
    # show schema of song_data
    song_df.printSchema()

    # extract columns to create songs table
    songs_table = song_df.select("song_id", "title", "artist_id", "year", "duration").distinct()
    
    # show first 5 records of songs table dataframe
    songs_table.show(5)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").mode("overwrite").parquet(output_data + "songs_table.parquet")

    # extract columns to create artists table
    artists_table = song_df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude").distinct()
    
    # show first 5 records of artists table dataframe
    artists_table.show(5)
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "artists_table.parquet")


def process_log_data(spark, input_data, output_data):
    """
     Description: This function reads data from Log JSON files and inserts into songplays, users, and time tables directories 
                  partitioned parquet format in S3.
     Arguments:
        spark: spark session
        input_data: source files from S3 folder
        output_data: parquet output files from S3 folder
     Returns: None
    """
    
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'
    
    # read log data file
    log_df = spark.read.json(log_data)
    
    # show first record of log_data
    log_df.take(1)
    
    # show schema of log_data
    log_df.printSchema()
    
    # filter by actions for song plays
    log_df = log_df.filter(log_df.page == 'NextSong')

    # extract columns for users table    
    users_table = log_df.select("userId", "firstName", "lastName", "gender", "level").distinct()
    
    # show first 5 records of users table dataframe
    users_table.show(5)
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + "users_table.parquet")

    # create timestamp column from original timestamp column
    log_df = log_df.withColumn("ts", (F.round(col('ts')/1000)).cast(TimestampType()))
    
    # extract columns to create time table
    time_table = log_df.selectExpr("ts AS start_time") \
            .withColumn("hour", F.hour("start_time")) \
            .withColumn("day", F.dayofmonth("start_time")) \
            .withColumn("week", F.weekofyear("start_time")) \
            .withColumn("month", F.month("start_time")) \
            .withColumn("year", F.year("start_time")) \
            .withColumn("weekday", F.dayofweek("start_time")).distinct()
    
    # show first 5 records of time table dataframe
    time_table.show(5)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").mode("overwrite").parquet(output_data + "time_table.parquet")

    # read in song data to use for songplays table
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = log_df.join(song_df, (log_df.artist == song_df.artist_name)\
                        & (log_df.song == song_df.title) & (log_df.length == song_df.duration))\
                        .join(time_table,(log_df.ts == time_table.start_time))\
                        .select(time_table.start_time,"userId", "level", "song_id", \
                        "artist_id", "sessionId", "location", "userAgent",time_table.year,"month")   
    
    songplays_table = songplays_table.withColumn("col_id", F.monotonically_increasing_id())
    window = Window.orderBy(F.col('col_id'))
    songplays_table = songplays_table.withColumn("songplay_id",F.row_number().over(window))\
            .select("songplay_id","start_time","userId","level","song_id","artist_id", "sessionId","location", "userAgent","year","month")
    
    # show first 5 records of songplays table dataframe
    songplays_table.show(5)
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").mode("overwrite").parquet(output_data + "songplays_table.parquet")


def main():
    """
    Description: This function creates a Spark session then reads song and log data from Log JSON files from S3 
                    and inserts them into fact and dimension tables 
    Arguments: None
    Returns: None
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://foustawsbucket/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
