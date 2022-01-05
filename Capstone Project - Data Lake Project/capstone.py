import configparser
import os
import boto3
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import pyspark.sql.functions as F
from pyspark.sql.functions import col

config = configparser.ConfigParser()
config.read('capstone.cfg')

def create_spark_session():
    
    """
    Description: This function creates a Spark Session 
    Arguments: None
    Returns: Spark session object
    """ 
    spark = SparkSession.builder.\
        appName("Capstone Project - US Immigration Data").\
        config("spark.jars.repositories", "https://repos.spark-packages.org/").\
        config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.0").\
        config("spark.hadoop.fs.s3a.endpoint", "s3-us-west-2.amazonaws.com") .\
        config("spark.hadoop.fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID']).\
        config("spark.hadoop.fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY']).\
        config("spark.sql.broadcastTimeout", "36000").\
        getOrCreate()
    return spark


def process_data(spark, config):
    """
     Description: This function reads all files under I94_Folder. It also reads city demographics, airport data and                  labels/descriptions of I94 SAS files. Then it cleans dataframes then it writes the 3 dataframes into partitioned              parquet format in S3.
     Arguments:
        spark: Spark session object
        config: config object
     Returns: None
    """
    
    # Get config variables related to data
    OUTPUT_DATA = config['DATA']['OUTPUT_DATA']
    OUTPUT_DIR = config['DATA']['OUTPUT_DIR']
    I94_FOLDER = config['DATA']['I94_FOLDER']
    US_CITY_DEMO = config['DATA']['US_CITY_DEMO']
    AIRPORT_CODE = config['DATA']['AIRPORT_CODE']
    I94_DATA_DICT = config['DATA']['I94_DATA_DICT']
    
    # Load all files under I94_FOLDER config variable
    for f,filename in enumerate(os.listdir(I94_FOLDER)):
        if f == 0:
            df = spark.read.format('com.github.saurfang.sas.spark').load(I94_FOLDER + filename)
            df_i94 = df
        else:
             df = spark.read.format('com.github.saurfang.sas.spark').load(I94_FOLDER + filename)
             df_i94 = append_dataframes(df_i94,df) 

    # Read the city demographics CSV file
    df_city = spark.read.csv(US_CITY_DEMO, sep = ';', header=True)
    
    # Read the airport codes CSV file
    df_airport = spark.read.csv(AIRPORT_CODE, header=True)
    
    # Read SAS labels and descriptions file then remove special characters
    with open(I94_DATA_DICT) as f:
        lines = f.readlines()
    lines = [line.replace('"','').replace('\n','').replace("'",'').replace(' ','').replace('\t','') for line in lines]
    
    # Get list of all country codes and names from SAS labels and descriptions file
    country_value_pairs = list()
    countries = lines[10:245]
    country_value_pairs = get_value_pairs(country_value_pairs, countries)
    country_schema = StructType([
        StructField("country_code", StringType()),
        StructField("country_name", StringType())])
    df_countries = spark.createDataFrame(data=country_value_pairs,schema=country_schema)
    df_countries_new = df_countries.withColumn('country_code', df_countries['country_code'].cast(DoubleType()))
    
    # Get list of all visa codes and names from SAS labels and descriptions file
    visas = lines[1046:1049]
    visa_value_pairs = list()
    visa_value_pairs = get_value_pairs(visa_value_pairs, visas)
    visa_schema = StructType([
        StructField("visa_code", StringType()),
        StructField("visa_type", StringType())])
    df_visas = spark.createDataFrame(data=visa_value_pairs,schema=visa_schema)
    df_visas_new = df_visas.withColumn('visa_code', df_visas['visa_code'].cast(DoubleType()))
    
    # Join df_countries and df_visas with i_94 
    df_i94_new = df_i94.join(df_countries_new).where(df_i94['i94res'] == df_countries_new['country_code'])\
        .join(df_visas_new).where(df_i94['i94visa'] == df_visas_new['visa_code'])

    # Remove duplicate rows of immigration table along with columns that are not relevant
    immigration_table = df_i94_new.select("i94yr", "i94mon", "i94cit","i94port", "arrdate", "i94mode",   "i94addr","depdate","i94bir","count","matflag","biryear","dtaddto","gender","insnum","airline","admnum","fltno","visatype","country_code","country_name","visa_code","visa_type").distinct()
        
    # Remove duplicate rows of city table and replace all column names 
    # that have a space with underscore from city_table. Drop any null values of city table.
    city_table = df_city.select("City", "State", col("Median Age").alias("Median_Age"), \
        col("Male Population").alias("Male_Population"), col("Female Population").alias("Female_Population"), \
        col("Total Population").alias("Total_Population"), col("Number of Veterans").alias("Number_of_Veterans"), \
        "Foreign-born", col("Average Household Size").alias("Average_Household_Size"), \
        col("State Code").alias("State_Code"), "Race", "count").na.drop().distinct()

    # Remove duplicate rows and any rows with null values of airport table
    airport_table = df_airport.na.drop().distinct()
    
    # Create parquet file from immigration_table
    immigration_table.write.partitionBy("i94yr","i94mon","i94port").mode("ignore").parquet(OUTPUT_DATA + "immigration_table.parquet")
    # Create table view from immigration_table
    immigration_table.createOrReplaceTempView("immigration_view")
    
    # Create parquet file from city_table
    city_table.write.partitionBy("state_code").mode("ignore").parquet(OUTPUT_DATA + "city_table.parquet")
    # Create table view from city_table
    city_table.createOrReplaceTempView("city_view")
    
    # Create parquet file from airport_table
    airport_table.write.partitionBy("iata_code").mode("ignore").parquet(OUTPUT_DATA + "airport_table.parquet")
    # Create table view from airport_table
    airport_table.createOrReplaceTempView("airport_view")

    # Call quality check function to check that each of the 3 table dataframes has records
    quality_check(config, immigration_table,'immigration_table','count')
    quality_check(config, city_table,'city_table','count')
    quality_check(config, airport_table,'airport_table','count')
    
    # Call quality check function to check that each parquet directory has been created
    parquet_dir_check = OUTPUT_DIR + 'immigration_table.parquet/'
    quality_check(config, parquet_dir_check,'immigration_table.parquet','file_exist')
    parquet_dir_check = OUTPUT_DIR + 'city_table.parquet/'
    quality_check(config, parquet_dir_check,'city_table.parquet','file_exist')
    parquet_dir_check = OUTPUT_DIR + 'airport_table.parquet/'
    quality_check(config, parquet_dir_check,'airport_table.parquet','file_exist')
    
def append_dataframes(dfa,dfb):
    """
    Description: This function merges 2 dataframes
    Arguments: Dataframe (a), dataframe (b)
    Returns: A new dataframe after union of 2 dataframes
    """
    lista = dfa.columns
    listb = dfb.columns
    for col in listb:
        if(col not in lista):
            dfa = dfa.withColumn(col, F.lit(None))
    for col in lista:
        if(col not in listb):
            dfb = dfb.withColumn(col, F.lit(None))
    return dfa.unionByName(dfb)

def get_value_pairs(value_pairs, pairs):
    """
    Description: This function retrieves the pair codes and names from SAS labels and descriptions file
    Arguments: Empty list of codes/names, lines to get codes/names from SAS file
    Returns: List of codes/names
    """
    for pair in pairs:
        value_code = pair.split("=")[0]
        value_name = pair.split("=")[1]
        value_pairs.append((value_code, value_name))
    return value_pairs

def quality_check(config, check_data,check_name,check_function):
    """
    Description: This function runs quality checks
    Arguments: Config object, quality check, quality check name, check function
    Returns: Number of records for the quality check
    """
    
    if check_function == 'file_exist':    
        s3 = boto3.resource('s3')
        BUCKET_NAME = config['DATA']['BUCKET_NAME']
        bucket = s3.Bucket(BUCKET_NAME)
        objs = list(bucket.objects.filter(Prefix=check_data))
        result = len(objs)
        if (len(objs) > 0):
            result = 'exist'
    elif check_function == 'count': 
        result = check_data.count() 
        
    if result == 0:
        print("FAILED DATA QUALITY CHECK for {} with zero records".format(check_name))
    else:
        print("PASSED DATA QUALITY CHECK for {} with {} records".format(check_name, result))
        
def main():
    """
    Description: This function reads config file, retrieves AWS access keys, creates a Spark session then reads immigration,            city demographics and airport data from local workspace folder to transform data then creates parquet files in S3. 
    Arguments: None
    Returns: None
    """
    
    # Read config file
    config = configparser.ConfigParser()
    config.read_file(open('capstone.cfg'))
    os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']

    spark = create_spark_session()
    
    process_data(spark, config)    

if __name__ == "__main__":
    main()
