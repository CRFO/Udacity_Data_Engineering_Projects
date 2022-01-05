# Project Title: Data Lake Project

## Data Engineering Capstone Project

### Project Summary
This project gathers the US I94 immigration data along with US airport code data and US city demographics data to create a data lake using Pyspark for future use.

### Scope 

Create a data lake with Pyspark with the immigration, airport data and US city demographics by partitioning parquet files in table directories stored in AWS S3 for future use following schema-on-read semantics. The output parquet table directories could be used to analyze immigration trends focucing on immigration origins and airport destinations. The generated partitioned parquet files in table directories are:

- immigration_table
- city_table
- airport_table

#### Example of Future Use:

- Which US airports are the most traveled to in a year? Which ones are the most traveled to in a month for a particular year? What would it be the busiest monthly prediction for the upcoming years? What would it be the busiest days for the upcoming months? Such predictions could help out CBP (U.S. Customs and Border Protection) to forecast US airports insights by relying on advanced analytics. 

### Describe Data 

I-94 Immigration Data: The SAS files come from US National Tourism and Trade Office and have a data dictionary file named "I94_SAS_Labels_Description.SAS". This project loads all immigration data for year 2016. All 12 files have more than 40 million records which fulfills the project requeriment to have a minimum of 1 million record in a file. This project also loads countries and visa categories from "I94_SAS_Labels_Description.SAS".

US City Demographic Data: This data is from OpenSoft and has US city demographics data. For more information, go to
https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/

Airport Code Data: This data is a list of US airport codes, US region, and other information related to aiport data. For more information, go to
https://datahub.io/core/airport-codes#data

### Conceptual Data Model

Generate partitioned parquet files in table directories:

| Table Name      | Description        | Partition By  | 
| --- | --- | --- | 
| immigration_table | Fact table that has US i94 immigration data | i94year, i94mon and i94port |
| city_table | Dimension table that has city demographics data | State Code |
| airport_table | Dimension table that has US airport data | iata_code |

The immigration_table has been partitioned by year, month and airport code for better performance on aggregration queries. This could be even more helpful when loading i94 files from many years. The aiport_table has been aggregrated by iata_code (airport code) while city_table has been aggregrated by state_code. Data dictionary of data model is included below.

This data model was chosen since the project intention is to create a data lake with Pyspark with the immigration, airport and US city demographics data by partitioning parquet files in table directories stored in AWS S3 for future use following schema-on-read semantics.

### Mapping Out Data Pipelines

1. Load all 12 I94 immigration files, city demographics file, airport code file, country information and visa data to Spark dataframes.
2. Join country and visa data to I94 immigration Spark dataframe.
3. Remove duplicated records for each of the 3 Spark dataframes: immigration_table, city_table and airport_table.
4. Remove columns that are not relevant from immigration_table.
5. Remove every row that has any null value from city_table and airport_table.
6. Create parquet table directories partitioned them by the columns listed above from the cleaned Spark dataframes.
7. Create a view for each of the cleaned Spark dataframes to execute SQL queries.
8. Execute quality checks on cleaned Spark dataframes and parquet table directories:
    - Check if each of the 3 Spark dataframes (immigration_table, city_table and airport_table) has records.
    - Check if each parquet directory exists in S3 bucket.
    
### Data Dictionary of Data Model

- **immigration_table**:  It came from 12 files related to i94 immigration data residing in Udacity workspace with code and visa category data.

| Column Name | Description | 
| ----------- | ----------- |
| i94yr | 4 digit year |
| i94mon | 2 digit month |
| i94cit | 3 digit code of country in transit |
| ***i94port*** | 3 character code of aiport (***matches aita_code from airport_table***) |
| arrdate | arrival date |
| i94mode | 1 digit travel code |
| ***i94addr*** | 2 digit state code (***matches state_code from city_table***) |
| depdate | departure date |
| i94bir | age in years |
| count | count summary |
| matflag | match of arrival and departure |
| biryear | 4 digit of birth year |
| dtaddto | date to stay in US |
| gender | gender of the immigrant |
| insnum | immigration number |
| airline | airline code |
| admnjum | admission number |
| fltno | flight number |
| visatype | type of visa |
| country_code | 3 digit code of origin country |
| country_name | name of origin country |
| visa_code | 1 digit visa code |
| visa_code | visa category |

 - **city_table**: It came from US cities demographics file.
 
 | Column Name | Description | 
 | ----------- | ----------- |
 | city | name of city |
 | state | name of state |
 | median_age | median age of city |
 | male_population | population of males |
 | female_population | population of females |
 | total_population | total population of city |
 | number_of_veterans | number of veterans of city |
 | foreign_born | foreign born |
 | average_household_size | size of average household |
 | ***state_code*** | 2 digit state code (***matches i94addr from immigration_table***) |
 | race | race |
 | count | count |
 
 - **airport_table**: It came from US airport codes file.
 
 | Column Name | Description | 
 | ----------- | ----------- |
 | ident | airport identification |
 | type | airport type |
 | name | airport name | 
 | elevation_ft | feet elevation |
 | continent | continent |
 | iso_country | country code |
 | iso_region | 2 digit country - 2 digit state |
 | municipality | municipality |
 | gps code | GPS code |
 | ***iata_code*** | airport code (***matches i94_port from immigration_table***) |
 | local_code | local code |
 | coordinates | coordinates |
 
### Tools/Technologies

- This project uses Python version 3.6.3 with Pyspark.
- Apache Spark (Pyspark) is used due to its parallelism and scalability when handling large datasets.
- Parquet output files in columnar format are used for aggregation.

### Propose How Often the Data Should Be Updated and Why.

- The I94 immigration data is aggregrated on year, month and airport code basis. Therefore updating the data monthly would be ideal.

### Steps to Execute Code
- Update variables in capstone.cfg as needed.
- Execute "python capstone.py" from project root in terminal. Note: Another option is to execute Python Notebook "Capstone_Project.ipynb".

### Approach under the Following Scenarios:

#### The data was increased by 100x.

- Use AWS EMR & Spark to process the data. This can be executed after creating a AWS EMR cluster, copying capstone.cfg and capstone.py files and running capstone.py to hadoop file system in cluster then executing pythoin file with the spark-submit command.
 
##### The data populates a dashboard that must be updated on a daily basis by 7am every day.
 - A schematized data warehouse should be created based on the parquet output files using Apache Airflow to schedule a Spark job to run on a daily basis. In case of any issue arises, email notification can be setup to notify a team to fix any errors. 
 
##### The database needed to be accessed by 100+ people.
 - Use AWS Redshift to load the parquet output files. Then a schematized data warehouse should be created based on the parquet output files. Apache Cassandra could also be used due to its scalability and good read performance for large datasets.
 