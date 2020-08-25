# Project: Data Pipelines with Airflow
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

I have been asked to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Project Description
This project introduces the core concepts of Apache Airflow. To complete the project, I needed to create my own custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step. The project also inlcudes connections to Amazon S3 buckets and Redshift. I confonfigured a DAG with default parameters including doest not have depencies on past runs, retry on failure three times, retry after 5 minute pause, Catchup turned off, and no email on retry. All of these defaults are easily changed when needed. 

## Building Staging Operator
The Staging Operator reads JSON formated files from S3 to Amazon Redshift. Utilizing the Postgres connection, the operator creates and runs a SQL COPY statement to read and load the staging tables in Redshift. The statement is created from parameters based on the S3 file data. The operator can used either JSON 'auto' or a JSON log_path. This allows flexiblity in JSON files usage. 

## Building Fact and Dimension Operators
The Fact and Dimension Operators take the data stored in the two Staging tables and populates the song_plays fact table and users, songs, artists, and time dimension tables. I also included a parameter to allow switching between insert and append mode. Insert mode clears the tables before populating the tables, append mode just populates the table.

## Data Quality Operator
The data quality operator runs data checks on the data. The first data check is to check for empty tables with no rows. The second check check for NULL values in the id columns of the tables. For each test, the test result is compared to an expected result. If they don't match an exception is raised and the task will retry and fail eventually.

## Schema
I used a star schema that is optimized for queries on song play analysis.  
The Fact Table is songplay_table it has six of the eight columns are from staging_events data (start_time, userId, level, sessionId, location, userAgent) and two from staging_songs (song_id, artist_id).  
The Dimension Tables are as follows:  
users - _users in the app_ (user_id, first_name, last_name, gender, level)   
songs - _songs in the music database_ (song_id, title, artist_id, year, duration)  
artists - _artist in the music database_ (artist_id, name, location, lattitude, longitude)  
time - _timestamps of records in songplays broken into units in sperate columns_ (start_time, hour, day, week, month, year, weekday)  


![Project 3 Schema](https://github.com/seisolo76/UDACITY-Project-3-Data-Warehouse/blob/master/Project%203%20schema.png)

## Project Files
### DAG files
create_tables.py - creates the tables in redshift.
etl.py - main program to extract the data from two s3 buckets. Transform from two data sources to five tables. Data quality checks performed.
### Helper files
create_table_sql.py - contains the SQL code to create the tables in redshift.
sql_quries.py - contains the SWL code to load the tables in redshift
### Operators
data_quality.py - contains the data quality operator.
load_dimension.py - contains the load_dimension table operator.
load_fact.py - contains the load fact table operator.
stage_redshift.py - contains the staging tables operator.
README.MD - This file

## Execute Script
use Airflow to execute both create_tables.py and etl.py. Ensure that the connections for redshift and aws_credentials are created in airflow.

