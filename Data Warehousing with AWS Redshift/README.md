## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app.
The analytics team is particularly interested in understanding what songs users are listening to.
I need to create tables to retrive data using queries


## Project Description
In this project, I'll apply data modeling with Redshift using AWS and build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

### Project Datasets
The datasets that reside in S3
Song data: s3://udacity-dend/song_data
Log data: s3://udacity-dend/log_data
Log data json path: s3://udacity-dend/log_json_path.json

### Files
create_tables.py drops and creates tables. We run this file to reset your tables before each time you run your ETL scripts.

etl.py reads and processes files from song_data and log_data and loads them into your tables.

sql_queries.py contains all your sql queries, and is imported into create_tables.py and etl.py files.

### SCHEMA
##### Fact Table
songplays - records in log data associated with song plays i.e. records with page NextSong
Columns:
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
##### Dimension Tables
users - users in the app
Columns:
user_id, first_name, last_name, gender, level

songs - songs in music database
Columns:
song_id, title, artist_id, year, duration

artists - artists in music database
Columns:
artist_id, name, location, latitude, longitude

time - timestamps of records in songplays broken down into specific units
Columns:
start_time, hour, day, week, month, year, weekday

![alt text](https://udacity-reviews-uploads.s3.us-west-2.amazonaws.com/_attachments/38715/1584109948/Song_ERD.png)