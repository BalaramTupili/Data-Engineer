## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app.
The analytics team is particularly interested in understanding what songs users are listening to.
I need to create tables to retrive data using queries

## Project Description
In this project, I'll apply data modeling with Postgres and build an ETL pipeline using Python. To complete the project, I will need to define fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.

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

