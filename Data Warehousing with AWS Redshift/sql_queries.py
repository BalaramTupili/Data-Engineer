import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS  staging_events 
(
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender CHAR(1),
    itemInSession INTEGER,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT,
    userAgent VARCHAR,
    userId INTEGER
    )
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS  staging_songs 
(
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude numeric(11,8),
    artist_longitude numeric(11,8),
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year INTEGER
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS  users
(
    user_id int PRIMARY KEY,
    first_name varchar,
    last_name varchar,
    gender char(1),
    level varchar
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS  songs
(
    song_id varchar PRIMARY KEY,
    title varchar,
    artist_id varchar ,
    year int,
    duration float
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS  artists
(
    artist_id varchar PRIMARY KEY,
    name varchar,
    location varchar,
    latitude numeric(11,8),
    longitude numeric(11,8)
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS  times
(
    start_time TIMESTAMP PRIMARY KEY,
    hour int ,
    day int ,
    week int ,
    month int,
    year int,
    weekday int
)
""")


songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
    songplay_id INTEGER IDENTITY(0,1) , 
    start_time TIMESTAMP, 
    user_id INTEGER, 
    level varchar ,
    song_id  VARCHAR,
    artist_id VARCHAR ,
    session_id INTEGER ,
    location varchar, 
    user_agent varchar,
    primary key(songplay_id)
)
""")
# STAGING TABLES

staging_events_copy = ("""
 copy staging_events from 's3://udacity-dend/log_data' 
credentials 'aws_iam_role={}' compupdate off
 region 'us-west-2'
  JSON 's3://udacity-dend/log_json_path.json';
""").format(config.get('IAM_ROLE', 'ARN'))

staging_songs_copy = ("""
copy staging_songs from 's3://udacity-dend/song_data' 
credentials 'aws_iam_role={}' compupdate off
 region 'us-west-2'
 JSON 'auto' ;
""").format(config.get('IAM_ROLE', 'ARN'))


user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level )
select distinct  userId, firstName, lastName, gender, level from staging_events
 WHERE page = 'NextSong' and userId is not null
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration )
select distinct song_id, title, artist_id, year, duration  from staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists ( artist_id, name, location, latitude, longitude )
select distinct  artist_id,artist_name,artist_location,artist_latitude,artist_longitude from  staging_songs
""")


time_table_insert = ("""
INSERT INTO times ( start_time, hour, day, week, month, year, weekday)
SELECT distinct sp.start_time,

EXTRACT (HOUR FROM sp.start_time),
 EXTRACT (DAY FROM sp.start_time),

EXTRACT (WEEK FROM sp.start_time), 
EXTRACT (MONTH FROM sp.start_time),

EXTRACT (YEAR FROM sp.start_time), 
EXTRACT (WEEKDAY FROM sp.start_time) FROM songplays sp ;
""")


songplay_table_insert = ("""
INSERT INTO songplays ( start_time, user_id, level, song_id, artist_id, session_id, location, user_agent )

SELECT distinct
TIMESTAMP 'epoch' + events.ts/1000 * INTERVAL '1 second' as start_time,

events.userId AS user_id,

events.level AS level,

songs.song_id AS song_id,

songs.artist_id AS artist_id,

events.sessionId AS session_id,

events.location AS location,

events.userAgent AS user_agent

FROM staging_events AS events

JOIN staging_songs AS songs

ON (events.artist = songs.artist_name)

AND (events.song = songs.title)

AND (events.length = songs.duration)

WHERE events.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [ user_table_insert, song_table_insert, artist_table_insert,  songplay_table_insert ,time_table_insert]
