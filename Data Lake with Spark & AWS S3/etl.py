import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import types as t

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS_KEYS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS_KEYS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """This function creates new session """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """This Function loads songs data from s3 bucket and process data and loads songs and  artists table data as parquet files to s3 bucket """
    # get filepath to song data file
    song_data = input_data+ 'song_data/*/*/*/*.json' 
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("songs_table")
    # extract columns to create songs table
    songs_table = spark.sql("""
                    SELECT DISTINCT 
                            song_id, 
                            title, 
                            artist_id, 
                            year, 
                            duration
                    FROM songs_table
                            """)
    songs_table.printSchema()
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite")\
    .partitionBy("year", "artist_id")\
        .parquet(output_data+"songs_table/")

    # extract columns to create artists table
    artists_table = spark.sql("""
        SELECT  DISTINCT
                artist_id        AS artist_id,
                artist_name      AS name,
                artist_location  AS location,
                artist_latitude  AS latitude,
                artist_longitude AS longitude
        FROM songs_table
    """)
    artists_table.printSchema()
    # write artists table to parquet files
    artists_table.write.mode("overwrite")\
        .parquet(output_data+"artists_table/")


def process_log_data(spark, input_data, output_data):
        """This Function loads events log data from s3 bucket and process data and loads users, time and songplay tables data as parquet files to s3 bucket """
    # get filepath to log data file
    log_data = input_data+ 'log_data'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_filtered = df.filter(df.page == 'NextSong')
    df_filtered.createOrReplaceTempView("events_table")
    # extract columns for users table    
    users_table = spark.sql("""
                            SELECT  DISTINCT userId    ,
                                             firstName ,
                                             lastName  ,
                                             gender,
                                             level
                            FROM events_table
                            WHERE  userId is not null
                            """)
    users_table.printSchema()
    # write users table to parquet files
    users_table.write.mode("overwrite")\
    .parquet(output_data+"users_table/")
    
    
    #reference :https://medium.com/@ayplam/developing-pyspark-udfs-d179db0ccc87
    # create timestamp column from original timestamp column
    @udf(returnType= t.StringType())
    def get_datetime(ts):
        return datetime.fromtimestamp(ts / 1000.0)\
                       .strftime('%Y-%m-%d %H:%M:%S')
    df_filtered_time = df_filtered.withColumn("datetime", \
                        get_datetime("ts"))
    df_filtered_time.createOrReplaceTempView("event_time_table")
    # extract columns to create time table
    time_table = spark.sql("""
                           SELECT  DISTINCT datetime AS start_time,
                                             hour(datetime) AS hour,
                                             day(datetime)  AS day,
                                             weekofyear(datetime) AS week,
                                             month(datetime) AS month,
                                             year(datetime) AS year,
                                             dayofweek(datetime) AS weekday
                            FROM event_time_table
                            """)
    time_table.printSchema()
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite")\
    .partitionBy("year", "month")\
    .parquet(output_data+"time_table/")

    # read in song data to use for songplays table
    # get filepath to song data file
    song_data = input_data+ 'song_data/*/*/*/*.json'
    
    # read song data file
    song_df = spark.read.json(song_data)
    song_df.createOrReplaceTempView("songs_table")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                               SELECT monotonically_increasing_id() as songplay_id,
                                        lt.datetime AS start_time,
                                        month(lt.datetime) as month,
                                        year(lt.datetime) as year,
                                        lt.userId as user_id,
                                        lt.level as level,
                                        st.song_id as song_id,
                                        st.artist_id as artist_id,
                                        lt.sessionId as session_id,
                                        lt.location as location,
                                        lt.userAgent as user_agent
                                FROM event_time_table lt
                                JOIN songs_table st 
                                on lt.artist = st.artist_name and lt.song = st.title
                            """)

    songplays_table.printSchema()
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite")\
    .partitionBy("year", "month")\
    .parquet(output_data+"songplays_table/")


def main():
    """This is main function to call remaining functions to process data"""
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/" 
    output_data = "s3a://mydend-data-lake/Assignments/"
    
    process_log_data(spark, input_data, output_data)
    process_song_data(spark, input_data, output_data)    
    


if __name__ == "__main__":
    main()
