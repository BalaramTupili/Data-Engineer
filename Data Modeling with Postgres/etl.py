import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
 
def process_song_file(cur, filepath):
    """This Function process song data to insert into song and artist tables"""
    # open song file
    df = pd.read_json(filepath, lines=True);

    # insert song record and each file conatins one record
    song_data = list(df[['song_id','title','artist_id','year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """This Function process log data to insert into user, time and songplay tables"""
    # open log file
    df = pd.read_json(filepath, lines=True);

    # filter by NextSong action
    time_data = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(time_data['ts'])
    # insert time data records
    dctnry = {'start_time': t,'hour':t.dt.hour,'day':t.dt.day,'week':t.dt.week ,'month': t.dt.month,'year': t.dt.year,'weekday': t.dt.weekday}
    time_df = pd.DataFrame(dctnry)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]
    # insert user records
    for i, row in user_df.iterrows():
        if row.userId == ''  or not row.userId:
            continue
        cur.execute(user_table_insert,(row.userId,row.firstName,row.lastName,row.gender,row.level))

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None
            
        if row.userId == ''  or not row.userId:
            continue

        
        time_id = pd.to_datetime(row.ts)

        # insert songplay record
        songplay_data = (time_id,row.userId,row.level,song_id,artist_id,row.sessionId,row.location,row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """This function retrives all the json files and calls func to insert data"""
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """Main function to process data"""
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()