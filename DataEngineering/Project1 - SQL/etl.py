import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import datetime

def process_song_file(cur, filepath):
    """
    Extracts data from the song data json files in the song_data directory/subdirectories
    Converts  data into format readable to PostGreSQL insert statements
    Calls INSERT queries song_table_insert and artist_table_insert to load into sparkifydb
    Parameters
    - cur - cursor for PostgreSQL database
    - filepath - relative path to single json file containing song data

    DB Insertions
    ['song_id', 'title', 'artist_id', 'year', 'duration'] -> songs
    ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'] -> artists

    fields title, artist_name, artist_location are likely to contain the ' character, thus escape characters need to be used to handle these
    """
    # open song file
    df = pd.read_json(filepath, lines=True)
    # Replace ' with '' to act as escape character in PostGreSQL queries
    df['title'] = df['title'].str.replace("'", "''")
    df['artist_name'] = df['artist_name'].str.replace("'", "''")
    df['artist_location'] = df['artist_location'].str.replace("'", "''")

    song_list = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values.tolist()
    for song_data in song_list:
    # insert song record
        cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_list = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values.tolist()
    for artist_data in artist_list:
        # artist_data = ""
        cur.execute(artist_table_insert, artist_data)

def process_log_file(cur, filepath):
    """
    Extracts data from the log data json files in the log_data directory/subdirectories
    Converts  data into format readable to PostGreSQL insert statements
    Calls INSERT queries user_table_insert, time_table_insert and songplay_table_insert to load into sparkifydb

    Parameters
    - cur - cursor for PostgreSQL database
    - filepath - relative path to single json file containing log data of songs that have been played

    DB Insertions
    ["start_time", "hour", "day", "week", "month", "year", "weekday"] -> time
    ['userId', 'firstName', 'lastName', 'gender', 'level'] -> artists -> users
    ['start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent'] -> songplays

    - song_id, artist_id are determined from the query song_select, which extracts these from a join of tables songs and artists, with 
    search terms songs.title AND artists.name AND songs.duration
    - fields firstName, lastName are likely to contain the ' character, thus escape characters need to be used to handle these
    """
    # open log file
    print(filepath)
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = list(zip(t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday))
    column_labels = ["start_time", "hour", "day", "week", "month", "year", "weekday"]
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = pd.read_json(filepath, lines=True)[['userId', 'firstName', 'lastName', 'gender', 'level']]
    # filter out user rows without a userId, as this contains null and possibly corrupt data 
    # print(user_df)
    print(user_df.dtypes)

    user_df = user_df[user_df['userId'].astype('str') != '']
    # Replace ' with '' to act as escape character in PostGreSQL queries
    user_df['firstName'] = df['firstName'].str.replace("'", "''")
    user_df['lastName'] = df['lastName'].str.replace("'", "''")

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        # songplay_id is a serial object that auto-increments - functions as primary key
        songplay_data = [
            pd.to_datetime(row.ts, unit='ms'), 
            row.userId, 
            row.level, 
            songid, 
            artistid, 
            row.sessionId, 
            row.location,
            row.userAgent
            ]
        cur.execute(songplay_table_insert, songplay_data)

def process_data(cur, conn, filepath, func):
    """
        Generates a list of filepaths to all song or log data, and iterates through this list with the relevant function to handle the json data file to perform ETL.
        Parameters
        - cur - cursor for PostgreSQL database
        - conn - connection object for PostGreSQL database
        - filepath - relative path to single json file containing song data
        - func - function to use to handle the json data files
    """
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
    """
    Entry point for etl program
    Initialises connection to sparkifydb
    Calls process_data with func argument to handle song or log file data respectively
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    try:
        process_data(cur, conn, filepath='data/song_data', func=process_song_file)
        process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    except Exception as e:
        print(str(e))
    finally:
        conn.close()

if __name__ == "__main__":
    main()