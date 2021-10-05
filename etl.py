import os
import re
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
     Description: This function process the songs file 
                  and insert data in artist and song table..

            Parameters:
                    cur: cursor object to execute queries  
                    filepath: path of the song files
            Return: 
                None
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)
    
    # replace special characters with space to avoid any potential errors     
    song_data = df['title'] = df['title'].map(lambda x: re.sub(r'\W+', ' ', x))
    
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]]
    song_data = list(song_data.values[0])
    # insert song record
    cur.execute(song_table_insert, song_data)
    
    # replace special characters with space to avoid any potential errors     
    df['artist_name'] = df['artist_name'].map(lambda x: re.sub(r'\W+', ' ', x))
    # insert artist record
    artist_data = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]]
    artist_data = list(artist_data.values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
     Description: This function process the log file and
                  insert data in user and time table.

            Parameters:
                    cur: cursor object to execute queries  
                    filepath: path of the song files
            Return:
                None
            
    """
    # open log file
    df = pd.read_json(filepath, lines=True)
    # replace special characters with space to avoid any potential errors     
    df['artist'] = df['artist'].map(lambda x: re.sub(r'\W+', ' ', str(x)))
    df['song'] = df['song'].map(lambda x: re.sub(r'\W+', ' ', str(x)))

    # filter by NextSong action
    df = df[df['page'] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.DatetimeIndex(df['ts'])
    
    # insert time data records
    time_data = (t.time, t.hour, t.day, t.weekofyear, t.month, t.year, t.weekday)
    column_labels = ("timestamp","hour", "day", "week_of_year", "month", "year", "weekday")
    
    # dict creation for time_df      
    timeDict = {}
    for i in range(len(time_data)):
        timeDict[column_labels[i]] = time_data[i]
    time_df = pd.DataFrame.from_dict(timeDict)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName', 'lastName', 'gender', 'level']]

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
        songplay_data = (pd.to_datetime(row.ts).time(), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Description: This function is responsible for listing the files in a directory,
    and then executing the ingest process for each file according to the function
    that performs the transformation to save it to the database.
    
    Process the songs file and insert data in artist and song table.

            Parameters:
                    cur: cursor object to execute queries  
                    conn: connection of database
                    filepath: path of the files (song and log files)
                    func:  function that transforms the data and inserts it into the database.
                    
            Return: 
                returns the path of files 
            
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()