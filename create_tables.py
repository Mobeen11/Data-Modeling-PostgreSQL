import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """
    
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def select_songs():
    """
    select the song and artist from the database
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    cur.execute("""
                SELECT *
                FROM songs
                JOIN artists
                ON songs.artist_id = artists.artist_id
                WHERE songs.title ~* 'I Didn''t Mean To' and artists.name ~* 'Casual' and songs.duration = 218.93179;
                """)
    
    print("cur: ", cur.fetchone())
    conn.close()

def select_time():
    """
    select the time and user
    """
#     timestamp, user ID, level, song ID, artist ID, session ID, location, and user agent
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    cur.execute("""
                SELECT *
                FROM timestamps
                JOIN users
                ON songs.artist_id = artists.artist_id
                WHERE songs.title ~* 'I Didn''t Mean To' and artists.name ~* 'Casual' and songs.duration = 218.93179;
                """)
    
    print("cur: ", cur.fetchone())
    conn.close()

def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)
    
#     select_songs()
    conn.close()


if __name__ == "__main__":
    main()