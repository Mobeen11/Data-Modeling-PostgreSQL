# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time_table;"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
songplay_id serial PRIMARY KEY NOT NULL, start_time varchar(255), user_id int NOT NULL, level varchar(255) , song_id varchar(255), artist_id varchar(255), session_id int NOT NULL, location varchar(255), user_agent varchar(255));
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
user_id varchar(255) PRIMARY KEY NOT NULL , first_name varchar(255), last_name varchar(255), gender varchar(255), level varchar(255)
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
song_id varchar(255) PRIMARY KEY NOT NULL, title varchar(255), artist_id varchar(255) NOT NULL, year int, duration FLOAT NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
artist_id varchar(255) PRIMARY KEY NOT NULL, name varchar(255), location varchar(255), latitude numeric, longitude numeric
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS timestamps(
start_time varchar(255) PRIMARY KEY, hour int, day varchar(255), week varchar(255), month varchar(255), year int, weekday varchar(255)
);
""")

# INSERT RECORDS
songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (songplay_id) 
DO NOTHING;
""")
# #     timestamp, user ID, level, song ID, artist ID, session ID, location, and user agent
user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level;
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id)
DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) 
DO NOTHING;
""")


time_table_insert = ("""
INSERT INTO timestamps(start_time, hour, day, week, month, year, weekday) VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time) DO NOTHING;
""")

# FIND SONGS
# song ID and artist ID based on the title, artist name, and duration
song_select = ("""
SELECT song_id, artists.artist_id
FROM songs
JOIN artists
ON songs.artist_id = artists.artist_id
WHERE songs.title ~* %s and artists.name ~* %s and songs.duration = %s;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]