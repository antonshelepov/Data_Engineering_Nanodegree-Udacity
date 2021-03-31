songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# create tables
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id SERIAL, \
                                                                    start_time TIMESTAMP NOT NULL, \
                                                                    user_id INTEGER NOT NULL, \
                                                                    level VARCHAR NOT NULL, \
                                                                    song_id VARCHAR, \
                                                                    artist_id VARCHAR, \
                                                                    session_id INTEGER NOT NULL, \
                                                                    location TEXT, \
                                                                    user_agent TEXT, \
                                                                    PRIMARY KEY(songplay_id));
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id INTEGER, \
                                                            first_name VARCHAR, \
                                                            last_name VARCHAR, \
                                                            gender VARCHAR, \
                                                            level VARCHAR NOT NULL, \
                                                            PRIMARY KEY(user_id));
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id VARCHAR, \
                                                            title TEXT NOT NULL, \
                                                            artist_id VARCHAR NOT NULL, \
                                                            year INTEGER, \
                                                            duration NUMERIC, \
                                                            PRIMARY KEY(song_id));
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id VARCHAR, \
                                                                artist_name TEXT NOT NULL, \
                                                                artist_location VARCHAR, \
                                                                artist_latitude NUMERIC, \
                                                                artist_longitude NUMERIC, \
                                                                PRIMARY KEY(artist_id));
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time TIMESTAMP, \
                                                          hour INTEGER NOT NULL, \
                                                          day INTEGER NOT NULL, \
                                                          week INTEGER NOT NULL, \
                                                          month INTEGER NOT NULL, \
                                                          year INTEGER NOT NULL, \
                                                          weekday INTEGER NOT NULL,
                                                          PRIMARY KEY(start_time));
""")

# insert records

songplay_table_insert = ("""INSERT INTO songplays (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent) 
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s) 
                            ON CONFLICT (songplay_id) 
                                DO NOTHING;
""")
  
user_table_insert = ("""INSERT INTO users (user_id,first_name,last_name,gender,level) 
                        VALUES (%s,%s,%s,%s,%s) 
                        ON CONFLICT (user_id) 
                            DO UPDATE SET level = EXCLUDED.level;
""")

song_table_insert = ("""INSERT INTO songs (song_id,title,artist_id,year,duration) 
                        VALUES (%s,%s,%s,%s,%s) 
                        ON CONFLICT (song_id) 
                            DO NOTHING;
""")

artist_table_insert = ("""INSERT INTO artists (artist_id,artist_name,artist_location,artist_latitude,artist_longitude) 
                          VALUES (%s,%s,%s,%s,%s) 
                          ON CONFLICT (artist_id) 
                            DO NOTHING;
""")


time_table_insert = ("""INSERT INTO time (start_time,hour,day,week,month,year,weekday) 
                        VALUES (%s,%s,%s,%s,%s,%s,%s) 
                        ON CONFLICT (start_time) 
                            DO NOTHING;
""")

# find songs

song_select = ("""SELECT song_id, songs.artist_id 
                  FROM songs 
                  JOIN artists 
                    ON songs.artist_id=artists.artist_id 
                  WHERE songs.title=%s 
                    AND artists.artist_name=%s 
                    AND songs.duration=%s;
""")

# query lists
create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
