# drop tables

songplay_table_drop = "drop table if exists songplays;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists songs;"
artist_table_drop = "drop table if exists artists;"
time_table_drop = "drop table if exists time;"

# create tables
#enum_gen = ("""create type gen as enum ('m','f','other');""")
#enum_lev = ("""create type lev as enum ('paid','free');""")
songplay_table_create = ("""create table if not exists songplays (songplay_id int, \
                                                                    start_time timestamp, \
                                                                    user_id int, \
                                                                    level varchar, \
                                                                    song_id int, \
                                                                    artist_id varchar, \
                                                                    session_id int, \
                                                                    location text, \
                                                                    user_agent text, \
                                                                    primary key (songplay_id));
""")

user_table_create = ("""create table if not exists users (user_id int, \
                                                            first_name varchar, \
                                                            last_name varchar, \
                                                            gender varchar, \
                                                            level varchar, \
                                                            primary key (user_id));
""")

song_table_create = ("""create table if not exists songs (song_id varchar, \
                                                            title text, \
                                                            artist_id varchar, \
                                                            year int, \
                                                            duration numeric, \
                                                            primary key (song_id));
""")

artist_table_create = ("""create table if not exists artists (artist_id varchar, \
                                                                artist_name text, \
                                                                artist_location varchar, \
                                                                artist_latitude numeric, \
                                                                artist_longitude numeric, \
                                                                primary key (artist_id));
""")

time_table_create = ("""create table if not exists time (start_time timestamp, \
                                                          hour int, \
                                                          day int, \
                                                          week int, \
                                                          month int, \
                                                          year int, \
                                                          weekday int,
                                                          primary key (start_time));
""")

# insert records

songplay_table_insert = ("""INSERT INTO songplays (songplay_id,start_time,user_id,level,song_id,artist_id,session_id,location,user_agent) 
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) 
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
#enums_queries = [enum_gen,enum_lev]
create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]