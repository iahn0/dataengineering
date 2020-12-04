import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


ARN      = config.get("IAM_ROLE", "ARN")

#(DB_USER,DB_PASSWORD,DB_NAME)


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_songs_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_songs (num_songs INTEGER, artist_id TEXT, artist_latitude NUMERIC,artist_longitude NUMERIC,artist_location TEXT,artist_name TEXT,song_id TEXT,title TEXT, duration NUMERIC, year INTEGER);
""")
                             
staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (artist TEXT, auth TEXT, firstName TEXT,gender TEXT,iteminSession TEXT,lastName TEXT,length NUMERIC,level TEXT, location TEXT, method TEXT, page TEXT, registration NUMERIC, sessionid INTEGER, song TEXT, status INTEGER, ts bigint, userAgent TEXT, userid INTEGER );
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (songplay_id bigint identity(1,1) PRIMARY KEY NOT NULL, start_time bigint NOT NULL, user_id int, level varchar, song_id varchar, artist_id varchar, session_id int, location varchar, user_agent varchar);
""")
#start_time bigint to timestamp has error

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (user_id int PRIMARY KEY NOT NULL, first_name varchar, last_name varchar, gender varchar, level varchar);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY NOT NULL, title varchar, artist_id varchar, year int, duration decimal);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY NOT NULL, artist_name varchar, artist_location varchar, artist_latitude decimal, artist_longitude decimal);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (start_time timestamp PRIMARY KEY NOT NULL, hour int, day int, week int, month int, year int, weekday int);
""")


# STAGING TABLES
staging_songs_copy = ("""
    copy staging_songs from {}
    iam_role {}
    compupdate off region 'us-west-2' json 'auto'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])


staging_events_copy = ("""
    copy staging_events from {}
    iam_role {}
    compupdate off region 'us-west-2' json {}
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])



# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id,artist_id,session_id,location,user_agent) 
SELECT e.ts AS start_time,
e.userid AS user_id,
e.level AS level,
s.song_id AS song_id,
s.artist_id AS artist_id,
e.sessionid AS session_id,
e.location AS location,
e.userAgent AS user_agent
FROM staging_events AS e
JOIN staging_songs AS s
ON (e.artist = s.artist_name)
AND (e.song = s.title)
AND (e.length = s.duration)
WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level) 
SELECT DISTINCT userid AS user_id,
firstName AS first_name,
lastName AS last_name,
gender,
level
FROM staging_events
WHERE page='NextSong' AND userid IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT song_id,
title,
artist_id,
year,
duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude) 
SELECT artist_id,
artist_name,
artist_location,
artist_latitude,
artist_longitude
FROM staging_songs
""")

#resource : Mentor help at https://knowledge.udacity.com/questions/74200
#resource : Meonto help at https://knowledge.udacity.com/questions/47005
#trdoutvr : https://knowledge.udacity.com/questions/230423
    

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
Select distinct ts
,start_time
,EXTRACT(HOUR FROM start_time) As hour
,EXTRACT(DAY FROM start_time) As day
,EXTRACT(WEEK FROM start_time) As week
,EXTRACT(MONTH FROM start_time) As month
,EXTRACT(YEAR FROM start_time) As year
,EXTRACT(DOW FROM start_time) As weekday
FROM (
SELECT distinct ts,'1970-01-01'::date + ts/1000 * interval '1 second' as start_time
FROM staging_events)
""")

'''
time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT start_time, 
        EXTRACT(hour from start_time),
        EXTRACT(day from start_time),
        EXTRACT(week from start_time),
        EXTRACT(month from start_time),
        EXTRACT(year from start_time),
        EXTRACT(dayofweek from start_time)
FROM songplays
""")
'''

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
