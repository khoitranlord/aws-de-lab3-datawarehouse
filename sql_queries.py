import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN                    = config.get("IAM_ROLE", "ARN")

LOG_DATA               = config.get("S3", "LOG_DATA")
LOG_JSONPATH           = config.get("S3", "LOG_JSONPATH")
SONG_DATA              = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = """
    DROP TABLE IF EXISTS staging_events;
    """
staging_songs_table_drop = """
    DROP TABLE IF EXISTS staging_songs
    """
songplay_table_drop = """
    DROP TABLE IF EXISTS songplay
    """
user_table_drop = """
    DROP TABLE IF EXISTS users
    """
song_table_drop = """
    DROP TABLE IF EXISTS songs
    """
artist_table_drop = """
    DROP TABLE IF EXISTS artists
    """
time_table_drop = """
    DROP TABLE IF EXISTS time
    """

# CREATE TABLES


staging_songs_table_create = ("""
 CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs INTEGER NOT NULL,
        artist_id VARCHAR NOT NULL SORTKEY DISTKEY,
        artist_latitude NUMERIC,
        artist_longitude NUMERIC,
        artist_location VARCHAR,
        artist_name VARCHAR NOT NULL,
        song_id VARCHAR NOT NULL,
        title VARCHAR NOT NULL,
        duration NUMERIC NOT NULL,
        year NUMERIC
    );
""")

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist        varchar,
        auth          varchar NOT NULL,
        firstName     text,
        gender         char,
        itemInSession integer NOT NULL,
        lastName      text,
        length        numeric,
        level         text NOT NULL,
        location      varchar,
        method        varchar NOT NULL,
        page          text,
        registration  numeric,
        sessionId     integer NOT NULL SORTKEY DISTKEY,
        song          varchar,
        status        integer NOT NULL,
        ts            timestamp NOT NULL,
        userAgent     varchar,
        userId        integer 
 )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id int IDENTITY(0,1) NOT NULL PRIMARY KEY , 
        start_time  timestamp         NOT NULL SORTKEY DISTKEY, 
        user_id     int               NOT NULL, 
        level       text              NOT NULL, 
        song_id     varchar,
        artist_id   varchar, 
        session_id  int               NOT NULL, 
        location    varchar, 
        user_agent  varchar           NOT NULL
    )
    DISTSTYLE KEY;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs ( 
        song_id   varchar DISTKEY PRIMARY KEY, 
        title     varchar NOT NULL,
        artist_id varchar, 
        year      int     SORTKEY, 
        duration  numeric
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users ( 
        user_id    int  NOT NULL PRIMARY KEY, 
        first_name text, 
        last_name  text SORTKEY, 
        gender     char, 
        level      text NOT NULL
    ) diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time ( 
        start_time timestamp NOT NULL SORTKEY PRIMARY KEY, 
        hour       int       NOT NULL, 
        day        int       NOT NULL, 
        week       int       NOT NULL, 
        month      int       NOT NULL,
        year       int       NOT NULL, 
        weekday    int       NOT NULL
    ) diststyle all;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists ( 
        artist_id varchar NOT NULL PRIMARY KEY, 
        name      varchar NOT NULL SORTKEY, 
        location  text, 
        latitude  numeric, 
        longitude numeric
    ) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    json {}
    TIMEFORMAT 'epochmillisecs';
""").format(LOG_DATA,ARN,LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    json 'auto';
""").format(SONG_DATA,ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (song_id, artist_id, session_id, level, start_time, user_id, location, user_agent)
        SELECT DISTINCT ss.song_id, ss.artist_id, se.sessionId,  se.level, se.ts, se.userId, se.location, se.userAgent
        FROM staging_events se LEFT JOIN staging_songs ss ON ( se.song = ss.title AND se.artist = ss.artist_name)
        WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, level, gender)
        SELECT DISTINCT userId, firstName, lastName, level, gender
        FROM staging_events se1
        WHERE userId IS NOT null
        AND ts = (SELECT max(ts) FROM staging_events se2 WHERE se1.userId = se2.userId)
        ORDER BY userId DESC;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, artist_id, title, year, duration)
        SELECT DISTINCT song_id, artist_id, title, year, duration
        FROM staging_songs
        where song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
        SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, weekday, year)
        SELECT DISTINCT ts, 
                        EXTRACT(hour FROM ts) AS hour,
                        EXTRACT(day FROM ts) AS day,
                        EXTRACT(week FROM ts) AS week,
                        EXTRACT(month FROM ts) AS month,
                        EXTRACT(weekday FROM ts) as weekday,
                        EXTRACT(year FROM ts) as year
        FROM staging_events
        WHERE page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
