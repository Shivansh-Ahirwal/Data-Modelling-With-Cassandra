# QUERIES FOR PROJECT: DATA MODELLING WITH CASSANDRA


# CREATE QUERIES FOR TABLES AND KEYSPACES

CREATE_SESSION_SONGS_TABLE ="""
    CREATE TABLE IF NOT EXISTS session_songs
    (sessionId int, itemInSession int, artist text, song_title text, song_length float,
    PRIMARY KEY(sessionId, itemInSession))
    """

CREATE_EVENT_LOG_TABLE = """
    CREATE TABLE IF NOT EXISTS event_log 
    (artist text, 
    song text, 
    first_name text, 
    last_name text,
    user_id int,
    session_id int,
    item_in_session int, 
    primary key ((user_id, session_id), item_in_session))
    """

CREATE_SONG_USERS_TABLE = """
    create table if not exists song_users (
    song text, user_id int, first_name text, last_name text, 
    primary key (song, user_id))
    """

CREATE_KEYSPACE_SPARKIFY = """
    create keyspace if not exists sparkify 
    with replication = {'class': 'SimpleStrategy' , 'replication_factor': 1 }
    """


# DROP QUERIES FOR TABLES AND KEYSPACES

DROP_SESSION_SONGS_TABLE = """
    DROP TABLE IF EXISTS session_songs
    """

DROP_EVENT_LOG_TABLE = """
    DROP TABLE IF EXISTS event_log
    """

DROP_SONG_USERS_TABLE = """
    DROP TABLE IF EXISTS song_users
    """

DROP_KEYSPACE_SPARKIFY = """
    DROP KEYSPACE IF EXISTS sparkify
    """

# INSERT DATA INTO TABLES QUERIES

INSERT_INTO_SESSION_SONGS = """
    INSERT INTO TABLE session_songs (song, user_id, first_name, last_name) VALUES(%s,%s,%s,%s)
    """

INSERT_INTO_EVENT_LOG = """
    INSERT INTO event_log (artist, song, first_name, last_name, user_id, session_id, item_in_session) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

INSERT_INTO_SONG_USERS = """
    INSERT INTO song_users (song, user_id, first_name, last_name) VALUES (%s, %s, %s, %s)
    """