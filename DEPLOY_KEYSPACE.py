from cassandra.cluster import Cluster
from CQL_QUERIES import *

def create_keyspace(name):
    
    try:
        cluster = Cluster(['cassandra'])
        session = cluster.connect()
        print("Connection Established")
    except Exception as e:
        print("Connection Failed")
    
#     CREATING KEYSPACE FOR ALL THE TABLES
    session.execute(CREATE_KEYSPACE_SPARKIFY)
    
    
#     NOW SETTING SPACE TO BE USED FOR ACCESSING TABLES
    session.set_keyspace(name)
    
    
#     DROPPING ALL THE TABLES IF EXISTS
    session.execute(DROP_SESSION_SONGS_TABLE)
    session.execute(DROP_EVENT_LOG_TABLE)
    session.execute(DROP_SONG_USERS_TABLE)
    
    
#     CREATING ALL TABLES NOW,
    session.execute(CREATE_SESSION_SONGS_TABLE)
    session.execute(CREATE_EVENT_LOG_TABLE)
    session.execute(CREATE_SONG_USERS_TABLE)
    
    
def main():
    create_keyspace("sparkify")
    
    session.shutdown()
    cluster.shutdown()
    
if __name__ == "__main__":
    
    main()