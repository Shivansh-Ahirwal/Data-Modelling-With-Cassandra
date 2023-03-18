import cassandra
from cassandra.cluster import Cluster
import os
import glob
import csv
import json
import pandas as pd
from CQL_QUERIES import *


def all_files_in_folder(file_path):
    return (glob.glob(file_path+'/*'))

def process_event_data_file(file_path,session,keyspace_name):
    
#     BECAUSE WE ALREADY DEPLOYED DATABASE WE JUST NEED TO INSERT THE VALUES AT PLACE
    
    all_file_path_list = all_files_in_folder(file_path)
    
    full_data_rows_list = [] 
    
    for f in all_file_path_list:
        
        with open(f,'r', encoding = 'utf8', newline='') as csvfile:
            csvreader = csv.reader(csvfile) 
            next(csvreader)
        
            for line in csvreader:
                full_data_rows_list.append(line) 
                
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

    with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                    'level','location','sessionId','song','userId'])
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))
            
    session.set_keyspace(keyspace_name)
    
    file = 'event_datafile_new.csv'

    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) 
        for line in csvreader:
            artist_name, user_name, gender, itemInSession, user_last_name, length, level, location, sessionId, song, userId = line
            session.execute(INSERT_INTO_SESSION_SONGS, (int(sessionId), int(itemInSession), artist_name, song, float(length)))
    
        for line in csvreader:
            session.execute(INSERT_INTO_EVENT_LOG, (line[0], line[9], line[1], line[4], int(line[10]), int(line[8]), int(line[3])))
            
    df = pd.read_csv(file, usecols=[1, 4, 9, 10])
    df.drop_duplicates(inplace=True)

    for ix, row in df.iterrows():
        session.execute(INSERT_INTO_SONG_USERS, (row['song'], row['userId'], row['firstName'], row['lastName']))
            
def main():
    try:
        cluster = Cluster(['cassandra'])
        session = cluster.connect()
        print("Connection Established")
    except Exception as e:
        print("Connection Failed")
    
    file_path = os.getcwd()+'/event_data'
    process_event_data_file(file_path,session=session,sparkify)
    
    session.shutdown()
    cluster.shutdown()
    
if __name__ == "__main__":
    main()