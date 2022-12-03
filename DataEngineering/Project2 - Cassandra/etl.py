# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
from cassandra.cluster import Cluster
from prettytable import PrettyTable

# Get path to data folder
filepath = os.getcwd() + '/event_data'

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))

# initiating an empty list of rows that will be generated from each file
full_data_rows_list = [] 

# for every filepath in the file path list 
for f in file_path_list:
    # read csv file 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile:
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)

        for line in csvreader:
            full_data_rows_list.append(line)

print("Number of data files: ", str(len(full_data_rows_list)))

file = 'event_datafile_new.csv'

csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)
with open(file, 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist', 'firstName', 'gender', 'itemInSession', 'lastName', 'length', \
                'level', 'location', 'sessionId', 'song', 'userId'])

    # Skip blank rows
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

with open(file, 'r', encoding = 'utf8') as f:
    print("Number of rows of event data: ", str(sum(1 for line in f)))

# Set network IP address of Cassandra cluster
cluster = Cluster(['127.0.0.1'])

# create session object
session = cluster.connect()

# Create keyspace to sparkify
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS sparkify
    WITH REPLICATION = 
    {'class' : 'SimpleStrategy', 'replication_factor' : 1}"""
    )
except Exception as e:
    print(e)

try:
    session.set_keyspace('sparkify')
except Exception as e:
    print(e)

"""
Table song_info_by_sessionId_itemInSession
Returns - artist, song, length
Search Terms - sessionId, itemInSession
Primary key - sessionId
Clustering column - itemInSession
"""
query = "CREATE TABLE IF NOT EXISTS song_info_by_sessionId_itemInSession "
query = query + "(sessionId int, itemInSession int, artist text, song text, length float, PRIMARY KEY (sessionId, itemInSession))"
try:
    session.execute(query)
except Exception as e:
    print(e)

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) 
    for line in csvreader:
        try:
            query = "INSERT INTO song_info_by_sessionId_itemInSession (sessionId, itemInSession, artist, song, length)"
            query = query + "VALUES (%s, %s, %s, %s, %s)"

            session.execute(query, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))
        except Exception as e:
            print(e)

query = "SELECT artist, song, length FROM song_info_by_sessionId_itemInSession "\
    "WHERE sessionId = 338 AND itemInSession = 4"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for row in rows:
    # Note that the row identifiers are lowercase despite any uppercase characters in the table create query
    print("Artist: ", row.artist, "Song: ", row.song,  "Length: ", row.length)

"""
Table songlist_by_user_session
Returns - artist, song, firstName, lastName
Search Terms - userid, sessionid
Primary key - userid, sessionid
Clustering column - itemInSession
Sorted by itemInSession
"""

query = "CREATE TABLE IF NOT EXISTS songlist_by_user_session "
query = query + "(userid int, sessionid int, itemInSession int, artist text, song text, firstName text, lastName text, PRIMARY KEY ((userid, sessionid), itemInSession)) " \
    "WITH CLUSTERING ORDER BY (itemInSession desc)"
try:
    session.execute(query)
except Exception as e:
    print(e)

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        try:
            query = "INSERT INTO songlist_by_user_session (userid, sessionid, itemInSession, artist, song, firstName, lastName)"
            query = query + "VALUES (%s, %s, %s, %s, %s, %s, %s)"

            session.execute(query, (int(line[10]), int(line[8]), int(line[3]), line[0], line[9], line[1], line[4]))
        except Exception as e:
            print(e)

query = "SELECT artist, song, firstName, lastName FROM songlist_by_user_session WHERE userid = 10 AND sessionid = 182"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for row in rows:
    # Note that the output is auto converted to lowercase
    print("Artist: ", row.artist, "Song: ", row.song,  "First Name: ", row.firstname, 
          "Last Name: ",  row.lastname)


"""
Table user_list_by_song
* Note that userid has been included so that multiple users with the same first and last names can be shown individually
Returns - firstName, lastName, userid
Search Terms - song, userid
Primary key - song, userid
Clustering column - itemInSession
Sorted by userid desc
"""

query = "CREATE TABLE IF NOT EXISTS user_list_by_song "
query = query + "(song text, userid int, firstName text, lastName text, PRIMARY KEY (song, userid)) " \
    "WITH CLUSTERING ORDER BY (userid desc)"
try:
    session.execute(query)
except Exception as e:
    print(e)

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        try:
            query = "INSERT INTO user_list_by_song (userid, song, firstName, lastName)"
            query = query + "VALUES (%s, %s, %s, %s)"

            session.execute(query, (int(line[10]), line[9], line[1], line[4]))
        except Exception as e:
            print(e)

query = "SELECT userid, firstName, lastName FROM user_list_by_song WHERE song = 'All Hands Against His Own'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for row in rows:
    # Note that the output is auto converted to lowercase
    print("UserId: ", row.userid, "First Name: ", row.firstname, "Last Name: ",  row.lastname)

query_drop = "DROP TABLE song_info_by_sessionId_itemInSession"
try:
    rows = session.execute(query_drop)
except Exception as e:
    print(e)

query_drop = "DROP TABLE songlist_by_user_session"
try:
    rows = session.execute(query_drop)
except Exception as e:
    print(e)

query_drop = "DROP TABLE user_list_by_song"
try:
    rows = session.execute(query_drop)
except Exception as e:
    print(e)

session.shutdown()
cluster.shutdown()