import cassandra
from cassandra.cluster import Cluster

# Create connection to database
try:
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
except Exception as e:
    print(e)
print('connected')

# Create keyspace
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity
    WITH REPLICATION = 
    {'class' : 'SimpleStrategy', 'replication_factor' : 1}"""
    )
except Exception as e:
    print(e)

# Connect to keyspace
try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)

try:
    session.execute("""SELECT * FROM music_library""")
except Exception as e:
    print(e)


# 2 queries for the data 
# get all albums in the music livrary released in a given year
# get all albums in hte music library created by a given artist
# 1 query - 1 table - create 2 tables that partition the data differently

query = "Create TABLE IF NOT EXISTS music_library"
query = query + "(year int, artist_name text, album_name text, PRIMARY KEY (year, artist_name))"
# query = "DROP TABLE music_library"
try:
    session.execute(query)
except Exception as e:
    print(e)

query = "Create TABLE IF NOT EXISTS album_library"
query = query + "(year int, artist_name text, album_name text, PRIMARY KEY (artist_name, year))"
# query = "DROP TABLE music_library"
try:
    session.execute(query)
except Exception as e:
    print(e)

query_insert_music_library = """"""
query_insert_album_library = """"""

query = "SELECT COUNT(*) FROM music_library"
try:
    count = session.execute(query)
    print(count)
except Exception as e:
    print(e)
