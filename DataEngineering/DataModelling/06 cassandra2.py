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
# get all albums in the music library created by a given artist
# 1 query - 1 table - create 2 tables that partition the data differently

# An excellent introduction to primary keys in CassandraDB
# https://www.morsecodist.io/blog/cassandra-basics-primary-keys

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

query_insert_music_library = "INSERT INTO music_library (year, artist_name, album_name) "
query_insert_music_library += "VALUES (%s, %s, %s)"

query_insert_album_library = "INSERT INTO album_library (artist_name, year, album_name) "
query_insert_album_library += "VALUES (%s, %s, %s)"

music_library_data = [
    (1970, "The Beatles", "Let it Be"),
    (1965, "The Beatles", "Rubber Soul"),
    (1965, "The Who", "My Generation"),
    (1966, "The Monkees", "The Monkees"),
    (1970, "The Carpenters", "Close To You")
]
music_library_data_count = len(music_library_data)
missing_data = 0
for data in music_library_data:
    try:
        session.execute(query_insert_music_library, data)
    except Exception as e:
        print(e)
        missing_data += 1

print (str(music_library_data_count - missing_data) + " Entries entered, " + str(missing_data) + " Entries failed entry")

album_library_data =[
    ("The Beatles", 1970, "Let it Be"),
    ("The Beatles", 1965, "Rubber Soul"),
    ("The Who", 1965, "My Generation"),
    ("The Monkees", 1966, "The Monkees"),
    ("The Carpenters", 1970, "Close To You")
]
album_library_data_count = len(album_library_data)
missing_data = 0
for data in album_library_data:
    try:
        session.execute(query_insert_album_library, data)
    except Exception as e:
        print(e)
        missing_data += 1
print(str(album_library_data_count - missing_data) + " Entries entered, " + str(missing_data) + " Entries failed entry")

query = "SELECT * FROM music_library WHERE YEAR = 1970"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for row in rows:
    print(row.year, row.artist_name, row.album_name,)

query = "SELECT * FROM album_library WHERE ARTIST_NAME = 'The Beatles'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for row in rows:
    print(row.artist_name, row.year,  row.album_name,)

query_drop = "DROP TABLE music_library"
try:
    rows = session.execute(query_drop)
except Exception as e:
    print(e)

query_drop = "DROP TABLE album_library"
try:
    rows = session.execute(query_drop)
except Exception as e:
    print(e)

session.shutdown()
cluster.shutdown()