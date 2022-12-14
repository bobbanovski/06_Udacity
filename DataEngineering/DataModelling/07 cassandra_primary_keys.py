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

# Dataset
# | Year    | City          | Artist_Name       | Album_Name        |
# | 1965    | Oxford        | The Beatles       | Rubber Soul
# | 1970    | Liverpool     | The Beatles       | Let It Be
# | 1966    | Los Angeles   | The Monkees       | The Monkees
# | 1970    | San Diego     | The Carpenters    | Close To You
# | 1965    | London        | The Who           | My Generation

# Modelling decisions
# What should be primary key, partition key
# Customer looking for year - need to validate that data has an even spread across nodes when querying the data
query = "Create TABLE IF NOT EXISTS music_library "
query = query + "(year int, artist_name text, album_name text, city text, PRIMARY KEY (year, album_name))"
# query = "DROP TABLE music_library"
try:
    session.execute(query)
except Exception as e:
    print(e)

query = "INSERT INTO music_library (year, artist_name, album_name, city)"
query = query + " VALUES (%s, %s, %s, %s)"
insert_queries = [
    (1970, 'The Beatles', 'Let it Be', 'Liverpool'),
    (1965, 'The Beatles', 'Rubber Soul', 'Oxford'),
    (1965, 'The Who', 'My Generation', 'London'),
    (1966, 'The Monkees', 'The Monkees', 'Los Angeles')
]
for item in insert_queries:
    try:
        session.execute(query, item)
    except Exception as e:
        print(e)

# Required query - get all albums for a year
# Only returns a single line - incorrect primary key assigned - add album_name to form composite key
query_select = "select * from music_library WHERE YEAR=1965"
try:
    rows = session.execute(query_select)
    for row in rows:
        print(row.year, row.artist_name, row.album_name, row.city)
except Exception as e:
    print(e)

query_drop = "DROP TABLE music_library"
try:
    rows = session.execute(query_drop)
except Exception as e:
    print(e)

session.shutdown()
cluster.shutdown()