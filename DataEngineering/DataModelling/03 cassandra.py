import cassandra
from cassandra.cluster import Cluster

try:
    cluster = Cluster()
    session = cluster.connect()
except Exception as e:
    print(e)

try:
    session.execute("""SELECT * FROM music_library""")
except Exception as e:
    print(e)

try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity
    WITH REPLICATION = 
    {'class' : 'SimpleStrategy', 'replication_factor' : 1}"""
    )
except Exception as e:
    print(e)

# Connect to new keyspace
try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)

query = "Create TABLE IF NOT EXISTS music_library"
query = query + "(year int, song_title text, artist_name text, album_name text, single boolean, PRIMARY KEY (year, artist_name))"
# query = "DROP TABLE music_library"
try:
    session.execute(query)
except Exception as e:
    print(e)

query = "SELECT COUNT(*) FROM music_library"
try:
    count = session.execute(query)
    print(count)
except Exception as e:
    print(e)

query = "INSERT INTO music_library (year, song_title, artist_name, album_name, single)"
query += "VALUES (%s, %s, %s, %s, %s)"

try:
    count = session.execute(query, (1970, "Let It Be", "The Beatles", "Across The Universe", False))
except Exception as e:
    print(e)

try:
    count = session.execute(query, (1965, "Think For Yourself", "The Beatles", "Rubber Soul", False))
except Exception as e:
    print(e)

query = "SELECT * FROM music_library"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for row in rows:
    print(row.year, row.album_name, row.artist_name)