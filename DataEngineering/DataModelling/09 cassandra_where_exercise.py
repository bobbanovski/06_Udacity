# Partition key must be included in the query, any clustering column can be used - in same order as in primary key
# SELECT * is possible however thoroughly discouraged, as a full table scan on a big data system could be catastrophic

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

# We want to ask 4 question of our data
# 1. Give me every album in my music library that was released in a 1965 year
# 2. Give me the album that is in my music library that was released in 1965 by "The Beatles"
# 3. Give me all the albums released in a given year that was made in London
# 4. Give me the city that the album "Rubber Soul" was recorded

