import psycopg2

try:
    conn = psycopg2.connect('host=127.0.0.1 dbname=udacity user=student password=student')
except psycopg2.Error as e:
    print("Error: Could not make connection to PostGres db")
    print(e)

conn.set_session(autocommit=True)
cur = conn.cursor()

try:
    cur.execute("DROP TABLE IF EXISTS music_library")
    cur.execute("CREATE TABLE IF NOT EXISTS music_library (album_id int, \
        album_name varchar, artist_name varchar, \
            year int, songs text[]);")
except psycopg2.Error as e:
    print("Error creating table")

try:
    cur.execute("INSERT INTO music_library (album_id, album_name, artist_name, year, songs) \
        VALUES (%s, %s, %s, %s, %s)", \
            (2, "Let it be", "The Beatles", 1970, ["Let it be", "Across the universe"]))
except psycopg2.Error as e:
    print("Error inserting rows")
    print(e)

try:
    cur.execute("INSERT INTO music_library (album_id, album_name, artist_name, year, songs) \
        VALUES (%s, %s, %s, %s, %s)", \
            (2, "Rubber Soul", "The Beatles", 1965, ["Michelle", "Think for yourself", "In this Life"]))
except psycopg2.Error as e:
    print("Error inserting rows")
    print(e)


try:
    cur.execute("SELECT * FROM music_library")
except psycopg2.Error as e:
    print("Error in Select")
    print(e)

row = cur.fetchone()
while row:
    print(row)
    row=cur.fetchone()

# 1NF - break up list into individual rows

try:
    cur.execute("DROP TABLE IF EXISTS music_library_1nf")
    cur.execute("CREATE TABLE IF NOT EXISTS music_library_1nf (album_id int, \
        album_name varchar, artist_name varchar, \
            year int, song_name text);")
except psycopg2.Error as e:
    print("Error creating table")
    print(e)

try:
    insert_queries = [
        (2, "Let it be", "The Beatles", 1970, "Across the universe"),
        (2, "Let it be", "The Beatles", 1970, "Let it be"),
        (1, "Rubber Soul", "The Beatles", 1965, "Michelle"),
        (1, "Rubber Soul", "The Beatles", 1965, "Think for yourself"),
        (1, "Rubber Soul", "The Beatles", 1965, "In this Life")
    ]
    for item in insert_queries:
        cur.execute("INSERT INTO music_library_1nf (album_id, album_name, artist_name, year, song_name) \
            VALUES (%s, %s, %s, %s, %s)", \
                item
                )
except psycopg2.Error as e:
    print("Error inserting rows")
    print(e)

# 2NF Reduce duplication of Album names - primary key of album id is not unique
# Break into album library and song library
try:
    cur.execute("DROP TABLE IF EXISTS album_library_2nf")
    cur.execute("CREATE TABLE IF NOT EXISTS album_library_2nf (album_id int, \
        album_name varchar, artist_id int, year int);")
except psycopg2.Error as e:
    print("Error creating table")
    print(e)

try:
    
    insert_queries = [
        (2, "Let it be", 1, 1970),
        (1, "Rubber Soul", 1, 1965)
    ]
    for item in insert_queries:
        cur.execute(
            "INSERT INTO album_library_2nf (album_id, album_name, artist_id, year) \
            VALUES (%s, %s, %s, %s)", item
        )
except psycopg2.Error as e:
    print("Error inserting rows")
    print(e)

try:
    cur.execute("DROP TABLE IF EXISTS artist_library_2nf")
    cur.execute("CREATE TABLE IF NOT EXISTS artist_library_2nf (artist_id int, \
        artist_name varchar);")
except psycopg2.Error as e:
    print("Error creating table")
    print(e)

try:
    
    insert_queries = [
        (1, "The Beatles")
    ]
    for item in insert_queries:
        cur.execute(
            "INSERT INTO artist_library_2nf (artist_id, artist_name) \
            VALUES (%s, %s)", item
        )
except psycopg2.Error as e:
    print("Error inserting rows")
    print(e)

try:
    cur.execute("DROP TABLE IF EXISTS song_library_2nf")
    cur.execute("CREATE TABLE IF NOT EXISTS song_library_2nf (song_id int, \
        album_id int, song_name varchar);")
except psycopg2.Error as e:
    print("Error creating table")
    print(e)

try:
    insert_queries = [
        (1, 2, "Across the universe"),
        (2, 2, "Let it be"),
        (3, 1, "Michelle"),
        (4, 1, "Think for yourself"),
        (5, 1, "In this Life")
    ]
    for item in insert_queries:
        cur.execute("INSERT INTO song_library_2nf (song_id, album_id, song_name) \
            VALUES (%s, %s, %s)", \
                item
                )
except psycopg2.Error as e:
    print("Error inserting rows")
    print(e)

try:
    cur.execute("SELECT * FROM album_library_2nf JOIN \
        song_library_2nf ON album_library_2nf.album_id = song_library_2nf.album_id JOIN \
        artist_library_2nf ON album_library_2nf.artist_id = artist_library_2nf.artist_id")
except psycopg2.Error as e:
    print("Error in Select")
    print(e)

row = cur.fetchone()
while row:
    print(row)
    row=cur.fetchone()

cur.close()
conn.close()