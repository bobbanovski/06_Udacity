import psycopg2

try:
    conn = psycopg2.connect('host=127.0.0.1 dbname=udacity user=student password=student')
except psycopg2.Error as e:
    print("Error: Could not make connection to PostGres db")
    print(e)

conn.set_session(autocommit=True)
cur = conn.cursor()

try:
    cur = conn.cursor()
except psycopg2.Error as e:
    print("Error: Could not get cursor to DB")
    print(e)

# try:
#     cur.execute('CREATE DATABASE udacity')
# except psycopg2.Error as e:
#     print(e)
try:
    cur.execute('CREATE TABLE IF NOT EXISTS music_library (album_name varchar, artist_name varchar, year int);')
except psycopg2.Error as e:
    print(e)

try:
    cur.execute('select * from music_library')
    print(cur.fetchall())
except psycopg2.Error as e:
    print(e)

# insert rows
# try:
#     cur.execute("INSERT INTO music_library (album_name, artist_name, year) \
#         VALUES (%s, %s, %s)", \
#             ("Let it Be", "The Beatles", 1970))
# except psycopg2.Error as e:
#     print("error with inserting rows")
#     print(e)