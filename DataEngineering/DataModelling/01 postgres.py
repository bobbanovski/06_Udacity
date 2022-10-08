import psycopg2

conn = psycopg2.connect('host=127.0.0.1 dbname=studentdb user=student password=student')
conn.set_session(autocommit=True)
cur = conn.cursor()
# Get cursor to execute query
cur.execute('CREATE TABLE test123 (col1 int, col2 int, col3 int);')
cur.execute('select * FROM test123')
cur.execute('select count(*) ')

cur.execute('DROP TABLE test123')