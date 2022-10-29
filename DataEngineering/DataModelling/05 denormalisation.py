import psycopg2

try: 
    conn = psycopg2.connect("host=127.0.0.1 dbname=udacity user=student password=student")
except psycopg2.Error as e: 
    print("Error: Could not make connection to the Postgres database")
    print(e)
try: 
    cur = conn.cursor()
except psycopg2.Error as e: 
    print("Error: Could not get cursor to the Database")
    print(e)
conn.set_session(autocommit=True)

try:
    cur.execute("DROP TABLE IF EXISTS transactions2")
    cur.execute("DROP TABLE IF EXISTS albums_sold")
    cur.execute("DROP TABLE IF EXISTS employees")
    cur.execute("DROP TABLE IF EXISTS sales")
except psycopg2.Error as e:
    print("Error: Could not process")
    print(e)

try:
    cur.execute(
        "CREATE TABLE IF NOT EXISTS transactions2 (transaction_id int, customer_name varchar, cashier_id int, year int)"
        )
except psycopg2.Error as e:
    print("Error: Could not create table - transactions2")
    print(e)

try:
    cur.execute("CREATE TABLE IF NOT EXISTS albums_sold (album_id int, transaction_id int, album_name varchar)")
except psycopg2.Error as e:
    print("Error: Could not create table - albums_sold")
    print(e)

try:
    cur.execute("CREATE TABLE IF NOT EXISTS employees (employee_id int, employee_name varchar)")
except psycopg2.Error as e:
    print("Error: Could not create table - employees")
    print(e)

try:
    cur.execute("CREATE TABLE IF NOT EXISTS sales (transaction_id int, amount_spent decimal)")
except psycopg2.Error as e:
    print("Error: Could not create table - sales")
    print(e)

print('done')

# Insert data into normalised tables

# try: 
#     cur.execute("INSERT INTO transactions2 (transaction_id, customer_name, cashier_id, year) \
#                  VALUES (%s, %s, %s, %s)", \
#                  (1, "Amanda", 1, 2000))
# except psycopg2.Error as e: 
#     print("Error: Inserting Rows")
#     print (e)

try:
    insert_queries = [
        (1, "Amanda", 1, 2000),
        (2, "Toby", 1, 2000),
        (3, "Max", 2, 2018)
    ]
    for item in insert_queries:
        cur.execute("INSERT INTO transactions2 (transaction_id, customer_name, cashier_id, year) \
            VALUES (%s, %s, %s, %s)", \
                item
                )
except psycopg2.Error as e:
    print("Error inserting rows")
    print(e)


# try: 
#     cur.execute("INSERT INTO transactions2 (transaction_id, customer_name, cashier_id, year) \
#                  VALUES (%s, %s, %s, %s)", \
#                  (1, "Amanda", 1, 2000))
# except psycopg2.Error as e: 
#     print("Error: Inserting Rows")
#     print (e)

try:
    insert_queries = [
        (1, 1, "Rubber Soul"),
        (2, 1, "Let It Be"),
        (3, 2, "My Generation"),
        (4, 3, "Meet the Beatles"),
        (5, 3, "Help!"),
    ]
    for item in insert_queries:
        cur.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name) \
            VALUES (%s, %s, %s)", \
                item
                )
except psycopg2.Error as e:
    print("Error inserting rows")
    print(e)

try:
    insert_queries = [
        (1, "Sam"),
        (2, "Bob")
    ]
    for item in insert_queries:
        cur.execute("INSERT INTO employees (employee_id, employee_name) \
            VALUES (%s, %s)", \
                item
                )
except psycopg2.Error as e:
    print("Error inserting rows")
    print(e)

try:
    insert_queries = [
        (1, 40),
        (2, 19),
        (3, 45)
    ]
    for item in insert_queries:
        cur.execute("INSERT INTO sales (transaction_id, amount_spent) \
            VALUES (%s, %s)", \
                item
                )
except psycopg2.Error as e:
    print("Error inserting rows")
    print(e)


#TO-DO: Complete the statement below to perform a 3 way JOIN on the 4 tables you have created.
# transactions2 albums_sold employees employees sales
try:
    cur.execute("SELECT * FROM transactions2  \
        JOIN albums_sold ON transactions2.transaction_id = albums_sold.transaction_id \
        JOIN sales ON transactions2.transaction_id = sales.transaction_id \
        JOIN employees ON transactions2.cashier_id = employees.employee_id")
except psycopg2.Error as e:
    print("Error in Select")
    print(e)

row = cur.fetchone()
while row:
    print(row)
    row=cur.fetchone()

"""Following queries need to be run - requires Denormalisation
Query 1 : select transaction_id, customer_name, amount_spent FROM <min number of tables>
Query 2: select cashier_name, SUM(amount_spent) FROM <min number of tables> GROUP BY cashier_name
"""
# todo add amount_spent to the transactions table
try:
    cur.execute("DROP TABLE IF EXISTS transactions_denormalised")
    cur.execute(
        "CREATE TABLE IF NOT EXISTS transactions_denormalised (transaction_id int, customer_name varchar, amount_spent decimal)"
        )
except psycopg2.Error as e:
    print("Error: Could not create table - transactions_denormalised")
    print(e)

try:
    insert_queries = [
        (1, "Amanda", 40),
        (1, "Toby", 19),
        (3, "Max", 45)
    ]
    for item in insert_queries:
        cur.execute(
            "INSERT INTO transactions_denormalised (transaction_id, customer_name, amount_spent) \
            VALUES (%s, %s, %s)",
                item
            )
except psycopg2.Error as e:
    print("Error inserting rows")
    print(e)

try:
    cur.execute("DROP TABLE IF EXISTS sales_denormalised")
    cur.execute(
        "CREATE TABLE IF NOT EXISTS sales_denormalised (transaction_id int, cashier_name varchar, cashier_id int, amount_spent decimal)"
        )
except psycopg2.Error as e:
    print("Error: Could not create table - sales_denormalised")
    print(e)

try:
    insert_queries = [
        (1, "Sam", 1, 40),
        (2, "Sam", 1, 19),
        (3, "Bob", 2, 45)
    ]
    for item in insert_queries:
        cur.execute(
            "INSERT INTO sales_denormalised (transaction_id, cashier_name, cashier_id, amount_spent) \
            VALUES (%s, %s, %s, %s)",
                item
            )
except psycopg2.Error as e:
    print("Error inserting rows")
    print(e)

try:
    cur.execute("SELECT cashier_name, SUM(amount_spent) FROM sales_denormalised GROUP BY cashier_name")
except psycopg2.Error as e:
    print("Error in Select")
    print(e)

rows = cur.fetchall()
for row in rows:
    print(row)

cur.close()
conn.close()