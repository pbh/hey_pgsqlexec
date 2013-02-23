import psycopg2

def get_pg_connection():
    return psycopg2.connect("dbname=pgsqlexectest user=pbh host=localhost")

