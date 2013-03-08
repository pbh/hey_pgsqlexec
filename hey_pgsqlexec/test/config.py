import psycopg2

def get_pg_connection_string():
    return "dbname=pgsqlexectest user=pbh host=localhost"

def get_pg_connection():
    return psycopg2.connect(get_pg_connection_string())
