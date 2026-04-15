import psycopg

def get_connection():
    conn = psycopg.connect(
        host="localhost",
        port=5432,
        dbname="crypto_pipeline",
        user="postgres",
        password="Chaitanya@150799"
    )
    return conn