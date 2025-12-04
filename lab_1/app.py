from flask import Flask
import threading
import psycopg2
from psycopg2.extras import RealDictCursor

print("starting Lost-update")

app = Flask(__name__)
DB_HOST = "localhost"
DB_NAME = "HLS"
DB_USER = "postgres"
DB_PASS = "misube2105"
DB_PORT = 5432

def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port=DB_PORT
    )
def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_counter (
            USER_ID SERIAL PRIMARY KEY,
            Counter  BIGINT NOT NULL
            Version  BIGINT NOT NULL
        );
    """)
    # Reset counter to 0
    cur.execute("DELETE FROM user_counter;")
    cur.execute("INSERT INTO user_counter VALUES (1,0,0);")
    conn.commit()
    cur.close()
    conn.close()

init_db()

@app.route("/inc")
def inc():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE user_counter SET value = value + 1 RETURNING value;")
    new_value = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return str(new_value)

@app.route("/count")
def count():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT value FROM counter LIMIT 1;")
    current_value = cur.fetchone()[0]
    cur.close()
    conn.close()
    return str(current_value)
