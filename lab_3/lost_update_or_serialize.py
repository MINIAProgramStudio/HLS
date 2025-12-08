import threading
import time
import random
from tqdm import tqdm
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import extensions


DB_HOST = "localhost"
DB_NAME = "HLS"
DB_USER = "postgres"
DB_PASS = "misube2105"
DB_PORT = 5432
CALLS_PER_CLIENT = 10_000
NUM_CLIENTS = 10

SERIALISE = True

print_lock = threading.Lock()

def get_conn():
    if SERIALISE:
        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT,
        )
        conn.set_isolation_level(extensions.ISOLATION_LEVEL_SERIALIZABLE)
        return conn
    else:
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
            Counter BIGINT NOT NULL,
            Version BIGINT NOT NULL
        );
    """)
    # Reset counter to 0
    cur.execute("DELETE FROM user_counter;")
    cur.execute("INSERT INTO user_counter VALUES (1, 0, 0);")
    conn.commit()
    cur.close()
    conn.close()

def worker(client_id):
    bar = tqdm(range(CALLS_PER_CLIENT), position=client_id, desc = str(client_id), leave=False)
    conn = get_conn()
    cur = conn.cursor()
    for i in bar:
        
        cur.execute("SELECT counter FROM user_counter WHERE user_id = 1")
        counter = cur.fetchone()[0]
        counter += 1
        cur.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s", (counter, 1))
        conn.commit()
    cur.close()
    conn.close()
    with print_lock:
        bar.close()

def main():
    if SERIALISE:
        print("Serialise")
    else:
        print("Lost-upddate")
    print("Initialising DB")
    init_db()
    time.sleep(1)

    conn = get_conn()
    cur = conn.cursor()
    print("DB initialised.")
    cur.execute("SELECT Counter FROM user_counter WHERE USER_ID = 1;")
    print("Counter state:", cur.fetchone()[0])
    conn.commit()
    cur.close()
    conn.close()
    print(f"Starting {NUM_CLIENTS} clients x {CALLS_PER_CLIENT} calls each...")
    
    
    start = time.time()

    threads = []
    for i in range(NUM_CLIENTS):
        t = threading.Thread(target=worker, args=(i,))
        t.start()
        threads.append(t)
    print("MAIN: Threads created")
    for t in threads:
        t.join()
    print("MAIN: Threads joined")
    end = time.time()
    elapsed = end - start
    final_count = -1
    i = 0
    conn = get_conn()
    cur = conn.cursor()
    print("DB initialised.")
    cur.execute("SELECT Counter FROM user_counter WHERE USER_ID = 1;")
    final_count = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()

    total_calls = CALLS_PER_CLIENT * NUM_CLIENTS
    throughput = total_calls / elapsed
    
    print(f"Final count: {final_count}")
    print(f"Total time:  {elapsed:.2f} s")
    print(f"Throughput:  {throughput:.2f} requests/sec")

if __name__ == "__main__":
    main()
    input()