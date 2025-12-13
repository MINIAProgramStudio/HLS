import threading
import time
from tqdm import tqdm
from pymongo import MongoClient, ReturnDocument, WriteConcern

MONGO_HOST = "127.0.0.1"
MONGO_PORTS = [27017, 27018, 27019]
REPLICA_SET = "rs0"
DB_NAME = "HLS"
COLLECTION_NAME = "user_counter"

hosts = ",".join(f"{MONGO_HOST}:{port}" for port in MONGO_PORTS)
MONGO_URI = f"mongodb://{hosts}/?replicaSet={REPLICA_SET}&retryWrites=true&w=majority"

CALLS_PER_CLIENT = 10_000
NUM_CLIENTS = 10

print_lock = threading.Lock()

def get_client():
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)

def init_db():
    client = get_client()
    db = client[DB_NAME]
    coll = db[COLLECTION_NAME]

    coll.delete_many({})
    coll.insert_one({
        "user_id": 1,
        "counter": 0,
        "version": 0
    })
    print("MongoDB initialized: counter reset to 0")
    client.close()

def worker(client_id):
    client = get_client()
    coll = client[DB_NAME][COLLECTION_NAME].with_options(
        write_concern=WriteConcern(w="majority", wtimeout=5000)
    )

    bar = tqdm(total=CALLS_PER_CLIENT, 
               position=client_id, 
               leave=False)

    for _ in range(CALLS_PER_CLIENT):
        coll.find_one_and_update(
            {"user_id": 1},
            {"$inc": {"counter": 1}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        bar.update(1)
    with print_lock:
        bar.close()
    client.close()

def main():
    print("MongoDB In-Place Update with Optimistic Concurrency Control (version field)")
    print("Initializing DB...")
    init_db()
    time.sleep(2)

    client = get_client()
    doc = client[DB_NAME][COLLECTION_NAME].find_one({"user_id": 1})
    print(f"Initial counter: {doc['counter']}")
    client.close()

    print(f"Starting {NUM_CLIENTS} clients with {CALLS_PER_CLIENT:,} increments each...")
    start_time = time.time()

    threads = []
    for i in range(NUM_CLIENTS):
        t = threading.Thread(target=worker, args=(i,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    end_time = time.time()
    elapsed = end_time - start_time

    client = get_client()
    final_doc = client[DB_NAME][COLLECTION_NAME].find_one({"user_id": 1})
    final_counter = final_doc["counter"]
    client.close()

    total_calls = CALLS_PER_CLIENT * NUM_CLIENTS
    throughput = total_calls / elapsed
    
    print(f"Final count: {final_counter}")
    print(f"Total time:  {elapsed:.2f} s")
    print(f"Throughput:  {throughput:.2f} requests/sec")

if __name__ == "__main__":
    main()
    input("\nPress Enter to exit...")