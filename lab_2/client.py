import requests
import threading
import time
import random
from tqdm import tqdm

SERVER = "http://127.0.0.1:8080"
CALLS_PER_CLIENT = 10
NUM_CLIENTS = 10

def worker(client_id):
    for i in tqdm(range(CALLS_PER_CLIENT), desc = str(client_id)):
        trying = True
        while trying:
            try:
                requests.get(f"{SERVER}/inc")
                trying = False
            except requests.exceptions.RequestException:
                print(client_id, "worker, request denied, N =",j*CALLS_PER_CLIENT//N_REPORTS + i)
                time.sleep(random.random())

def main():
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
    while final_count == -1 and i < 5:
        try:
            final_count = int(requests.get(f"{SERVER}/count").text)
            print(final_count)
        except:
            final_count = -1
            i+=1
            time.sleep(1)

    total_calls = CALLS_PER_CLIENT * NUM_CLIENTS
    throughput = total_calls / elapsed

    print(f"Final count: {final_count}")
    print(f"Total time:  {elapsed:.2f} s")
    print(f"Throughput:  {throughput:.2f} requests/sec")

if __name__ == "__main__":
    main()
    input()