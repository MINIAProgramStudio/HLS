import requests
import threading
import time

SERVER = "http://127.0.0.1:8080"
CALLS_PER_CLIENT = 10_000
NUM_CLIENTS = 5

def worker(client_id):
    for i in range(CALLS_PER_CLIENT):
        try:
            requests.get(f"{SERVER}/inc")
        except requests.exceptions.RequestException:
            pass

def main():
    print(f"Starting {NUM_CLIENTS} clients x {CALLS_PER_CLIENT} calls each...")
    start = time.time()

    threads = []
    for i in range(NUM_CLIENTS):
        t = threading.Thread(target=worker, args=(i,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    end = time.time()
    elapsed = end - start
    final_count = -1
    i = 0
    while final_count == -1 or i < 5:
        try:
            final_count = int(requests.get(f"{SERVER}/count").text)
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