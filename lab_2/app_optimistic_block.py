from flask import Flask
from hazelcast.client import HazelcastClient
import time
import random

print("Starting with optimistic lock (CAS)")

app = Flask(__name__)

HZ_ADDRESSES = [
    "127.0.0.1:5701",
    "127.0.0.1:5702",
    "127.0.0.1:5703"
]

COUNTER_NAME = "likes"

client = HazelcastClient(cluster_members=HZ_ADDRESSES)
counter_map = client.get_map("map").blocking()


counter_map.put(COUNTER_NAME, 0)
print(f"Hazelcast connected. Counter '{COUNTER_NAME}' reset to 0.")


def cas_increment():
    backoff = 0.0005
    max_backoff = 0.05

    while True:
        old = counter_map.get(COUNTER_NAME)
        new = old + 1
        if counter_map.replace_if_same(COUNTER_NAME, old, new):
            return new
        jitter = random.uniform(0, backoff)
        time.sleep(backoff + jitter)
        backoff = min(backoff * 2, max_backoff)

@app.route("/inc")
def inc():
    new = cas_increment()
    return str(new)

@app.route("/count")
def count():
    return str(counter_map.get(COUNTER_NAME))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
