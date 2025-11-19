from flask import Flask
from hazelcast.client import HazelcastClient

print("Starting with pessimistic block")

app = Flask(__name__)

HZ_ADDRESSES = [
    "127.0.0.1:5701",
    "127.0.0.1:5702",
    "127.0.0.1:5703"
]

client = HazelcastClient(cluster_members=HZ_ADDRESSES)
counter_map = client.get_map("likes-map").blocking()

COUNTER_KEY = "likes"

counter_map.put(COUNTER_KEY, 0)
print("Hazelcast connected. Counter reset.")


@app.route("/inc")
def inc():
    counter_map.lock(COUNTER_KEY)
    value = counter_map.get(COUNTER_KEY)
    new_value = value + 1
    counter_map.put(COUNTER_KEY, new_value)
    counter_map.unlock(COUNTER_KEY)
    return str(new_value)
        


@app.route("/count")
def count():
    value = counter_map.get(COUNTER_KEY)
    return str(value)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
