from flask import Flask
from hazelcast.client import HazelcastClient

print("Starting with pessimistic block")

app = Flask(__name__)

HZ_ADDRESSES = [
    "127.0.0.1:5701",
    "127.0.0.1:5702",
    "127.0.0.1:5703"
]

COUNTER_NAME = "likes"

print("Connecting to Hazelcast...")
client = HazelcastClient(cluster_members=HZ_ADDRESSES)
counter_map = client.get_map("map").blocking()



counter_map.put(COUNTER_NAME, 0)
print(f"Hazelcast connected. Counter '{COUNTER_NAME}' reset to 0.")


@app.route("/inc")
def inc():
    counter_map.lock(COUNTER_NAME)
    value = counter_map.get(COUNTER_NAME)
    new_value = value + 1
    counter_map.put(COUNTER_NAME, new_value)
    counter_map.unlock(COUNTER_NAME)
    return str(new_value)
        


@app.route("/count")
def count():
    value = counter_map.get(COUNTER_NAME)
    return str(value)