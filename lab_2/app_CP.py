from flask import Flask
from hazelcast import HazelcastClient

print("Starting with IAtomicLong")

app = Flask(__name__)

HZ_ADDRESSES = [
    "127.0.0.1:5701",
    "127.0.0.1:5702",
    "127.0.0.1:5703"
]

COUNTER_NAME = "likes"

client = HazelcastClient(cluster_members=HZ_ADDRESSES)
counter = client.cp_subsystem.get_atomic_long(COUNTER_NAME).blocking()

counter.set(0)
print(f"Hazelcast connected. Counter '{COUNTER_NAME}' reset to 0.")


@app.route("/inc")
def inc():
    new_value = counter.increment_and_get()
    return str(new_value)


@app.route("/count")
def count():
    return str(counter.get())