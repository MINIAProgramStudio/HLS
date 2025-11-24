from flask import Flask
from hazelcast.client import HazelcastClient

print("Starting with no block")

app = Flask(__name__)

HZ_ADDRESSES = [
    "127.0.0.1:5701",
    "127.0.0.1:5702",
    "127.0.0.1:5703"
]

COUNTER_NAME = "likes"

print("Connecting to Hazelcast...")
client = HazelcastClient(
    cluster_members=HZ_ADDRESSES
)

cp = client.cp_subsystem
counter = cp.get_atomic_long(COUNTER_NAME).blocking()

counter.set(0)
print(f"Hazelcast connected. Counter '{COUNTER_NAME}' reset to 0.")


@app.route("/inc")
def inc():
    new_value = counter.get() + 1
    counter.set(new_value)
    return str(new_value)


@app.route("/count")
def count():
    current = counter.get()
    return str(current)


if __name__ == "__main__":
    try:
        app.run(host="0.0.0.0", port=5000, debug=False)
    finally:
        client.shutdown()