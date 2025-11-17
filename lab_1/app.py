from flask import Flask
import threading
import os
print("LOADING APP MODULE, PID:", os.getpid())
app = Flask(__name__)

lock = threading.Lock()
counter = 0

def synchronized(f):
    def wrapper(*args, **kwargs):
        with lock:
            return f(*args, **kwargs)
    return wrapper

@app.route("/inc")
@synchronized
def inc():
    global counter
    counter += 1
    return str(counter)

@app.route("/count")
def count():
    with lock:
        return str(counter)