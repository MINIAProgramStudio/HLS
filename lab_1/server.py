from flask import Flask
import threading

app = Flask(__name__)

counter = 0
lock = threading.Lock()

@app.route("/inc")
def inc():
    global counter
    with lock:
        counter += 1
        return str(counter)

@app.route("/count")
def count():
    with lock:
        return str(counter)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, threaded=False, use_reloader=False)