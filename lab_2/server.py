from waitress import serve
from app_pessimistic_block import app

if __name__ == "__main__":
    serve(app, host="0.0.0.0", port=8080, threads=5)