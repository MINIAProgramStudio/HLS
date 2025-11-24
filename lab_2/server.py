from waitress import serve
from app_optimistic_block import app

if __name__ == "__main__":
    serve(app, host="0.0.0.0", port=5000, threads=20)