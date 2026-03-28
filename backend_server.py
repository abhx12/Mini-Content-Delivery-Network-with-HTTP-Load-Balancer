"""
backend_servers.py
------------------
Simple Flask-based backend servers that simulate real-world latency.
Run multiple instances on different ports:
    python backend_servers.py 8001
    python backend_servers.py 8002
    python backend_servers.py 8003
"""

import sys
import time
import random
from flask import Flask, request, jsonify

app = Flask(__name__)

# Server ID is passed as a command-line argument (port number)
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8001
SERVER_NAME = f"Server-{PORT}"


@app.route("/", defaults={"path": ""})
@app.route("/<path:path>", methods=["GET"])
def handle_request(path):
    """
    Handle all GET requests.
    Simulates real-world latency with an artificial delay of 1-2 seconds.
    """
    url_path = "/" + path if path else "/"

    # Security: Only allow GET requests (Flask handles this via methods=[])
    print(f"[{SERVER_NAME}] Received GET request for {url_path}")

    # Simulate real-world processing latency
    delay = random.uniform(1.0, 2.0)
    time.sleep(delay)

    response_data = {
        "server": SERVER_NAME,
        "path": url_path,
        "message": f"Response from {SERVER_NAME}",
        "delay_seconds": round(delay, 2),
    }

    print(f"[{SERVER_NAME}] Responding to {url_path} after {delay:.2f}s delay")
    return jsonify(response_data), 200


if __name__ == "__main__":
    print(f"[{SERVER_NAME}] Starting on port {PORT}...")
    app.run(host="0.0.0.0", port=PORT, threaded=True)