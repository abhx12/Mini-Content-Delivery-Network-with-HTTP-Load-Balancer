"""
cdn_server.py - FINAL WORKING VERSION
Thread‑pooled CDN with coalescing, caching, rate limiting, and path validation.
"""

import sys
import time
import threading
import logging
import re
import json
import posixpath
from urllib.parse import unquote
from http.server import HTTPServer, BaseHTTPRequestHandler
from concurrent.futures import ThreadPoolExecutor

from load_balancer import LoadBalancer

# Logging
def setup_logging():
    fmt = logging.Formatter("%(asctime)s %(message)s", datefmt="%H:%M:%S")
    logger = logging.getLogger("CDN")
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    fh = logging.FileHandler("logs.txt", mode="a", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger

logger = setup_logging()

# Config
CDN_HOST = "0.0.0.0"
CDN_PORT = 9000
CACHE_TTL = 60
THREAD_POOL_SIZE = 10

BACKEND_SERVERS = [
    "http://localhost:8001",
    "http://localhost:8002",
    "http://localhost:8003",
]

BLOCKED_PATH_PREFIXES = [
    "/etc/", "/proc/", "/sys/", "/dev/", "/root/", "/var/", "/usr/",
    "/bin/", "/sbin/", "/tmp/", "/windows/", "/winnt/", "/system32/",
]

# Shared State
cache_lock = threading.Lock()
cache: dict = {}

# Coalescing state: path -> Condition object
coalesce_lock = threading.Lock()
coalesce_state: dict = {}

rate_limit_lock = threading.Lock()
rate_limit_store: dict = {}

lb = LoadBalancer(BACKEND_SERVERS, health_check_interval=15)


# Helpers
def is_rate_limited(ip: str) -> bool:
    now = time.time()
    with rate_limit_lock:
        record = rate_limit_store.get(ip)
        if not record or (now - record.get("window_start", 0)) > 10:
            rate_limit_store[ip] = {"count": 1, "window_start": now}
            return False
        record["count"] += 1
        return record["count"] > 20


VALID_PATH_RE = re.compile(r"^/[a-zA-Z0-9/_\-\.]*$")

def normalize_path(raw: str) -> str:
    return posixpath.normpath(unquote(raw))


def is_valid_path(path: str) -> bool:
    if not VALID_PATH_RE.match(path) or ".." in path:
        return False
    lower = path.lower()
    for p in BLOCKED_PATH_PREFIXES:
        if lower.startswith(p) or lower == p.rstrip("/"):
            return False
    return True


def cache_get(path: str):
    with cache_lock:
        entry = cache.get(path)
        if not entry or time.time() > entry["expires_at"]:
            cache.pop(path, None)
            return None
        return entry["data"]


def cache_set(path: str, data: dict):
    with cache_lock:
        cache[path] = {"data": data, "expires_at": time.time() + CACHE_TTL}


# ==================== COALESCING ====================
def get_response(path: str):
    """
    Returns a tuple (response_dict, source) where source is:
      "cache"     - served from cache immediately
      "coalesced" - waited for a fetcher, got response from cache after fetcher finished
      "backend"   - was the fetcher and got response from backend
    """
    # First, check if we already have a valid cache entry
    cached = cache_get(path)
    if cached is not None:
        logger.info(f"[CACHE HIT] {path}")
        return cached, "cache"

    # Acquire coalescing lock to check or create fetcher
    with coalesce_lock:
        if path in coalesce_state:
            # We are a waiter
            cond = coalesce_state[path]
            # IMPORTANT: acquire the condition's lock while still holding coalesce_lock
            cond.acquire()
            # Now we can release coalesce_lock safely
        else:
            # We are the fetcher
            cond = threading.Condition()
            coalesce_state[path] = cond
            is_fetcher = True

    if 'is_fetcher' in locals() and is_fetcher:
        # Fetcher: fetch from backend
        try:
            logger.info(f"[FORWARDED] {path} -> Sending to Load Balancer")
            result = lb.forward(path)
            logger.info(f"[RESPONSE RECEIVED] {path} <- {result.get('server','?')} | {result.get('response_time',0):.3f}s")

            # Store in cache
            cache_set(path, result)
            logger.info(f"[CACHE SET]  {path} -> Cached successfully")
            return result, "backend"
        except Exception as exc:
            logger.error(f"[ERROR] {path} -> Backend fetch failed: {exc}")
            error_resp = {"server": "ERROR", "status_code": 502, "body": {"error": str(exc)}, "response_time": 0.0}
            cache_set(path, error_resp)
            raise
        finally:
            with coalesce_lock:
                cond = coalesce_state.pop(path, None)
            if cond is not None:
                with cond:
                    cond.notify_all()
    else:
        # Waiter: we have already acquired the condition lock.
        start_wait = time.time()
        cond.wait(timeout=30)
        waited = time.time() - start_wait
        cond.release()

        # After wait, the cache should be set (either by the fetcher or a timeout)
        cached = cache_get(path)
        if cached is None:
            # If the wait timed out, try one more time
            cached = cache_get(path)
            if cached is None:
                raise RuntimeError("Coalesced request failed – no cache entry after wait")
        logger.info(f"[COALESCED] {path} waited {waited:.3f}s")
        return cached, "coalesced"


# Handler
class CDNHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def do_GET(self):
        start_time = time.time()
        client_ip = self.client_address[0]
        path = normalize_path(self.path.split("?")[0])

        if not is_valid_path(path):
            self._respond(400, {"error": "Invalid URL path."})
            logger.warning(f"[BLOCKED]    {client_ip} invalid path: {path}")
            return

        if is_rate_limited(client_ip):
            self._respond(429, {"error": "Rate limit exceeded."})
            logger.warning(f"[RATE LIMIT] {client_ip} exceeded limit on {path}")
            return

        try:
            response, source = get_response(path)
            elapsed = time.time() - start_time
            if source == "cache":
                logger.info(f"[SERVED] {path} from cache | Total: {elapsed:.3f}s")
            elif source == "coalesced":
                logger.info(f"[SERVED] {path} (coalesced) | Total: {elapsed:.3f}s")
            else:
                logger.info(f"[SERVED] {path} from backend | Total: {elapsed:.3f}s")
            self._respond(200, response)
        except Exception as e:
            self._respond(500, {"error": "Internal server error"})
            logger.error(f"[ERROR] Failed to serve {path}: {e}")

    def do_POST(self):
        self._respond(405, {"error": "Only GET requests are allowed."})

    def _respond(self, status: int, body: dict):
        try:
            payload = json.dumps(body).encode('utf-8')
        except Exception:
            payload = json.dumps({"error": "Internal error"}).encode('utf-8')
            status = 500

        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


# Server with thread pool
class ThreadPoolHTTPServer(HTTPServer):
    def __init__(self, *args, max_workers=10, **kwargs):
        super().__init__(*args, **kwargs)
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

    def process_request(self, request, client_address):
        self.executor.submit(self._process_request, request, client_address)

    def _process_request(self, request, client_address):
        self.finish_request(request, client_address)
        self.shutdown_request(request)

    def shutdown(self):
        self.executor.shutdown(wait=False)
        super().shutdown()


if __name__ == "__main__":
    server = ThreadPoolHTTPServer((CDN_HOST, CDN_PORT), CDNHandler, max_workers=THREAD_POOL_SIZE)
    logger.info(f"[CDN] Mini CDN started on http://{CDN_HOST}:{CDN_PORT}")
    logger.info(f"[CDN] Cache TTL: {CACHE_TTL}s | Rate limit: 20 req/10s")
    logger.info(f"[CDN] Backend pool: {BACKEND_SERVERS}")
    logger.info(f"[CDN] Thread pool size: {THREAD_POOL_SIZE}")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("[CDN] Shutting down...")
        lb.stop()
        server.shutdown()
        server.server_close()