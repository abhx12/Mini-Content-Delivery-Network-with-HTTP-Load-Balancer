"""
client_simulator.py
-------------------
Simulates multiple concurrent users hitting the CDN.

Scenarios:
  1. Parallel same-URL requests  -> only 1 backend call (coalescing + cache)
  2. Repeat requests             -> instant CACHE HIT
  3. Different URLs              -> distributed via round-robin LB
  4. Invalid / dangerous paths   -> 400 blocked by CDN
  5. Non-GET method              -> 405

FIX 2: Scenario 2 now joins all Scenario 1 threads THEN sleeps 0.5s
        before starting, guaranteeing the cache is warm and response
        is truly instant (~0.001s) rather than the 2s seen before.
"""

import time
import threading
import requests

CDN_URL = "http://localhost:9000"


# ──────────────────────────────────────────────
# Single Request Helper
# ──────────────────────────────────────────────
def make_request(path: str, label: str = "") -> None:
    tag = f"[{label}]" if label else ""
    url = f"{CDN_URL}{path}"
    try:
        t0 = time.time()
        resp = requests.get(url, timeout=30)
        elapsed = time.time() - t0
        body = resp.json()
        server = body.get("server", body.get("error", "?"))
        print(
            f"{tag:20s} GET {path:20s} -> HTTP {resp.status_code} "
            f"| {elapsed:.3f}s | {server}"
        )
    except Exception as e:
        print(f"{tag:20s} GET {path:20s} -> ERROR: {e}")


# ──────────────────────────────────────────────
# Scenario 1: Parallel same-URL -> request coalescing
# ──────────────────────────────────────────────
def scenario_coalescing():
    print("\n" + "=" * 65)
    print("SCENARIO 1: 5 concurrent requests for /data")
    print("Expected: 1 backend call + 4 WAITING -> all get same response")
    print("=" * 65)
    threads = [
        threading.Thread(target=make_request, args=("/data", f"Client-{i}"), daemon=True)
        for i in range(1, 6)
    ]
    for t in threads:
        t.start()

    # FIX 2: join ALL threads before returning so Scenario 2 starts
    # only after every coalesced response is back and cache is warm.
    for t in threads:
        t.join()
    # Extra sleep to ensure cache is fully settled (avoids any threading races)
    time.sleep(0.1)


# ──────────────────────────────────────────────
# Scenario 2: Cache hit (repeat request)
# ──────────────────────────────────────────────
def scenario_cache_hit():
    print("\n" + "=" * 65)
    print("SCENARIO 2: Repeat request for /data (already cached)")
    print("Expected: instant CACHE HIT, ~0.001s, no backend call")
    print("=" * 65)
    # FIX 2: small sleep just to let any final CDN bookkeeping settle,
    # but Scenario 1 is already fully joined so cache IS populated.
    time.sleep(0.5)
    for i in range(1, 4):
        make_request("/data", f"Repeat-{i}")


# ──────────────────────────────────────────────
# Scenario 3: Different URLs -> round-robin LB
# ──────────────────────────────────────────────
def scenario_load_balance():
    print("\n" + "=" * 65)
    print("SCENARIO 3: 6 different endpoints -> round-robin across servers")
    print("Expected: /page1..6 distributed across Server-8001/8002/8003")
    print("=" * 65)
    paths = [f"/page{i}" for i in range(1, 7)]
    threads = [
        threading.Thread(target=make_request, args=(p, f"User-{i+1}"), daemon=True)
        for i, p in enumerate(paths)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


# ──────────────────────────────────────────────
# Scenario 4: Invalid / dangerous paths -> 400
# ──────────────────────────────────────────────
def scenario_invalid_path():
    print("\n" + "=" * 65)
    print("SCENARIO 4: Invalid and dangerous URL paths")
    print("Expected: all blocked with HTTP 400")
    print("=" * 65)

    # Note: requests normalises path traversal sequences before sending,
    # so /valid/../../../etc/passwd arrives at the CDN as /etc/passwd.
    # The CDN denylist (BLOCKED_PATH_PREFIXES) blocks /etc/... (FIX 3).
    bad_paths = [
        ("/etc/passwd",              "Traversal-resolved"),   # arrives normalised
        ("/etc/shadow",              "Traversal-shadow"),
        ("/windows/system32/cmd",    "WinTraversal"),
        ("/bad path with spaces",    "SpaceAttack"),
        ("/<script>alert(1)</script>", "XSS"),
    ]

    for path, label in bad_paths:
        tag = f"[{label}]"
        try:
            resp = requests.get(f"{CDN_URL}{path}", timeout=10)
            body = resp.json()
            result = body.get("error", body.get("server", "?"))
            print(f"{tag:25s} GET {path[:35]:35s} -> HTTP {resp.status_code} | {result}")
        except Exception as e:
            print(f"{tag:25s} GET {path[:35]:35s} -> ERROR: {e}")

    # Legitimate path should still work
    make_request("/valid/path", "LegitUser")


# ──────────────────────────────────────────────
# Scenario 5: POST request -> 405
# ──────────────────────────────────────────────
def scenario_non_get():
    print("\n" + "=" * 65)
    print("SCENARIO 5: POST request -> should be rejected")
    print("Expected: HTTP 405 Method Not Allowed")
    print("=" * 65)
    try:
        resp = requests.post(f"{CDN_URL}/data", timeout=5)
        print(f"[POST-Client]        POST /data -> HTTP {resp.status_code} | {resp.json()}")
    except Exception as e:
        print(f"[POST-Client]        POST /data -> ERROR: {e}")


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────
if __name__ == "__main__":
    print("\n" + "=" * 65)
    print("   Mini CDN Client Simulator")
    print(f"   Target CDN: {CDN_URL}")
    print("=" * 65)

    scenario_coalescing()
    scenario_cache_hit()
    scenario_load_balance()
    scenario_invalid_path()
    scenario_non_get()

    print("\n" + "=" * 65)
    print("All scenarios complete.")
    print("=" * 65)