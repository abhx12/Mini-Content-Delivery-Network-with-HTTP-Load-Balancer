"""
load_balancer.py
----------------
Layer 2: HTTP Load Balancer
- Round Robin server selection
- Health checks (periodic)
- Retry on failure
- Request forwarding with timeout
- Per-request logging
"""

import time
import threading
import logging
import requests

# ──────────────────────────────────────────────
# Logging Setup
# ──────────────────────────────────────────────
logger = logging.getLogger("LoadBalancer")


class LoadBalancer:
    def __init__(self, backend_servers: list[str], health_check_interval: int = 10):
        """
        Args:
            backend_servers: List of backend URLs, e.g. ["http://localhost:8001", ...]
            health_check_interval: Seconds between health checks
        """
        self.all_servers = list(backend_servers)          # full original list
        self.healthy_servers = list(backend_servers)      # servers currently alive
        self._lock = threading.Lock()                     # protects round-robin index + healthy list
        self._rr_index = 0                               # round-robin pointer
        self.request_timeout = 5                         # seconds to wait for backend
        self.max_retries = 3                             # retry attempts on failure

        # Start background health-check thread
        self._health_check_interval = health_check_interval
        self._stop_event = threading.Event()
        self._health_thread = threading.Thread(
            target=self._health_check_loop, daemon=True
        )
        self._health_thread.start()
        logger.info(f"[LB] Initialized with servers: {self.all_servers}")

    # ──────────────────────────────────────────
    # Round-Robin Server Selection
    # ──────────────────────────────────────────
    def _get_next_server(self) -> str | None:
        """Return next healthy server using round-robin; None if none available."""
        with self._lock:
            if not self.healthy_servers:
                return None
            server = self.healthy_servers[self._rr_index % len(self.healthy_servers)]
            self._rr_index = (self._rr_index + 1) % len(self.healthy_servers)
            return server

    # ──────────────────────────────────────────
    # Forward Request to Backend
    # ──────────────────────────────────────────
    def forward(self, path: str) -> dict:
        """
        Forward a GET request to a backend server.
        Retries up to max_retries times on failure.

        Returns:
            dict with keys: server, status_code, body, response_time
        Raises:
            RuntimeError if no healthy server can fulfill the request.
        """
        tried_servers = set()

        for attempt in range(1, self.max_retries + 1):
            server = self._get_next_server()

            # Skip already-tried servers when possible
            if server in tried_servers:
                # Try to pick a different one
                with self._lock:
                    remaining = [s for s in self.healthy_servers if s not in tried_servers]
                if remaining:
                    server = remaining[0]
                else:
                    break  # All servers tried

            if server is None:
                raise RuntimeError("[LB] No healthy backend servers available.")

            tried_servers.add(server)
            server_label = self._server_label(server)

            logger.info(f"[LB] Forwarding {path} → {server_label} (attempt {attempt})")

            try:
                start = time.time()
                response = requests.get(
                    f"{server}{path}",
                    timeout=self.request_timeout
                )
                elapsed = time.time() - start

                logger.info(
                    f"[LB] Response received from {server_label} | "
                    f"Status: {response.status_code} | Time: {elapsed:.3f}s"
                )

                return {
                    "server": server_label,
                    "status_code": response.status_code,
                    "body": response.json(),
                    "response_time": elapsed,
                }

            except requests.exceptions.RequestException as e:
                logger.warning(
                    f"[LB] {server_label} failed on attempt {attempt}: {e}. "
                    f"Marking unhealthy and retrying..."
                )
                self._mark_unhealthy(server)

        raise RuntimeError(
            f"[LB] All retry attempts exhausted for path '{path}'."
        )

    # ──────────────────────────────────────────
    # Health Checks
    # ──────────────────────────────────────────
    def _health_check_loop(self):
        """Background thread: periodically ping each server."""
        while not self._stop_event.is_set():
            time.sleep(self._health_check_interval)
            self._run_health_checks()

    def _run_health_checks(self):
        """Ping all known servers; restore or remove from healthy list."""
        for server in self.all_servers:
            alive = self._ping(server)
            label = self._server_label(server)
            with self._lock:
                if alive and server not in self.healthy_servers:
                    self.healthy_servers.append(server)
                    logger.info(f"[LB] [HEALTH] {label} is back ONLINE ✓")
                elif not alive and server in self.healthy_servers:
                    self.healthy_servers.remove(server)
                    logger.warning(f"[LB] [HEALTH] {label} is OFFLINE ✗")

    def _ping(self, server: str) -> bool:
        """Return True if server responds to a quick GET /."""
        try:
            resp = requests.get(f"{server}/", timeout=2)
            return resp.status_code < 500
        except Exception:
            return False

    def _mark_unhealthy(self, server: str):
        """Immediately remove a server from the healthy pool."""
        with self._lock:
            if server in self.healthy_servers:
                self.healthy_servers.remove(server)
                logger.warning(
                    f"[LB] {self._server_label(server)} removed from healthy pool."
                )

    # ──────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────
    def _server_label(self, server: str) -> str:
        """Convert URL to friendly label, e.g. http://localhost:8001 → Server-8001."""
        port = server.split(":")[-1]
        return f"Server-{port}"

    def status(self) -> dict:
        """Return current load balancer status."""
        with self._lock:
            return {
                "all_servers": self.all_servers,
                "healthy_servers": self.healthy_servers,
                "rr_index": self._rr_index,
            }

    def stop(self):
        """Stop the health-check thread."""
        self._stop_event.set()