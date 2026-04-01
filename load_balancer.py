"""
load_balancer.py
----------------
Layer 2: HTTP Load Balancer
- Least Connections server selection ✅ (UPDATED)
- Health checks (periodic)
- Retry on failure
- Request forwarding with timeout
"""

import time
import threading
import logging
import requests

logger = logging.getLogger("LoadBalancer")


class LoadBalancer:
    def __init__(self, backend_servers: list[str], health_check_interval: int = 10):
        self.all_servers = list(backend_servers)
        self.healthy_servers = list(backend_servers)

        self._lock = threading.Lock()

        # ✅ Track active connections per server
        self.active_connections = {server: 0 for server in backend_servers}

        self.request_timeout = 5
        self.max_retries = 3

        # Health check thread
        self._health_check_interval = health_check_interval
        self._stop_event = threading.Event()
        self._health_thread = threading.Thread(
            target=self._health_check_loop, daemon=True
        )
        self._health_thread.start()

        logger.info(f"[LB] Initialized with servers: {self.all_servers}")

    # ──────────────────────────────────────────
    # ✅ LEAST CONNECTIONS SELECTION
    # ──────────────────────────────────────────
    def _get_least_connection_server(self) -> str | None:
        with self._lock:
            if not self.healthy_servers:
                return None

            # Pick server with minimum active connections
            server = min(
                self.healthy_servers,
                key=lambda s: self.active_connections.get(s, 0)
            )

            # Increment active connections
            self.active_connections[server] += 1

            return server

    def _release_connection(self, server: str):
        """Decrement active connection count after request completes"""
        with self._lock:
            if server in self.active_connections:
                self.active_connections[server] = max(
                    0, self.active_connections[server] - 1
                )

    # ──────────────────────────────────────────
    # Forward Request
    # ──────────────────────────────────────────
    def forward(self, path: str) -> dict:
        tried_servers = set()

        for attempt in range(1, self.max_retries + 1):
            server = self._get_least_connection_server()

            if server is None:
                raise RuntimeError("[LB] No healthy backend servers available.")

            if server in tried_servers:
                continue

            tried_servers.add(server)
            label = self._server_label(server)

            logger.info(f"[LB] Forwarding {path} → {label} (attempt {attempt})")

            try:
                start = time.time()

                response = requests.get(
                    f"{server}{path}",
                    timeout=self.request_timeout
                )

                elapsed = time.time() - start

                logger.info(
                    f"[LB] Response from {label} | "
                    f"Status: {response.status_code} | {elapsed:.3f}s"
                )

                return {
                    "server": label,
                    "status_code": response.status_code,
                    "body": response.json(),
                    "response_time": elapsed,
                }

            except requests.exceptions.RequestException as e:
                logger.warning(
                    f"[LB] {label} failed: {e}. Marking unhealthy..."
                )
                self._mark_unhealthy(server)

            finally:
                # ✅ IMPORTANT: always release connection
                self._release_connection(server)

        raise RuntimeError(f"[LB] All retry attempts failed for '{path}'.")

    # ──────────────────────────────────────────
    # Health Checks
    # ──────────────────────────────────────────
    def _health_check_loop(self):
        while not self._stop_event.is_set():
            time.sleep(self._health_check_interval)
            self._run_health_checks()

    def _run_health_checks(self):
        for server in self.all_servers:
            alive = self._ping(server)
            label = self._server_label(server)

            with self._lock:
                if alive and server not in self.healthy_servers:
                    self.healthy_servers.append(server)
                    logger.info(f"[LB] {label} is ONLINE ✓")

                elif not alive and server in self.healthy_servers:
                    self.healthy_servers.remove(server)
                    logger.warning(f"[LB] {label} is OFFLINE ✗")

    def _ping(self, server: str) -> bool:
        try:
            resp = requests.get(f"{server}/", timeout=2)
            return resp.status_code < 500
        except Exception:
            return False

    def _mark_unhealthy(self, server: str):
        with self._lock:
            if server in self.healthy_servers:
                self.healthy_servers.remove(server)
                logger.warning(f"[LB] {self._server_label(server)} removed")

    # ──────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────
    def _server_label(self, server: str) -> str:
        port = server.split(":")[-1]
        return f"Server-{port}"

    def status(self) -> dict:
        with self._lock:
            return {
                "healthy_servers": self.healthy_servers,
                "active_connections": self.active_connections,
            }

    def stop(self):
        self._stop_event.set()