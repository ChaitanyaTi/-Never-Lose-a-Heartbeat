"""
Network Monitor & Mode Switcher
Continuously monitors cloud connectivity, automatically transitions between 'online' and 'offline'
within <= 10 seconds, and coordinates local vs cloud processing engines.
"""

import logging
import threading
import time
import urllib.request
import urllib.error
from typing import Callable, Optional

from pipeline_app.config import (
    CLOUD_URL,
    NETWORK_PROBE_INTERVAL,
    OFFLINE_CONSECUTIVE_FAILURES,
    ONLINE_CONSECUTIVE_SUCCESSES,
)
from pipeline_app.db import DatabaseManager

logger = logging.getLogger("NetworkMonitor")


class NetworkMonitor:
    """Probes cloud health and switches pipeline modes within <= 10 seconds."""

    def __init__(
        self,
        db: DatabaseManager,
        cloud_url: str = CLOUD_URL,
        on_mode_change: Optional[Callable[[str], None]] = None,
        probe_interval: float = NETWORK_PROBE_INTERVAL,
    ):
        self.db = db
        self.cloud_url = cloud_url.rstrip("/")
        self.on_mode_change = on_mode_change
        self.probe_interval = probe_interval

        self.current_mode = self.db.get_pipeline_mode()
        self.consecutive_failures = 0
        self.consecutive_successes = 0
        self.running = False
        self.lock = threading.Lock()

    def check_cloud_health(self) -> bool:
        """Probes the /health endpoint with a 1.5-second timeout."""
        health_url = f"{self.cloud_url}/health"
        try:
            req = urllib.request.Request(health_url, headers={"User-Agent": "MayaCardio-Pipeline"})
            with urllib.request.urlopen(req, timeout=1.5) as resp:
                return resp.status == 200
        except Exception:
            return False

    def probe_step(self):
        healthy = self.check_cloud_health()
        with self.lock:
            if healthy:
                self.consecutive_failures = 0
                self.consecutive_successes += 1
                if self.current_mode == "offline" and self.consecutive_successes >= ONLINE_CONSECUTIVE_SUCCESSES:
                    logger.info(
                        f"Network restored! Switching mode: offline -> online (within {self.consecutive_successes * self.probe_interval}s)"
                    )
                    self.current_mode = "online"
                    self.db.set_pipeline_mode("online")
                    if self.on_mode_change:
                        self.on_mode_change("online")
            else:
                self.consecutive_successes = 0
                self.consecutive_failures += 1
                if self.current_mode == "online" and self.consecutive_failures >= OFFLINE_CONSECUTIVE_FAILURES:
                    logger.warning(
                        f"Network dropped! Switching mode: online -> offline (within {self.consecutive_failures * self.probe_interval}s)"
                    )
                    self.current_mode = "offline"
                    self.db.set_pipeline_mode("offline")
                    if self.on_mode_change:
                        self.on_mode_change("offline")

    def run(self):
        self.running = True
        logger.info(f"Network monitor started. Target cloud: {self.cloud_url} | Initial mode: {self.current_mode}")
        while self.running:
            try:
                self.probe_step()
            except Exception as e:
                logger.error(f"Error in network probe: {e}")
            time.sleep(self.probe_interval)

    def stop(self):
        self.running = False
