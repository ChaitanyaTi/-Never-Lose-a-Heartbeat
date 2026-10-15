"""
Device Pipeline Daemon Coordinator
Runs the collector, network monitor, sync worker, and offline processor concurrently.
"""

import logging
import signal
import sys
import threading
import time
from typing import Optional

from pipeline_app.collector import BiosignalCollector
from pipeline_app.config import CLOUD_URL, MOCK_HOST, MOCK_PORT
from pipeline_app.db import DatabaseManager
from pipeline_app.network_monitor import NetworkMonitor
from pipeline_app.offline_processor import DeviceOfflineProcessor
from pipeline_app.sync_worker import CloudSyncWorker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger("PipelineDaemon")


class PipelineDaemon:
    """Master coordinator for all device-side pipeline services."""

    def __init__(
        self,
        mock_host: str = MOCK_HOST,
        mock_port: int = MOCK_PORT,
        cloud_url: str = CLOUD_URL,
    ):
        self.db = DatabaseManager()
        self.collector = BiosignalCollector(self.db, host=mock_host, port=mock_port)
        self.network_monitor = NetworkMonitor(
            self.db, cloud_url=cloud_url, on_mode_change=self._on_mode_change
        )
        self.sync_worker = CloudSyncWorker(self.db, cloud_url=cloud_url)
        self.offline_processor = DeviceOfflineProcessor(self.db)

        self.running = False
        self.threads = []

    def _on_mode_change(self, new_mode: str):
        logger.info(f"[Coordinator] Mode changed to: {new_mode.upper()}")
        if new_mode == "online":
            # Trigger immediate catch-up sync
            threading.Thread(target=self.sync_worker.step, daemon=True).start()

    def start(self):
        self.running = True
        logger.info("Starting MayaCardio Pipeline Daemon...")

        t_monitor = threading.Thread(target=self.network_monitor.run, name="NetMonitor", daemon=True)
        t_collector = threading.Thread(target=self.collector.run, name="Collector", daemon=True)
        t_sync = threading.Thread(target=self.sync_worker.run, name="SyncWorker", daemon=True)
        t_proc = threading.Thread(target=self.offline_processor.run, name="OfflineProc", daemon=True)

        self.threads = [t_monitor, t_collector, t_sync, t_proc]
        for t in self.threads:
            t.start()

        logger.info("All pipeline background services started successfully.")

    def stop(self):
        logger.info("Stopping Pipeline Daemon...")
        self.running = False
        self.collector.stop()
        self.network_monitor.stop()
        self.sync_worker.stop()
        self.offline_processor.stop()
        self.db.close()
        logger.info("Pipeline Daemon stopped cleanly.")


def main():
    daemon = PipelineDaemon()

    def signal_handler(sig, frame):
        logger.info("Received termination signal.")
        daemon.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    daemon.start()
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
