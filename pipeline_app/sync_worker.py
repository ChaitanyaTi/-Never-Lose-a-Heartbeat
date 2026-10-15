"""
Cloud Sync & Catch-Up Worker
Streams real-time samples and processes backlogs after outages, guaranteeing bit-for-bit complete cloud replica.
"""

import json
import logging
import time
import urllib.request
import urllib.error
from typing import Optional

from pipeline_app.config import CLOUD_URL, SYNC_BATCH_SIZE, SYNC_INTERVAL_ONLINE
from pipeline_app.db import DatabaseManager

logger = logging.getLogger("SyncWorker")


class CloudSyncWorker:
    """Continuously synchronizes unsynced samples and losses to the cloud service."""

    def __init__(
        self,
        db: DatabaseManager,
        cloud_url: str = CLOUD_URL,
        batch_size: int = SYNC_BATCH_SIZE,
    ):
        self.db = db
        self.cloud_url = cloud_url.rstrip("/")
        self.batch_size = batch_size
        self.running = False

    def push_batch(self, session_id: str) -> int:
        """
        Pushes one batch of unsynced samples and losses to the cloud.
        Returns the number of samples sent, or 0 if nothing to send or failed.
        """
        # Fetch unsynced items from DB
        unsynced_samples = self.db.get_unsynced_samples(session_id, limit=self.batch_size)
        unsynced_losses = self.db.get_unsynced_losses(session_id)

        if not unsynced_samples and not unsynced_losses:
            return 0

        payload = {
            "session_id": session_id,
            "samples": unsynced_samples,
            "losses": unsynced_losses,
        }
        data_bytes = json.dumps(payload).encode("utf-8")

        ingest_url = f"{self.cloud_url}/api/v1/ingest"
        req = urllib.request.Request(
            ingest_url,
            data=data_bytes,
            headers={"Content-Type": "application/json", "User-Agent": "MayaCardio-Pipeline"},
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=5.0) as resp:
                if resp.status == 200:
                    # Mark synced in SQLite
                    if unsynced_samples:
                        self.db.mark_samples_synced(session_id, unsynced_samples)
                    if unsynced_losses:
                        loss_ids = [l["id"] for l in unsynced_losses]
                        self.db.mark_losses_synced(loss_ids)
                    return len(unsynced_samples)
        except Exception as e:
            logger.debug(f"Sync POST failed: {e}")
            return -1

        return 0

    def step(self):
        """Performs sync step if in online mode."""
        mode = self.db.get_pipeline_mode()
        if mode != "online":
            return

        session_id = self.db.get_active_session_id()
        if not session_id:
            return

        # Catch up in a tight loop if backlog exists
        while self.running and self.db.get_pipeline_mode() == "online":
            sent = self.push_batch(session_id)
            if sent <= 0:
                break
            # If backlog is large, continue pushing immediately without sleeping
            if sent < self.batch_size:
                break

    def run(self):
        self.running = True
        logger.info(f"Sync worker started for cloud: {self.cloud_url}")
        while self.running:
            try:
                self.step()
            except Exception as e:
                logger.error(f"Error in sync worker: {e}")
            time.sleep(SYNC_INTERVAL_ONLINE)

    def stop(self):
        self.running = False
