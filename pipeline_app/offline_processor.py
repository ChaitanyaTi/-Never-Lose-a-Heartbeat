"""
Device Offline Processor
Performs Section 3 window processing directly on the device when in offline mode.
"""

import json
import logging
import sqlite3
import time
from typing import Dict, List, Optional

from pipeline_app.db import DatabaseManager
from pipeline_app.processor import WindowProcessor

logger = logging.getLogger("OfflineProcessor")


class DeviceOfflineProcessor:
    """Computes Section 3 analytics locally on the device when disconnected."""

    def __init__(self, db: DatabaseManager):
        self.db = db
        self.processor = WindowProcessor()
        self.running = False
        self.current_session_id: Optional[str] = None

    def step(self):
        mode = self.db.get_pipeline_mode()
        if mode != "offline":
            return

        session_id = self.db.get_active_session_id()
        if not session_id:
            return

        if session_id != self.current_session_id:
            self.processor = WindowProcessor()
            self.current_session_id = session_id

        conn = self.db._conn()
        row = conn.execute(
            "SELECT MAX(sample_index) as max_idx FROM samples WHERE session_id = ?", (session_id,)
        ).fetchone()
        if not row or row["max_idx"] is None:
            return

        max_idx = row["max_idx"]
        max_window = max_idx // 400

        # Query existing offline processed windows
        cursor = conn.execute(
            "SELECT DISTINCT window FROM results WHERE session_id = ? AND mode = 'offline'",
            (session_id,),
        )
        processed_windows = {r["window"] for r in cursor}

        for w in range(max_window + 1):
            if w in processed_windows:
                continue

            # Need samples from start of window (and lookback for HR if w >= 4)
            start_idx = 400 * w
            end_idx = start_idx + 399
            hr_start = 400 * (w - 4) if w >= 4 else start_idx

            samples_cur = conn.execute(
                """
                SELECT sensor, sample_index, ch1, ch2, ch3 
                FROM samples 
                WHERE session_id = ? AND sample_index >= ? AND sample_index <= ?
                ORDER BY sample_index ASC
                """,
                (session_id, hr_start, end_idx),
            )

            s1_dict = {}
            s2_dict = {}
            for r in samples_cur:
                idx = r["sample_index"]
                if r["sensor"] == 1:
                    s1_dict[idx] = [r["ch1"], r["ch2"], r["ch3"]]
                    self.processor.feed_sensor1_sample(idx, r["ch2"])
                else:
                    s2_dict[idx] = [r["ch1"], r["ch2"]]

            res = self.processor.compute_window(w, s1_dict, s2_dict, mode="offline")
            self.db.insert_result(
                session_id=session_id,
                window=w,
                mode="offline",
                missing_1=res["missing_1"],
                missing_2=res["missing_2"],
                complete=res["complete"],
                mean_1=res["mean_1"],
                mean_2=res["mean_2"],
                min_1=res["min_1"],
                min_2=res["min_2"],
                max_1=res["max_1"],
                max_2=res["max_2"],
                magnitude=res["magnitude"],
                heart_rate=res["heart_rate"],
            )

    def run(self):
        self.running = True
        logger.info("Device offline processor running...")
        while self.running:
            try:
                self.step()
            except Exception as e:
                logger.error(f"Error in offline processor: {e}")
            time.sleep(0.5)

    def stop(self):
        self.running = False
