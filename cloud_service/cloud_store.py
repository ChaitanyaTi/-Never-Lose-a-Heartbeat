"""
Cloud Service Storage & Processing Store
Maintains the cloud-side deduplicated replica of sessions, samples, and window results.
"""

import json
import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Dict, Generator, List, Optional

from pipeline_app.processor import WindowProcessor

logger = logging.getLogger("CloudStore")
_local = threading.local()


_cloud_connections = []
_cloud_conns_lock = threading.Lock()


class CloudStore:
    """Manages cloud persistence and online processing."""

    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = str(db_path or (Path("data") / "cloud_store.db"))
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
        self.processors: Dict[str, WindowProcessor] = {}
        self.lock = threading.Lock()

    def _get_conn(self) -> sqlite3.Connection:
        if not hasattr(_local, "conn") or _local.conn is None:
            conn = sqlite3.connect(self.db_path, timeout=10.0, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute("PRAGMA synchronous = NORMAL;")
            _local.conn = conn
            with _cloud_conns_lock:
                _cloud_connections.append(conn)
        return _local.conn

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        with conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS cloud_sessions (
                    session_id TEXT PRIMARY KEY,
                    created_at REAL
                );

                CREATE TABLE IF NOT EXISTS cloud_samples (
                    session_id TEXT NOT NULL,
                    sensor INTEGER NOT NULL,
                    sample_index INTEGER NOT NULL,
                    ch1 INTEGER NOT NULL,
                    ch2 INTEGER NOT NULL,
                    ch3 INTEGER,
                    created_at REAL NOT NULL,
                    PRIMARY KEY (session_id, sensor, sample_index)
                );

                CREATE TABLE IF NOT EXISTS cloud_losses (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL,
                    sensor INTEGER NOT NULL,
                    from_index INTEGER NOT NULL,
                    to_index INTEGER,
                    reason TEXT NOT NULL,
                    created_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS cloud_results (
                    session_id TEXT NOT NULL,
                    window INTEGER NOT NULL,
                    mode TEXT NOT NULL,
                    missing_1 INTEGER NOT NULL,
                    missing_2 INTEGER NOT NULL,
                    complete INTEGER NOT NULL,
                    mean_1 TEXT,
                    mean_2 TEXT,
                    min_1 TEXT,
                    min_2 TEXT,
                    max_1 TEXT,
                    max_2 TEXT,
                    magnitude REAL,
                    heart_rate REAL,
                    processed_at REAL NOT NULL,
                    PRIMARY KEY (session_id, window, mode)
                );
                """
            )
        conn.close()

    def close(self):
        with _cloud_conns_lock:
            for c in list(_cloud_connections):
                try:
                    c.close()
                except Exception:
                    pass
            _cloud_connections.clear()
        if hasattr(_local, "conn"):
            _local.conn = None

    def ingest_batch(self, session_id: str, samples: List[dict], losses: List[dict]) -> dict:
        conn = self._get_conn()
        now = time.time()
        inserted_samples = 0
        inserted_losses = 0

        with conn:
            conn.execute(
                "INSERT OR IGNORE INTO cloud_sessions (session_id, created_at) VALUES (?, ?)",
                (session_id, now),
            )

            # Insert samples
            if samples:
                records = []
                for s in samples:
                    sensor = s["sensor"]
                    idx = s["index"]
                    vals = s["values"]
                    ch1 = vals[0]
                    ch2 = vals[1]
                    ch3 = vals[2] if sensor == 1 and len(vals) > 2 else None
                    records.append((session_id, sensor, idx, ch1, ch2, ch3, now))

                c = conn.executemany(
                    """
                    INSERT OR IGNORE INTO cloud_samples 
                    (session_id, sensor, sample_index, ch1, ch2, ch3, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    records,
                )
                inserted_samples = c.rowcount

            # Insert losses
            if losses:
                loss_records = []
                for lr in losses:
                    loss_records.append(
                        (session_id, lr["sensor"], lr["from_index"], lr.get("to_index"), lr.get("reason", "unknown"), now)
                    )
                c_loss = conn.executemany(
                    """
                    INSERT INTO cloud_losses 
                    (session_id, sensor, from_index, to_index, reason, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    loss_records,
                )
                inserted_losses = c_loss.rowcount

        # Trigger online processing for any newly complete 1-second windows
        self.trigger_online_processing(session_id)

        return {"inserted_samples": inserted_samples, "inserted_losses": inserted_losses}

    def trigger_online_processing(self, session_id: str):
        """Processes available windows in the cloud with mode='online' within <= 5s."""
        conn = self._get_conn()
        with self.lock:
            if session_id not in self.processors:
                self.processors[session_id] = WindowProcessor()
            proc = self.processors[session_id]

        # Find latest sample index
        row = conn.execute(
            "SELECT MAX(sample_index) as max_idx FROM cloud_samples WHERE session_id = ?", (session_id,)
        ).fetchone()
        if not row or row["max_idx"] is None:
            return

        max_idx = row["max_idx"]
        max_window = max_idx // 400

        # Query existing processed windows in cloud
        cursor = conn.execute(
            "SELECT DISTINCT window FROM cloud_results WHERE session_id = ? AND mode = 'online'",
            (session_id,),
        )
        processed_windows = {r["window"] for r in cursor}

        # Process any window up to max_window - 1 (or max_window if full)
        for w in range(max_window + 1):
            if w in processed_windows:
                continue

            # Check if we should process window w
            # Fetch samples for window w
            start_idx = 400 * w
            end_idx = start_idx + 399

            # Lookback for HR requires up to 400(w-4) if w >= 4
            hr_start = 400 * (w - 4) if w >= 4 else start_idx
            query_cursor = conn.execute(
                """
                SELECT sensor, sample_index, ch1, ch2, ch3 
                FROM cloud_samples 
                WHERE session_id = ? AND sample_index >= ? AND sample_index <= ?
                ORDER BY sample_index ASC
                """,
                (session_id, hr_start, end_idx),
            )

            s1_dict = {}
            s2_dict = {}
            for r in query_cursor:
                idx = r["sample_index"]
                if r["sensor"] == 1:
                    s1_dict[idx] = [r["ch1"], r["ch2"], r["ch3"]]
                    proc.feed_sensor1_sample(idx, r["ch2"])
                else:
                    s2_dict[idx] = [r["ch1"], r["ch2"]]

            # Compute window result
            res = proc.compute_window(w, s1_dict, s2_dict, mode="online")
            now = time.time()
            with conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO cloud_results
                    (session_id, window, mode, missing_1, missing_2, complete,
                     mean_1, mean_2, min_1, min_2, max_1, max_2, magnitude, heart_rate, processed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        session_id,
                        w,
                        "online",
                        res["missing_1"],
                        res["missing_2"],
                        1 if res["complete"] else 0,
                        json.dumps(res["mean_1"]) if res["mean_1"] is not None else None,
                        json.dumps(res["mean_2"]) if res["mean_2"] is not None else None,
                        json.dumps(res["min_1"]) if res["min_1"] is not None else None,
                        json.dumps(res["min_2"]) if res["min_2"] is not None else None,
                        json.dumps(res["max_1"]) if res["max_1"] is not None else None,
                        json.dumps(res["max_2"]) if res["max_2"] is not None else None,
                        res["magnitude"],
                        res["heart_rate"],
                        now,
                    ),
                )

    def get_sample_count(self, session_id: str) -> int:
        conn = self._get_conn()
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM cloud_samples WHERE session_id = ?", (session_id,)
        ).fetchone()
        return row["cnt"] if row else 0

    def export_samples_stream(self, session_id: str) -> Generator[str, None, None]:
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT sensor, sample_index, ch1, ch2, ch3 
            FROM cloud_samples 
            WHERE session_id = ? 
            ORDER BY sample_index ASC, sensor ASC
            """,
            (session_id,),
        )
        for row in cursor:
            vals = (
                [row["ch1"], row["ch2"], row["ch3"]]
                if row["sensor"] == 1
                else [row["ch1"], row["ch2"]]
            )
            yield json.dumps({"sensor": row["sensor"], "index": row["sample_index"], "values": vals})

    def export_losses_stream(self, session_id: str) -> Generator[str, None, None]:
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT sensor, from_index, to_index, reason 
            FROM cloud_losses 
            WHERE session_id = ? 
            ORDER BY id ASC
            """,
            (session_id,),
        )
        for row in cursor:
            yield json.dumps({
                "sensor": row["sensor"],
                "from_index": row["from_index"],
                "to_index": row["to_index"],
                "reason": row["reason"],
            })

    def get_results_stream(self, session_id: str) -> Generator[str, None, None]:
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT window, mode, missing_1, missing_2, complete,
                   mean_1, mean_2, min_1, min_2, max_1, max_2, magnitude, heart_rate
            FROM cloud_results
            WHERE session_id = ?
            ORDER BY window ASC, mode ASC
            """,
            (session_id,),
        )
        for row in cursor:
            res_dict = {
                "window": row["window"],
                "mode": row["mode"],
                "missing_1": row["missing_1"],
                "missing_2": row["missing_2"],
                "complete": bool(row["complete"]),
                "mean": {
                    "sensor_1": json.loads(row["mean_1"]) if row["mean_1"] else None,
                    "sensor_2": json.loads(row["mean_2"]) if row["mean_2"] else None,
                },
                "min": {
                    "sensor_1": json.loads(row["min_1"]) if row["min_1"] else None,
                    "sensor_2": json.loads(row["min_2"]) if row["min_2"] else None,
                },
                "max": {
                    "sensor_1": json.loads(row["max_1"]) if row["max_1"] else None,
                    "sensor_2": json.loads(row["max_2"]) if row["max_2"] else None,
                },
                "magnitude": round(row["magnitude"], 1) if row["magnitude"] is not None else None,
                "heart_rate": round(row["heart_rate"], 1) if row["heart_rate"] is not None else None,
            }
            yield json.dumps(res_dict)
