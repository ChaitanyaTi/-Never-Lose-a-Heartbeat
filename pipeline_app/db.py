"""
Pipeline Storage Engine (SQLite + WAL)
Provides crash-proof, power-safe ACID storage for sensor samples, loss logs, and window results.
"""

import json
import logging
import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Dict, Generator, List, Optional, Tuple

from pipeline_app.config import DB_DIR, DB_PATH

logger = logging.getLogger("PipelineDB")

_local = threading.local()


_all_connections = []
_conns_lock = threading.Lock()

def get_db_connection(db_path: Optional[Path] = None) -> sqlite3.Connection:
    """Returns a thread-local SQLite connection configured for concurrent WAL operation."""
    path = str(db_path or DB_PATH)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    
    conn = sqlite3.connect(path, timeout=10.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    
    # Configure WAL mode and durability pragmas
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = FULL;")  # fsync on every commit to bound power-cut loss <= 0.25s
    conn.execute("PRAGMA busy_timeout = 10000;")
    conn.execute("PRAGMA foreign_keys = ON;")

    with _conns_lock:
        _all_connections.append(conn)
    return conn


def init_db(db_path: Optional[Path] = None):
    """Initializes tables and indexes."""
    conn = get_db_connection(db_path)
    with conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY,
                first_seen_at REAL,
                last_seen_at REAL,
                is_closed INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS samples (
                session_id TEXT NOT NULL,
                sensor INTEGER NOT NULL,
                sample_index INTEGER NOT NULL,
                ch1 INTEGER NOT NULL,
                ch2 INTEGER NOT NULL,
                ch3 INTEGER,
                is_synced INTEGER DEFAULT 0,
                created_at REAL NOT NULL,
                PRIMARY KEY (session_id, sensor, sample_index)
            );

            CREATE INDEX IF NOT EXISTS idx_samples_sync 
            ON samples (session_id, is_synced, sensor, sample_index);

            CREATE TABLE IF NOT EXISTS losses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                sensor INTEGER NOT NULL,
                from_index INTEGER NOT NULL,
                to_index INTEGER,
                reason TEXT NOT NULL,
                is_synced INTEGER DEFAULT 0,
                created_at REAL NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_losses_sync 
            ON losses (session_id, is_synced);

            CREATE TABLE IF NOT EXISTS results (
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
                is_synced INTEGER DEFAULT 0,
                PRIMARY KEY (session_id, window, mode)
            );

            CREATE TABLE IF NOT EXISTS pipeline_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )
    conn.close()


class DatabaseManager:
    """Manages transactional database operations for the pipeline."""

    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or DB_PATH
        init_db(self.db_path)

    def _conn(self) -> sqlite3.Connection:
        if not hasattr(_local, "conn") or _local.conn is None:
            _local.conn = get_db_connection(self.db_path)
        return _local.conn

    def close(self):
        with _conns_lock:
            for c in list(_all_connections):
                try:
                    c.close()
                except Exception:
                    pass
            _all_connections.clear()
        if hasattr(_local, "conn"):
            _local.conn = None

    def register_session(self, session_id: str):
        now = time.time()
        conn = self._conn()
        with conn:
            conn.execute(
                """
                INSERT INTO sessions (session_id, first_seen_at, last_seen_at)
                VALUES (?, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET last_seen_at = excluded.last_seen_at
                """,
                (session_id, now, now),
            )
            conn.execute(
                """
                INSERT INTO pipeline_state (key, value)
                VALUES ('active_session_id', ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (session_id,),
            )

    def get_active_session_id(self) -> Optional[str]:
        conn = self._conn()
        row = conn.execute("SELECT value FROM pipeline_state WHERE key = 'active_session_id'").fetchone()
        if row:
            return row["value"]
        # Fallback to most recent session
        row = conn.execute("SELECT session_id FROM sessions ORDER BY last_seen_at DESC LIMIT 1").fetchone()
        return row["session_id"] if row else None

    def set_pipeline_mode(self, mode: str):
        conn = self._conn()
        with conn:
            conn.execute(
                """
                INSERT INTO pipeline_state (key, value)
                VALUES ('current_mode', ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (mode,),
            )

    def get_pipeline_mode(self) -> str:
        conn = self._conn()
        row = conn.execute("SELECT value FROM pipeline_state WHERE key = 'current_mode'").fetchone()
        return row["value"] if row else "online"

    def get_max_sample_index(self, session_id: str, sensor: int) -> int:
        conn = self._conn()
        row = conn.execute(
            "SELECT MAX(sample_index) as max_idx FROM samples WHERE session_id = ? AND sensor = ?",
            (session_id, sensor),
        ).fetchone()
        return row["max_idx"] if (row and row["max_idx"] is not None) else -1

    def insert_samples_batch(
        self, session_id: str, sensor: int, first_index: int, data: List[List[int]]
    ) -> int:
        """
        Atomically writes a batch of samples to disk.
        Uses INSERT OR IGNORE to automatically discard duplicates.
        """
        if not data:
            return 0
        now = time.time()
        records = []
        for i, values in enumerate(data):
            idx = first_index + i
            ch1 = values[0]
            ch2 = values[1]
            ch3 = values[2] if sensor == 1 and len(values) > 2 else None
            records.append((session_id, sensor, idx, ch1, ch2, ch3, 0, now))

        conn = self._conn()
        with conn:
            cursor = conn.executemany(
                """
                INSERT OR IGNORE INTO samples 
                (session_id, sensor, sample_index, ch1, ch2, ch3, is_synced, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                records,
            )
            # Update session last_seen_at
            conn.execute(
                "UPDATE sessions SET last_seen_at = ? WHERE session_id = ?",
                (now, session_id),
            )
            return cursor.rowcount

    def insert_loss(
        self, session_id: str, sensor: int, from_index: int, to_index: Optional[int], reason: str
    ):
        now = time.time()
        conn = self._conn()
        with conn:
            conn.execute(
                """
                INSERT INTO losses (session_id, sensor, from_index, to_index, reason, is_synced, created_at)
                VALUES (?, ?, ?, ?, ?, 0, ?)
                """,
                (session_id, sensor, from_index, to_index, reason, now),
            )

    def insert_result(
        self,
        session_id: str,
        window: int,
        mode: str,
        missing_1: int,
        missing_2: int,
        complete: bool,
        mean_1: Optional[List[float]],
        mean_2: Optional[List[float]],
        min_1: Optional[List[int]],
        min_2: Optional[List[int]],
        max_1: Optional[List[int]],
        max_2: Optional[List[int]],
        magnitude: Optional[float],
        heart_rate: Optional[float],
    ):
        now = time.time()
        conn = self._conn()
        with conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO results
                (session_id, window, mode, missing_1, missing_2, complete,
                 mean_1, mean_2, min_1, min_2, max_1, max_2, magnitude, heart_rate, processed_at, is_synced)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                """,
                (
                    session_id,
                    window,
                    mode,
                    missing_1,
                    missing_2,
                    1 if complete else 0,
                    json.dumps(mean_1) if mean_1 is not None else None,
                    json.dumps(mean_2) if mean_2 is not None else None,
                    json.dumps(min_1) if min_1 is not None else None,
                    json.dumps(min_2) if min_2 is not None else None,
                    json.dumps(max_1) if max_1 is not None else None,
                    json.dumps(max_2) if max_2 is not None else None,
                    magnitude,
                    heart_rate,
                    now,
                ),
            )

    def get_status(self, session_id: Optional[str] = None) -> dict:
        sess_id = session_id or self.get_active_session_id()
        current_mode = self.get_pipeline_mode()
        if not sess_id:
            return {
                "mode": current_mode,
                "samples_saved": 0,
                "samples_in_cloud": 0,
                "backlog": 0,
            }

        conn = self._conn()
        row_saved = conn.execute(
            "SELECT COUNT(*) as cnt FROM samples WHERE session_id = ?", (sess_id,)
        ).fetchone()
        samples_saved = row_saved["cnt"] if row_saved else 0

        row_synced = conn.execute(
            "SELECT COUNT(*) as cnt FROM samples WHERE session_id = ? AND is_synced = 1", (sess_id,)
        ).fetchone()
        samples_in_cloud = row_synced["cnt"] if row_synced else 0

        backlog = max(0, samples_saved - samples_in_cloud)
        return {
            "mode": current_mode,
            "samples_saved": samples_saved,
            "samples_in_cloud": samples_in_cloud,
            "backlog": backlog,
        }

    def export_samples_stream(self, session_id: str) -> Generator[str, None, None]:
        """Yields JSON line per sample for pipeline export --session ID --from device."""
        conn = self._conn()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT sensor, sample_index, ch1, ch2, ch3 
            FROM samples 
            WHERE session_id = ? 
            ORDER BY sample_index ASC, sensor ASC
            """,
            (session_id,),
        )
        for row in cursor:
            if row["sensor"] == 1:
                vals = [row["ch1"], row["ch2"], row["ch3"]]
            else:
                vals = [row["ch1"], row["ch2"]]
            yield json.dumps({"sensor": row["sensor"], "index": row["sample_index"], "values": vals})

    def export_losses_stream(self, session_id: str) -> Generator[str, None, None]:
        """Yields JSON line per loss for pipeline export --session ID --from device."""
        conn = self._conn()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT sensor, from_index, to_index, reason 
            FROM losses 
            WHERE session_id = ? 
            ORDER BY id ASC
            """,
            (session_id,),
        )
        for row in cursor:
            loss_dict = {
                "sensor": row["sensor"],
                "from_index": row["from_index"],
                "to_index": row["to_index"],
                "reason": row["reason"],
            }
            yield json.dumps(loss_dict)

    def get_results_stream(self, session_id: str) -> Generator[str, None, None]:
        """Yields JSON line per window and mode with Section 3 outputs."""
        conn = self._conn()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT window, mode, missing_1, missing_2, complete,
                   mean_1, mean_2, min_1, min_2, max_1, max_2, magnitude, heart_rate
            FROM results
            WHERE session_id = ?
            ORDER BY window ASC, mode ASC
            """,
            (session_id,),
        )
        for row in cursor:
            # Reconstruct exact Section 3 structure
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

    def get_unsynced_samples(self, session_id: str, limit: int = 4000) -> List[dict]:
        conn = self._conn()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT sensor, sample_index, ch1, ch2, ch3 
            FROM samples 
            WHERE session_id = ? AND is_synced = 0 
            ORDER BY sample_index ASC, sensor ASC 
            LIMIT ?
            """,
            (session_id, limit),
        )
        items = []
        for row in cursor:
            vals = (
                [row["ch1"], row["ch2"], row["ch3"]]
                if row["sensor"] == 1
                else [row["ch1"], row["ch2"]]
            )
            items.append({"sensor": row["sensor"], "index": row["sample_index"], "values": vals})
        return items

    def mark_samples_synced(self, session_id: str, items: List[dict]):
        if not items:
            return
        conn = self._conn()
        with conn:
            conn.executemany(
                """
                UPDATE samples 
                SET is_synced = 1 
                WHERE session_id = ? AND sensor = ? AND sample_index = ?
                """,
                [(session_id, it["sensor"], it["index"]) for it in items],
            )

    def get_unsynced_losses(self, session_id: str) -> List[dict]:
        conn = self._conn()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, sensor, from_index, to_index, reason 
            FROM losses 
            WHERE session_id = ? AND is_synced = 0 
            ORDER BY id ASC
            """,
            (session_id,),
        )
        return [
            {
                "id": row["id"],
                "sensor": row["sensor"],
                "from_index": row["from_index"],
                "to_index": row["to_index"],
                "reason": row["reason"],
            }
            for row in cursor
        ]

    def mark_losses_synced(self, loss_ids: List[int]):
        if not loss_ids:
            return
        conn = self._conn()
        with conn:
            conn.executemany(
                "UPDATE losses SET is_synced = 1 WHERE id = ?",
                [(lid,) for lid in loss_ids],
            )
