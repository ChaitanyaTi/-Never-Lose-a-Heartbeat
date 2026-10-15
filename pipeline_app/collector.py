"""
Biosignal Collector Daemon
Maintains persistent TCP connection to biosignal-mock, performs real-time CRC-32 verification,
handles fault injections (corrupt packets, duplicates, gaps, restarts), and writes to SQLite WAL.
"""

import json
import logging
import socket
import time
import zlib
from typing import Dict, Optional

from pipeline_app.config import MOCK_HOST, MOCK_PORT
from pipeline_app.db import DatabaseManager

logger = logging.getLogger("Collector")


class BiosignalCollector:
    """Reads sensor data continuously from biosignal-mock over TCP."""

    def __init__(self, db: DatabaseManager, host: str = MOCK_HOST, port: int = MOCK_PORT):
        self.db = db
        self.host = host
        self.port = port
        self.running = False

        self.current_session_id: Optional[str] = None
        # Track next expected sample index per sensor
        self.expected_indices: Dict[int, int] = {1: 0, 2: 0}

        self.sock: Optional[socket.socket] = None
        self.reader = None

    def connect(self) -> bool:
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None
            self.reader = None

        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(2.5)  # detect halts / disconnects
            self.sock.connect((self.host, self.port))
            self.reader = self.sock.makefile(encoding="utf-8")
            logger.info(f"Connected to mock service at {self.host}:{self.port}")
            return True
        except Exception as e:
            logger.debug(f"Connection to mock failed: {e}")
            if self.sock:
                try:
                    self.sock.close()
                except Exception:
                    pass
            self.sock = None
            self.reader = None
            return False

    def read_sensor_batch(self, sensor: int, from_index: Optional[int] = None) -> Optional[dict]:
        """
        Sends read command, receives reply, and validates CRC-32.
        Returns parsed reply dict if valid, or None if error/corrupt.
        """
        if not self.sock:
            if not self.connect():
                return None

        req = {"cmd": "read", "sensor": sensor}
        if from_index is not None:
            req["from_index"] = from_index

        try:
            cmd_str = json.dumps(req) + "\n"
            self.sock.sendall(cmd_str.encode("utf-8"))
            line = self.reader.readline()
            if not line:
                logger.warning(f"Connection closed by mock server for sensor {sensor}")
                self.connect()
                return None

            line = line.strip()
            if not line:
                return None

            # Fast CRC-32 check on raw data text
            # Format: ... "crc32": "...", "data": [[...]] }
            idx_data = line.find('"data":')
            if idx_data == -1:
                logger.warning(f"No 'data' field in reply: {line[:100]}")
                return None

            # Data text is from the first '[' after '"data":' to the last ']'
            start_bracket = line.find("[", idx_data)
            end_bracket = line.rfind("]")
            if start_bracket == -1 or end_bracket == -1:
                logger.warning("Corrupt JSON structure in data field")
                return None

            raw_data_text = line[start_bracket : end_bracket + 1]
            calc_crc = hex(zlib.crc32(raw_data_text.encode("utf-8")) & 0xFFFFFFFF)[2:].zfill(8).lower()

            reply = json.loads(line)
            received_crc = reply.get("crc32", "").lower()

            if calc_crc != received_crc:
                logger.warning(
                    f"Corrupt reply detected for sensor {sensor} (CRC mismatch: expected {calc_crc}, got {received_crc})! Re-requesting..."
                )
                return {"_corrupt": True, "sensor": sensor}

            return reply

        except socket.timeout:
            logger.warning(f"Mock read timeout on sensor {sensor} - server may have paused")
            self.connect()
            return None
        except Exception as e:
            logger.warning(f"Socket error reading sensor {sensor}: {e}")
            self.connect()
            return None

    def process_reply(self, reply: dict) -> bool:
        """Processes a valid mock reply, handles session changes, losses, and writes samples."""
        session_id = reply["session_id"]
        sensor = reply["sensor"]
        first_index = reply["first_index"]
        count = reply["count"]
        lost_ranges = reply.get("lost", [])
        data = reply.get("data", [])

        # 1. Session change handling
        if self.current_session_id is None:
            self.current_session_id = session_id
            self.db.register_session(session_id)
            # Reconstruct expected indices from DB if resuming after crash
            for s in (1, 2):
                max_idx = self.db.get_max_sample_index(session_id, s)
                self.expected_indices[s] = max_idx + 1 if max_idx >= 0 else 0
            logger.info(f"Initialized session {session_id} with expected indices: {self.expected_indices}")
        elif session_id != self.current_session_id:
            # Mock restarted!
            logger.warning(
                f"Mock restart detected! Previous session: {self.current_session_id}, New: {session_id}"
            )
            # Record unread loss for previous session with to_index = null
            for s in (1, 2):
                last_idx = self.expected_indices[s]
                self.db.insert_loss(self.current_session_id, s, last_idx, None, "session_restart")

            # Register new session
            self.current_session_id = session_id
            self.db.register_session(session_id)
            self.expected_indices = {1: 0, 2: 0}

        # 2. Record explicit losses reported by mock
        for lr in lost_ranges:
            f_idx = lr["from_index"]
            t_idx = lr.get("to_index")
            reason = lr.get("reason", "unknown")
            logger.info(f"Sensor {sensor} lost range reported: [{f_idx}, {t_idx}] ({reason})")
            self.db.insert_loss(session_id, sensor, f_idx, t_idx, reason)
            if t_idx is not None and t_idx >= self.expected_indices[sensor]:
                self.expected_indices[sensor] = t_idx + 1

        # 3. Check for unexpected gap between expected index and first_index
        if first_index > self.expected_indices[sensor]:
            gap_from = self.expected_indices[sensor]
            gap_to = first_index - 1
            logger.warning(f"Unannounced gap detected on sensor {sensor}: [{gap_from}, {gap_to}]. Recording loss.")
            self.db.insert_loss(session_id, sensor, gap_from, gap_to, "stream_gap")
            self.expected_indices[sensor] = first_index

        # 4. Insert samples atomically into SQLite
        if count > 0 and data:
            self.db.insert_samples_batch(session_id, sensor, first_index, data)
            new_next = first_index + count
            if new_next > self.expected_indices[sensor]:
                self.expected_indices[sensor] = new_next

        return reply.get("session_end", False)

    def step(self):
        """Performs one poll cycle for both sensors with automatic retry on corruption."""
        for sensor in (1, 2):
            expected = self.expected_indices.get(sensor, 0)
            reply = self.read_sensor_batch(sensor)

            # Handle corrupt packet fault injection: re-read using from_index
            if reply and reply.get("_corrupt"):
                logger.info(f"Re-reading corrupt packet for sensor {sensor} from index {expected}")
                reply = self.read_sensor_batch(sensor, from_index=expected)

            if reply and not reply.get("_corrupt"):
                self.process_reply(reply)

    def run(self):
        """Continuous ingestion loop."""
        self.running = True
        logger.info("Collector daemon running...")
        while self.running:
            try:
                self.step()
                # Yield slightly to avoid burning CPU when mock has no new samples
                time.sleep(0.05)
            except Exception as e:
                logger.error(f"Error in collector loop: {e}", exc_info=True)
                time.sleep(0.5)

    def stop(self):
        self.running = False
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
