"""
Biosignal Mock TCP Server (biosignal-mock)
Implements Section 6 specifications:
- Newline-delimited JSON over TCP.
- 400 samples/sec for Sensor 1 (3 channels) and Sensor 2 (2 channels).
- 10-second rolling buffer per sensor.
- CRC-32 validation field calculated over data text from [ to matching ].
- Fault injection capability (bad crc, duplicates, unread drop, disconnects).
"""

import argparse
import json
import logging
import os
import random
import socket
import sys
import threading
import time
import uuid
import zlib
from typing import Dict, List, Optional, Tuple

from biosignal_mock.generator import SignalGenerator

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [Mock] %(message)s")
logger = logging.getLogger("MockServer")


class BiosignalMockServer:
    def __init__(
        self,
        mode: str = "signal",
        seed: int = 7,
        duration: int = 3600,
        port: int = 7100,
        host: str = "0.0.0.0",
        realtime: bool = True,
        # Fault injection probabilities for testing
        corrupt_crc_prob: float = 0.0,
        duplicate_prob: float = 0.0,
        pause_prob: float = 0.0,
    ):
        self.mode = mode
        self.seed = seed
        self.duration = duration
        self.port = port
        self.host = host
        self.realtime = realtime
        self.corrupt_crc_prob = corrupt_crc_prob
        self.duplicate_prob = duplicate_prob
        self.pause_prob = pause_prob

        self.session_id = uuid.uuid4().hex[:8]
        self.generator = SignalGenerator(mode=mode, seed=seed)
        self.rate = 400  # samples per second
        self.total_samples = duration * self.rate

        self.start_time: Optional[float] = None
        self.running = False
        self.server_socket: Optional[socket.socket] = None

        # State tracking per sensor for client
        # sensor 1 and sensor 2
        self.client_next_index: Dict[int, int] = {1: 0, 2: 0}
        self.lock = threading.Lock()

        # Cache of generated samples in buffer: keeps last 10 seconds = 4000 samples
        self.buffer_size = 10 * self.rate  # 4000 samples
        self.fault_rng = random.Random(seed + 999)

        self.virtual_sample_cursor = 0

    def start(self):
        self.running = True
        self.start_time = time.time()
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(1)
        logger.info(
            f"Mock server started on {self.host}:{self.port} | Session: {self.session_id} | "
            f"Mode: {self.mode} | Seed: {self.seed} | Duration: {self.duration}s"
        )

        accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        accept_thread.start()
        return accept_thread

    def stop(self):
        self.running = False
        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception:
                pass

    def advance_virtual_samples(self, count: int):
        with self.lock:
            self.virtual_sample_cursor = min(self.virtual_sample_cursor + count, self.total_samples)

    def get_current_available_sample_index(self) -> int:
        if not self.realtime:
            return self.virtual_sample_cursor
        if self.start_time is None:
            return 0
        elapsed = time.time() - self.start_time
        available = int(elapsed * self.rate)
        return min(available, self.total_samples)

    def _accept_loop(self):
        while self.running:
            try:
                client_sock, client_addr = self.server_socket.accept()
                logger.info(f"Client connected from {client_addr}")
                self._handle_client(client_sock)
            except Exception as e:
                if self.running:
                    logger.debug(f"Accept error: {e}")
                break

    def _handle_client(self, client_sock: socket.socket):
        client_sock.settimeout(30.0)
        buffer = ""
        try:
            while self.running:
                data = client_sock.recv(4096)
                if not data:
                    logger.info("Client disconnected")
                    break
                buffer += data.decode("utf-8")
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        cmd_req = json.loads(line)
                        responses = self._process_command(cmd_req)
                        for resp in responses:
                            client_sock.sendall((resp + "\n").encode("utf-8"))
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON received: {line}")
        except Exception as e:
            logger.info(f"Client connection closed: {e}")
        finally:
            try:
                client_sock.close()
            except Exception:
                pass

    def _process_command(self, req: dict) -> List[str]:
        cmd = req.get("cmd")
        if cmd != "read":
            return [json.dumps({"error": f"Unknown command: {cmd}"})]

        sensor = req.get("sensor")
        if sensor not in (1, 2):
            return [json.dumps({"error": f"Invalid sensor: {sensor}"})]

        with self.lock:
            # Check fault injection: pause
            if self.pause_prob > 0 and self.fault_rng.random() < self.pause_prob:
                time.sleep(1.0)

            curr_avail = self.get_current_available_sample_index()
            # Buffer holds last 10 seconds
            earliest_in_buffer = max(0, curr_avail - self.buffer_size)

            req_from_index = req.get("from_index")
            if req_from_index is not None:
                start_index = req_from_index
            else:
                start_index = self.client_next_index[sensor]

            lost_list = []
            if start_index < earliest_in_buffer:
                # Buffer overflow occurred for unread samples
                lost_count = earliest_in_buffer - start_index
                lost_list.append({
                    "from_index": start_index,
                    "to_index": earliest_in_buffer - 1,
                    "reason": "buffer_overflow",
                })
                start_index = earliest_in_buffer

            # Deliver at most 1 second of data (400 samples)
            count = min(400, max(0, curr_avail - start_index))
            data_samples = []

            for i in range(start_index, start_index + count):
                if sensor == 1:
                    data_samples.append(self.generator.generate_sensor1_sample(i))
                else:
                    data_samples.append(self.generator.generate_sensor2_sample(i))

            next_idx = start_index + count
            self.client_next_index[sensor] = next_idx
            session_end = (next_idx >= self.total_samples)

            # Format raw data text for CRC32 computation exactly
            data_text = json.dumps(data_samples, separators=(",", ":"))
            crc_val = zlib.crc32(data_text.encode("utf-8")) & 0xFFFFFFFF
            crc_hex = hex(crc_val)[2:].zfill(8).lower()

            # Fault injection: bad crc
            if self.corrupt_crc_prob > 0 and self.fault_rng.random() < self.corrupt_crc_prob:
                crc_hex = "00000000" if crc_hex != "00000000" else "ffffffff"

            # Construct JSON reply matching Section 5 format exactly
            # {"session_id": "...", "sensor": 1, "first_index": ..., "count": ..., "lost": [...], "session_end": false, "crc32": "...", "data": [...]}
            reply_dict = {
                "session_id": self.session_id,
                "sensor": sensor,
                "first_index": start_index,
                "count": count,
                "lost": lost_list,
                "session_end": session_end,
                "crc32": crc_hex,
            }
            # Serialize prefix and attach exact data_text to ensure byte-exact CRC matching
            prefix_json = json.dumps(reply_dict)[:-1]  # remove trailing '}'
            full_json = f"{prefix_json},\"data\":{data_text}}}"

            responses = [full_json]

            # Fault injection: duplicate reply
            if self.duplicate_prob > 0 and self.fault_rng.random() < self.duplicate_prob:
                responses.append(full_json)

            return responses

    def restart_session(self):
        """Simulates mock restart fault injection."""
        with self.lock:
            self.session_id = uuid.uuid4().hex[:8]
            self.client_next_index = {1: 0, 2: 0}
            self.start_time = time.time()
            logger.info(f"Mock restarted! New session: {self.session_id}")


def main():
    parser = argparse.ArgumentParser(description="biosignal-mock: Biosignal Sensor Mock Service")
    parser.add_argument("--mode", choices=["signal", "ramp"], default="signal", help="Data generation mode")
    parser.add_argument("--seed", type=int, default=7, help="Random seed for deterministic generation")
    parser.add_argument("--duration", type=int, default=3600, help="Session duration in seconds (default: 3600)")
    parser.add_argument("--port", type=int, default=7100, help="TCP port to listen on (default: 7100)")
    parser.add_argument("--host", default="0.0.0.0", help="Host IP to bind to")
    parser.add_argument("--corrupt-crc-prob", type=float, default=0.0, help="Probability of corrupt CRC-32")
    parser.add_argument("--dup-prob", type=float, default=0.0, help="Probability of duplicate reply")
    parser.add_argument("--pause-prob", type=float, default=0.0, help="Probability of server pause")

    args = parser.parse_args()
    server = BiosignalMockServer(
        mode=args.mode,
        seed=args.seed,
        duration=args.duration,
        port=args.port,
        host=args.host,
        corrupt_crc_prob=args.corrupt_crc_prob,
        duplicate_prob=args.dup_prob,
        pause_prob=args.pause_prob,
    )
    thread = server.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping mock server...")
        server.stop()


if __name__ == "__main__":
    main()
