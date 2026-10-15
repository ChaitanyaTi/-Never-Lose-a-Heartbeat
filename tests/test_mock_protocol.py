import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import json
import socket
import time
import zlib
from biosignal_mock.server import BiosignalMockServer


def test_mock_tcp_and_crc32():
    server = BiosignalMockServer(port=7198, realtime=False)
    server.advance_virtual_samples(400)
    server.start()
    time.sleep(0.2)

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("127.0.0.1", 7198))
        req = json.dumps({"cmd": "read", "sensor": 1}) + "\n"
        s.sendall(req.encode("utf-8"))
        
        reader = s.makefile(encoding="utf-8")
        line = reader.readline()
        s.close()

        assert line, "No response from mock server"
        resp = json.loads(line)

        # Extract data text from the raw line: between "data": and the final }
        # Note the raw data text starts with [ and ends with ]
        idx_data = line.find('"data":')
        assert idx_data != -1
        raw_data_text = line[idx_data + 7 : line.rfind("}")].strip()

        calc_crc = hex(zlib.crc32(raw_data_text.encode("utf-8")) & 0xFFFFFFFF)[2:].zfill(8).lower()
        
        assert resp["crc32"] == calc_crc, f"CRC mismatch: expected {calc_crc}, got {resp['crc32']}"
        assert resp["sensor"] == 1
        assert resp["first_index"] == 0
        assert resp["count"] == 400
        assert len(resp["data"]) == 400
        print(f"Mock TCP and CRC32 test passed! session_id={resp['session_id']}")
    finally:
        server.stop()


if __name__ == "__main__":
    test_mock_tcp_and_crc32()
