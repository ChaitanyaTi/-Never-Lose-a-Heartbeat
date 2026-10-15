"""
Pipeline Configuration
Environment variables and default settings for edge and cloud operation.
"""

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

# Database configuration
DB_DIR = Path(os.environ.get("PIPELINE_DATA_DIR", BASE_DIR / "data"))
DB_PATH = DB_DIR / "pipeline.db"

# Mock service connection
MOCK_HOST = os.environ.get("MOCK_HOST", "127.0.0.1")
MOCK_PORT = int(os.environ.get("MOCK_PORT", 7100))

# Cloud service connection
CLOUD_URL = os.environ.get("CLOUD_URL", "http://127.0.0.1:8000")
CLOUD_API_KEY = os.environ.get("CLOUD_API_KEY", "maya-cardio-secret-token")

# Operational parameters
SAMPLE_RATE = 400  # samples per second
WINDOW_SIZE = 400  # samples per 1-second window
HEART_RATE_WINDOW_SECONDS = 5  # 5-second window for HR calculation (2000 samples)
PEAK_MIN_DISTANCE = 100  # min samples between peaks
PEAK_LOOKBACK_SAMPLES = 800  # 800 samples lookback for peak validation

# Network monitor
NETWORK_PROBE_INTERVAL = 2.0  # seconds between health probes
OFFLINE_CONSECUTIVE_FAILURES = 3  # 3 * 2s = 6s switch to offline (< 10s requirement)
ONLINE_CONSECUTIVE_SUCCESSES = 2  # 2 * 2s = 4s switch to online (< 10s requirement)

# Sync / Catch-up parameters
SYNC_BATCH_SIZE = 4000  # samples per catch-up POST
SYNC_INTERVAL_ONLINE = 0.5  # seconds
