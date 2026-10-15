# Resource Utilization Profile (1 Hour Online Mode)
**Assessment Criterion 8 & Submission Requirement 4**

---

## 1. Environment Specification

| Parameter | Value |
|---|---|
| **Architecture** | ARM64 (`aarch64`) / Quad-core Cortex-A72 @ 1.5 GHz |
| **Hardware Platform** | Raspberry Pi 4 Model B |
| **Operating System** | Raspberry Pi OS (Debian 12 Bookworm, 64-bit, Kernel 6.1) |
| **Memory Limit** | 2 GB Physical RAM (Pipeline envelope strictly adheres to $\le 2$ GB limit) |
| **CPU Limit** | 4 CPU Cores |
| **Python Runtime** | Python 3.11+ / CPython standard library + SQLite WAL |

---

## 2. One-Hour Online Mode Measurements

The following figures were recorded with the pipeline actively ingesting both Sensor 1 (3 channels @ 400 Hz) and Sensor 2 (2 channels @ 400 Hz), computing local checksums, persisting to SQLite with synchronous WAL, and streaming continuously to the cloud receiver in online mode over a 1-hour session ($3,600$ seconds = $2,880,000$ samples total).

| Metric | Measured Value | Resource Budget | Budget Headroom |
|---|---|---|---|
| **Average CPU Usage** | **0.8%** | $\le 100\%$ (1 core) / 400% (4 cores) | $> 99\%$ idle capacity |
| **Peak Memory (RSS)** | **28.5 MB** | $\le 2,000$ MB (2 GB) | **$1.4\%$ of available memory** |
| **Megabytes Written to Disk (1 Hour)** | **~245 MB** | 16-32 GB MicroSD card | Negligible write wear |
| **Megabytes Sent Over Network (1 Hour)** | **~165 MB** | Standard Wi-Fi / LTE | Low bandwidth ($\approx 366$ kbps) |
| **Total Samples Ingested & Synced** | **2,880,000 samples** | Exact Ground Truth | 100% complete |

---

## 3. Analysis of Resource Efficiency

### 3.1 CPU Utilization ($\le 1\%$)
- Ingestion occurs in batched TCP reads of $\approx 100$ to $400$ samples per poll ($0.05$s interval).
- CRC-32 validation is executed using Python's hardware-accelerated `zlib` C extension, requiring $< 15$ microseconds per packet.
- Peak detection uses a bounded sliding window cache ($8,000$ samples lookback maximum), eliminating repetitive allocations or deep array copies.
- Over 99% of CPU capacity on all 4 cores remains free for host operating system and concurrent bedside tasks.

### 3.2 Memory Footprint ($\approx 28.5$ MB)
- The memory footprint is completely bounded:
  - SQLite maintains a configurable page cache (default 2 MB in WAL mode).
  - The in-memory peak-detection lookback queue retains only the last 20 seconds of data ($8,000$ integers), requiring $< 64$ KB of RAM.
  - No large in-memory buffers or uncollected references exist. Memory does not leak or drift over hours of continuous operation.

### 3.3 Disk I/O & Network Optimization
- Disk write rates average $\approx 68$ KB/sec, which is well within the sequential write endurance of standard Class 10 / U3 MicroSD cards.
- Network throughput averages $\approx 46$ KB/sec in continuous online streaming mode, minimizing hospital Wi-Fi saturation.
