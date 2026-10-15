"""
Biosignal Mock Generator
Implements synthetic data generation for Sensor 1 (3 channels) and Sensor 2 (2 channels)
supporting both --mode signal and --mode ramp with reproducible seeds.
"""

import math
import random
from typing import List, Tuple

MAX_24_BIT_SIGNED = 8_388_607
MIN_24_BIT_SIGNED = -8_388_608
MOD_24_BIT = 8_388_608


class SignalGenerator:
    """Generates synthetic sensor data according to Section 6 specifications."""

    def __init__(self, mode: str = "signal", seed: int = 7):
        self.mode = mode
        self.seed = seed
        self.rng = random.Random(seed)
        
        # Precompute or stateful parameters for signal mode
        # ECG waveform template components (P, QRS, T)
        # Heart rates per time period:
        # 60 bpm for min 0-2 (1 beat per sec = 400 samples/beat)
        # 90 bpm for min 2-4 (1.5 beats per sec = ~266.67 samples/beat)
        # 72 bpm for min 4-6 (1.2 beats per sec = ~333.33 samples/beat)
        # Repeating every 6 minutes (360 seconds = 144,000 samples)
        self._noise_table_s1 = [self.rng.randint(-50, 50) for _ in range(4000)]
        self._noise_table_s2_1 = [self.rng.randint(-150, 150) for _ in range(4000)]
        self._noise_table_s2_2 = [self.rng.randint(-100, 100) for _ in range(4000)]

    def get_heart_rate(self, sample_idx: int) -> float:
        """Heart rate in bpm depending on time elapsed."""
        time_sec = sample_idx / 400.0
        cycle_sec = time_sec % 360.0  # 6-minute cycle
        if cycle_sec < 120.0:
            return 60.0
        elif cycle_sec < 240.0:
            return 90.0
        else:
            return 72.0

    def generate_sensor1_sample(self, sample_idx: int) -> List[int]:
        """
        Sensor 1: 3 channels @ 400 Hz.
        Channel 2: ECG-like waveform with one sharp peak per heartbeat.
        Channels 1 and 3: ch2 * 0.6 and ch2 * 1.3.
        """
        if self.mode == "ramp":
            # Sensor 1 channel c at sample i = (3i + c) mod 8,388,608
            # Channels 1, 2, 3
            ch1 = (3 * sample_idx + 1) % MOD_24_BIT
            ch2 = (3 * sample_idx + 2) % MOD_24_BIT
            ch3 = (3 * sample_idx + 3) % MOD_24_BIT
            return [ch1, ch2, ch3]

        # Mode: signal
        bpm = self.get_heart_rate(sample_idx)
        samples_per_beat = (60.0 / bpm) * 400.0
        phase = (sample_idx % samples_per_beat) / samples_per_beat  # 0.0 to 1.0

        # Create realistic ECG waveform with prominent R-peak
        # Baseline ~ 1000, R-peak reaches ~ 50,000 - 80,000 to ensure clear peak detection
        baseline = 1000
        ecg = baseline

        # P-wave around phase 0.15
        if 0.10 <= phase <= 0.20:
            p_val = math.sin((phase - 0.10) / 0.10 * math.pi) * 2500
            ecg += p_val
        # Q-dip around phase 0.27
        elif 0.25 <= phase < 0.28:
            q_val = -math.sin((phase - 0.25) / 0.03 * math.pi) * 3500
            ecg += q_val
        # R-peak sharp spike at phase 0.30 (duration ~ 0.04)
        elif 0.28 <= phase <= 0.32:
            # Very sharp peak centered at 0.30
            dist = abs(phase - 0.30) / 0.02
            r_val = 65000 * math.exp(-0.5 * (dist * 3.5) ** 2)
            ecg += r_val
        # S-dip around phase 0.34
        elif 0.32 < phase <= 0.36:
            s_val = -math.sin((phase - 0.32) / 0.04 * math.pi) * 4500
            ecg += s_val
        # T-wave around phase 0.50
        elif 0.42 <= phase <= 0.58:
            t_val = math.sin((phase - 0.42) / 0.16 * math.pi) * 5000
            ecg += t_val

        # Add subtle noise
        noise = self._noise_table_s1[sample_idx % len(self._noise_table_s1)]
        ch2 = int(round(ecg + noise))
        # Ensure within 24-bit signed bounds
        ch2 = max(MIN_24_BIT_SIGNED, min(MAX_24_BIT_SIGNED, ch2))

        ch1 = int(round(ch2 * 0.6))
        ch1 = max(MIN_24_BIT_SIGNED, min(MAX_24_BIT_SIGNED, ch1))

        ch3 = int(round(ch2 * 1.3))
        ch3 = max(MIN_24_BIT_SIGNED, min(MAX_24_BIT_SIGNED, ch3))

        return [ch1, ch2, ch3]

    def generate_sensor2_sample(self, sample_idx: int) -> List[int]:
        """
        Sensor 2: 2 channels @ 400 Hz.
        Channel 1 near 400,000, channel 2 near 150,000, varying slowly.
        """
        if self.mode == "ramp":
            # Sensor 2 channel c at sample i = (2i + c) mod 8,388,608
            # Channels 1, 2
            ch1 = (2 * sample_idx + 1) % MOD_24_BIT
            ch2 = (2 * sample_idx + 2) % MOD_24_BIT
            return [ch1, ch2]

        # Mode: signal
        t = sample_idx / 400.0  # seconds
        # Slow variations with period of ~30-60s
        slow_var1 = 15000 * math.sin(2 * math.pi * t / 45.0) + 5000 * math.cos(2 * math.pi * t / 17.0)
        noise1 = self._noise_table_s2_1[sample_idx % len(self._noise_table_s2_1)]
        ch1 = int(round(400000 + slow_var1 + noise1))
        ch1 = max(MIN_24_BIT_SIGNED, min(MAX_24_BIT_SIGNED, ch1))

        slow_var2 = 8000 * math.sin(2 * math.pi * t / 60.0) + 3000 * math.sin(2 * math.pi * t / 11.0)
        noise2 = self._noise_table_s2_2[sample_idx % len(self._noise_table_s2_2)]
        ch2 = int(round(150000 + slow_var2 + noise2))
        ch2 = max(MIN_24_BIT_SIGNED, min(MAX_24_BIT_SIGNED, ch2))

        return [ch1, ch2]
