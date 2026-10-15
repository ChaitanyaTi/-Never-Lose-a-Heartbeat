"""
Pipeline Section 3 Processing Engine
Implements the exact mathematical processing logic required for both online (cloud) and offline (device) modes:
- 1-second windowing: samples 400w to 400w + 399
- missing_1, missing_2, complete
- per-channel mean, min, max (rounded to 1 decimal place)
- sensor 2 magnitude: mean of sqrt(ch1^2 + ch2^2)
- 4-condition peak detection and 5-second heart rate calculation
"""

import math
from typing import Dict, List, Optional, Tuple


class WindowProcessor:
    """Computes Section 3 window metrics and heart rate from raw sample streams."""

    def __init__(self):
        # Cache for Sensor 1 Channel 2 samples for peak detection: index -> value
        self.s1_ch2_samples: Dict[int, int] = {}
        # Chronological list of confirmed peak sample indices
        self.peaks: List[int] = []
        # Last confirmed peak index
        self.last_peak_index: Optional[int] = None
        # Highest sample index evaluated for peak detection
        self.last_evaluated_sample_index: int = -1

    def feed_sensor1_sample(self, sample_index: int, ch2_value: int):
        """Add Sensor 1 Channel 2 sample and update peak detection."""
        self.s1_ch2_samples[sample_index] = ch2_value
        # Prune old samples older than 20 seconds (8000 samples) to prevent memory leak
        if len(self.s1_ch2_samples) > 10000:
            min_keep = sample_index - 8000
            for k in list(self.s1_ch2_samples.keys()):
                if k < min_keep:
                    del self.s1_ch2_samples[k]

        # Evaluate candidate peaks up to sample_index - 1 (since we need sample i + 1 available)
        start_eval = max(800, self.last_evaluated_sample_index + 1)
        end_eval = sample_index - 1
        for i in range(start_eval, end_eval + 1):
            if self._is_peak(i):
                self.peaks.append(i)
                self.last_peak_index = i
            self.last_evaluated_sample_index = i

    def _is_peak(self, i: int) -> bool:
        """
        Peak rule (Section 3):
        Sample i is a peak when all four of these hold:
        1. the 800 samples before it, and the sample after it, are available;
        2. it is larger than both neighbours;
        3. it is larger than half of the maximum of the 800 samples before it;
        4. it is at least 100 samples after the previous peak.
        """
        # Condition 1: 800 samples before it (i - 800 to i - 1) and sample after it (i + 1)
        # Total 802 samples must be in s1_ch2_samples
        if i + 1 not in self.s1_ch2_samples or i - 1 not in self.s1_ch2_samples:
            return False

        # Fast contiguous availability check
        # Check boundary and quick sample check
        if (i - 800) not in self.s1_ch2_samples:
            return False

        for k in range(i - 800, i + 2):
            if k not in self.s1_ch2_samples:
                return False

        xi = self.s1_ch2_samples[i]
        prev_val = self.s1_ch2_samples[i - 1]
        next_val = self.s1_ch2_samples[i + 1]

        # Condition 2: larger than both neighbours
        if not (xi > prev_val and xi > next_val):
            return False

        # Condition 3: larger than half of the maximum of the 800 samples before it
        # Lookback range: i - 800 to i - 1
        max_lookback = max(self.s1_ch2_samples[k] for k in range(i - 800, i))
        if not (xi > 0.5 * max_lookback):
            return False

        # Condition 4: at least 100 samples after the previous peak
        if self.last_peak_index is not None:
            if (i - self.last_peak_index) < 100:
                return False

        return True

    def compute_window(
        self,
        window: int,
        sensor1_samples: Dict[int, List[int]],  # idx -> [ch1, ch2, ch3]
        sensor2_samples: Dict[int, List[int]],  # idx -> [ch1, ch2]
        mode: str = "offline",
    ) -> dict:
        """
        Computes all Section 3 metrics for window w (samples 400w to 400w + 399).
        """
        start_idx = 400 * window
        end_idx = start_idx + 399

        # 1. Missing counts
        avail_s1 = [sensor1_samples[i] for i in range(start_idx, end_idx + 1) if i in sensor1_samples]
        avail_s2 = [sensor2_samples[i] for i in range(start_idx, end_idx + 1) if i in sensor2_samples]

        missing_1 = 400 - len(avail_s1)
        missing_2 = 400 - len(avail_s2)
        complete = (missing_1 == 0 and missing_2 == 0)

        # 2. Per-channel min, max, mean (null if none)
        # Sensor 1 (3 channels)
        if avail_s1:
            ch1_s1 = [s[0] for s in avail_s1]
            ch2_s1 = [s[1] for s in avail_s1]
            ch3_s1 = [s[2] for s in avail_s1]
            mean_1 = [
                round(sum(ch1_s1) / len(ch1_s1), 1),
                round(sum(ch2_s1) / len(ch2_s1), 1),
                round(sum(ch3_s1) / len(ch3_s1), 1),
            ]
            min_1 = [min(ch1_s1), min(ch2_s1), min(ch3_s1)]
            max_1 = [max(ch1_s1), max(ch2_s1), max(ch3_s1)]
        else:
            mean_1 = None
            min_1 = None
            max_1 = None

        # Sensor 2 (2 channels)
        if avail_s2:
            ch1_s2 = [s[0] for s in avail_s2]
            ch2_s2 = [s[1] for s in avail_s2]
            mean_2 = [
                round(sum(ch1_s2) / len(ch1_s2), 1),
                round(sum(ch2_s2) / len(ch2_s2), 1),
            ]
            min_2 = [min(ch1_s2), min(ch2_s2)]
            max_2 = [max(ch1_s2), max(ch2_s2)]
            
            # Magnitude: Mean of sqrt(ch1^2 + ch2^2) over available sensor 2 samples
            mags = [math.sqrt(s[0] ** 2 + s[1] ** 2) for s in avail_s2]
            magnitude = round(sum(mags) / len(mags), 1)
        else:
            mean_2 = None
            min_2 = None
            max_2 = None
            magnitude = None

        # 3. Heart rate rule
        # No rate if:
        # - w < 4
        # - if any sample in last 5 seconds (samples 400(w - 4) to 400(w + 1) - 1) is missing
        # - or if there are fewer than 2 peaks
        heart_rate = None
        if window >= 4:
            hr_start = 400 * (window - 4)
            hr_end = 400 * (window + 1) - 1  # 2000 samples total
            # Check availability of all 2000 samples for Sensor 1
            missing_in_5s = False
            for k in range(hr_start, hr_end + 1):
                if k not in sensor1_samples:
                    missing_in_5s = True
                    break

            if not missing_in_5s:
                # Peaks in the 5-second interval
                window_peaks = [p for p in self.peaks if hr_start <= p <= hr_end]
                if len(window_peaks) >= 2:
                    k_peaks = len(window_peaks)
                    # Gap between consecutive peaks:
                    # mean gap in seconds = (p_last - p_first) / (400 * (k - 1))
                    gap_sec = (window_peaks[-1] - window_peaks[0]) / (400.0 * (k_peaks - 1))
                    if gap_sec > 0:
                        bpm = 60.0 / gap_sec
                        heart_rate = round(bpm, 1)

        return {
            "window": window,
            "mode": mode,
            "missing_1": missing_1,
            "missing_2": missing_2,
            "complete": complete,
            "mean_1": mean_1,
            "mean_2": mean_2,
            "min_1": min_1,
            "min_2": min_2,
            "max_1": max_1,
            "max_2": max_2,
            "magnitude": magnitude,
            "heart_rate": heart_rate,
        }
