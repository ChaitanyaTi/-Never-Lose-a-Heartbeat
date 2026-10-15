import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import math
from pipeline_app.processor import WindowProcessor
from biosignal_mock.generator import SignalGenerator


def test_processing_ramp_mode():
    proc = WindowProcessor()
    gen = SignalGenerator(mode="ramp", seed=7)
    
    # Generate 5 windows of ramp data (2000 samples, w=0, 1, 2, 3, 4)
    s1_samples = {}
    s2_samples = {}
    for i in range(2400):
        s1 = gen.generate_sensor1_sample(i)
        s2 = gen.generate_sensor2_sample(i)
        s1_samples[i] = s1
        s2_samples[i] = s2
        proc.feed_sensor1_sample(i, s1[1])

    # Window 0: samples 0 to 399
    res0 = proc.compute_window(0, s1_samples, s2_samples, mode="offline")
    assert res0["missing_1"] == 0
    assert res0["missing_2"] == 0
    assert res0["complete"] is True
    assert res0["mode"] == "offline"
    assert res0["heart_rate"] is None  # w < 4, so heart_rate must be None

    # Check min/max/mean for ramp window 0
    # Sensor 1 ch1 = (3i + 1) % 8388608. For i=0: 1, i=399: 1198.
    assert res0["min_1"] == [1, 2, 3]
    assert res0["max_1"] == [1198, 1199, 1200]
    expected_mean_ch1 = round(sum((3 * i + 1) for i in range(400)) / 400.0, 1)
    assert res0["mean_1"][0] == expected_mean_ch1

    # Check magnitude for sensor 2 window 0
    # ch1 = 2i + 1, ch2 = 2i + 2
    expected_mag = round(
        sum(math.sqrt((2 * i + 1) ** 2 + (2 * i + 2) ** 2) for i in range(400)) / 400.0, 1
    )
    assert res0["magnitude"] == expected_mag
    print("Ramp mode window processing test passed!")


def test_heart_rate_signal_mode():
    proc = WindowProcessor()
    gen = SignalGenerator(mode="signal", seed=7)

    # In minutes 0-2, HR is 60 bpm = 1 beat per second = 400 samples per peak!
    # Let's generate 6 windows (samples 0 to 2399, w=0, 1, 2, 3, 4, 5)
    s1_samples = {}
    s2_samples = {}
    for i in range(2500):
        s1 = gen.generate_sensor1_sample(i)
        s2 = gen.generate_sensor2_sample(i)
        s1_samples[i] = s1
        s2_samples[i] = s2
        proc.feed_sensor1_sample(i, s1[1])

    # For window 4 (samples 1600 to 1999), the 5-sec window is samples 0 to 1999.
    # 5 seconds at 60 bpm should contain peaks at intervals of ~400 samples
    res4 = proc.compute_window(4, s1_samples, s2_samples, mode="online")
    print(f"Window 4 HR: {res4['heart_rate']} bpm, peaks found: {proc.peaks}")
    assert res4["heart_rate"] is not None
    # Heart rate should be 60.0 bpm
    assert abs(res4["heart_rate"] - 60.0) <= 1.0, f"Expected ~60.0 bpm, got {res4['heart_rate']}"

    # Test missing sample in the 5-sec window -> must produce None for heart_rate!
    del s1_samples[1000]  # delete 1 sample inside the 5-second interval
    res4_missing = proc.compute_window(4, s1_samples, s2_samples, mode="online")
    assert res4_missing["heart_rate"] is None, "HR must be None if any sample in 5s is missing!"
    print("Heart rate 4-condition peak detection and 5-sec windowing test passed!")


if __name__ == "__main__":
    test_processing_ramp_mode()
    test_heart_rate_signal_mode()
