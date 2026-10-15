import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from biosignal_mock.generator import SignalGenerator
from biosignal_mock.ground_truth import verify_export
from pipeline_app.db import DatabaseManager


def test_storage_and_deduplication():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_pipeline.db"
        db = DatabaseManager(db_path=db_path)
        session_id = "test-session-001"
        db.register_session(session_id)

        gen = SignalGenerator(mode="signal", seed=7)
        # Create 800 samples for Sensor 1 and Sensor 2
        s1_data_0_400 = [gen.generate_sensor1_sample(i) for i in range(400)]
        s1_data_400_800 = [gen.generate_sensor1_sample(i) for i in range(400, 800)]
        s2_data_0_400 = [gen.generate_sensor2_sample(i) for i in range(400)]
        s2_data_400_800 = [gen.generate_sensor2_sample(i) for i in range(400, 800)]

        # Insert first batch
        db.insert_samples_batch(session_id, 1, 0, s1_data_0_400)
        db.insert_samples_batch(session_id, 2, 0, s2_data_0_400)

        # Test duplicate insertion: re-insert first batch (simulating duplicate mock reply)
        db.insert_samples_batch(session_id, 1, 0, s1_data_0_400)
        db.insert_samples_batch(session_id, 2, 0, s2_data_0_400)

        # Insert second batch
        db.insert_samples_batch(session_id, 1, 400, s1_data_400_800)
        db.insert_samples_batch(session_id, 2, 400, s2_data_400_800)

        status = db.get_status(session_id)
        # Total samples saved: 800 for sensor 1 + 800 for sensor 2 = 1600 samples
        assert status["samples_saved"] == 1600, f"Expected 1600 samples, got {status['samples_saved']}"
        assert status["backlog"] == 1600

        # Export samples and verify with check_ground_truth
        export_file = Path(tmpdir) / "export.jsonl"
        with open(export_file, "w", encoding="utf-8") as f:
            for line in db.export_samples_stream(session_id):
                f.write(line + "\n")
            for line in db.export_losses_stream(session_id):
                f.write(line + "\n")

        report = verify_export(str(export_file), mode="signal", seed=7)
        print("Verification Report:", report)
        assert report["passed"] is True, f"Ground truth check failed: {report['reasons']}"
        assert report["duplicates_count"] == {1: 0, 2: 0}, "Duplicates detected in export!"
        assert report["silent_losses"] == {1: [], 2: []}, "Silent losses detected in export!"
        db.close()
        print("Storage & Deduplication test passed!")


if __name__ == "__main__":
    test_storage_and_deduplication()
