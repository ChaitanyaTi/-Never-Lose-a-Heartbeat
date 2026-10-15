import os
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from biosignal_mock.server import BiosignalMockServer
from biosignal_mock.ground_truth import verify_export
from pipeline_app.collector import BiosignalCollector
from pipeline_app.db import DatabaseManager


def test_corrupt_crc_and_duplicate_injection():
    """Tests resilience against corrupt CRC-32 replies and duplicate replies."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        db_path = tmp_path / "fault_test.db"

        # Start mock server with 30% corrupt CRC probability and 30% duplicate reply probability
        mock = BiosignalMockServer(
            port=7194,
            duration=30,
            realtime=True,
            corrupt_crc_prob=0.30,
            duplicate_prob=0.30,
        )
        mock.start()

        db = DatabaseManager(db_path=db_path)
        collector = BiosignalCollector(db, host="127.0.0.1", port=7194)

        # Collect for 4 seconds under heavy adversarial CRC and duplicate injection
        start_t = time.time()
        while time.time() - start_t < 4.0:
            collector.step()
            time.sleep(0.05)

        collector.stop()
        mock.stop()

        session_id = db.get_active_session_id()
        assert session_id is not None
        status = db.get_status(session_id)
        print(f"Status after corrupt CRC and duplicate test: {status}")

        export_file = tmp_path / "export_fault.jsonl"
        with open(export_file, "w", encoding="utf-8") as f:
            for line in db.export_samples_stream(session_id):
                f.write(line + "\n")
            for line in db.export_losses_stream(session_id):
                f.write(line + "\n")

        rep = verify_export(str(export_file), mode="signal", seed=7)
        print("Fault Verification Report:", rep)
        assert rep["passed"] is True, f"Failed verification: {rep['reasons']}"
        assert rep["duplicates_count"] == {1: 0, 2: 0}, "Duplicates found despite deduplication!"
        assert rep["corruptions_count"] == {1: 0, 2: 0}, "Corrupt values accepted!"
        assert rep["silent_losses"] == {1: [], 2: []}, "Silent losses found!"
        db.close()
        print("Corrupt CRC and Duplicate resilience verified!")


def test_session_restart_fault():
    """Tests mock restart (starts a new session, unread data of old session logged with to_index: null)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        db_path = tmp_path / "restart_test.db"

        mock = BiosignalMockServer(port=7193, duration=30, realtime=True)
        mock.start()

        db = DatabaseManager(db_path=db_path)
        collector = BiosignalCollector(db, host="127.0.0.1", port=7193)

        # Collect from initial session
        for _ in range(5):
            collector.step()
            time.sleep(0.1)

        first_session = collector.current_session_id
        assert first_session is not None
        print(f"First session: {first_session}")

        # SIMULATE FAULT: Mock restarts mid-operation!
        print("\n--- INJECTING MOCK RESTART ---")
        mock.restart_session()
        time.sleep(0.2)

        # Collect from restarted mock
        for _ in range(5):
            collector.step()
            time.sleep(0.1)

        second_session = collector.current_session_id
        assert second_session != first_session, "Failed to recognize new session after restart!"
        print(f"Second session detected: {second_session}")

        # Check loss recorded for the first session with to_index = null
        conn = db._conn()
        losses = conn.execute(
            "SELECT sensor, from_index, to_index, reason FROM losses WHERE session_id = ?",
            (first_session,),
        ).fetchall()

        print(f"Loss records for terminated session: {[dict(r) for r in losses]}")
        assert len(losses) >= 1, "Must record loss for unread data of restarted session!"
        assert any(r["to_index"] is None for r in losses), "Loss must have to_index: null when end is unknown!"
        assert any(r["reason"] == "session_restart" for r in losses)

        collector.stop()
        mock.stop()
        db.close()
        print("Mock restart handling verified!")


def test_process_crash_and_recovery():
    """Tests sudden process termination simulation and crash recovery."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        db_path = tmp_path / "crash_test.db"

        # Step 1: Write some data
        db1 = DatabaseManager(db_path=db_path)
        sess = "crash-session-123"
        db1.register_session(sess)
        from biosignal_mock.generator import SignalGenerator
        gen = SignalGenerator()
        s1 = [gen.generate_sensor1_sample(i) for i in range(500)]
        s2 = [gen.generate_sensor2_sample(i) for i in range(500)]
        db1.insert_samples_batch(sess, 1, 0, s1)
        db1.insert_samples_batch(sess, 2, 0, s2)
        st1 = db1.get_status(sess)
        assert st1["samples_saved"] == 1000
        # Simulate ungraceful crash (close connection abruptly)
        db1.close()

        # Step 2: Restart pipeline on same database
        db2 = DatabaseManager(db_path=db_path)
        assert db2.get_active_session_id() == sess
        assert db2.get_max_sample_index(sess, 1) == 499
        assert db2.get_max_sample_index(sess, 2) == 499

        # Append next batch seamlessly
        s1_next = [gen.generate_sensor1_sample(i) for i in range(500, 1000)]
        s2_next = [gen.generate_sensor2_sample(i) for i in range(500, 1000)]
        db2.insert_samples_batch(sess, 1, 500, s1_next)
        db2.insert_samples_batch(sess, 2, 500, s2_next)

        st2 = db2.get_status(sess)
        assert st2["samples_saved"] == 2000

        # Verify export integrity
        export_file = tmp_path / "crash_export.jsonl"
        with open(export_file, "w", encoding="utf-8") as f:
            for line in db2.export_samples_stream(sess):
                f.write(line + "\n")

        rep = verify_export(str(export_file), mode="signal", seed=7)
        print("Crash Recovery Report:", rep)
        assert rep["passed"] is True, f"Crash export failed: {rep['reasons']}"
        assert rep["duplicates_count"] == {1: 0, 2: 0}
        assert rep["silent_losses"] == {1: [], 2: []}
        db2.close()
        print("Process crash recovery verified!")


if __name__ == "__main__":
    test_corrupt_crc_and_duplicate_injection()
    test_session_restart_fault()
    test_process_crash_and_recovery()
