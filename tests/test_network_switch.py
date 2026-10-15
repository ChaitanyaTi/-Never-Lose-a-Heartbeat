import os
import sys
import tempfile
import time
from pathlib import Path
import threading

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from biosignal_mock.server import BiosignalMockServer
from biosignal_mock.ground_truth import verify_export
from cloud_service.cloud_store import CloudStore
from pipeline_app.collector import BiosignalCollector
from pipeline_app.db import DatabaseManager
from pipeline_app.network_monitor import NetworkMonitor
from pipeline_app.offline_processor import DeviceOfflineProcessor
from pipeline_app.sync_worker import CloudSyncWorker


def test_automatic_network_switching_and_catchup():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        db_path = tmp_path / "edge_pipeline.db"
        cloud_db_path = tmp_path / "cloud_store.db"

        # 1. Start mock server (realtime mode)
        mock = BiosignalMockServer(port=7195, duration=60, realtime=True)
        mock.start()

        # 2. Local database and cloud store
        db = DatabaseManager(db_path=db_path)
        cloud_store = CloudStore(db_path=cloud_db_path)

        # We simulate the cloud sync and health probe locally with a controllable health flag
        network_available = [True]

        def probe_health_sim():
            return network_available[0]

        collector = BiosignalCollector(db, host="127.0.0.1", port=7195)
        monitor = NetworkMonitor(db, probe_interval=1.0)
        # Override check_cloud_health with the controllable simulator
        monitor.check_cloud_health = probe_health_sim

        # Direct in-process sync worker for the test
        sync_worker = CloudSyncWorker(db)

        def sync_step_sim():
            if db.get_pipeline_mode() != "online":
                return
            sess = db.get_active_session_id()
            if not sess:
                return
            unsynced_samples = db.get_unsynced_samples(sess, limit=4000)
            unsynced_losses = db.get_unsynced_losses(sess)
            if unsynced_samples or unsynced_losses:
                cloud_store.ingest_batch(sess, unsynced_samples, unsynced_losses)
                if unsynced_samples:
                    db.mark_samples_synced(sess, unsynced_samples)
                if unsynced_losses:
                    db.mark_losses_synced([l["id"] for l in unsynced_losses])

        offline_proc = DeviceOfflineProcessor(db)

        # Run collection and monitoring
        running = [True]

        def collector_loop():
            while running[0]:
                collector.step()
                time.sleep(0.05)

        def monitor_loop():
            while running[0]:
                monitor.probe_step()
                time.sleep(1.0)

        def sync_loop():
            while running[0]:
                if network_available[0]:
                    sync_step_sim()
                time.sleep(0.2)

        def offline_proc_loop():
            while running[0]:
                offline_proc.step()
                time.sleep(0.3)

        threads = [
            threading.Thread(target=collector_loop, daemon=True),
            threading.Thread(target=monitor_loop, daemon=True),
            threading.Thread(target=sync_loop, daemon=True),
            threading.Thread(target=offline_proc_loop, daemon=True),
        ]
        for t in threads:
            t.start()

        # Let system run in ONLINE mode for 3 seconds
        time.sleep(3.0)
        status_online = db.get_status()
        assert status_online["mode"] == "online"
        assert status_online["samples_saved"] > 0
        session_id = db.get_active_session_id()
        print(f"Online status after 3s: {status_online} (Session: {session_id})")

        # SIMULATE NETWORK CUT
        print("\n--- CUTTING NETWORK ---")
        cut_time = time.time()
        network_available[0] = False

        # Monitor should transition to offline within <= 10s (probe 1s * 3 failures = 3s)
        switched_to_offline = False
        while time.time() - cut_time < 10.0:
            if db.get_pipeline_mode() == "offline":
                switched_to_offline = True
                switch_latency = time.time() - cut_time
                print(f"Switched to OFFLINE mode in {switch_latency:.2f} seconds (< 10s requirement satisfied!)")
                break
            time.sleep(0.5)

        assert switched_to_offline, "Failed to switch to offline mode within 10 seconds!"

        # Let system collect in OFFLINE mode for 4 seconds (backlog builds up)
        time.sleep(4.0)
        status_offline = db.get_status(session_id)
        print(f"Offline status: {status_offline}")
        assert status_offline["mode"] == "offline"
        assert status_offline["backlog"] > 0, "Backlog must accumulate while offline!"

        # Check offline results generated on device
        local_results = list(db.get_results_stream(session_id))
        print(f"Offline results generated on device: {len(local_results)} windows")
        assert len(local_results) > 0, "Device must process windows while offline!"

        # SIMULATE NETWORK RESTORATION
        print("\n--- RESTORING NETWORK ---")
        restore_time = time.time()
        network_available[0] = True

        # Monitor should transition back to online within <= 10s (probe 1s * 2 successes = 2s)
        switched_to_online = False
        while time.time() - restore_time < 10.0:
            if db.get_pipeline_mode() == "online":
                switched_to_online = True
                restore_latency = time.time() - restore_time
                print(f"Switched back to ONLINE mode in {restore_latency:.2f} seconds (< 10s requirement satisfied!)")
                break
            time.sleep(0.5)

        assert switched_to_online, "Failed to switch back to online mode within 10 seconds!"

        # Stop collector first so no new samples are added during final sync
        collector.stop()
        mock.stop()
        time.sleep(0.5)

        # Final catch-up drain
        drain_start = time.time()
        while time.time() - drain_start < 5.0:
            sync_step_sim()
            st = db.get_status(session_id)
            if st["backlog"] == 0:
                print(f"Final catchup complete: {st['samples_saved']} samples on device and in cloud.")
                break
            time.sleep(0.1)

        # Stop remaining threads
        running[0] = False
        time.sleep(0.5)

        # Verify exports from device and cloud with check_ground_truth
        device_export_file = tmp_path / "device_export.jsonl"
        with open(device_export_file, "w", encoding="utf-8") as f:
            for line in db.export_samples_stream(session_id):
                f.write(line + "\n")
            for line in db.export_losses_stream(session_id):
                f.write(line + "\n")

        cloud_export_file = tmp_path / "cloud_export.jsonl"
        with open(cloud_export_file, "w", encoding="utf-8") as f:
            for line in cloud_store.export_samples_stream(session_id):
                f.write(line + "\n")
            for line in cloud_store.export_losses_stream(session_id):
                f.write(line + "\n")

        # Ground truth verification on device export
        rep_device = verify_export(str(device_export_file), mode="signal", seed=7)
        print("Device Ground Truth Report:", rep_device)
        assert rep_device["passed"] is True, f"Device export failed ground truth: {rep_device['reasons']}"
        assert rep_device["duplicates_count"] == {1: 0, 2: 0}
        assert rep_device["silent_losses"] == {1: [], 2: []}

        # Ground truth verification on cloud export (Criterion 6: Complete cloud copy)
        rep_cloud = verify_export(str(cloud_export_file), mode="signal", seed=7)
        print("Cloud Ground Truth Report:", rep_cloud)
        assert rep_cloud["passed"] is True, f"Cloud export failed ground truth: {rep_cloud['reasons']}"
        assert rep_cloud["duplicates_count"] == {1: 0, 2: 0}
        assert rep_cloud["silent_losses"] == {1: [], 2: []}

        # Verify cloud sample count matches device sample count exactly
        assert rep_device["sensors"][1]["samples_received"] == rep_cloud["sensors"][1]["samples_received"]
        assert rep_device["sensors"][2]["samples_received"] == rep_cloud["sensors"][2]["samples_received"]

        db.close()
        cloud_store.close()
        print("\nALL NETWORK SWITCHING, ZERO-LOSS, AND CATCH-UP TESTS PASSED!")


if __name__ == "__main__":
    test_automatic_network_switching_and_catchup()
