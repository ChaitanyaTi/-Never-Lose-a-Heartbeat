"""
Required Pipeline CLI Implementation (Command: 'pipeline')
Provides:
- pipeline status
- pipeline export --session ID --from device / --from cloud
- pipeline results --session ID
- pipeline start (starts the background daemon)
"""

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

from pipeline_app.config import CLOUD_URL
from pipeline_app.db import DatabaseManager


def cmd_status(args, db: DatabaseManager):
    """pipeline status: prints one JSON line with current mode, samples saved, samples in cloud, backlog."""
    # Attempt to query cloud for live cloud samples if possible
    status = db.get_status(session_id=args.session if hasattr(args, "session") else None)
    
    # Try reaching cloud endpoint for real-time verification if online
    if status["mode"] == "online":
        sess_id = args.session if hasattr(args, "session") and args.session else db.get_active_session_id()
        if sess_id:
            try:
                url = f"{CLOUD_URL}/api/v1/status?session_id={sess_id}"
                req = urllib.request.Request(url, headers={"User-Agent": "MayaCardio-CLI"})
                with urllib.request.urlopen(req, timeout=1.5) as resp:
                    if resp.status == 200:
                        c_data = json.loads(resp.read().decode("utf-8"))
                        status["samples_in_cloud"] = c_data.get("samples_in_cloud", status["samples_in_cloud"])
                        status["backlog"] = max(0, status["samples_saved"] - status["samples_in_cloud"])
            except Exception:
                pass

    print(json.dumps(status))


def cmd_export(args, db: DatabaseManager):
    """pipeline export --session ID --from device (or --from cloud)"""
    session_id = args.session
    source = args.source

    if source == "device":
        # Stream samples from local disk
        for line in db.export_samples_stream(session_id):
            print(line)
        # Stream losses from local disk
        for line in db.export_losses_stream(session_id):
            print(line)
    elif source == "cloud":
        # Stream from cloud service
        url = f"{CLOUD_URL}/api/v1/export?session_id={session_id}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "MayaCardio-CLI"})
            with urllib.request.urlopen(req, timeout=30.0) as resp:
                for raw_line in resp:
                    line = raw_line.decode("utf-8").strip()
                    if line:
                        print(line)
        except Exception as e:
            sys.stderr.write(f"Error querying cloud export: {e}\n")
            sys.exit(1)
    else:
        sys.stderr.write(f"Unknown source: {source}\n")
        sys.exit(1)


def cmd_results(args, db: DatabaseManager):
    """pipeline results --session ID: one JSON line per window and mode, with section 3 outputs."""
    session_id = args.session

    # Fetch local results
    local_results = list(db.get_results_stream(session_id))
    for res_line in local_results:
        print(res_line)

    # Fetch cloud results if available and distinct
    try:
        url = f"{CLOUD_URL}/api/v1/results?session_id={session_id}"
        req = urllib.request.Request(url, headers={"User-Agent": "MayaCardio-CLI"})
        with urllib.request.urlopen(req, timeout=5.0) as resp:
            for raw_line in resp:
                line = raw_line.decode("utf-8").strip()
                if line and line not in local_results:
                    print(line)
    except Exception:
        pass


def cmd_start(args):
    """pipeline start: launches the pipeline daemon."""
    from pipeline_app.daemon import PipelineDaemon
    daemon = PipelineDaemon()
    daemon.start()
    try:
        import time
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        daemon.stop()


def main():
    parser = argparse.ArgumentParser(prog="pipeline", description="Never Lose a Heartbeat Data Pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # status
    p_status = subparsers.add_parser("status", help="Get pipeline status")
    p_status.add_argument("--session", default=None, help="Optional session ID")

    # export
    p_export = subparsers.add_parser("export", help="Export session data")
    p_export.add_argument("--session", required=True, help="Session ID")
    p_export.add_argument("--from", dest="source", choices=["device", "cloud"], required=True, help="Source to export from")

    # results
    p_results = subparsers.add_parser("results", help="Get Section 3 window processing results")
    p_results.add_argument("--session", required=True, help="Session ID")

    # start
    p_start = subparsers.add_parser("start", help="Start background pipeline daemon")

    args = parser.parse_args()

    db = DatabaseManager()
    try:
        if args.command == "status":
            cmd_status(args, db)
        elif args.command == "export":
            cmd_export(args, db)
        elif args.command == "results":
            cmd_results(args, db)
        elif args.command == "start":
            cmd_start(args)
    finally:
        db.close()


if __name__ == "__main__":
    main()
