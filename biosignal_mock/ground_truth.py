"""
check_ground_truth: Ground Truth Verifier
Compares a pipeline export (from device or cloud) against the mock service's ground truth.
Flags:
- Silent data losses (unrecorded missing samples)
- Data corruption / value mismatches
- Duplicate sample deliveries
- Unaccounted gaps
"""

import argparse
import json
import sys
from typing import Dict, List, Optional, Set, Tuple

from biosignal_mock.generator import SignalGenerator


def verify_export(
    export_file_path: str,
    mode: str = "signal",
    seed: int = 7,
    expected_count: Optional[int] = None,
) -> dict:
    generator = SignalGenerator(mode=mode, seed=seed)

    # Store received samples: sensor -> dict of index -> values
    received_samples: Dict[int, Dict[int, List[int]]] = {1: {}, 2: {}}
    duplicates: Dict[int, List[int]] = {1: [], 2: []}
    corruptions: Dict[int, List[Tuple[int, list, list]]] = {1: [], 2: []}
    
    # Store loss records: sensor -> list of (from_index, to_index, reason)
    loss_records: Dict[int, List[dict]] = {1: [], 2: []}

    total_lines = 0
    with open(export_file_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            total_lines += 1
            try:
                record = json.loads(line)
            except json.JSONDecodeError as e:
                return {
                    "passed": False,
                    "error": f"Invalid JSON on line {line_num}: {e}",
                }

            sensor = record.get("sensor")
            if sensor not in (1, 2):
                return {
                    "passed": False,
                    "error": f"Invalid sensor {sensor} on line {line_num}",
                }

            # Check if loss line
            if "from_index" in record:
                loss_records[sensor].append(record)
            elif "index" in record:
                idx = record["index"]
                vals = record.get("values", [])
                
                # Check duplicates
                if idx in received_samples[sensor]:
                    duplicates[sensor].append(idx)
                else:
                    received_samples[sensor][idx] = vals

                # Check data integrity against generator
                expected_vals = (
                    generator.generate_sensor1_sample(idx)
                    if sensor == 1
                    else generator.generate_sensor2_sample(idx)
                )
                if vals != expected_vals:
                    corruptions[sensor].append((idx, vals, expected_vals))
            else:
                return {
                    "passed": False,
                    "error": f"Unknown record structure on line {line_num}: {line}",
                }

    # Evaluate completeness & silent losses
    report = {
        "total_lines_read": total_lines,
        "sensors": {},
        "silent_losses": {1: [], 2: []},
        "duplicates_count": {1: len(duplicates[1]), 2: len(duplicates[2])},
        "corruptions_count": {1: len(corruptions[1]), 2: len(corruptions[2])},
        "passed": True,
        "reasons": [],
    }

    for s in (1, 2):
        s_received = received_samples[s]
        s_losses = loss_records[s]

        # Covered indices via loss records
        covered_loss_indices: Set[int] = set()
        has_open_ended_loss = False
        for lr in s_losses:
            from_idx = lr["from_index"]
            to_idx = lr.get("to_index")
            if to_idx is None:
                has_open_ended_loss = True
            else:
                for i in range(from_idx, to_idx + 1):
                    covered_loss_indices.add(i)

        max_idx = max(s_received.keys()) if s_received else 0
        if expected_count is not None:
            max_idx = max(max_idx, expected_count - 1)

        # Check for silent losses
        silent_loss = []
        for i in range(max_idx + 1):
            if i not in s_received and i not in covered_loss_indices:
                silent_loss.append(i)

        report["sensors"][s] = {
            "samples_received": len(s_received),
            "loss_records": len(s_losses),
            "max_sample_index": max_idx,
            "silent_loss_count": len(silent_loss),
            "has_open_ended_loss": has_open_ended_loss,
        }
        report["silent_losses"][s] = silent_loss[:50]  # sample first 50

        if silent_loss:
            report["passed"] = False
            report["reasons"].append(
                f"Sensor {s}: {len(silent_loss)} silent losses detected! (e.g. indices {silent_loss[:5]})"
            )

        if duplicates[s]:
            report["passed"] = False
            report["reasons"].append(
                f"Sensor {s}: {len(duplicates[s])} duplicate samples detected! (e.g. indices {duplicates[s][:5]})"
            )

        if corruptions[s]:
            report["passed"] = False
            report["reasons"].append(
                f"Sensor {s}: {len(corruptions[s])} corrupt values detected! First mismatch at {corruptions[s][0][0]}"
            )

    return report


def main():
    parser = argparse.ArgumentParser(description="check_ground_truth: Validate export against ground truth")
    parser.add_argument("export_file", help="Path to exported session JSON lines file")
    parser.add_argument("--mode", choices=["signal", "ramp"], default="signal", help="Mock mode used (default: signal)")
    parser.add_argument("--seed", type=int, default=7, help="Mock seed used (default: 7)")
    parser.add_argument("--expected-count", type=int, default=None, help="Expected total count of samples if known")

    args = parser.parse_args()
    result = verify_export(
        args.export_file,
        mode=args.mode,
        seed=args.seed,
        expected_count=args.expected_count,
    )

    print(json.dumps(result, indent=2))
    if not result.get("passed"):
        sys.exit(1)


if __name__ == "__main__":
    main()
