#!/usr/bin/env python3
"""
generate_sample_inputs.py

Creates the input CSV for generate_metrics.py.

It does NOT run the simulator. It only creates combinations of:
    num_nodes, num_edges, bandwidth, latency, failure_rate, loss_rate

Default design:
    num_nodes: 3 to 51, step 3
    num_edges: at most 5 uniformly spaced values between n-1 and n(n-1)/2
    bandwidth: 5 values
    latency: 3 values
    failure_rate: 4 values fixed by user
    loss_rate: 2 values

Run:
    python generate_sample_inputs.py

Then run simulator:
    python generate_metrics.py --input generated_inputs.csv --output final_results.csv
"""

from __future__ import annotations

import csv
from pathlib import Path

# ============================================================
# Change variations here, not from terminal
# ============================================================

OUTPUT_FILE = "generated_inputs.csv"

NODE_START = 3
NODE_END = 51          # inclusive
NODE_STEP = 3
MAX_EDGE_SAMPLES = 5

# Suitable for ML: covers slow, medium, and fast links.
# Unit: Mbps
BANDWIDTH_VALUES = [1, 5, 10, 50, 100]

# Suitable for ML: LAN-ish, campus/WAN-ish, and slower WAN delay.
# Unit: milliseconds
LATENCY_VALUES = [5, 25, 100]

# User-specified values.
# failure_rate means: mean_crash_time = 1 / failure_rate seconds
FAILURE_RATE_VALUES = [0.001, 0.005, 0.01, 0.025]

# Suitable for ML: reliable vs mildly lossy.
# If you want harsher cases later, try [0.0, 0.05] or [0.01, 0.10].
LOSS_RATE_VALUES = [0.0, 0.03]


# ============================================================
# Helper functions
# ============================================================

def edge_samples_for_n(n: int, max_samples: int = 5) -> list[int]:
    """
    Return at most max_samples edge counts uniformly distributed between:
        min_edges = n - 1
        max_edges = n(n - 1)/2

    If fewer than max_samples possible edge counts exist, return all possible.
    """
    min_edges = n - 1
    max_edges = n * (n - 1) // 2

    possible = list(range(min_edges, max_edges + 1))
    if len(possible) <= max_samples:
        return possible

    if max_samples == 1:
        return [min_edges]

    values = []
    for i in range(max_samples):
        x = round(min_edges + i * (max_edges - min_edges) / (max_samples - 1))
        x = int(max(min_edges, min(max_edges, x)))
        if x not in values:
            values.append(x)

    # If rounding accidentally produced duplicate values, fill missing values.
    candidate = min_edges
    while len(values) < max_samples and candidate <= max_edges:
        if candidate not in values:
            values.append(candidate)
        candidate += 1

    return sorted(values)


def generate_rows() -> list[dict[str, float | int]]:
    rows: list[dict[str, float | int]] = []

    for n in range(NODE_START, NODE_END + 1, NODE_STEP):
        edge_values = edge_samples_for_n(n, MAX_EDGE_SAMPLES)

        for m in edge_values:
            for bandwidth in BANDWIDTH_VALUES:
                for latency in LATENCY_VALUES:
                    for failure_rate in FAILURE_RATE_VALUES:
                        for loss_rate in LOSS_RATE_VALUES:
                            rows.append({
                                "num_nodes": n,
                                "num_edges": m,
                                "bandwidth": bandwidth,
                                "latency": latency,
                                "failure_rate": failure_rate,
                                "loss_rate": loss_rate,
                            })

    return rows


def main() -> None:
    rows = generate_rows()
    output_path = Path(OUTPUT_FILE)

    with output_path.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "num_nodes",
                "num_edges",
                "bandwidth",
                "latency",
                "failure_rate",
                "loss_rate",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"Generated input file: {output_path}")
    print(f"Total rows: {len(rows)}")
    print("Node values:", list(range(NODE_START, NODE_END + 1, NODE_STEP)))
    print("Bandwidth values:", BANDWIDTH_VALUES)
    print("Latency values:", LATENCY_VALUES)
    print("Failure-rate values:", FAILURE_RATE_VALUES)
    print("Loss-rate values:", LOSS_RATE_VALUES)


if __name__ == "__main__":
    main()
