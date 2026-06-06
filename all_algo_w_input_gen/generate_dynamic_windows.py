# #!/usr/bin/env python3
# from __future__ import annotations
# import argparse
# from simulator.dynamic_topology import generate_dynamic_windows, read_initial_inputs, write_windows_csv

# def main():
#     ap=argparse.ArgumentParser(); ap.add_argument('--input',default='dynamic_initial_input.csv'); ap.add_argument('--output',default='dynamic_windows.csv'); ap.add_argument('--seed',type=int,default=42); a=ap.parse_args()
#     allw=[]
#     for row in read_initial_inputs(a.input):
#         allw.extend(generate_dynamic_windows(**row, seed=a.seed))
#     write_windows_csv(allw,a.output); print(f'Saved {len(allw)} dynamic windows to {a.output}')
# if __name__=='__main__': main()

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import random
from dataclasses import dataclass, asdict
from pathlib import Path


@dataclass
class DynamicWindow:
    scenario_id: int
    window_id: int
    start_time: float
    end_time: float
    num_nodes: int
    num_edges: int
    bandwidth: float
    latency: float
    failure_rate: float
    loss_rate: float


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


def max_edges(n: int) -> int:
    return n * (n - 1) // 2


def min_edges(n: int) -> int:
    return n - 1


def read_initial_inputs(path: str) -> list[dict]:
    rows = []

    with open(path, newline="") as f:
        reader = csv.DictReader(f)

        required = [
            "num_nodes",
            "num_edges",
            "bandwidth",
            "latency",
            "failure_rate",
            "loss_rate",
        ]

        missing = [c for c in required if c not in reader.fieldnames]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        for i, row in enumerate(reader):
            rows.append({
                "scenario_id": int(row.get("scenario_id", i) or i),
                "num_nodes": int(row["num_nodes"]),
                "num_edges": int(row["num_edges"]),
                "bandwidth": float(row["bandwidth"]),
                "latency": float(row["latency"]),
                "failure_rate": float(row["failure_rate"]),
                "loss_rate": float(row["loss_rate"]),
                "total_time": float(row.get("total_time", 5000) or 5000),
                "check_interval": float(row.get("check_interval", 100) or 100),

                "min_nodes": int(row.get("min_nodes", 10) or 10),
                "max_nodes": int(row.get("max_nodes", 60) or 60),

                "min_bandwidth": float(row.get("min_bandwidth", 5) or 5),
                "max_bandwidth": float(row.get("max_bandwidth", 100) or 100),

                "min_latency": float(row.get("min_latency", 5) or 5),
                "max_latency": float(row.get("max_latency", 120) or 120),

                "min_failure_rate": float(row.get("min_failure_rate", 0.001) or 0.001),
                "max_failure_rate": float(row.get("max_failure_rate", 0.025) or 0.025),

                "min_loss_rate": float(row.get("min_loss_rate", 0.0) or 0.0),
                "max_loss_rate": float(row.get("max_loss_rate", 0.10) or 0.10),
            })

    return rows


def generate_dynamic_windows(row: dict, seed: int = 42) -> list[DynamicWindow]:
    scenario_id = row["scenario_id"]
    rng = random.Random(seed + scenario_id * 100000)

    total_time = row["total_time"]
    check_interval = row["check_interval"]

    n = row["num_nodes"]
    e = row["num_edges"]

    bandwidth = row["bandwidth"]
    latency = row["latency"]
    failure_rate = row["failure_rate"]
    loss_rate = row["loss_rate"]

    windows = []
    count = int(total_time // check_interval)

    for wid in range(count):
        start = wid * check_interval
        end = start + check_interval

        if wid > 0:
            # node join/fail
            node_change = rng.choice([-4, -3, -2, -1, 0, 0, 1, 2, 3, 4])
            n = int(clamp(n + node_change, row["min_nodes"], row["max_nodes"]))

            # edge change due to topology update
            e_change = rng.randint(-2 * n, 2 * n)
            e = int(clamp(e + e_change, min_edges(n), max_edges(n)))

            # dynamic network parameters
            bandwidth *= rng.uniform(0.70, 1.30)
            latency *= rng.uniform(0.70, 1.35)
            failure_rate *= rng.uniform(0.65, 1.40)
            loss_rate *= rng.uniform(0.60, 1.50)

        bandwidth = float(clamp(bandwidth, row["min_bandwidth"], row["max_bandwidth"]))
        latency = float(clamp(latency, row["min_latency"], row["max_latency"]))
        failure_rate = float(clamp(failure_rate, row["min_failure_rate"], row["max_failure_rate"]))
        loss_rate = float(clamp(loss_rate, row["min_loss_rate"], row["max_loss_rate"]))

        e = int(clamp(e, min_edges(n), max_edges(n)))

        windows.append(DynamicWindow(
            scenario_id=scenario_id,
            window_id=wid,
            start_time=float(start),
            end_time=float(end),
            num_nodes=int(n),
            num_edges=int(e),
            bandwidth=round(bandwidth, 4),
            latency=round(latency, 4),
            failure_rate=round(failure_rate, 6),
            loss_rate=round(loss_rate, 6),
        ))

    return windows


def write_windows_csv(windows: list[DynamicWindow], output: str) -> None:
    p = Path(output)
    p.parent.mkdir(parents=True, exist_ok=True)

    fields = list(DynamicWindow.__dataclass_fields__.keys())

    with open(p, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for w in windows:
            writer.writerow(asdict(w))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="dynamic_initial_input.csv")
    parser.add_argument("--output", default="dynamic_windows.csv")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    all_windows = []

    for row in read_initial_inputs(args.input):
        all_windows.extend(generate_dynamic_windows(row, seed=args.seed))

    write_windows_csv(all_windows, args.output)
    print(f"Saved {len(all_windows)} dynamic windows to {args.output}")


if __name__ == "__main__":
    main()