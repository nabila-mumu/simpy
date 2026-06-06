#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path

from simulator.experiment import ScenarioInput, ScenarioOutput, run_current_scenario


INPUT_COLUMNS = ["num_nodes", "num_edges", "bandwidth", "latency", "failure_rate", "loss_rate"]
OUTPUT_COLUMNS = list(ScenarioOutput.__dataclass_fields__.keys())


def read_inputs(path: str) -> list[ScenarioInput]:
    rows: list[ScenarioInput] = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        missing = [c for c in INPUT_COLUMNS if c not in (reader.fieldnames or [])]
        if missing:
            raise ValueError(f"Input CSV missing columns: {missing}")

        for row in reader:
            rows.append(
                ScenarioInput(
                    num_nodes=int(row["num_nodes"]),
                    num_edges=int(row["num_edges"]),
                    bandwidth=float(row["bandwidth"]),
                    latency=float(row["latency"]),
                    failure_rate=float(row["failure_rate"]),
                    loss_rate=float(row["loss_rate"]),
                )
            )
    return rows


def format_row(row: dict) -> dict:
    out = {}
    for k, v in row.items():
        if isinstance(v, float):
            out[k] = f"{v:.6f}"
        else:
            out[k] = v
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Input CSV containing num_nodes,num_edges,bandwidth,latency,failure_rate,loss_rate")
    parser.add_argument("--output", default="results.csv", help="Output CSV path")
    parser.add_argument("--sim-time", type=float, default=1000.0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    inputs = read_inputs(args.input)
    out_path = Path(args.output)

    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()

        for i, scenario in enumerate(inputs):
            result = run_current_scenario(scenario, sim_time=args.sim_time, seed=args.seed + i * 10_000)
            writer.writerow(format_row(result.to_dict()))

            if not args.quiet:
                print(
                    f"[{i + 1}/{len(inputs)}] "
                    f"n={scenario.num_nodes}, e={scenario.num_edges}, "
                    f"cycle={result.hamiltonian_cycle_found}, "
                    f"bully=({result.bully_time:.4f}s,{result.bully_msg_count:.2f}msg,{result.bully_elections}el), "
                    f"ring=({result.ring_time:.4f}s,{result.ring_msg_count:.2f}msg,{result.ring_elections}el), "
                    f"bidir=({result.bidir_ring_time:.4f}s,{result.bidir_ring_msg_count:.2f}msg,{result.bidir_ring_elections}el), "
                    f"raft=({result.raft_time:.4f}s,{result.raft_msg_count:.2f}msg,{result.raft_elections}el), "
                    f"echo=({result.echo_time:.4f}s,{result.echo_msg_count:.2f}msg,{result.echo_elections}el)"
                )

    print(f"Saved: {out_path}")


if __name__ == "__main__":
    main()
