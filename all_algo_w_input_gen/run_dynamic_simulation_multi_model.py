#!/usr/bin/env python3
from __future__ import annotations

import argparse

from simulator.dynamic_experiment_multi_model import run_dynamic_multi_model_experiment


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--windows", default="dynamic_windows.csv")
    parser.add_argument("--models-dir", default="models")
    parser.add_argument("--window-output", default="outputs/dynamic_window_results_multi_model.csv")
    parser.add_argument("--long-output", default="outputs/dynamic_long_results_multi_model.csv")
    parser.add_argument("--summary-output", default="outputs/dynamic_summary_multi_model.csv")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    run_dynamic_multi_model_experiment(
        windows_csv=args.windows,
        models_dir=args.models_dir,
        window_output_csv=args.window_output,
        long_output_csv=args.long_output,
        summary_output_csv=args.summary_output,
        seed=args.seed,
    )

    print(f"Saved wide window output: {args.window_output}")
    print(f"Saved long output: {args.long_output}")
    print(f"Saved summary output: {args.summary_output}")


if __name__ == "__main__":
    main()
