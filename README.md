# A Supervised Learning Approach to Determine Optimum Leader Election Algorithm in Topology-Aware Distributed Systems

A Python research prototype for **simulating and comparing distributed leader-election algorithms** under changing network conditions, and for **selecting an election algorithm using machine learning**. The repository contains the simulator, scenario generators, training notebooks, saved selector models, and CSV experiment results.

## What this project does

- Simulates **Bully**, **Ring**, **Bidirectional Ring**, **Raft leader election**, and **Echo-wave leader election**.
- Generates connected network graphs and models node failures, message loss, bandwidth, latency, and multi-hop communication delays.
- Measures average election time, average message count, successful elections, and failed election attempts.
- Creates a labeled dataset by evaluating different algorithms across generated network scenarios.
- Trains algorithm-selection models using **linear regression**, **logistic regression**, and **TensorFlow/Keras neural networks**.
- Evaluates fixed-algorithm baselines against model-based selection on **time-windowed, changing network conditions**.

## Algorithms

| Algorithm | Implementation | Note |
| --- | --- | --- |
| Bully | `simulator/algorithms/bully.py` | Priority/ID-based leader election |
| Ring | `simulator/algorithms/ring.py` | Election messages circulate around a ring |
| Bidirectional Ring | `simulator/algorithms/bidir_ring.py` | Ring-based election in two directions |
| Raft (election) | `simulator/algorithms/raft.py` | Leader-election component only |
| Echo-wave | `simulator/algorithms/echo.py` | Election using echo-style message propagation |

The Ring and Bidirectional Ring experiments **require a Hamiltonian cycle in the generated graph**. If the simulator does not find one, those strategies are not executed for that scenario; the dynamic model-selection runner falls back to Raft if a model selects an unavailable ring strategy. The cycle search is bounded, so “not found” should not automatically be interpreted as proof that no Hamiltonian cycle exists.

## Network simulation

The simulator uses randomized connected graphs and configurable network conditions:

| Input | Meaning |
| --- | --- |
| `num_nodes` | Number of nodes |
| `num_edges` | Number of graph edges |
| `bandwidth` | Link bandwidth in Mbps |
| `latency` | Network latency in milliseconds |
| `failure_rate` | Node failure-rate parameter |
| `loss_rate` | Probability of message loss per hop |

The network model accounts for message size, serialization delay, latency, jitter, and hop count; multi-hop delivery probability is modeled using `(1 - loss_rate) ** hops`. Failure schedules are generated and shared across algorithm comparisons for a given static scenario. Generated network windows can change the number of nodes and edges as well as network parameters.

### Recorded metrics

- **Election time:** Average time for completed leader elections.
- **Message count:** Average messages exchanged per completed election.
- **Completed elections:** Number of successful election completions.
- **Failed attempts:** Number of election attempts recorded as failures.

Zero-valued time/message entries should be interpreted carefully: they may indicate that no election completed or that a ring strategy was unavailable, **not** zero-cost successful execution.

## Machine-learning algorithm selection

The training and dynamic inference code uses **10 pre-run features**:

1. `num_nodes`
2. `num_edges`
3. `graph_density`
4. `avg_degree`
5. `num_connected_component`
6. `bandwidth`
7. `latency`
8. `failure_rate`
9. `loss_rate`
10. `hamiltonian_cycle_found`

Output-only fields such as `*_elections` and `*_failed_attempts` are explicitly disallowed as model inputs in the dynamic inference adapter to avoid this form of target leakage.

The saved selector models cover three objectives—**election time**, **message count**, and a **combined score**—across three model families:

| Model family | Saved files | Selection method |
| --- | --- | --- |
| Linear regression | `linear_regression_{time,msg,score}.pkl` | Predicts algorithm-specific costs and chooses the lowest viable prediction |
| Logistic regression | `logistic_regression_{time,msg,score}.pkl` | Predicts an algorithm label |
| Deep learning | `dl_{time,msg,score}.h5` | Keras classifier predicts an algorithm label |

The notebooks `model_comprehensive.ipynb` and `train_selector_10_features.ipynb` contain modeling, feature analysis, training/evaluation, and selector-related code. The comprehensive notebook also includes visualizations and comparative analysis. Models have been saved under `all_algo_w_input_gen/models/`, together with Keras class/feature metadata and associated files.

### Dynamic comparison score

The dynamic experiment produces a reporting score using weighted election time, message count, and failed attempts:

```text
window_score = election_time * max(1, completed_elections)
             + 0.01 * message_count * max(1, completed_elections)
             + 5.0 * failed_attempts
```

**Lower scores are better according to this chosen weighting.** The weighting is an experimental design choice, not a universal measure of distributed-system performance.

## Requirements

- Python **3.10+** recommended (the source uses modern type-hint syntax).
- Python packages used across scripts and notebooks: `numpy`, `pandas`, `scikit-learn`, `joblib`, `tensorflow`, `matplotlib`, `seaborn`, `jupyter`.
- Standard-library modules include `csv`, `random`, `argparse`, `pickle`, `dataclasses`, and `pathlib`.

A pinned dependency manifest is not included in the supplied archive; versions below are therefore **not pinned**. TensorFlow is needed to load the included `.h5` neural-network models. Python and TensorFlow version compatibility may require selecting a supported environment.

```bash
python -m venv .venv
# On Linux/macOS:
source .venv/bin/activate
# On Windows (PowerShell):
# .venv\Scripts\Activate.ps1

python -m pip install numpy pandas scikit-learn joblib tensorflow matplotlib seaborn jupyter
```

## Running the experiments

Run the following commands **from `all_algo_w_input_gen/`**, after setting up dependencies:

```bash
cd all_algo_w_input_gen
```

### 1. Generate static scenario inputs (optional)

```bash
python generate_sample_inputs.py
```

This writes `generated_inputs.csv`. The generator varies node/edge counts, bandwidth, latency, failure rate, and loss rate. Its input ranges can be changed in the script.

### 2. Run static leader-election simulations

For a small sample:

```bash
python generate_metrics.py --input sample_inputs.csv --output sample_results.csv --sim-time 1000 --seed 42
```

For the generated full scenario grid (**potentially computationally intensive**):

```bash
python generate_metrics.py --input generated_inputs.csv --output results.csv --sim-time 1000 --seed 42
```

The output contains graph features plus time, message count, successful elections, and failed attempts for each of the five algorithms. A previously generated dataset is included as `final_results_v5.csv`; the commands above do not automatically recreate that exact file or guarantee identical values.

### 3. Inspect the existing static dataset

```bash
python generate_summary.py
```

This script reads the existing `final_results_v5.csv` and reports distributions of the algorithms with the best time and message count, ignoring zero-valued results for those comparisons.

### 4. Generate dynamic network windows

```bash
python generate_dynamic_windows.py --input dynamic_initial_input.csv --output dynamic_windows.csv --seed 42
```

Network properties evolve from one window to the next. The input CSV supports `total_time`, `check_interval`, and optional network-parameter bounds.

### 5. Run model-based dynamic selection

The supplied archive already contains trained models in `models/`:

```bash
python run_dynamic_simulation_multi_model.py \
  --windows dynamic_windows.csv \
  --models-dir models \
  --window-output outputs/dynamic_window_results_multi_model.csv \
  --long-output outputs/dynamic_long_results_multi_model.csv \
  --summary-output outputs/dynamic_summary_multi_model.csv \
  --seed 42
```

This compares each available ML selector against fixed Bully, Ring, Bidirectional Ring, Raft, and Echo strategies under the same simulated window conditions. Model files that cannot be loaded are skipped with a diagnostic message; check the console to confirm which strategies were actually evaluated.

### 6. Explore or retrain selectors

From the repository root, open the notebooks:

```bash
jupyter notebook model_comprehensive.ipynb
jupyter notebook train_selector_10_features.ipynb
```

Check the dataset/model paths inside the notebooks before execution. The training notebook references `all_algo_w_input_gen/final_results_v5.csv`, so it is designed to be launched from the repository root. The notebooks contain the model-training code; the dynamic runner loads saved models rather than retraining them.

## Included experiment outputs

| File | Purpose |
| --- | --- |
| `final_results_v5.csv` | Static scenario data and algorithm performance measures |
| `outputs/dynamic_window_results_multi_model.csv` | Wide-format comparison of all strategies by window |
| `outputs/dynamic_long_results_multi_model.csv` | Long-format window-by-strategy observations |
| `outputs/dynamic_summary_multi_model.csv` | Aggregated strategy performance metrics |

The provided dynamic summary contains **50 windows over 5,000 units of simulated time**. Treat these as results of the archived simulation configuration—not a general performance guarantee across networks.


## Experimental Results and Thesis Documentation

This project was developed as part of our academic thesis, **A Supervised Learning Approach to Determine Optimum Leader Election Algorithm in Topology-Aware Distributed Systems**.

The complete experimental methodology, simulation configurations, performance comparisons, results, and detailed analysis are documented in the thesis report, which was submitted as part of my thesis defense.

The thesis includes:

- Detailed implementation and experimental methodology.
- Network simulation scenarios and parameter configurations.
- Comparative performance analysis of five leader election algorithms.
- Evaluation of supervised machine learning models to predict the most efficient algorithm for a given network environment
- Analysis of election time, message overhead, and election success rates.
- A topology-aware framework to dynamically select leader election algorithms based on network conditions
- Discussion of experimental findings, limitations, and potential improvements.

### Full Thesis Report

For a comprehensive explanation of the experiments, results, and analysis, please refer to the thesis document:

**[View Full Thesis Report](./thesis_book.pdf)**

## Limitations and interpretation

- This is a simulation with simplified network and failure models, not an empirical benchmark on real distributed infrastructure.
- The Raft implementation covers leader election rather than full replicated state-machine consensus.
- Ring methods depend on successful Hamiltonian-cycle discovery and are not always feasible.
- Model quality depends on the generated training scenarios and the choice of selection objective/score weights.
- Precomputed data and trained models are included, but exact regeneration can depend on environment, versions, random seeds, and settings in the notebooks/scripts.

## Research scope

The central question investigated here is whether **network-aware machine-learning selection** can improve leader-election behavior compared with choosing one fixed election algorithm across all network conditions. The implementation provides a reproducible framework for studying this trade-off through simulation, saved models, and per-window evaluation.

## Authors

- **2005050** - Nabila Tabassum
- **2005056** - Azmal Karim

