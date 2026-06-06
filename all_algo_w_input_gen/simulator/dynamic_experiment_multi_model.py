from __future__ import annotations

import csv
import pickle
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any

import numpy as np

from simulator.algorithms.bidir_ring import BidirRingAlgorithm
from simulator.algorithms.bully import BullyAlgorithm
from simulator.algorithms.echo import EchoWaveElectionAlgorithm
from simulator.algorithms.raft import RaftElectionAlgorithm
from simulator.algorithms.ring import RingAlgorithm
from simulator.dynamic_topology import DynamicWindow, read_windows_csv
from simulator.failures import generate_failure_schedule
from simulator.graph import (
    avg_degree,
    connected_components,
    find_hamiltonian_cycle,
    graph_density,
    random_connected_graph,
)
from simulator.network import NetworkConfig


ALGORITHMS = ["bully", "ring", "bidir_ring", "raft", "echo"]

# Pure pre-run input features only. No election count and no failed-attempt leakage.
DEFAULT_FEATURE_COLS = [
    "num_nodes",
    "num_edges",
    "graph_density",
    "avg_degree",
    "num_connected_component",
    "bandwidth",
    "latency",
    "failure_rate",
    "loss_rate",
    "hamiltonian_cycle_found",
]

# If class metadata is missing, this is the most common alphabetical LabelEncoder order.
DEFAULT_CLASS_ORDER = ["bidir_ring", "bully", "echo", "raft", "ring"]

# These weights are only for reporting a comparable dynamic score in the summary.
# The score model itself already learned score from the training dataset.
TIME_WEIGHT = 1.0
MSG_WEIGHT = 0.01
FAILED_ATTEMPT_WEIGHT = 5.0

LABEL_ALIASES = {
    "BiDirRing": "bidir_ring",
    "BidirRing": "bidir_ring",
    "BiRing": "bidir_ring",
    "bidirring": "bidir_ring",
    "bidir_ring": "bidir_ring",
    "Ring": "ring",
    "ring": "ring",
    "Bully": "bully",
    "bully": "bully",
    "Raft": "raft",
    "raft": "raft",
    "Echo": "echo",
    "echo": "echo",
}


@dataclass
class DynamicLongResult:
    scenario_id: int
    window_id: int
    start_time: float
    end_time: float
    duration: float

    strategy: str
    model_family: str
    objective: str
    selected_algorithm: str

    num_nodes: int
    num_edges: int
    graph_density: float
    avg_degree: float
    num_connected_component: int
    hamiltonian_cycle_found: int

    bandwidth: float
    latency: float
    failure_rate: float
    loss_rate: float

    election_time: float
    msg_count: float
    completed_elections: int
    failed_attempts: int
    window_score: float


@dataclass
class DynamicSummaryResult:
    strategy: str
    model_family: str
    objective: str
    total_windows: int
    total_duration: float
    total_completed_elections: int
    total_failed_attempts: int
    avg_election_time: float
    avg_msg_count: float
    total_election_time: float
    total_msg_count: float
    total_score: float
    avg_score_per_window: float
    avg_score_per_completed_election: float


def _window_seed(w: DynamicWindow, base_seed: int, salt: int) -> int:
    return (
        base_seed
        + w.scenario_id * 1_000_003
        + w.window_id * 100_003
        + w.num_nodes * 1009
        + w.num_edges * 917
        + int(w.bandwidth * 101)
        + int(w.latency * 103)
        + int(w.failure_rate * 1_000_000) * 107
        + int(w.loss_rate * 1_000_000) * 109
        + salt
    )


def _normalize_label(x: Any) -> str:
    s = str(x)
    return LABEL_ALIASES.get(s, LABEL_ALIASES.get(s.lower(), s.lower()))


def _load_model_file(path: Path) -> Any:
    try:
        import joblib
        return joblib.load(path)
    except Exception:
        with open(path, "rb") as f:
            return pickle.load(f)


def _read_lines(path: Path) -> list[str]:
    return [x.strip() for x in path.read_text().splitlines() if x.strip()]


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _objective_from_name(name: str) -> str:
    name = name.lower()
    if "score" in name:
        return "score"
    if "msg" in name:
        return "msg"
    if "time" in name:
        return "time"
    return "unknown"


def _family_from_name(name: str) -> str:
    name = name.lower()
    if name.startswith("linear_regression"):
        return "linear_regression"
    if name.startswith("logistic_regression"):
        return "logistic_regression"
    if name.startswith("dl"):
        return "dl"
    return "unknown"


class ModelAdapter:
    """
    Supports these model files:
      linear_regression_time.pkl, linear_regression_msg.pkl, linear_regression_score.pkl
      logistic_regression_time.pkl, logistic_regression_msg.pkl, logistic_regression_score.pkl
      dl_time.h5, dl_msg.h5, dl_score.h5

    It intentionally ignores metadata files such as dl_time_metadata.pkl.
    It uses only the 10 pure input features.
    """

    def __init__(self, path: Path):
        self.path = path
        self.name = path.stem
        self.suffix = path.suffix.lower()
        self.objective = _objective_from_name(self.name)
        self.model_family = _family_from_name(self.name)

        self.obj: Any = None
        self.kind = "unknown"
        self.feature_cols: list[str] = DEFAULT_FEATURE_COLS[:]
        self.class_order: list[str] = DEFAULT_CLASS_ORDER[:]
        self.scaler: Any = None

        if self.suffix == ".pkl":
            self.obj = _load_model_file(path)
            self._configure_pickle()
        elif self.suffix == ".h5":
            self._configure_h5()
        else:
            raise ValueError(f"Unsupported model file: {path}")

        # Hard safety: never use leakage columns even if a stale model bundle claims them.
        bad = [c for c in self.feature_cols if c.endswith("_elections") or c.endswith("_failed_attempts")]
        if bad:
            raise ValueError(
                f"{path.name} uses leaked output features {bad}. Retrain using only the 10 input features."
            )

    def _configure_pickle(self) -> None:
        obj = self.obj

        if not isinstance(obj, dict):
            if hasattr(obj, "predict"):
                self.kind = "classifier"
                return
            raise ValueError(f"Unsupported pickle format: {self.path.name}")

        self.feature_cols = obj.get("features", obj.get("feature_cols", DEFAULT_FEATURE_COLS))
        self.scaler = obj.get("scaler", None)

        # Logistic classifier bundle.
        if "model" in obj:
            self.obj = obj["model"]
            self.kind = "classifier"

            classes = obj.get("classes", None)
            if classes is None and "label_encoder" in obj:
                try:
                    classes = obj["label_encoder"].classes_
                except Exception:
                    classes = None
            if classes is not None:
                self.class_order = [_normalize_label(c) for c in classes]
            return

        # Linear regression bundle: five regressors, one per algorithm.
        for key in ["models_by_algo", "regressors", "models"]:
            if key in obj and isinstance(obj[key], dict):
                self.obj = {_normalize_label(k): v for k, v in obj[key].items()}
                self.kind = "regressor_set"
                return

        keys = {_normalize_label(k) for k in obj.keys()}
        if any(k in keys for k in ALGORITHMS):
            self.obj = {_normalize_label(k): v for k, v in obj.items()}
            self.kind = "regressor_set"
            return

        raise ValueError(f"Could not understand pickle format: {self.path.name}")

    def _configure_h5(self) -> None:
        from tensorflow.keras.models import load_model

        self.obj = load_model(self.path)
        self.kind = "keras_classifier"

        feat_file = self.path.with_name(self.path.stem + "_features.txt")
        class_file = self.path.with_name(self.path.stem + "_classes.txt")
        meta_file = self.path.with_name(self.path.stem + "_metadata.pkl")

        if feat_file.exists():
            self.feature_cols = _read_lines(feat_file)
        if class_file.exists():
            self.class_order = [_normalize_label(c) for c in _read_lines(class_file)]

        # Optional metadata pkl from notebook.
        if meta_file.exists():
            try:
                meta = _load_model_file(meta_file)
                if isinstance(meta, dict):
                    self.feature_cols = meta.get("features", meta.get("feature_cols", self.feature_cols))
                    classes = meta.get("classes", None)
                    if classes is None and "label_encoder" in meta:
                        try:
                            classes = meta["label_encoder"].classes_
                        except Exception:
                            classes = None
                    if classes is not None:
                        self.class_order = [_normalize_label(c) for c in classes]
            except Exception:
                pass

        # Fallback from matching logistic pkl.
        if (not feat_file.exists() or not class_file.exists()) and self.objective in {"time", "msg", "score"}:
            pkl_path = self.path.with_name(f"logistic_regression_{self.objective}.pkl")
            if pkl_path.exists():
                try:
                    meta = _load_model_file(pkl_path)
                    if isinstance(meta, dict):
                        if not feat_file.exists():
                            self.feature_cols = meta.get("features", meta.get("feature_cols", self.feature_cols))
                        if not class_file.exists():
                            classes = meta.get("classes", None)
                            if classes is None and "label_encoder" in meta:
                                try:
                                    classes = meta["label_encoder"].classes_
                                except Exception:
                                    classes = None
                            if classes is not None:
                                self.class_order = [_normalize_label(c) for c in classes]
                except Exception:
                    pass

        expected = self._keras_input_dim()
        if expected is not None and expected != len(self.feature_cols):
            raise ValueError(
                f"{self.path.name} expects {expected} features, but adapter has {len(self.feature_cols)}. "
                f"Check {feat_file.name} or {meta_file.name}."
            )

    def _keras_input_dim(self) -> int | None:
        try:
            shp = self.obj.input_shape
            if isinstance(shp, (list, tuple)):
                if isinstance(shp[0], (list, tuple)):
                    return int(shp[0][-1])
                return int(shp[-1])
        except Exception:
            return None
        return None

    def _make_values(self, feature: dict[str, float]) -> list[float]:
        return [_safe_float(feature.get(c, 0.0)) for c in self.feature_cols]

    def _make_X_for_sklearn(self, feature: dict[str, float]):
        values = self._make_values(feature)
        arr = np.asarray([values], dtype=float)
        if self.scaler is not None:
            return self.scaler.transform(arr)
        try:
            import pandas as pd
            return pd.DataFrame([{c: _safe_float(feature.get(c, 0.0)) for c in self.feature_cols}])
        except Exception:
            return arr

    def predict_algorithm(self, feature: dict[str, float]) -> str:
        if self.kind == "classifier":
            X = self._make_X_for_sklearn(feature)
            raw = self.obj.predict(X)[0]
            if isinstance(raw, (int, np.integer)) or (isinstance(raw, float) and float(raw).is_integer()):
                idx = int(raw)
                if 0 <= idx < len(self.class_order):
                    return self.class_order[idx]
            return _normalize_label(raw)

        if self.kind == "keras_classifier":
            arr = np.asarray([self._make_values(feature)], dtype=float)
            y = np.asarray(self.obj.predict(arr, verbose=0))
            idx = int(np.argmax(y[0]))
            return self.class_order[idx] if idx < len(self.class_order) else "raft"

        if self.kind == "regressor_set":
            X = self._make_X_for_sklearn(feature)
            scores: dict[str, float] = {}
            for alg, reg in self.obj.items():
                alg = _normalize_label(alg)
                if alg not in ALGORITHMS:
                    continue
                try:
                    scores[alg] = float(reg.predict(X)[0])
                except Exception:
                    continue
            positive = {a: v for a, v in scores.items() if v > 0}
            if positive:
                return min(positive, key=positive.get)
            if scores:
                return min(scores, key=scores.get)
            return "raft"

        return "raft"


def load_all_models(models_dir: str) -> list[ModelAdapter]:
    models_path = Path(models_dir)
    wanted = [
        "linear_regression_time.pkl",
        "linear_regression_msg.pkl",
        "linear_regression_score.pkl",
        "logistic_regression_time.pkl",
        "logistic_regression_msg.pkl",
        "logistic_regression_score.pkl",
        "dl_time.h5",
        "dl_msg.h5",
        "dl_score.h5",
    ]

    out: list[ModelAdapter] = []
    for name in wanted:
        p = models_path / name
        if not p.exists():
            print(f"[SKIP] Missing model: {name}")
            continue
        try:
            out.append(ModelAdapter(p))
            print(f"[OK] Loaded model: {p.name}")
        except Exception as e:
            print(f"[SKIP] Could not load {p.name}: {e}")
    return out


def _run_single_algorithm(algorithm: str, w: DynamicWindow, g, cycle, network: NetworkConfig, schedule, duration: float, base_seed: int):
    if algorithm == "bully":
        return BullyAlgorithm(g, network, schedule, duration, seed=_window_seed(w, base_seed, 19)).run()
    if algorithm == "ring":
        return None if cycle is None else RingAlgorithm(cycle, network, schedule, duration, seed=_window_seed(w, base_seed, 23)).run()
    if algorithm == "bidir_ring":
        return None if cycle is None else BidirRingAlgorithm(cycle, network, schedule, duration, seed=_window_seed(w, base_seed, 29)).run()
    if algorithm == "raft":
        return RaftElectionAlgorithm(g, network, schedule, duration, seed=_window_seed(w, base_seed, 31)).run()
    if algorithm == "echo":
        return EchoWaveElectionAlgorithm(g, network, schedule, duration, seed=_window_seed(w, base_seed, 37)).run()
    raise ValueError(f"Unknown algorithm: {algorithm}")


def _result_dict(res):
    if res is None:
        return {"time": 0.0, "msg_count": 0.0, "elections": 0, "failed_attempts": 0}
    return {
        "time": _safe_float(res.avg_time),
        "msg_count": _safe_float(res.avg_msg_count),
        "elections": int(res.completed_elections),
        "failed_attempts": int(res.failed_attempts),
    }


def _window_score(r: dict) -> float:
    # Penalize failed attempts even if there are zero completed elections.
    election_cost = r["time"] * max(1, r["elections"])
    msg_cost = r["msg_count"] * max(1, r["elections"])
    return TIME_WEIGHT * election_cost + MSG_WEIGHT * msg_cost + FAILED_ATTEMPT_WEIGHT * r["failed_attempts"]


def _base_feature_row(w: DynamicWindow, g, hcycle: int) -> dict[str, float]:
    return {
        "num_nodes": w.num_nodes,
        "num_edges": w.num_edges,
        "graph_density": graph_density(w.num_nodes, w.num_edges),
        "avg_degree": avg_degree(w.num_nodes, w.num_edges),
        "num_connected_component": connected_components(g),
        "bandwidth": w.bandwidth,
        "latency": w.latency,
        "failure_rate": w.failure_rate,
        "loss_rate": w.loss_rate,
        "hamiltonian_cycle_found": hcycle,
    }


def run_dynamic_multi_model_experiment(windows_csv: str, models_dir: str, window_output_csv: str, long_output_csv: str, summary_output_csv: str, seed: int = 42) -> None:
    windows = read_windows_csv(windows_csv)
    adapters = load_all_models(models_dir)

    wide_rows: list[dict] = []
    long_rows: list[DynamicLongResult] = []

    for w in windows:
        duration = w.end_time - w.start_time

        g = random_connected_graph(w.num_nodes, w.num_edges, seed=_window_seed(w, seed, 11))
        cycle = find_hamiltonian_cycle(g, seed=_window_seed(w, seed, 13))

        network = NetworkConfig(bandwidth_mbps=w.bandwidth, latency_ms=w.latency, loss_rate=w.loss_rate)
        schedule = generate_failure_schedule(w.num_nodes, duration, w.failure_rate, seed=_window_seed(w, seed, 17))

        hcycle = 1 if cycle is not None else 0
        feature = _base_feature_row(w, g, hcycle)
        fixed: dict[str, dict] = {}

        base = dict(
            scenario_id=w.scenario_id,
            window_id=w.window_id,
            start_time=w.start_time,
            end_time=w.end_time,
            duration=duration,
            num_nodes=w.num_nodes,
            num_edges=w.num_edges,
            graph_density=feature["graph_density"],
            avg_degree=feature["avg_degree"],
            num_connected_component=feature["num_connected_component"],
            bandwidth=w.bandwidth,
            latency=w.latency,
            failure_rate=w.failure_rate,
            loss_rate=w.loss_rate,
            hamiltonian_cycle_found=hcycle,
        )

        # Fixed algorithm baselines.
        for alg in ALGORITHMS:
            fixed[alg] = _result_dict(_run_single_algorithm(alg, w, g, cycle, network, schedule, duration, seed))
            r = fixed[alg]
            r["score"] = _window_score(r)

            base[f"fixed_{alg}_time"] = r["time"]
            base[f"fixed_{alg}_msg_count"] = r["msg_count"]
            base[f"fixed_{alg}_elections"] = r["elections"]
            base[f"fixed_{alg}_failed_attempts"] = r["failed_attempts"]
            base[f"fixed_{alg}_score"] = r["score"]

            long_rows.append(_make_long(w, duration, f"fixed_{alg}", "fixed", "baseline", alg, feature, hcycle, r))

        # ML strategies: each model selects an algorithm for this window.
        for ad in adapters:
            selected = ad.predict_algorithm(feature)
            if selected in {"ring", "bidir_ring"} and cycle is None:
                selected = "raft"
            if selected not in ALGORITHMS:
                selected = "raft"

            r = fixed[selected]
            prefix = f"model_{ad.name}"

            base[f"{prefix}_selected_algorithm"] = selected
            base[f"{prefix}_time"] = r["time"]
            base[f"{prefix}_msg_count"] = r["msg_count"]
            base[f"{prefix}_elections"] = r["elections"]
            base[f"{prefix}_failed_attempts"] = r["failed_attempts"]
            base[f"{prefix}_score"] = r["score"]

            long_rows.append(_make_long(w, duration, prefix, ad.model_family, ad.objective, selected, feature, hcycle, r))

        wide_rows.append(base)

    _write_dict_rows(wide_rows, window_output_csv)
    _write_long_rows(long_rows, long_output_csv)
    _write_summary(_summarize(long_rows), summary_output_csv)


def _make_long(w, duration, strategy, model_family, objective, selected, feature, hcycle, r):
    return DynamicLongResult(
        w.scenario_id,
        w.window_id,
        w.start_time,
        w.end_time,
        duration,
        strategy,
        model_family,
        objective,
        selected,
        w.num_nodes,
        w.num_edges,
        feature["graph_density"],
        feature["avg_degree"],
        feature["num_connected_component"],
        hcycle,
        w.bandwidth,
        w.latency,
        w.failure_rate,
        w.loss_rate,
        r["time"],
        r["msg_count"],
        r["elections"],
        r["failed_attempts"],
        r["score"],
    )


def _write_dict_rows(rows: list[dict], path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    fields = []
    for r in rows:
        for k in r:
            if k not in fields:
                fields.append(k)
    with open(path, "w", newline="") as f:
        wr = csv.DictWriter(f, fieldnames=fields)
        wr.writeheader()
        for r in rows:
            wr.writerow(_format_row(r))


def _write_long_rows(rows: list[DynamicLongResult], path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    fields = list(DynamicLongResult.__dataclass_fields__.keys())
    with open(path, "w", newline="") as f:
        wr = csv.DictWriter(f, fieldnames=fields)
        wr.writeheader()
        for r in rows:
            wr.writerow(_format_row(asdict(r)))


def _summarize(rows: list[DynamicLongResult]) -> list[DynamicSummaryResult]:
    groups: dict[str, list[DynamicLongResult]] = {}
    for r in rows:
        groups.setdefault(r.strategy, []).append(r)

    out = []
    for strategy, rs in sorted(groups.items()):
        total_completed = sum(r.completed_elections for r in rs)
        total_failed = sum(r.failed_attempts for r in rs)
        total_msg = sum(r.msg_count * r.completed_elections for r in rs)
        total_time = sum(r.election_time * r.completed_elections for r in rs)
        total_score = sum(r.window_score for r in rs)

        out.append(DynamicSummaryResult(
            strategy=strategy,
            model_family=rs[0].model_family,
            objective=rs[0].objective,
            total_windows=len(rs),
            total_duration=sum(r.duration for r in rs),
            total_completed_elections=total_completed,
            total_failed_attempts=total_failed,
            avg_election_time=total_time / total_completed if total_completed else 0.0,
            avg_msg_count=total_msg / total_completed if total_completed else 0.0,
            total_election_time=total_time,
            total_msg_count=total_msg,
            total_score=total_score,
            avg_score_per_window=total_score / len(rs) if rs else 0.0,
            avg_score_per_completed_election=total_score / total_completed if total_completed else total_score,
        ))
    return out


def _write_summary(rows: list[DynamicSummaryResult], path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    fields = list(DynamicSummaryResult.__dataclass_fields__.keys())
    with open(path, "w", newline="") as f:
        wr = csv.DictWriter(f, fieldnames=fields)
        wr.writeheader()
        for r in rows:
            wr.writerow(_format_row(asdict(r)))


def _format_row(row: dict) -> dict:
    return {k: (f"{v:.6f}" if isinstance(v, float) else v) for k, v in row.items()}
