#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path


def load_model_file(path: Path):
    try:
        import joblib
        return joblib.load(path)
    except Exception:
        import pickle
        with open(path, "rb") as f:
            return pickle.load(f)


def write_list(path: Path, values) -> None:
    path.write_text("\n".join(str(x) for x in values) + "\n")


def get_features_and_classes(bundle):
    features = bundle.get("features", bundle.get("feature_cols", None))
    classes = bundle.get("classes", None)
    if classes is None and "label_encoder" in bundle:
        try:
            classes = bundle["label_encoder"].classes_
        except Exception:
            classes = None
    return features, classes


def main() -> None:
    models = Path("models")

    pairs = [
        ("time", "logistic_regression_time.pkl", "dl_time_features.txt", "dl_time_classes.txt"),
        ("msg", "logistic_regression_msg.pkl", "dl_msg_features.txt", "dl_msg_classes.txt"),
        ("score", "logistic_regression_score.pkl", "dl_score_features.txt", "dl_score_classes.txt"),
    ]

    for label, pkl_name, feat_name, cls_name in pairs:
        p = models / pkl_name
        if not p.exists():
            print(f"[SKIP] Missing {p}")
            continue

        bundle = load_model_file(p)
        if not isinstance(bundle, dict):
            print(f"[SKIP] {pkl_name} is not a bundle dictionary")
            continue

        features, classes = get_features_and_classes(bundle)

        if features is None:
            print(f"[SKIP] {pkl_name} has no features / feature_cols key")
            continue
        if classes is None:
            print(f"[SKIP] {pkl_name} has no classes / label_encoder key")
            continue

        write_list(models / feat_name, features)
        write_list(models / cls_name, classes)

        print(f"[OK] wrote {models / feat_name}")
        print(f"[OK] wrote {models / cls_name}")


if __name__ == "__main__":
    main()
