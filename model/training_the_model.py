from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.model_selection import train_test_split


REQUIRED = ["source", "organization", "device", "alert_type", "severity", "timestamp"]


@dataclass
class PairSample:
    i: int
    j: int
    same_incident: int
    features: List[float]


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in REQUIRED:
        out[c] = out.get(c, "").astype(str).str.strip()
    out["ts"] = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")
    out = out.dropna(subset=["ts"]).reset_index(drop=True)
    return out


def heuristic_incident_id(df: pd.DataFrame, window_minutes: int = 5) -> pd.Series:
    # Weak-label baseline incident assignment.
    # Alerts from different devices can still be part of the same shared incident
    # when they occur for the same organization and alert type in a short window.
    grp_cols = ["source", "organization", "alert_type"]
    work = df.sort_values(grp_cols + ["ts"]).copy()

    ids = pd.Series(index=work.index, dtype="object")
    window = pd.Timedelta(minutes=window_minutes)

    for _, g in work.groupby(grp_cols, sort=False):
        seq = 0
        last = None
        for idx, row in g.iterrows():
            if last is None or (row["ts"] - last) > window:
                seq += 1
            last = row["ts"]
            ids.loc[idx] = f"{row['source']}|{row['organization']}|{row['alert_type']}|{seq}"

    return ids.sort_index()


def pair_features(df: pd.DataFrame, tfidf: TfidfVectorizer, i: int, j: int) -> List[float]:
    a = df.iloc[i]
    b = df.iloc[j]

    dt_min = abs((a["ts"] - b["ts"]).total_seconds()) / 60.0
    same_source = 1.0 if a["source"] == b["source"] else 0.0
    same_org = 1.0 if a["organization"] == b["organization"] else 0.0
    same_device = 1.0 if a["device"] == b["device"] else 0.0
    same_type = 1.0 if a["alert_type"] == b["alert_type"] else 0.0
    same_sev = 1.0 if a["severity"] == b["severity"] else 0.0

    # Text similarity from alert_type.
    vec = tfidf.transform([a["alert_type"], b["alert_type"]])
    denom = (np.linalg.norm(vec[0].toarray()) * np.linalg.norm(vec[1].toarray())) + 1e-9
    cosine = float((vec[0] @ vec[1].T).toarray()[0, 0] / denom)

    return [dt_min, same_source, same_org, same_device, same_type, same_sev, cosine]


def build_pair_dataset(df: pd.DataFrame, weak_ids: pd.Series, tfidf: TfidfVectorizer, max_dt_minutes: int = 120) -> Tuple[np.ndarray, np.ndarray]:
    # Build candidate pairs only inside same source+org to keep size manageable.
    X: List[List[float]] = []
    y: List[int] = []

    work = df.copy()
    work["weak_id"] = weak_ids.values

    for _, g in work.groupby(["source", "organization"], sort=False):
        idxs = list(g.index)
        for p in range(len(idxs)):
            i = idxs[p]
            for q in range(p + 1, len(idxs)):
                j = idxs[q]
                dt = abs((work.loc[i, "ts"] - work.loc[j, "ts"]).total_seconds()) / 60.0
                if dt > max_dt_minutes:
                    continue
                feats = pair_features(work, tfidf, i, j)
                label = 1 if work.loc[i, "weak_id"] == work.loc[j, "weak_id"] else 0
                X.append(feats)
                y.append(label)

    if not X:
        raise ValueError("No pair samples produced. Increase data size or max_dt_minutes.")

    return np.array(X, dtype=float), np.array(y, dtype=int)


def main() -> None:
    parser = argparse.ArgumentParser(description="Train pairwise incident-link model from weak labels.")
    parser.add_argument("--input", default="stitched_alerts_dedup.csv")
    parser.add_argument("--outdir", default="parser/models")
    parser.add_argument("--window-minutes", type=int, default=5)
    parser.add_argument("--max-dt-minutes", type=int, default=120)
    parser.add_argument("--threshold", type=float, default=0.65, help="Probability threshold for linking in inference.")
    args = parser.parse_args()

    in_path = Path(args.input)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(in_path)
    df = normalize(df)

    weak_ids = heuristic_incident_id(df, window_minutes=args.window_minutes)

    tfidf = TfidfVectorizer(ngram_range=(1, 2), min_df=1)
    tfidf.fit(df["alert_type"].astype(str).tolist())

    X, y = build_pair_dataset(df, weak_ids, tfidf, max_dt_minutes=args.max_dt_minutes)

    classes, counts = np.unique(y, return_counts=True)
    if len(classes) < 2:
        raise ValueError(
            f"Training labels contain only one class: {classes.tolist()}. "
            "Adjust the input data or window settings."
        )

    stratify = y if counts.min() >= 2 else None
    if stratify is None:
        print(
            f"Warning: smallest class has {counts.min()} sample(s); "
            "falling back to non-stratified split."
        )

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=stratify
    )

    model = RandomForestClassifier(
        n_estimators=300,
        max_depth=12,
        random_state=42,
        class_weight="balanced_subsample",
        n_jobs=-1,
    )
    model.fit(X_train, y_train)

    probs = model.predict_proba(X_test)[:, 1]
    preds = (probs >= args.threshold).astype(int)

    print("AUC:", roc_auc_score(y_test, probs))
    print(classification_report(y_test, preds, digits=4))

    joblib.dump(model, outdir / "incident_pair_model.joblib")
    joblib.dump(tfidf, outdir / "incident_tfidf.joblib")
    (outdir / "incident_meta.json").write_text(
        json.dumps(
            {
                "threshold": args.threshold,
                "window_minutes": args.window_minutes,
                "max_dt_minutes": args.max_dt_minutes,
                "label_strategy": "source+organization+alert_type+time_window",
                "feature_order": [
                    "dt_min",
                    "same_source",
                    "same_org",
                    "same_device",
                    "same_type",
                    "same_severity",
                    "alert_type_cosine",
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    print(f"Saved model artifacts to: {outdir}")


if __name__ == "__main__":
    main()