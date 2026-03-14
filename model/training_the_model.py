from __future__ import annotations

import argparse
import json
import logging
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
from sqlalchemy import create_engine, text

from parser.pipeline_service import build_db_url, load_dotenv


REQUIRED = ["source", "organization", "device", "alert_type", "severity", "timestamp"]
LOGGER = logging.getLogger("incident_training")


@dataclass
class PairSample:
    i: int
    j: int
    same_incident: int
    features: List[float]


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    out = df.copy()
    for c in REQUIRED:
        out[c] = out.get(c, "").astype(str).str.strip()
    # Synthetic feeds contain mixed timestamp shapes (with and without fractional seconds).
    out["ts"] = pd.to_datetime(out["timestamp"], utc=True, errors="coerce", format="mixed")
    out = out.dropna(subset=["ts"]).reset_index(drop=True)
    dropped = before - len(out)
    LOGGER.info("Normalization complete: %s rows kept, %s rows dropped (invalid timestamps)", len(out), dropped)
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

    LOGGER.info("Built pair dataset: %s pairs (max_dt_minutes=%s)", len(X), max_dt_minutes)
    return np.array(X, dtype=float), np.array(y, dtype=int)


def _load_training_data(
    *,
    use_db: bool,
    input_path: Path,
    db_url_arg: str | None,
    db_table: str,
    db_schema: str,
) -> pd.DataFrame:
    if not use_db:
        return pd.read_csv(input_path)

    db_url = build_db_url(db_url_arg)
    engine = create_engine(db_url)
    table_ref = f'"{db_schema}"."{db_table}"'
    query = text(
        f"""
        SELECT source, organization, device, alert_type, severity, timestamp
        FROM {table_ref}
        """
    )
    with engine.connect() as conn:
        return pd.read_sql_query(query, conn)


def main() -> None:
    project_root = Path(__file__).resolve().parent.parent
    load_dotenv(project_root / ".env")

    parser = argparse.ArgumentParser(description="Train pairwise incident-link model from weak labels.")
    parser.add_argument("--input", default="stitched_alerts_dedup.csv")
    parser.add_argument("--use-db", action="store_true", help="Load training data from PostgreSQL instead of CSV input")
    parser.add_argument("--db-url", default=None, help="Optional DB URL override; falls back to env vars/.env")
    parser.add_argument("--db-table", default="stitched_alerts_dedup", help="Source DB table when --use-db is set")
    parser.add_argument("--db-schema", default="public", help="Source DB schema when --use-db is set")
    parser.add_argument("--outdir", default="parser/models")
    parser.add_argument("--window-minutes", type=int, default=5)
    parser.add_argument("--max-dt-minutes", type=int, default=120)
    parser.add_argument("--threshold", type=float, default=0.65, help="Probability threshold for linking in inference.")
    parser.add_argument("--log-level", default="INFO", help="Logging level: DEBUG, INFO, WARNING, ERROR")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    in_path = Path(args.input)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    df = _load_training_data(
        use_db=args.use_db,
        input_path=in_path,
        db_url_arg=args.db_url,
        db_table=args.db_table,
        db_schema=args.db_schema,
    )
    source_desc = f"{args.db_schema}.{args.db_table}" if args.use_db else str(in_path)
    LOGGER.info("Loaded %s rows from %s", len(df), source_desc)
    df = normalize(df)

    weak_ids = heuristic_incident_id(df, window_minutes=args.window_minutes)
    LOGGER.info("Weak labels generated with window_minutes=%s", args.window_minutes)

    tfidf = TfidfVectorizer(ngram_range=(1, 2), min_df=1)
    tfidf.fit(df["alert_type"].astype(str).tolist())
    LOGGER.info("TF-IDF fitted on %s alert_type values", len(df))

    X, y = build_pair_dataset(df, weak_ids, tfidf, max_dt_minutes=args.max_dt_minutes)

    classes, counts = np.unique(y, return_counts=True)
    class_summary = {int(k): int(v) for k, v in zip(classes, counts)}
    LOGGER.info("Pair label distribution: %s", class_summary)
    if len(classes) < 2:
        raise ValueError(
            f"Training labels contain only one class: {classes.tolist()}. "
            "Adjust the input data or window settings."
        )

    stratify = y if counts.min() >= 2 else None
    if stratify is None:
        LOGGER.warning(
            "Smallest class has %s sample(s); falling back to non-stratified split.",
            int(counts.min()),
        )

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=stratify
    )
    LOGGER.info("Train/test split complete: train=%s, test=%s", len(X_train), len(X_test))

    model = RandomForestClassifier(
        n_estimators=300,
        max_depth=12,
        random_state=42,
        class_weight="balanced_subsample",
        n_jobs=-1,
    )
    model.fit(X_train, y_train)
    LOGGER.info("Model training complete")

    probs = model.predict_proba(X_test)[:, 1]
    preds = (probs >= args.threshold).astype(int)

    auc = roc_auc_score(y_test, probs)
    LOGGER.info("AUC: %.6f", auc)
    LOGGER.info("Classification report:\n%s", classification_report(y_test, preds, digits=4))

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

    LOGGER.info("Saved model artifacts to: %s", outdir)


if __name__ == "__main__":
    main()