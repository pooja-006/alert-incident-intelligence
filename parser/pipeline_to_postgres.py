from __future__ import annotations

import argparse
from pathlib import Path
import subprocess
import sys

import pandas as pd
from sqlalchemy import create_engine
from parser.pipeline_service import (
    build_db_url,
    load_dotenv,
)


def run_script(script_path: Path, args: list[str], cwd: Path) -> None:
    command = [sys.executable, str(script_path), *args]
    print(f"Running: {' '.join(command)}")
    subprocess.run(command, check=True, cwd=cwd)


def load_to_postgres(csv_path: Path, db_url: str, table: str, schema: str | None, if_exists: str) -> None:
    df = pd.read_csv(csv_path)

    engine = create_engine(db_url)
    df.to_sql(
        name=table,
        con=engine,
        schema=schema,
        if_exists=if_exists,
        index=False,
        chunksize=1000,
        method="multi",
    )

    destination = f"{schema}.{table}" if schema else table
    print(f"Loaded {len(df)} rows into {destination}")


def main() -> None:
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    load_dotenv(project_root / ".env")

    parse_script = script_dir / "parse_alerts.py"
    dedupe_script = script_dir / "dedupe_stitched_alerts.py"

    stitched_csv = project_root / "stitched_alerts.csv"
    dedup_csv = project_root / "stitched_alerts_dedup.csv"

    parser = argparse.ArgumentParser(
        description="Run parse + dedupe pipeline and load deduplicated alerts into PostgreSQL."
    )
    parser.add_argument("--db-url", default=None, help="SQLAlchemy URL, e.g. postgresql+psycopg://user:pass@host:5432/db")
    parser.add_argument("--table", default="stitched_alerts_dedup", help="Destination table name")
    parser.add_argument("--schema", default=None, help="Destination schema")
    parser.add_argument(
        "--if-exists",
        default="replace",
        choices=["fail", "replace", "append"],
        help="Behavior when destination table already exists",
    )
    parser.add_argument("--skip-parse", action="store_true", help="Skip parse step")
    parser.add_argument("--skip-dedupe", action="store_true", help="Skip dedupe step")

    args = parser.parse_args()

    if not args.skip_parse:
        run_script(parse_script, [], project_root)

    if not args.skip_dedupe:
        run_script(
            dedupe_script,
            ["--input", str(stitched_csv), "--output", str(dedup_csv)],
            project_root,
        )

    db_url = build_db_url(args.db_url)
    load_to_postgres(
        csv_path=dedup_csv,
        db_url=db_url,
        table=args.table,
        schema=args.schema,
        if_exists=args.if_exists,
    )


if __name__ == "__main__":
    main()
