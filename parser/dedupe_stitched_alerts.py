from pathlib import Path
import argparse
import pandas as pd


def dedupe_alerts(input_csv: Path, output_csv: Path) -> None:
    df = pd.read_csv(input_csv)

    # Normalize whitespace so semantically identical rows compare the same.
    for col in ["source", "organization", "device", "alert_type", "severity", "timestamp"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    deduped = df.drop_duplicates(
        subset=["source", "organization", "device", "alert_type", "severity", "timestamp"],
        keep="first",
    )

    deduped.to_csv(output_csv, index=False)

    removed = len(df) - len(deduped)
    print(f"Input rows: {len(df)}")
    print(f"Output rows: {len(deduped)}")
    print(f"Duplicates removed: {removed}")
    print(f"Saved: {output_csv}")


def main() -> None:
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent

    parser = argparse.ArgumentParser(description="Remove duplicate rows from stitched alerts CSV.")
    parser.add_argument(
        "--input",
        type=Path,
        default=project_root / "stitched_alerts.csv",
        help="Path to input stitched CSV (default: project-root/stitched_alerts.csv)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=project_root / "stitched_alerts_dedup.csv",
        help="Path to output deduplicated CSV (default: project-root/stitched_alerts_dedup.csv)",
    )

    args = parser.parse_args()
    dedupe_alerts(args.input, args.output)


if __name__ == "__main__":
    main()
