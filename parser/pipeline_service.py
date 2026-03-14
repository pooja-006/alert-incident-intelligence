from __future__ import annotations

import os
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text

REQUIRED_COLUMNS = ["source", "organization", "device", "alert_type", "severity", "timestamp"]
VALID_SOURCES = {"meraki", "auvik", "ncentral"}
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def load_dotenv(dotenv_path: Path) -> None:
    if not dotenv_path.exists():
        return

    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        if line.startswith("export "):
            line = line[len("export "):].strip()

        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue

        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]

        os.environ.setdefault(key, value)


def normalize_postgres_url(db_url: str) -> str:
    if db_url.startswith("postgresql://"):
        return db_url.replace("postgresql://", "postgresql+psycopg://", 1)
    if db_url.startswith("postgres://"):
        return db_url.replace("postgres://", "postgresql+psycopg://", 1)
    return db_url


def build_db_url(cli_db_url: str | None = None) -> str:
    if cli_db_url:
        return normalize_postgres_url(cli_db_url)

    env_db_url = os.getenv("DATABASE_URL")
    if env_db_url:
        return normalize_postgres_url(env_db_url)

    host = os.getenv("PGHOST")
    port = os.getenv("PGPORT", "5432")
    database = os.getenv("PGDATABASE")
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")

    required = {
        "PGHOST": host,
        "PGDATABASE": database,
        "PGUSER": user,
        "PGPASSWORD": password,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise ValueError(
            "Database connection is not configured. Missing: "
            + ", ".join(missing)
            + ". Set db_url, DATABASE_URL, or PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD."
        )

    safe_password = quote_plus(password)
    return f"postgresql+psycopg://{user}:{safe_password}@{host}:{port}/{database}"


def _coerce_json_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        return [payload]
    if isinstance(payload, list):
        if not all(isinstance(item, dict) for item in payload):
            raise ValueError("JSON payload list must contain objects only.")
        return payload
    raise ValueError("JSON payload must be an object or list of objects.")


def parse_payload(source: str, payload: Any) -> list[dict[str, Any]]:
    source_key = source.strip().lower()
    if source_key not in VALID_SOURCES:
        raise ValueError(f"Unsupported source '{source}'. Expected one of: {sorted(VALID_SOURCES)}")

    alerts: list[dict[str, Any]] = []

    if source_key == "meraki":
        for alert in _coerce_json_records(payload):
            alerts.append(
                {
                    "source": "meraki",
                    "organization": alert.get("organizationName"),
                    "device": alert.get("deviceName"),
                    "alert_type": alert.get("alertType"),
                    "severity": alert.get("alertLevel"),
                    "timestamp": alert.get("occurredAt"),
                }
            )

    elif source_key == "auvik":
        for alert in _coerce_json_records(payload):
            alerts.append(
                {
                    "source": "auvik",
                    "organization": alert.get("companyName"),
                    "device": alert.get("entityName"),
                    "alert_type": alert.get("alertName"),
                    "severity": alert.get("alertSeverityString"),
                    "timestamp": alert.get("date"),
                }
            )

    elif source_key == "ncentral":
        if not isinstance(payload, str):
            raise ValueError("N-Central payload must be XML text.")

        xml_data = re.sub(r"<\?xml.*?\?>", "", payload)
        xml_data = "<root>" + xml_data + "</root>"

        root = ET.fromstring(xml_data)
        for n in root.findall("notification"):
            alerts.append(
                {
                    "source": "ncentral",
                    "organization": n.findtext("CustomerName"),
                    "device": n.findtext("DeviceName"),
                    "alert_type": n.findtext("AffectedService"),
                    "severity": n.findtext("QualitativeNewState"),
                    "timestamp": n.findtext("TimeOfStateChange"),
                }
            )

    return alerts


def dedupe_alerts(alerts: list[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(alerts)

    if df.empty:
        return pd.DataFrame(columns=REQUIRED_COLUMNS)

    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            df[col] = None
        df[col] = df[col].astype(str).str.strip()

    return df[REQUIRED_COLUMNS].drop_duplicates(subset=REQUIRED_COLUMNS, keep="first")


def _validate_identifier(identifier: str, field_name: str) -> str:
    if not IDENTIFIER_RE.match(identifier):
        raise ValueError(f"Invalid {field_name} '{identifier}'. Use letters, numbers, and underscore only.")
    return identifier


def append_deduped_to_postgres(
    df: pd.DataFrame,
    db_url: str,
    table: str = "stitched_alerts_dedup",
    schema: str = "public",
) -> int:
    table = _validate_identifier(table, "table")
    schema = _validate_identifier(schema, "schema")

    if df.empty:
        return 0

    engine = create_engine(db_url)
    table_ref = f'"{schema}"."{table}"'
    unique_idx = f"{table}_uniq_alert"

    create_schema_sql = text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    create_table_sql = text(
        f"""
        CREATE TABLE IF NOT EXISTS {table_ref} (
            source TEXT NOT NULL,
            organization TEXT NOT NULL,
            device TEXT NOT NULL,
            alert_type TEXT NOT NULL,
            severity TEXT NOT NULL,
            timestamp TEXT NOT NULL
        )
        """
    )
    create_idx_sql = text(
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS "{unique_idx}"
        ON {table_ref} (source, organization, device, alert_type, severity, timestamp)
        """
    )

    insert_sql = text(
        f"""
        INSERT INTO {table_ref} (source, organization, device, alert_type, severity, timestamp)
        VALUES (:source, :organization, :device, :alert_type, :severity, :timestamp)
        ON CONFLICT (source, organization, device, alert_type, severity, timestamp) DO NOTHING
        """
    )

    records = df.to_dict(orient="records")

    with engine.begin() as conn:
        conn.execute(create_schema_sql)
        conn.execute(create_table_sql)
        conn.execute(create_idx_sql)
        result = conn.execute(insert_sql, records)

    return int(result.rowcount or 0)
