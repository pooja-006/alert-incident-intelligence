from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text

from parser.pipeline_service import (
    append_deduped_to_postgres,
    append_incident_tables,
    build_db_url,
    dedupe_alerts,
    load_dotenv,
    parse_payload,
)


class IngestRequest(BaseModel):
    source: Literal["meraki", "auvik", "ncentral"]
    payload: Any = Field(
        ...,
        description=(
            "Vendor payload batch: JSON object/list for meraki/auvik, "
            "XML string or list of XML strings for ncentral"
        ),
    )
    db_url: str | None = Field(default=None, description="Optional DB URL override")
    table: str = Field(default="stitched_alerts_dedup")
    target_schema: str = Field(default="public")


app = FastAPI(title="Alert Pipeline API", version="1.0.0")

# Enable CORS for frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup_load_env() -> None:
    project_root = Path(__file__).resolve().parent.parent
    load_dotenv(project_root / ".env")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/ingest")
def ingest_alerts(req: IngestRequest) -> dict[str, Any]:
    incident_alert_rows = 0
    incident_rows = 0
    try:
        # Backend pipeline stages: normalize vendor payload -> dedupe -> persist.
        parsed = parse_payload(req.source, req.payload)
        deduped_df = dedupe_alerts(parsed)
        db_url = build_db_url(req.db_url)
        inserted = append_deduped_to_postgres(
            deduped_df,
            db_url=db_url,
            table=req.table,
            schema=req.target_schema,
        )

        if req.table == "stitched_alerts_dedup":
            incident_alert_rows, incident_rows = append_incident_tables(
                deduped_df,
                db_url=db_url,
                schema=req.target_schema,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - safety net for API responses
        raise HTTPException(status_code=500, detail=f"Pipeline execution failed: {exc}") from exc

    return {
        "source": req.source,
        "pipeline": ["parse", "dedupe", "persist"],
        "received": len(parsed),
        "deduped_batch": int(len(deduped_df)),
        "inserted": inserted,
        "alerts_with_incident_upserts": incident_alert_rows,
        "incident_upserts": incident_rows,
        "table": f"{req.target_schema}.{req.table}",
    }


def fetch_alerts(
    *,
    db_url: str,
    table: str = "stitched_alerts_dedup",
    schema: str = "public",
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    if limit < 1:
        raise ValueError("limit must be >= 1")
    if offset < 0:
        raise ValueError("offset must be >= 0")

    engine = create_engine(db_url)
    table_ref = f'"{schema}"."{table}"'
    query = text(
        f"""
        SELECT source, organization, device, alert_type, severity, timestamp
        FROM {table_ref}
        ORDER BY timestamp DESC
        LIMIT :limit OFFSET :offset
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, {"limit": limit, "offset": offset}).mappings().all()
    return [dict(row) for row in rows]


def fetch_alerts_with_ml(
    *,
    db_url: str,
    schema: str = "public",
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    if limit < 1:
        raise ValueError("limit must be >= 1")
    if offset < 0:
        raise ValueError("offset must be >= 0")

    engine = create_engine(db_url)
    stitched_ref = f'"{schema}"."stitched_alerts_dedup"'
    alerts_ref = f'"{schema}"."alerts_with_incident"'
    incidents_ref = f'"{schema}"."incidents"'
    query = text(
        f"""
        SELECT
            s.source,
            s.organization,
            s.device,
            s.alert_type,
            s.severity,
            s.timestamp,
            a.incident_id,
            i.incident_type,
            i.status
        FROM {stitched_ref} s
        LEFT JOIN {alerts_ref} a
          ON s.source = a.source
         AND s.organization = a.organization
         AND s.device = a.device
         AND s.alert_type = a.alert_type
         AND s.severity = a.severity
         AND s.timestamp = a.timestamp
        LEFT JOIN {incidents_ref} i
          ON a.incident_id = i.incident_id
        ORDER BY s.timestamp DESC
        LIMIT :limit OFFSET :offset
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(query, {"limit": limit, "offset": offset}).mappings().all()
    return [dict(row) for row in rows]


def aggregate_counts(
    *,
    db_url: str,
    group_column: str,
    table: str = "stitched_alerts_dedup",
    schema: str = "public",
) -> dict[str, int]:
    engine = create_engine(db_url)
    table_ref = f'"{schema}"."{table}"'
    if group_column not in {"severity", "device"}:
        raise ValueError("Unsupported group column")

    query = text(
        f"""
        SELECT {group_column} AS key, COUNT(*) AS count
        FROM {table_ref}
        GROUP BY {group_column}
        ORDER BY count DESC
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(query).mappings().all()

    return {str(row["key"] or "unknown"): int(row["count"]) for row in rows}


@app.get("/alerts")
def list_alerts(limit: int = 100, offset: int = 0, table: str = "stitched_alerts_dedup", schema: str = "public", db_url: str | None = None) -> dict[str, Any]:
    try:
        url = build_db_url(db_url)
        records = fetch_alerts(db_url=url, table=table, schema=schema, limit=limit, offset=offset)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=f"Failed to fetch alerts: {exc}") from exc

    return {"count": len(records), "items": records}


@app.get("/alerts/ml")
def list_alerts_ml(limit: int = 100, offset: int = 0, schema: str = "public", db_url: str | None = None) -> dict[str, Any]:
    try:
        url = build_db_url(db_url)
        records = fetch_alerts_with_ml(db_url=url, schema=schema, limit=limit, offset=offset)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=f"Failed to fetch ML alerts: {exc}") from exc

    return {"count": len(records), "items": records}


@app.get("/alerts/severity")
def list_alerts_by_severity(table: str = "stitched_alerts_dedup", schema: str = "public", db_url: str | None = None) -> dict[str, int]:
    try:
        url = build_db_url(db_url)
        return aggregate_counts(db_url=url, group_column="severity", table=table, schema=schema)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=f"Failed to fetch severity counts: {exc}") from exc


@app.get("/alerts/device")
def list_alerts_by_device(table: str = "stitched_alerts_dedup", schema: str = "public", db_url: str | None = None) -> dict[str, int]:
    try:
        url = build_db_url(db_url)
        return aggregate_counts(db_url=url, group_column="device", table=table, schema=schema)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=f"Failed to fetch device counts: {exc}") from exc
