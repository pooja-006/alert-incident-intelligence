from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from parser.pipeline_service import (
    append_deduped_to_postgres,
    build_db_url,
    dedupe_alerts,
    load_dotenv,
    parse_payload,
)


class IngestRequest(BaseModel):
    source: Literal["meraki", "auvik", "ncentral"]
    payload: Any = Field(..., description="Vendor payload: JSON object/list for meraki/auvik, XML string for ncentral")
    db_url: str | None = Field(default=None, description="Optional DB URL override")
    table: str = Field(default="stitched_alerts_dedup")
    target_schema: str = Field(default="public")


app = FastAPI(title="Alert Pipeline API", version="1.0.0")


@app.on_event("startup")
def startup_load_env() -> None:
    project_root = Path(__file__).resolve().parent.parent
    load_dotenv(project_root / ".env")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/ingest")
def ingest_alerts(req: IngestRequest) -> dict[str, Any]:
    try:
        parsed = parse_payload(req.source, req.payload)
        deduped_df = dedupe_alerts(parsed)
        db_url = build_db_url(req.db_url)
        inserted = append_deduped_to_postgres(
            deduped_df,
            db_url=db_url,
            table=req.table,
            schema=req.target_schema,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - safety net for API responses
        raise HTTPException(status_code=500, detail=f"Pipeline execution failed: {exc}") from exc

    return {
        "source": req.source,
        "received": len(parsed),
        "deduped_batch": int(len(deduped_df)),
        "inserted": inserted,
        "table": f"{req.target_schema}.{req.table}",
    }
