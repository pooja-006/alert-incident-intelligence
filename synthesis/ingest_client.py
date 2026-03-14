"""HTTP client to push synthetic alerts to the backend ingest API."""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import random
import re
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from typing import Any, Callable, Mapping

import requests

VALID_SOURCES = {"meraki", "auvik", "ncentral"}
DEFAULT_INPUT_BY_SOURCE = {
    "meraki": Path(__file__).with_name("synthetic_meraki.json"),
    "auvik": Path(__file__).with_name("synthetic_auvik.json"),
    "ncentral": Path(__file__).with_name("synthetic_ncentral.xml"),
}
DEFAULT_SYNTHESIS_SCRIPT_BY_SOURCE = {
    "meraki": Path(__file__).with_name("synthesis_meraki.py"),
    "auvik": Path(__file__).with_name("synthesis_auvik.py"),
    "ncentral": Path(__file__).with_name("synthesis_ncentral.py"),
}


def _post_ingest(
    *,
    source: str,
    payload: Any,
    ingest_url: str,
    db_url: str | None,
    table: str,
    schema: str,
    timeout: float,
) -> dict[str, Any] | None:
    body = {
        "source": source,
        "payload": payload,
        "db_url": db_url,
        "table": table,
        "target_schema": schema,
    }
    resp = requests.post(ingest_url, json=body, timeout=timeout)
    resp.raise_for_status()
    if not resp.content:
        return None
    try:
        return resp.json()
    except ValueError:
        return None


def _load_payload_from_file(source: str, input_path: Path) -> tuple[Any, int]:
    source_key = source.strip().lower()
    if source_key not in VALID_SOURCES:
        raise ValueError(f"Unsupported source '{source}'. Expected one of: {sorted(VALID_SOURCES)}")

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    if source_key in {"meraki", "auvik"}:
        payload = json.loads(input_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            payload = [payload]
        if not isinstance(payload, list):
            raise ValueError(f"{source_key} input must be a JSON object or list of objects.")
        if not all(isinstance(item, dict) for item in payload):
            raise ValueError(f"{source_key} JSON list must contain objects only.")
        return payload, len(payload)

    xml_text = input_path.read_text(encoding="utf-8")
    return xml_text, xml_text.count("<notification>")


def ingest_records(
    source: str,
    records: list[Mapping[str, object]],
    *,
    ingest_url: str | None,
    db_url: str | None,
    table: str,
    schema: str,
    timeout: float = 10.0,
    continue_on_error: bool = False,
    payload_encoder: Callable[[Mapping[str, object]], object] | None = None,
) -> None:
    """Send a batch of records to the backend ingest endpoint."""
    if not ingest_url:
        return

    encoder = payload_encoder or (lambda rec: rec)
    encoded_records = [encoder(rec) for rec in records]
    source_key = source.strip().lower()

    if source_key == "ncentral":
        payload = "\n".join(str(item) for item in encoded_records)
    else:
        payload = encoded_records

    try:
        result = _post_ingest(
            source=source,
            payload=payload,
            ingest_url=ingest_url,
            db_url=db_url,
            table=table,
            schema=schema,
            timeout=timeout,
        )
    except Exception as exc:  # pragma: no cover - network dependent
        msg = f"Ingest failed for {source} batch: {exc}"
        if continue_on_error:
            print(msg)
            return
        raise RuntimeError(msg) from exc

    inserted = result.get("inserted") if isinstance(result, dict) else None
    print(
        f"Ingested {len(records)} {source} records to backend at {ingest_url}"
        + (f" (inserted={inserted})" if inserted is not None else "")
    )


def _run_synthesis_ingest_cycle(
    *,
    source: str,
    ingest_url: str,
    db_url: str | None,
    table: str,
    schema: str,
    timeout: float,
    samples: int,
    epochs: int,
    jitter_seconds: int,
    near_dup_window: int,
    continue_on_error: bool,
    seed: int | None,
    fallback_light_synth: bool,
) -> None:
    script_path = DEFAULT_SYNTHESIS_SCRIPT_BY_SOURCE[source]
    command = [
        sys.executable,
        str(script_path),
        "--samples",
        str(samples),
        "--epochs",
        str(epochs),
        "--jitter-seconds",
        str(jitter_seconds),
        "--near-dup-window",
        str(near_dup_window),
        "--ingest-url",
        ingest_url,
        "--ingest-table",
        table,
        "--ingest-schema",
        schema,
        "--ingest-timeout",
        str(timeout),
    ]

    if db_url:
        command.extend(["--ingest-db-url", db_url])
    if continue_on_error:
        command.append("--continue-on-ingest-error")
    if seed is not None:
        command.extend(["--seed", str(seed)])

    print(f"Running synthesis cycle for {source}: {' '.join(command)}")
    result = subprocess.run(command, check=False, cwd=script_path.parent, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    if result.returncode == 0:
        return

    missing_dep = "ModuleNotFoundError" in (result.stderr or "")
    if fallback_light_synth and missing_dep:
        print(
            "CTGAN synthesis dependency missing; switching to lightweight built-in synthesis for this cycle."
        )
        payload, count = _build_lightweight_payload(source, samples=samples, jitter_seconds=jitter_seconds)
        ingest_result = _post_ingest(
            source=source,
            payload=payload,
            ingest_url=ingest_url,
            db_url=db_url,
            table=table,
            schema=schema,
            timeout=timeout,
        )
        inserted = ingest_result.get("inserted") if isinstance(ingest_result, dict) else None
        print(
            f"Lightweight ingest for {source}: sent={count}"
            + (f" inserted={inserted}" if inserted is not None else "")
        )
        return

    raise subprocess.CalledProcessError(result.returncode, command)


def _iso_utc_at_offset(offset_seconds: int) -> str:
    dt = datetime.now(timezone.utc) + timedelta(seconds=offset_seconds)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _build_lightweight_payload(source: str, *, samples: int, jitter_seconds: int) -> tuple[Any, int]:
    source_key = source.strip().lower()
    template_path = DEFAULT_INPUT_BY_SOURCE[source_key]

    if source_key in {"meraki", "auvik"}:
        template_payload, _ = _load_payload_from_file(source_key, template_path)
        if not isinstance(template_payload, list) or not template_payload:
            raise ValueError(f"No template records found for {source_key} in {template_path}")

        out: list[dict[str, Any]] = []
        for i in range(samples):
            src = template_payload[i % len(template_payload)]
            rec = dict(src)
            offset = i + random.randint(-jitter_seconds, jitter_seconds)
            ts = _iso_utc_at_offset(offset)

            if source_key == "meraki":
                rec["occurredAt"] = ts
                rec["sentAt"] = ts
                rec["alertId"] = f"{rec.get('alertId', 'alert')}-{int(time.time())}-{i}"
            else:
                rec["date"] = ts
                rec["alertId"] = f"{rec.get('alertId', 'alert')}-{int(time.time())}-{i}"

            out.append(rec)

        return out, len(out)

    xml_text, _ = _load_payload_from_file(source_key, template_path)
    if not isinstance(xml_text, str) or not xml_text.strip():
        raise ValueError(f"No template XML found for {source_key} in {template_path}")

    sanitized = re.sub(r"<\?xml.*?\?>", "", xml_text)
    wrapped = f"<root>{sanitized}</root>"
    root = ET.fromstring(wrapped)
    notifications = root.findall("notification")
    if not notifications:
        raise ValueError(f"No <notification> nodes found in {template_path}")

    parts: list[str] = []
    now_epoch = int(time.time())
    for i in range(samples):
        src = notifications[i % len(notifications)]
        node = deepcopy(src)
        offset = i + random.randint(-jitter_seconds, jitter_seconds)
        ts = _iso_utc_at_offset(offset)

        time_node = node.find("TimeOfStateChange")
        if time_node is None:
            time_node = ET.SubElement(node, "TimeOfStateChange")
        time_node.text = ts

        trigger_node = node.find("ActiveNotificationTriggerID")
        if trigger_node is None:
            trigger_node = ET.SubElement(node, "ActiveNotificationTriggerID")
        trigger_node.text = str(now_epoch + i)

        parts.append(ET.tostring(node, encoding="unicode"))

    return "\n".join(parts), len(parts)


def cli() -> None:
    parser = argparse.ArgumentParser(description="Send synthetic alert file to backend ingest pipeline.")
    parser.add_argument("--source", choices=sorted(VALID_SOURCES), help="Vendor source for one-shot file ingest")
    parser.add_argument(
        "--sources",
        default="",
        help="Comma-separated sources for interval mode, e.g. meraki,auvik,ncentral",
    )
    parser.add_argument("--input", type=Path, default=None, help="Path to synthetic input file")
    parser.add_argument("--ingest-url", default="http://localhost:8000/ingest", help="Backend ingest endpoint")
    parser.add_argument("--db-url", default=None, help="Optional DB URL override passed to backend")
    parser.add_argument("--table", default="stitched_alerts_dedup", help="Destination table")
    parser.add_argument("--schema", default="public", help="Destination schema")
    parser.add_argument("--timeout", type=float, default=10.0, help="HTTP timeout in seconds")
    parser.add_argument("--interval-seconds", type=float, default=0.0, help="If >0, run synthesis+ingest loop at this interval")
    parser.add_argument("--cycles", type=int, default=0, help="Number of interval cycles (0 means run forever)")
    parser.add_argument("--samples", type=int, default=100, help="Synthetic records per source per cycle")
    parser.add_argument("--epochs", type=int, default=20, help="CTGAN epochs per cycle")
    parser.add_argument("--jitter-seconds", type=int, default=900, help="Timestamp jitter for synthetic generation")
    parser.add_argument("--near-dup-window", type=int, default=300, help="Near-duplicate drop window in seconds")
    parser.add_argument("--continue-on-error", action="store_true", help="Continue interval loop when a source fails")
    parser.add_argument("--seed", type=int, default=None, help="Optional seed passed to synth generators")
    parser.add_argument(
        "--random-synthesizer",
        action="store_true",
        help="Pick one random source each cycle instead of running all selected sources",
    )
    parser.add_argument(
        "--min-alerts-per-minute",
        type=int,
        default=2,
        help="Lower bound for per-minute alert rate in random synthesizer mode",
    )
    parser.add_argument(
        "--max-alerts-per-minute",
        type=int,
        default=3,
        help="Upper bound for per-minute alert rate in random synthesizer mode",
    )
    parser.add_argument(
        "--fallback-light-synth",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="If CTGAN deps are missing, fallback to built-in timestamp-jitter synthesis",
    )
    args = parser.parse_args()

    if args.interval_seconds > 0:
        if args.min_alerts_per_minute < 1 or args.max_alerts_per_minute < 1:
            raise ValueError("--min-alerts-per-minute and --max-alerts-per-minute must be >= 1")
        if args.min_alerts_per_minute > args.max_alerts_per_minute:
            raise ValueError("--min-alerts-per-minute cannot be greater than --max-alerts-per-minute")

        selected = [s.strip().lower() for s in args.sources.split(",") if s.strip()]
        if not selected:
            selected = ["meraki", "auvik", "ncentral"]
        invalid = [s for s in selected if s not in VALID_SOURCES]
        if invalid:
            raise ValueError(f"Invalid source(s) in --sources: {invalid}. Allowed: {sorted(VALID_SOURCES)}")

        cycle = 0
        while True:
            cycle += 1
            print(f"\n=== interval cycle {cycle} started ===")

            if args.random_synthesizer:
                source_pool = [random.choice(selected)]
                target_per_minute = random.randint(args.min_alerts_per_minute, args.max_alerts_per_minute)
                samples_this_cycle = max(1, int(round(target_per_minute * args.interval_seconds / 60.0)))
                print(
                    f"Random synthesizer: source={source_pool[0]} target_per_minute={target_per_minute} "
                    f"samples_this_cycle={samples_this_cycle}"
                )
            else:
                source_pool = selected
                samples_this_cycle = args.samples

            for source in source_pool:
                try:
                    _run_synthesis_ingest_cycle(
                        source=source,
                        ingest_url=args.ingest_url,
                        db_url=args.db_url,
                        table=args.table,
                        schema=args.schema,
                        timeout=args.timeout,
                        samples=samples_this_cycle,
                        epochs=args.epochs,
                        jitter_seconds=args.jitter_seconds,
                        near_dup_window=args.near_dup_window,
                        continue_on_error=args.continue_on_error,
                        seed=args.seed,
                        fallback_light_synth=args.fallback_light_synth,
                    )
                except Exception as exc:
                    msg = f"Cycle {cycle} failed for source '{source}': {exc}"
                    if args.continue_on_error:
                        print(msg)
                        continue
                    raise RuntimeError(msg) from exc

            print(f"=== interval cycle {cycle} complete ===")
            if args.cycles > 0 and cycle >= args.cycles:
                print("Reached requested cycle count. Exiting interval mode.")
                return
            time.sleep(args.interval_seconds)

    if not args.source:
        raise ValueError("--source is required for one-shot file ingest mode (or use --interval-seconds for loop mode)")

    input_path = args.input or DEFAULT_INPUT_BY_SOURCE[args.source]
    payload, record_count = _load_payload_from_file(args.source, input_path)

    result = _post_ingest(
        source=args.source,
        payload=payload,
        ingest_url=args.ingest_url,
        db_url=args.db_url,
        table=args.table,
        schema=args.schema,
        timeout=args.timeout,
    )

    received = result.get("received") if isinstance(result, dict) else None
    inserted = result.get("inserted") if isinstance(result, dict) else None
    print(
        f"Sent {record_count} {args.source} records from {input_path} to {args.ingest_url}"
        + (f" | backend_received={received}" if received is not None else "")
        + (f" | inserted={inserted}" if inserted is not None else "")
    )


if __name__ == "__main__":
    cli()
