#!/usr/bin/env python3
"""
Nightly per-table snapshot copier for Quickbase table (Railway edition).

Rewritten from a Google Apps Script that used per-minute "ticks" and
persisted offset state to work around GAS's 6-minute execution limit.
Railway has no such limit, so this runs the whole job in one process
invocation, start to finish, then exits. Intended to be triggered by
Railway's Cron Job scheduler (or any external scheduler that runs
`python qb_snapshot_sync.py` on a schedule).

Copies (source field -> destination field), only when the value differs:
   13 -> 29
   18 -> 30
   19 -> 31

IMPORTANT: fields 13/18/19 are derived (summary/formula) fields computed
over tens of thousands of child records each. They are expensive for
Quickbase to compute. Do NOT batch-read many records' worth of these
fields in a single query -- that's what caused a production outage.
Records are read ONE AT A TIME with a delay between each (matching the
original script's approach), even though writes are still batched
(writes are cheap -- they're plain field sets, not derived calculations).

Env vars (see .env.example):
   QB_REALM, QB_USER_TOKEN, QB_TABLE_ID
   QB_RECORD_ID_FID, QB_KEY_FID, QB_FIELD_MAP (JSON, optional override)
   QB_ID_PAGE_SIZE, QB_UPSERT_BATCH_SIZE, QB_MAX_RETRIES
   QB_PER_RECORD_DELAY_MS   -- pacing delay between each expensive per-record read
   QB_MAX_RUNTIME_MINUTES   -- safety cap; 0 disables (not recommended)
   SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, EMAIL_FROM, EMAIL_TO
   DEBUG_HTTP (1/0)
"""

import json
import logging
import os
import random
import smtplib
import sys
import time
from email.mime.text import MIMEText
from typing import Any

import requests

# -------------------- Config --------------------

def _env_int(name: str, default: int) -> int:
    val = os.environ.get(name)
    return int(val) if val else default

def _env_bool(name: str, default: bool) -> bool:
    val = os.environ.get(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "on")

def _env_field_map(name: str, default: list[tuple[int, int]]) -> list[tuple[int, int]]:
    val = os.environ.get(name)
    if not val:
        return default
    parsed = json.loads(val)  # expects [[13,29],[18,30],[19,31]]
    return [(int(a), int(b)) for a, b in parsed]

CONFIG = {
    "REALM": os.environ.get("QB_REALM", "intuitcorp.quickbase.com"),
    "USER_TOKEN": os.environ.get("QB_USER_TOKEN", ""),
    "TABLE_ID": os.environ.get("QB_TABLE_ID", "bsnegk9r2"),

    "FIELD_MAP": _env_field_map("QB_FIELD_MAP", [(13, 29), (18, 30), (19, 31)]),
    "RECORD_ID_FID": _env_int("QB_RECORD_ID_FID", 3),
    "KEY_FID": _env_int("QB_KEY_FID", 8),

    # Paging for the cheap ID-only listing query (fine to be large -- it
    # selects only the record ID field, not the expensive derived ones).
    "ID_PAGE_SIZE": _env_int("QB_ID_PAGE_SIZE", 250),

    # Writes are batched (cheap: plain field sets, not derived calculations).
    "UPSERT_BATCH_SIZE": _env_int("QB_UPSERT_BATCH_SIZE", 50),
    "MAX_RETRIES": _env_int("QB_MAX_RETRIES", 4),

    # Reads of the derived fields happen ONE RECORD AT A TIME, paced by
    # this delay, to avoid overloading Quickbase's formula/summary engine.
    "PER_RECORD_DELAY_MS": _env_int("QB_PER_RECORD_DELAY_MS", 150),

    # Safety cap: if the run exceeds this many minutes, stop cleanly,
    # flush pending writes, and email a summary noting an early stop,
    # rather than running indefinitely. 0 disables the cap (not recommended
    # given tens of thousands of records at ~150ms/record).
    "MAX_RUNTIME_MINUTES": _env_int("QB_MAX_RUNTIME_MINUTES", 45),

    "USER_AGENT": "QB-Railway-Snapshot/1.0",
    "DEBUG_HTTP": _env_bool("DEBUG_HTTP", False),
    "MAX_DUMP_CHARS": 4000,

    "SMTP_HOST": os.environ.get("SMTP_HOST", ""),
    "SMTP_PORT": _env_int("SMTP_PORT", 587),
    "SMTP_USER": os.environ.get("SMTP_USER", ""),
    "SMTP_PASS": os.environ.get("SMTP_PASS", ""),
    "EMAIL_FROM": os.environ.get("EMAIL_FROM", ""),
    "EMAIL_TO": os.environ.get("EMAIL_TO", "gene_bixler1@intuit.com"),
    "EMAIL_ON_COMPLETE": _env_bool("EMAIL_ON_COMPLETE", True),
}

# -------------------- Logging --------------------

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(message)s",
)
logger = logging.getLogger("qb_snapshot")

def _log(level: str, msg: str, **kwargs):
    payload = {"level": level, "msg": msg, **kwargs}
    line = json.dumps(payload, default=str)
    if level == "ERROR":
        logger.error(line)
    elif level == "WARN":
        logger.warning(line)
    else:
        logger.info(line)

def info(msg, **kwargs):  _log("INFO", msg, **kwargs)
def warn(msg, **kwargs):  _log("WARN", msg, **kwargs)
def error(msg, **kwargs): _log("ERROR", msg, **kwargs)

# -------------------- Stats --------------------

def fresh_stats() -> dict[str, Any]:
    return {
        "found": 0,
        "processed": 0,
        "queued": 0,
        "updated": 0,
        "created": 0,
        "read_errors": 0,
        "write_errors": 0,
        "skipped_no_key": 0,
        "unchanged": 0,
        "samples": {"failed_writes": [], "diffs": []},
    }

def cap_push(arr: list, obj: Any, cap: int):
    if len(arr) < cap:
        arr.append(obj)

# -------------------- HTTP + retry --------------------

def redact_secrets(headers: dict) -> dict:
    h = dict(headers)
    if "Authorization" in h:
        h["Authorization"] = "QB-USER-TOKEN ***redacted***"
    return h

def safe_slice(s: str, n: int) -> str:
    if s is None:
        return ""
    s = str(s)
    return s if len(s) <= n else s[:n] + f" …[+{len(s) - n} chars]"

def backoff_with_jitter(attempt: int) -> float:
    base = 0.8 * (2 ** attempt)
    capped = min(base, 15.0)
    return random.uniform(0, capped)

def qb_fetch(method: str, path: str, body: dict | None = None) -> dict:
    url = f"https://api.quickbase.com{path}"
    headers = {
        "QB-Realm-Hostname": CONFIG["REALM"],
        "Authorization": f"QB-USER-TOKEN {CONFIG['USER_TOKEN']}",
        "Content-Type": "application/json",
        "User-Agent": CONFIG["USER_AGENT"],
    }
    payload = json.dumps(body) if body is not None else None
    max_retries = CONFIG["MAX_RETRIES"]
    attempt = 0
    last_err = None

    while attempt <= max_retries:
        try:
            if CONFIG["DEBUG_HTTP"]:
                info("HTTP request", method=method, url=url, attempt=attempt,
                     headers=redact_secrets(headers),
                     body=safe_slice(payload or "", CONFIG["MAX_DUMP_CHARS"]))

            resp = requests.request(method, url, headers=headers, data=payload, timeout=30)
            code = resp.status_code
            text = resp.text or ""
            req_id = resp.headers.get("x-request-id", "n/a")

            if CONFIG["DEBUG_HTTP"]:
                info("HTTP response", method=method, url=url, attempt=attempt, status=code,
                     request_id=req_id, body=safe_slice(text, CONFIG["MAX_DUMP_CHARS"]))

            if 200 <= code < 300:
                return json.loads(text) if text else {}

            retry_after = resp.headers.get("Retry-After")
            ra_seconds = float(retry_after) if retry_after and retry_after.isdigit() else None

            if code == 429 or 500 <= code <= 599:
                wait = ra_seconds if ra_seconds is not None else backoff_with_jitter(attempt)
                warn("Transient HTTP error; backing off", code=code, attempt=attempt,
                     wait_s=round(wait, 2), path=path, request_id=req_id)
                time.sleep(wait)
                attempt += 1
                continue

            snippet = safe_slice(text, 600)
            raise RuntimeError(f"Quickbase error {code} (req {req_id}): {snippet}")

        except (requests.RequestException, RuntimeError) as e:
            last_err = e
            if attempt >= max_retries:
                break
            wait = backoff_with_jitter(attempt)
            warn("Request failed; retrying", attempt=attempt, wait_s=round(wait, 2),
                 path=path, message=str(e))
            time.sleep(wait)
            attempt += 1

    raise RuntimeError(f"qb_fetch failed after retries: {last_err}")

# -------------------- Value helpers --------------------

def get_value(row: dict, fid: int) -> Any:
    """Tolerant of fid being a string or int in the response (fixes the
    silent-false-negative bug from the GAS version's strict `===` check)."""
    if not row:
        return None
    fields = row.get("fields")
    if isinstance(fields, list):
        for cell in fields:
            if cell and int(cell.get("fid", -1)) == int(fid):
                return cell.get("value")
    key = str(fid)
    if key in row:
        v = row[key]
        if isinstance(v, dict) and "value" in v:
            return v["value"]
        return v
    return None

def normalize(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, str):
        return v
    try:
        return json.dumps(v, sort_keys=True)
    except Exception:
        return str(v)

def values_equal(a: Any, b: Any) -> bool:
    return normalize(a) == normalize(b)

# -------------------- Paging + diffing --------------------

def unique_fids(fids: list[int]) -> list[int]:
    seen = []
    for f in fids:
        if f not in seen:
            seen.append(f)
    return seen

def fetch_record_id_page(table_id: str, skip: int, top: int) -> tuple[list[int], int | None]:
    """Cheap: selects only the record ID field. Returns (ids, total_records)."""
    body = {
        "from": table_id,
        "select": [CONFIG["RECORD_ID_FID"]],
        "sortBy": [{"fieldId": CONFIG["RECORD_ID_FID"], "order": "ASC"}],
        "options": {"skip": skip, "top": top},
    }
    resp = qb_fetch("POST", "/v1/records/query", body)
    ids = [get_value(row, CONFIG["RECORD_ID_FID"]) for row in resp.get("data", []) or []]
    total = (resp.get("metadata", {}) or {}).get("totalRecords")
    return [i for i in ids if i is not None], total

def fetch_single_record(table_id: str, recid: Any, select_fids: list[int]) -> dict | None:
    """Expensive: pulls the derived/summary fields for exactly one record."""
    body = {
        "from": table_id,
        "select": select_fids,
        "where": f"{{{CONFIG['RECORD_ID_FID']}.EX.'{recid}'}}",
        "options": {"skip": 0, "top": 1},
    }
    resp = qb_fetch("POST", "/v1/records/query", body)
    data = resp.get("data", []) or []
    return data[0] if data else None

def build_row_update_if_needed(record: dict, field_map: list[tuple[int, int]], stats: dict) -> list[dict] | None:
    recid = get_value(record, CONFIG["RECORD_ID_FID"])
    key = get_value(record, CONFIG["KEY_FID"])
    if key is None or key == "":
        warn("Missing key field; cannot upsert this row", recid=recid, key_fid=CONFIG["KEY_FID"])
        stats["skipped_no_key"] += 1
        return None

    cells = [{"fid": CONFIG["KEY_FID"], "value": key}]
    changed = False
    for src_fid, dst_fid in field_map:
        src = get_value(record, src_fid)
        dst = get_value(record, dst_fid)
        if not values_equal(src, dst):
            cells.append({"fid": dst_fid, "value": src})
            changed = True

    return cells if changed else None

# -------------------- Upsert --------------------

def upsert_batch(rows: list[list[dict]], stats: dict):
    if not rows:
        return
    records = []
    for cells in rows:
        obj = {}
        for c in cells:
            if not isinstance(c, dict) or "fid" not in c:
                continue
            if c["fid"] == CONFIG["RECORD_ID_FID"]:
                continue
            obj[str(c["fid"])] = {"value": c["value"]}
        records.append(obj)

    body = {"to": CONFIG["TABLE_ID"], "data": records}

    try:
        resp = qb_fetch("POST", "/v1/records", body)
        meta = resp.get("metadata", {}) or {}
        updated = len(meta.get("updatedRecordIds", []) or [])
        created = len(meta.get("createdRecordIds", []) or [])
        stats["updated"] += updated
        stats["created"] += created
        info("Upserted batch", batch_size=len(records), updated=updated, created=created)
    except Exception as e:
        stats["write_errors"] += 1
        cap_push(stats["samples"]["failed_writes"],
                  {"size": len(records), "first": records[0] if records else None, "error": str(e)}, 3)
        raise

# -------------------- Email --------------------

def send_summary_email(stats: dict, failed: bool = False, error_msg: str | None = None):
    if not CONFIG["SMTP_HOST"] or not CONFIG["EMAIL_FROM"]:
        warn("SMTP not configured; skipping email", host_set=bool(CONFIG["SMTP_HOST"]),
             from_set=bool(CONFIG["EMAIL_FROM"]))
        return

    status = "FAILED" if failed else "Updated"
    subject = f"QSP Table Sizes {status} — {time.strftime('%Y-%m-%d %H:%M:%S')}"

    rows = "".join(
        f"<tr><td><b>{k.replace('_', ' ').title()}</b></td><td>{v}</td></tr>"
        for k, v in stats.items() if k != "samples"
    )
    error_block = f"<p style='color:#b00'><b>Error:</b> {error_msg}</p>" if error_msg else ""

    html = f"""
    <div style="font-family:system-ui,Segoe UI,Arial,sans-serif">
      <h2 style="margin:0 0 10px">Nightly Snapshot Summary</h2>
      {error_block}
      <hr style="margin:12px 0">
      <table cellpadding="6" cellspacing="0" style="border-collapse:collapse">{rows}</table>
      <hr style="margin:12px 0">
      <div style="color:#666;margin-top:10px">QB-Railway-Snapshot v1</div>
    </div>"""

    msg = MIMEText(html, "html")
    msg["Subject"] = subject
    msg["From"] = CONFIG["EMAIL_FROM"]
    msg["To"] = CONFIG["EMAIL_TO"]

    with smtplib.SMTP(CONFIG["SMTP_HOST"], CONFIG["SMTP_PORT"]) as server:
        server.starttls()
        if CONFIG["SMTP_USER"]:
            server.login(CONFIG["SMTP_USER"], CONFIG["SMTP_PASS"])
        server.sendmail(CONFIG["EMAIL_FROM"], [CONFIG["EMAIL_TO"]], msg.as_string())

    info("Summary email sent", to=CONFIG["EMAIL_TO"])

# -------------------- Main run --------------------

def run():
    if not CONFIG["USER_TOKEN"]:
        raise RuntimeError("QB_USER_TOKEN is not set")

    select_fids = unique_fids(
        [CONFIG["RECORD_ID_FID"], CONFIG["KEY_FID"]] + [f for pair in CONFIG["FIELD_MAP"] for f in pair]
    )
    stats = fresh_stats()
    stats["stopped_early"] = False
    skip = 0
    pending: list[list[dict]] = []
    delay_s = CONFIG["PER_RECORD_DELAY_MS"] / 1000.0
    max_runtime_s = CONFIG["MAX_RUNTIME_MINUTES"] * 60 if CONFIG["MAX_RUNTIME_MINUTES"] > 0 else None
    started = time.monotonic()

    info("Starting run", table=CONFIG["TABLE_ID"], field_map=CONFIG["FIELD_MAP"],
         per_record_delay_ms=CONFIG["PER_RECORD_DELAY_MS"],
         max_runtime_minutes=CONFIG["MAX_RUNTIME_MINUTES"])

    stopped_early = False

    while True:
        ids, total = fetch_record_id_page(CONFIG["TABLE_ID"], skip, CONFIG["ID_PAGE_SIZE"])
        page_count = len(ids)
        if page_count == 0:
            break

        if skip == 0 and total is not None:
            est_minutes = round((total * delay_s) / 60, 1)
            info("Total records found", total_records=total,
                 estimated_read_minutes=est_minutes)

        stats["found"] += page_count

        for recid in ids:
            if max_runtime_s is not None and (time.monotonic() - started) > max_runtime_s:
                warn("Max runtime reached; stopping early and flushing pending writes",
                     elapsed_minutes=round((time.monotonic() - started) / 60, 1),
                     processed_so_far=stats["processed"])
                stopped_early = True
                break

            rec = None
            try:
                rec = fetch_single_record(CONFIG["TABLE_ID"], recid, select_fids)
            except Exception as e:
                stats["read_errors"] += 1
                warn("Read failed for record; skipping", recid=recid, error=str(e))

            if rec is None and stats["read_errors"] == 0:
                # query succeeded but returned nothing (record deleted mid-run, etc.)
                stats["read_errors"] += 1
                warn("Record missing from per-record query", recid=recid)

            stats["processed"] += 1

            if rec is not None:
                update = build_row_update_if_needed(rec, CONFIG["FIELD_MAP"], stats)
                if update:
                    cap_push(stats["samples"]["diffs"], {
                        "recid": get_value(rec, CONFIG["RECORD_ID_FID"]),
                        "key": get_value(rec, CONFIG["KEY_FID"]),
                        "set": [c for c in update if c["fid"] != CONFIG["KEY_FID"]],
                    }, 10)
                    stats["queued"] += 1
                    pending.append(update)
                else:
                    stats["unchanged"] += 1

            if len(pending) >= CONFIG["UPSERT_BATCH_SIZE"]:
                upsert_batch(pending, stats)
                pending = []

            if stats["processed"] % 250 == 0:
                info("Progress", processed=stats["processed"],
                     elapsed_minutes=round((time.monotonic() - started) / 60, 1))

            time.sleep(delay_s)

        if stopped_early:
            break

        skip += page_count
        if page_count < CONFIG["ID_PAGE_SIZE"]:
            break  # last page

    if pending:
        upsert_batch(pending, stats)

    stats["stopped_early"] = stopped_early
    info("Run complete", **{k: v for k, v in stats.items() if k != "samples"})
    return stats

def main():
    try:
        stats = run()
        if CONFIG["EMAIL_ON_COMPLETE"]:
            send_summary_email(stats)
    except Exception as e:
        error("Run failed", error=str(e))
        if CONFIG["EMAIL_ON_COMPLETE"]:
            try:
                send_summary_email(fresh_stats(), failed=True, error_msg=str(e))
            except Exception as email_err:
                error("Failed to send failure email", error=str(email_err))
        sys.exit(1)

if __name__ == "__main__":
    main()
