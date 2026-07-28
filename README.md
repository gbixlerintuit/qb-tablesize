# QB Snapshot Sync (Railway edition)

Nightly job that copies field values (13→29, 18→30, 19→31) within
Quickbase table `bsnegk9r2`, only where the destination differs from the
source, then emails a summary.

Rewritten from a Google Apps Script. Key differences from the old version:

- **No tick/resume state.** GAS had a 6-minute execution cap, so the old
  script sliced work across many scheduled "ticks" and persisted an
  offset. Railway runs to completion in one process, so that's gone —
  simpler, same result.
- **Reads are still one-record-at-a-time, paced by a delay** — same as
  the original GAS script, and deliberately kept that way. Fields
  13/18/19 are derived summary/formula fields computed over tens of
  thousands of child records each, and are expensive for Quickbase to
  calculate. Batch-reading many records' worth of these fields at once
  overloads Quickbase — this is what caused an outage during testing.
  Only the cheap record-ID listing query is paged; the actual field
  reads happen one at a time via `QB_PER_RECORD_DELAY_MS`.
- **Batched upserts** (default 50/batch, configurable) — safe to batch
  because writes are plain field sets, not derived calculations.
- **A runtime safety cap** (`QB_MAX_RUNTIME_MINUTES`, default 45
  minutes): if the run takes longer than this, it stops cleanly, flushes
  any pending writes, and emails a summary flagging the early stop,
  instead of running indefinitely.
- **Email via SMTP** instead of `MailApp` — point it at Gmail (with an
  app password), SendGrid, or whatever relay you've got.

### On re-tuning the pacing

If a run is still too aggressive for Quickbase, raise
`QB_PER_RECORD_DELAY_MS` (e.g. 300–500ms). If it's comfortably fast,
you can lower it. Watch the "Total records found" / "estimated_read_minutes"
log line at the start of a run (with `DEBUG_HTTP=1` or just the default
INFO logs) to get a sense of total runtime before it's fully committed,
and adjust `QB_MAX_RUNTIME_MINUTES` accordingly so a full run can
actually finish within the cap.

## Deploying to Railway

1. **Push this folder to a GitHub repo** (new or existing):
   ```
   git init
   git add .
   git commit -m "QB snapshot sync"
   git remote add origin https://github.com/<you>/<repo>.git
   git push -u origin main
   ```
2. **In Railway:** New Project → Deploy from GitHub repo → pick this repo.
3. **Set it up as a Cron Job**, not a always-on web service:
   - In the service settings, under "Cron Schedule," set something like
     `0 6 * * *` (6am UTC daily — adjust to whenever the old nightly
     trigger ran, converted to UTC).
   - Start command: `python qb_snapshot_sync.py`
   - Railway will run the script to completion on that schedule and stop;
     it does not need to stay running between executions.
4. **Set environment variables** (Service → Variables) — see
   `.env.example` for the full list. At minimum you need `QB_USER_TOKEN`,
   `SMTP_HOST`, `SMTP_USER`, `SMTP_PASS`, `EMAIL_FROM`.
5. **Test it manually first**: Railway lets you trigger a cron service's
   command on demand from the dashboard ("Run now" / redeploy), or you
   can run it locally:
   ```
   pip install -r requirements.txt
   export $(cat .env | xargs)   # after filling in .env from .env.example
   python qb_snapshot_sync.py
   ```

## Debugging

Set `DEBUG_HTTP=1` to log full request/response bodies (tokens are
redacted automatically) to stdout, which Railway captures in its logs
view — this replaces digging through Apps Script's Executions panel.
