# FactoryLens alert rules

Two SigNoz alert rules, so the project uses SigNoz's **alerting** surface — not
just traces, metrics, logs and dashboards. Both are **trace-based**,
so they need no pre-ingested metric and fire from the exact spans the pipeline
already emits.

| File | Fires when | Signal |
|------|-----------|--------|
| [`oee-below-floor.json`](oee-below-floor.json) | avg `oee` on any line's `aggregate` span drops below **0.65** | the headline number going bad |
| [`line-silent.json`](line-silent.json) | any `alert` span with `alert.kind = line_silent` appears | the streaming watchdog catching a dead line |

The OEE rule is the demo's punchline in alert form: line_3 sits at 0.58, so it
fires, and `factorylens ask "why is line_3's OEE low?"` answers from the same
spans the alert watched. The silence rule turns the piece-6 watchdog into a
SigNoz alert you can actually be paged on.

## Applying them

These are the exact bodies of `POST /api/v1/rules` (SigNoz v5 rule schema,
verified against v0.134.0-cloud). `preferredChannels` is left empty — set it to
your own notification channel name(s), or wire a channel in the UI after import.

**Via the management API** (Cloud or self-hosted):

```bash
BASE=https://<tenant>.<region>.signoz.cloud     # or http://localhost:8080 self-hosted
curl -s -X POST "$BASE/api/v1/rules" \
  -H "SIGNOZ-API-KEY: $SIGNOZ_API_KEY" -H 'Content-Type: application/json' \
  -d @alerts/oee-below-floor.json
curl -s -X POST "$BASE/api/v1/rules" \
  -H "SIGNOZ-API-KEY: $SIGNOZ_API_KEY" -H 'Content-Type: application/json' \
  -d @alerts/line-silent.json
```

**Via the UI:** Alerts → New Alert → paste the query/threshold. The JSON here is
the source of truth for the exact condition (avg oee < 0.65; count of
line_silent alerts > 0).

## Notes for the reader

- Both were created live and confirmed listed under Alerts on the project's
  workspace — not hand-written specs.
- `matchType: "1"` = *at least once* in the window; `op: "2"` = *below*,
  `op: "1"` = *above* (SigNoz's `CompareOperator` enum).
- A metric-based lag alert (`factorylens.ingest.lag_ms` p95) was intentionally
  left out of the committed set: a metric alert only validates once the metric
  has been ingested, so it can't be applied to a fresh workspace before a
  `factorylens stream` run. The two trace-based rules have no such dependency.
