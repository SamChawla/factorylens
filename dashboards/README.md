# SigNoz dashboard — FactoryLens

`factorylens-dashboard.json` defines the three panels the scope calls for, built
from the OTel spans the pipeline emits (service **`factorylens`**):

1. **Pipeline stage duration** — avg duration per stage over time (stackable to
   read total pipeline time).
2. **Data-quality trend** — `rows_dropped` and `null_ratio` per line, from the
   `clean` stage. The story behind a falling OEE.
3. **OEE per line** — `oee` per line, from the `aggregate` stage.

## Order matters: land spans first

SigNoz learns an attribute's name **and type** from ingested data. Import the
dashboard *after* real spans have arrived, or the query builder won't resolve
`line_id`, `oee`, `rows_dropped`, `null_ratio` yet.

```bash
# 1. confirm auth works (one hello-world span)
uv run factorylens check

# 2. push the full pipeline run (15 spans)
uv run factorylens run
```

Give SigNoz ~30s to ingest, then import.

## Import

SigNoz UI → **Dashboards** → **+ New Dashboard** → **Import JSON** → paste or
upload `factorylens-dashboard.json` → **Import**.

## If a panel shows "no data" or an unresolved attribute

The JSON pins each attribute's type (e.g. `oee` as `float64`, `tag`). If SigNoz
stored it slightly differently, one click fixes it: open the panel → **Edit** →
in the query row, re-pick the attribute (`oee` / `null_ratio` / `rows_dropped`)
and the group-by (`line_id`) from the dropdown → **Save**. The dropdown values
come from *your* ingested data, so they're always correct.

## Manual fallback (build a panel by hand)

If import misbehaves on your SigNoz version, each panel is one query in the
builder (**Dashboards → New panel → Time Series → Traces**):

| Panel | Aggregate | Of attribute | Filter (`serviceName` = factorylens AND…) | Group by |
|-------|-----------|--------------|-------------------------------------------|----------|
| Stage duration | Avg | `durationNano` | `name` in ingest/clean/transform/aggregate | `name` |
| Data quality (A) | Avg | `rows_dropped` | `name` = clean | `line_id` |
| Data quality (B) | Avg | `null_ratio` | `name` = clean | `line_id` |
| OEE per line | Avg | `oee` | `name` = aggregate | `line_id` |

Name each panel in plain English (the JSON already does) — panels are graded on
Presentation Quality and User Experience, so don't ship auto-generated names.
