# RiskLens Issue Log

Last updated: 2026-04-15
Source snapshot: `docs/table_stats.json` (collected 2026-04-15T05:13:21Z)

## Open (in progress)

### I-5 — Non-bronze `_s` / `_r` orphan tables
**Status:** silver ✅ cleaned; catalog / lineage / embeddings pending
**Category:** cleanup | **Severity:** low

Naming convention (per commits `4bdb048` + `70d1e5b`): `_r` = Real external, `_s` = Synthetic, **bronze layer only**. The refactor migrated catalog / lineage / embeddings / silver writes to non-suffixed targets but didn't drop the old tables.

**6 ACTIVE bronze tables — LEAVE ALONE:**
`bronze.prices_r`, `bronze.rates_r`, `bronze.trades_r`, `bronze.risk_outputs_s`, `bronze.pipeline_logs_s`, `bronze.quarantine_r`

**Cleanup approach:** archive-then-drop (PR #28 pattern), one dataset at a time, row-count verified before drop.

**13 ORPHAN tables:**

| Dataset | Table | Rows | Status |
|---------|-------|------|--------|
| silver | `prices_r` | 1,103 | ✅ archived + dropped 2026-04-15 |
| silver | `rates_r` | 588 | ✅ archived + dropped 2026-04-15 |
| silver | `risk_outputs_s` | 325 | ✅ archived + dropped 2026-04-15 |
| catalog | `assets_s` | 16 | pending |
| catalog | `ownership_s` | 16 | pending |
| catalog | `quality_scores_s` | 352 | pending |
| catalog | `schema_registry_s` | 0 | pending |
| catalog | `sla_status_s` | 352 | pending |
| catalog | `access_log_r` | 132 | pending |
| lineage | `edges_s` | 16 | pending |
| lineage | `nodes_s` | 24 | pending |
| embeddings | `chunks_s` | 40 | pending |
| embeddings | `vectors_s` | 40 | pending |

All 13 have **zero writers in code** and were modified in the same 2026-04-14 05:47–05:49 batch window as the gold orphans cleaned in PR #28.

**Silver rollback copies** (to be dropped once cleanup is fully verified):
- `risklens_silver._archive_prices_r_20260415` (1,103 rows)
- `risklens_silver._archive_rates_r_20260415` (588 rows)
- `risklens_silver._archive_risk_outputs_s_20260415` (325 rows)

## Resolved

### I-2 — Gold archive rollback tables
**Status:** ✅ resolved 2026-04-15
All 4 `_archive_*_20260415` tables dropped from `risklens_gold`. Zero code references any `_s` variant; the 1.37M-row anomaly was explained as schema-granularity change (method-level → aggregate), not data loss. `var_outputs` was never in production scope. BQ 7-day time travel covers any unexpected rollback need.

### I-1 — Git committer auto-derive warning
**Status:** ✅ resolved 2026-04-15
Set globally: `user.name = "Anand Swaroop"`, `user.email = "anand1012@gmail.com"`. Email confirmed from prior `git log` entries. Future commits will stop auto-deriving `anandswaroop@mac.mynetworksettings.com`.

### I-3 — silver.positions / gold.trade_positions 0 rows
**Status:** ✅ resolved 2026-04-15 — NOT A BUG
Root cause: `bronze.prices` and `bronze.rates` only have data for 2026-04-14 (Yahoo Finance / FRED APIs unavailable in current test env). `silver_enrich.py:62-65` correctly skips writes on empty DataFrames, so empty prices/rates → empty positions → empty trade_positions. This is documented in `docs/table_dataflow.html`. No fix needed unless we want synthetic historical backfill (optional, not in scope).

### I-4 — catalog.schema_registry_s 0 rows
**Status:** ✅ resolved 2026-04-15 — subsumed by I-5
Same root cause as the other 12 non-bronze orphans. Tracked under I-5 for cleanup.

## Notes

- Cleanup pattern for I-5: CTAS to `_archive_<table>_20260415` in the same dataset → verify row counts match source → drop original. Archive tables kept for 1 day as belt-and-suspenders on top of BQ's 7-day time travel.
