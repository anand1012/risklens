# RiskLens Issue Log

Last updated: 2026-04-15
Source snapshot: `docs/table_stats.json` (collected 2026-04-15T05:13:21Z)

## Open (in progress)

### I-6 — AI Chat SLA query: schema error on `sla_status` column
**Status:** investigated, fix pending approval | **Category:** bug | **Severity:** medium
**Discovered:** 2026-04-15 during AI Chat spot-check after silver cleanup

AI Chat response to *"Which assets have SLA breaches?"*:
> "Unfortunately, the live query hit a schema error (`sla_status` column not found in `risklens_catalog`), so I can't pull current breach counts from BigQuery right now."

**Root cause:** `sla_status` is listed as a table name in the AI Chat system prompt (`api/rag/chain.py:76`), but is **never registered as an asset in `risklens_catalog.assets`** (0 matching rows). As a result, `indexing/chunker.py:92` `_chunk_schema_registry()` never embeds a schema chunk for it into `risklens_embeddings.chunks`. With no schema context, Claude **hallucinates** column names — inventing `asset_name`, `breach_reason`, `breach_duration_min`, `last_checked`, and `sla_status` (as a column) — instead of the real ones from `setup_bigquery.py:432-439`: `asset_id`, `expected_refresh`, `actual_refresh`, `breach_flag`, `breach_duration_mins`, `checked_at`.

Failed hallucinated queries confirmed in `INFORMATION_SCHEMA.JOBS` from 2026-04-14 22:03Z onwards.

**Evidence from subagent investigation:**
- `api/rag/chain.py:76` — system prompt lists `sla_status` as a table but exposes no column info
- `indexing/chunker.py:92-142` — RAG schema chunker only indexes tables registered in `catalog.assets`
- `ingestion/jobs/gold_aggregate.py:143-147` — `update_sla_status()` writes the table but never calls `update_asset_catalog()` unlike other gold outputs
- `SELECT asset_id FROM catalog.assets WHERE asset_id LIKE '%sla%'` → 0 rows
- `SELECT COUNT(*) FROM embeddings.chunks WHERE asset_id = 'sla_status'` → 0 chunks

**Fix (not applied):**
1. Insert `sla_status` row into `risklens_catalog.assets` with domain=regulatory, layer=catalog
2. Seed `risklens_catalog.schema_registry` with its 6 real columns + descriptions
3. Re-run the RAG indexer so `risklens_embeddings.chunks` gets a schema chunk for `sla_status`
4. Re-test the SLA query in AI Chat

**Unblocks:** Same root-cause class as I-9; both should be fixed together by auditing every gold/catalog table's registration in `catalog.assets`.

**BLOCKS I-5 catalog batch** — must not drop catalog `_s` tables until this fix lands and we confirm nothing downstream implicitly depends on the current state.

### I-7 — Lineage graph: boxes don't show bronze / silver / gold layer labels
**Status:** investigated, fix pending approval | **Category:** ui | **Severity:** low
**Discovered:** 2026-04-15

**Root cause:** `ingestion/synthetic/generate.py:624` hardcodes `"layer": ""` (empty string) for every row in `gen_lineage()`. The `ASSETS` manifest (lines 62-87) has correct layer values (`bronze` / `silver` / `gold` / `reference`), but line 162 builds node tuples as `(asset_id, name, "table")` and discards the layer field. Only `silver_risk_enriched` renders correctly because a different code path populated that single row with `layer='silver', domain='risk'`.

Frontend at `frontend/src/views/Lineage.tsx:66-71` displays `node.layer` unconditionally — empty string → label vanishes.

**Fix (not applied):** In `gen_lineage()`, build `asset_layers = {a["asset_id"]: a["layer"] for a in ASSETS}` and use it when constructing the DataFrame so every table-type node inherits its correct layer. Then re-run the catalog bootstrap to repopulate `risklens_lineage.nodes`.

### I-8 — Lineage graph: "aggregates" edge labels appear faded / overlapping
**Status:** partial investigation | **Category:** ui | **Severity:** low
**Discovered:** 2026-04-15

Zoomed-in lineage view shows multiple `aggregates` labels rendered in dim / faded text while `enriches` and `transforms` labels render bright.

**Data state (verified via bq query):**
- `risklens_lineage.edges`: 28 rows, zero orphans (all `from_node_id` / `to_node_id` resolve to rows in `nodes`)
- Relationship counts: `aggregates=16`, `enriches=5`, `feeds=3`, `transforms=4`

**Probable cause:** 16 aggregates edges converging on a small number of gold nodes → edge labels overlap in the DAG layout, and whichever labels get z-ordered behind others look faded. Likely a frontend layout issue (label collision / offset), not a data bug.

**Next step:** inspect `frontend/src/views/Lineage.tsx` edge-label rendering (z-order, offset collision, opacity rules). No fix attempted yet.

### I-9 — AI Chat: schema hallucination on `processed_at` in gold tables (same class as I-6)
**Status:** open (same root cause class as I-6) | **Category:** bug | **Severity:** medium
**Discovered:** 2026-04-15

AI Chat response to *"when was gold layer loaded last?"*:
> "The query encountered a schema error — the `processed_at` column doesn't exist under that name in the gold layer tables queried."

`processed_at` is a real column on `gold.trade_positions` (partition field) but does NOT exist on most other gold tables, which use `calc_date` as the partition field. Claude is guessing a single column name across a heterogeneous gold dataset.

**Why this happens:** Same root-cause class as I-6. RAG retrieval either (a) has no per-table schema chunks for the gold tables, or (b) has chunks but Claude's reasoning across them is unreliable when different gold tables have different time columns. The chat's schema knowledge comes from `risklens_embeddings.chunks` (RAG), not a live `INFORMATION_SCHEMA` query.

**Fix (not applied) — options:**
1. **Ensure gold-table schemas are fully indexed** in `catalog.assets` + `catalog.schema_registry` + RAG, like the I-6 fix for `sla_status`. Audit all 8 gold tables for asset/schema registration.
2. **Add a "last refresh" helper** — a view or function `gold_last_loaded` that abstracts over the per-table time column, so the chat doesn't need to know whether a table uses `calc_date`, `processed_at`, or `rfet_date`.
3. **Tighten the system prompt** in `api/rag/chain.py` to list the time column per table.

Recommend fix #1 + #2 together for robustness.

### I-5 — Non-bronze `_s` / `_r` orphan tables
**Status:** silver ✅ cleaned; catalog / lineage / embeddings **PAUSED pending I-6 / I-9 fix**
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
- **AI Chat schema source confirmed (I-6 subagent):** chat uses RAG retrieval over `risklens_embeddings.chunks`, NOT live `INFORMATION_SCHEMA` queries. Implication: dropping the orphan `embeddings.chunks_s` / `vectors_s` is safe for the chat (they're not the live embeddings), but **live chunks/vectors must stay intact**.
- **I-6 and I-9 are the same bug class:** AI Chat hallucinates column names because RAG schema coverage is incomplete. The fix for both is to audit every table in `catalog.assets` + `catalog.schema_registry` and re-run the indexer.
