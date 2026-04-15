# RiskLens Issue Log

Last updated: 2026-04-15 (all 9 issues resolved)
Source snapshot: `docs/table_stats.json` (collected 2026-04-15T05:13:21Z)

## Groups — final state

| Group | Issues | Category | PR | Status |
|-------|--------|----------|----|--------|
| **A** | I-6, I-9 | AI Chat schema hallucination | [#32](https://github.com/anand1012/risklens/pull/32) | ✅ merged `0c04bd0` |
| **B** | I-7, I-8 | Lineage graph display | [#30](https://github.com/anand1012/risklens/pull/30) | ✅ merged `685cb32` + `e20d25c` (frontend rebuild + redeploy still pending) |
| **C** | I-5 catalog/lineage/embeddings | Orphan table cleanup | [#31](https://github.com/anand1012/risklens/pull/31) | ✅ merged `d84cab3` |

Also: [PR #29](https://github.com/anand1012/risklens/pull/29) — issue log bootstrap + I-5 silver cleanup — merged earlier in the day as `899696b`.

## Resolved

### I-1 — Git committer auto-derive warning
**Resolved 2026-04-15.** Set globally: `user.name = "Anand Swaroop"`, `user.email = "anand1012@gmail.com"`. Email confirmed from prior `git log` entries.

### I-2 — Gold archive rollback tables
**Resolved 2026-04-15.** All 4 `_archive_*_20260415` tables dropped from `risklens_gold`. The 1.37M-row anomaly was explained as schema-granularity change (row-level by VaR method × scenarios → aggregated `desk × risk_class × liquidity_horizon`), not data loss. `var_outputs` was never in production scope.

### I-3 — silver.positions / gold.trade_positions 0 rows
**Resolved 2026-04-15 — NOT A BUG.** `bronze.prices` and `bronze.rates` only have data for 2026-04-14 (Yahoo Finance / FRED APIs unavailable in current test env). `silver_enrich.py:62-65` correctly skips writes on empty DataFrames. Documented in `docs/table_dataflow.html`.

### I-4 — catalog.schema_registry_s 0 rows
**Resolved 2026-04-15 — subsumed by I-5.** Same root cause as the other 12 non-bronze orphans.

### I-5 — Non-bronze `_s` / `_r` orphan tables
**Resolved 2026-04-15 via PR #29 (silver) + PR #31 (catalog/lineage/embeddings).** All 13 orphans archived-then-dropped with row counts verified identical. Naming convention (commits `4bdb048` + `70d1e5b`): `_r` = Real external, `_s` = Synthetic, **bronze only**. 6 active bronze staging tables left intact: `prices_r`, `rates_r`, `trades_r`, `risk_outputs_s`, `pipeline_logs_s`, `quarantine_r`.

| Dataset | Table | Rows | PR |
|---------|-------|------|-----|
| silver | `prices_r` | 1,103 | #29 |
| silver | `rates_r` | 588 | #29 |
| silver | `risk_outputs_s` | 325 | #29 |
| catalog | `assets_s` | 16 | #31 |
| catalog | `ownership_s` | 16 | #31 |
| catalog | `quality_scores_s` | 352 | #31 |
| catalog | `schema_registry_s` | 0 | #31 |
| catalog | `sla_status_s` | 352 | #31 |
| catalog | `access_log_r` | 132 | #31 |
| lineage | `edges_s` | 16 | #31 |
| lineage | `nodes_s` | 24 | #31 |
| embeddings | `chunks_s` | 40 | #31 |
| embeddings | `vectors_s` | 40 | #31 |

**13 rollback copies still on disk** (negligible size, can be dropped after verification window):
- `risklens_silver._archive_{prices_r,rates_r,risk_outputs_s}_20260415`
- `risklens_catalog._archive_{assets_s,ownership_s,quality_scores_s,schema_registry_s,sla_status_s,access_log_r}_20260415`
- `risklens_lineage._archive_{edges_s,nodes_s}_20260415`
- `risklens_embeddings._archive_{chunks_s,vectors_s}_20260415`

### I-6 — AI Chat SLA query hallucination
**Resolved 2026-04-15 via PR #32 (`0c04bd0`).** Root cause was asset-catalog coverage: `sla_status` and 10 other production tables were never registered in `risklens_catalog.assets`, so `indexing/chunker.py:_chunk_schema_registry()` never embedded their schemas. Claude had no schema context → hallucinated column names.

**Fix delivered:**
- 11 new rows in `risklens_catalog.assets` (sla_status, quality_scores, ownership, schema_registry, desk_registry, access_log, lineage_nodes, lineage_edges, silver_positions, gold_rfet_results, silver_risk_enriched)
- 119 new rows in `risklens_catalog.schema_registry` (100 whole-asset seeds + 19 partial-coverage fills) — all pulled from BQ `INFORMATION_SCHEMA.COLUMNS`, no guessing
- `api/rag/chain.py` system prompt expanded with per-table column hints and partition-column annotations for gold/silver
- `ingestion/jobs/gold_aggregate.py` patched to emit catalog rows for `sla_status` and `quality_scores` so future runs stay in sync
- RAG indexer re-run (`python -m indexing.run_indexing --project risklens-frtb-2026 --bucket risklens-frtb-2026-indexes --truncate`) → 109 chunks written to live `risklens_embeddings.{chunks,vectors}`

**Verification:** `SELECT COUNT(*) FROM risklens_embeddings.chunks WHERE asset_id='sla_status'` → 2 chunks. Re-ran the failing query *"Which assets have SLA breaches?"* → Claude generated correct SQL using real columns (`breach_flag`, `breach_duration_mins`) → returned 8 breached gold assets.

### I-7 — Lineage graph: bronze/silver/gold layer labels missing
**Resolved 2026-04-15 via PR #30 (`685cb32`).** Root cause: `ingestion/synthetic/generate.py:624` hardcoded `"layer": ""` in `gen_lineage()`, discarding the layer field from the `ASSETS` manifest. Fix: built `asset_layers = {a["asset_id"]: a["layer"] for a in ASSETS}` and used it when constructing the nodes DataFrame. Also fixed the `domain` field which had the same bug.

**Data repopulated via targeted `MERGE`** against `risklens_lineage.nodes` (20 rows updated) — no full pipeline refresh. Distribution:
- Before: `layer='' × 27, layer='silver' × 1`
- After: `bronze=4, silver=5, gold=11, ''=8` (the 8 empties are 3 source nodes + 5 pipeline nodes, which don't map to ASSETS and correctly remain blank)
- Domain: now `market_data=6, risk=7, regulatory=4, reference=3, frtb=8`

### I-8 — Lineage graph: faded/overlapping `aggregates` edge labels
**Resolved 2026-04-15 via PR #30 (`e20d25c`).** Data was clean (28 edges, zero orphans, verified). Root cause was in the frontend DAG layout — 16 `aggregates` edges converging on 4–5 gold nodes caused label collision and stroke bleed-through.

**Fix in `frontend/src/views/Lineage.tsx`:**
1. Color edges by relationship (aggregates=violet, enriches=emerald, transforms=blue, feeds=amber) so convergent labels are visually distinct
2. Widened dagre `ranksep` 120→180 and `nodesep` 60→90 for more routing space
3. Opaque label background + rounded `labelBgPadding` so any residual overlap stays individually legible
4. Story-less edges now use `strokeOpacity=0.85` instead of a dimmed color (fixes the "faded" complaint)

**⚠️ Deploy still pending:** Frontend wasn't rebuilt because `npm` was not installed in the subagent's environment. I-7 is visible immediately (backend `MERGE` already applied). I-8 becomes visible after a manual `cd frontend && npm run build` + redeploy to the ingress at http://34.102.203.211.

### I-9 — AI Chat hallucination on `processed_at` in gold
**Resolved 2026-04-15 via PR #32 (`0c04bd0`).** Same root cause class as I-6: incomplete RAG schema coverage meant Claude guessed a single partition column (`processed_at`) across a heterogeneous gold dataset. Fixed together with I-6 by the asset-catalog audit + schema_registry seeding + system-prompt partition-column annotations. Re-tested *"when was gold layer loaded last?"* — Claude now uses `sla_status.actual_refresh` / `expected_refresh` correctly.

## Notes

- **Cleanup pattern for I-5:** CTAS to `_archive_<table>_20260415` in the same dataset → verify row counts match source → drop original. 13 archive tables kept for short verification window on top of BQ's 7-day time travel.
- **AI Chat schema source:** confirmed to be RAG retrieval over `risklens_embeddings.chunks`, not live `INFORMATION_SCHEMA`. The fix for I-6/I-9 was therefore an audit + re-index, not a query-planner change.
- **Frontend rebuild + redeploy is the only remaining work item from today's sweep** — everything else is merged and live.
