# RiskLens Issue Log

| #   | Description                                                  | Status   |
|-----|--------------------------------------------------------------|----------|
| I-1 | Git committer auto-derive warning                            | Resolved |
| I-2 | Gold `_archive_*` rollback tables left behind                | Resolved |
| I-3 | silver.positions / gold.trade_positions 0 rows               | Resolved |
| I-4 | catalog.schema_registry_s 0 rows                             | Resolved |
| I-5 | Non-bronze `_s` / `_r` orphan tables across 4 datasets       | Resolved |
| I-6 | AI Chat SLA query — hallucinated column names                | Resolved |
| I-7 | Lineage graph — bronze/silver/gold layer labels missing      | Resolved |
| I-8 | Lineage graph — faded/overlapping `aggregates` edge labels   | Resolved |
| I-9 | AI Chat — hallucinated `processed_at` partition column       | Resolved |
| I-10| Lineage graph 404 for catalog/lineage meta-tables            | Resolved |
| I-11| Duplicate rows in `risklens_catalog.assets` for 2 asset_ids  | Resolved |
| I-12| `silver_positions` missing from lineage graph (audit finding) | Resolved |

---

## I-10 — Lineage graph 404 for catalog/lineage meta-tables

**Symptom:** Clicking **Lineage** on a catalog-layer or lineage-layer asset (e.g. `quality_scores`) lands on `/lineage/{asset_id}` and the UI shows *"Node not found or no lineage data."* Confirmed live on `quality_scores`; same applies to `sla_status`, `ownership`, `schema_registry`, `access_log`, `desk_registry`, `lineage_nodes`, `lineage_edges` (8 affected assets).

**Root cause:** These 8 meta/housekeeping tables were registered in `risklens_catalog.assets` (in PR #32) so they'd appear in the Catalog tab and be embedded for RAG. But they do NOT exist in `risklens_lineage.nodes`, because they don't participate in the Bronze→Silver→Gold pipeline — they're written by `setup_bigquery.py` / `gold_aggregate.py` as governance sidecars, not by a pipeline job. The `/api/lineage/graph/{asset_id}` endpoint looks the seed up in `risklens_lineage.nodes` and raises 404 when not found.

**Fix options:**
- **A.** Hide the "Lineage" action on the catalog detail drawer when `asset.layer in ('catalog','lineage')` — cleanest, matches semantic reality.
- **B.** Return an empty-state graph from the API instead of 404, and have the frontend render a friendlier "This asset has no upstream pipeline" message — preserves the click path.
- **C.** Add synthetic lineage nodes for the meta-tables so the graph shows a single node and no edges — stretches the metaphor.

**Recommended:** A + B combined — hide the button for meta-layers and also return a clean empty-state so direct URL access doesn't 500.

---

## I-11 — Duplicate asset rows in `risklens_catalog.assets`

**Symptom:** `GET /api/assets` returns 31 rows but 2 asset_ids appear twice:

| asset_id              | layer  | name (row 1)                          | name (row 2)                                      |
|-----------------------|--------|---------------------------------------|---------------------------------------------------|
| `gold_rfet_results`   | gold   | Risk Factor Eligibility Test          | Risk Factor Eligibility Test (RFET)               |
| `silver_risk_enriched`| silver | Risk Outputs Enriched                 | Risk Outputs Enriched (with Market Data)          |

Distinct `name` / `domain` values → these are two separate INSERTs, not a join artifact.

**Root cause (likely):** PR #32's catalog seeding INSERTed new rows for existing `asset_id`s instead of MERGE-upsert. `risklens_catalog.assets` has no primary key / uniqueness constraint in BigQuery (BQ doesn't enforce them), so the duplicate survived.

**Fix:** One-shot de-dupe via `CREATE OR REPLACE TABLE ... AS SELECT ... QUALIFY ROW_NUMBER() OVER (PARTITION BY asset_id ORDER BY ...) = 1`, then patch the offending catalog-seeding SQL to use `MERGE` keyed on `asset_id`. Which duplicate to keep: prefer the newer PR #32 rows (richer names) — confirm before dropping.

---

## I-12 — `silver_positions` missing from lineage graph

**Symptom:** Found via a full catalog-vs-lineage audit (`curl /api/assets` × `curl /api/lineage/nodes`). Of 29 catalog assets: 20 pipeline assets have correct lineage nodes, 8 catalog/lineage meta-tables correctly return the I-10 META empty-state, and **1 outlier — `silver_positions` — returns 404** from `/api/lineage/graph/silver_positions?hops=2`.

**Root cause:** Two bugs, one in the lineage graph and one in the synthetic generator:
1. `ingestion/synthetic/generate.py:ASSETS` does not list `silver_positions`.
2. `ingestion/synthetic/generate.py:LINEAGE_EDGES` draws three **incorrect** direct edges `silver_trades / silver_prices / silver_rates → gold_trade_positions` (relationship `aggregates`, job `gold_aggregate_job`).

The actual code path, per `ingestion/jobs/silver_enrich.py:enrich_positions()` and `ingestion/jobs/gold_aggregate.py:build_trade_positions()`:
- `silver_trades × silver_prices × silver_rates → silver.positions` (silver_enrich_job)
- `silver.positions → gold.trade_positions` (gold_aggregate_job, **no join**, just a layer promotion per the comment: *"silver_enrich.py already joined trades × prices × rates into silver.positions, so this job just promotes the enriched silver table to gold"*)

So the synthetic lineage graph collapses a 2-hop path into 3 direct edges, hiding `silver_positions` entirely.

**Fix plan:**
1. Live BQ INSERT:
   - Add `silver_positions` row to `risklens_lineage.nodes` (type=table, layer=silver, domain=risk).
   - DELETE the 3 obsolete edges (`silver_trades/prices/rates → gold_trade_positions`).
   - INSERT 4 new edges: `silver_trades/prices/rates → silver_positions` (enriches, silver_enrich_job) + `silver_positions → gold_trade_positions` (feeds, gold_aggregate_job).
2. Patch `ingestion/synthetic/generate.py`:
   - Add `silver_positions` to ASSETS + ASSET_DESCRIPTIONS.
   - Replace the 3 obsolete LINEAGE_EDGES with the 4 new ones.
3. Optional (not required for the fix): add an edge story in `api/routers/lineage.py:EDGE_STORIES` for `silver_positions → gold_trade_positions` so users get a business-language click-through.

**Impact check:** `silver.positions` is currently 0 rows (known, see I-3) because upstream DTCC SDR returns 404. Lineage DAG accuracy is independent of row counts — fix is safe.
