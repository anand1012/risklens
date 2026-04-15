"""
Lineage router — data flow graph endpoints.

GET /lineage/nodes                   all lineage nodes
GET /lineage/graph/{asset_id}        subgraph centered on an asset (N hops upstream+downstream)
"""

from fastapi import APIRouter, Query

from api.db.bigquery import query_rows, project

router = APIRouter(prefix="/lineage", tags=["lineage"])

# ---------------------------------------------------------------------------
# Business transformation stories per edge (from_node_id, to_node_id)
# Written for a business / risk officer audience — not SQL, not tech jargon.
# ---------------------------------------------------------------------------
EDGE_STORIES: dict[tuple[str, str], dict] = {
    ("fred_api_source", "bronze_fred_rates"): {
        "title": "FRED API → Raw Rates Ingestion",
        "what": "Daily pull of macroeconomic time-series from the Federal Reserve Economic Data (FRED) REST API.",
        "business_impact": "Captures the four key FRTB rate inputs — Fed Funds rate, 10Y Treasury yield, HY credit spread, and EUR/USD FX — as published by the Fed. No transformations applied; data is immutable at this stage.",
        "frequency": "Daily at 18:00 ET after US market close",
        "owner": "Regulatory Reporting team",
    },
    ("dtcc_api_source", "bronze_dtcc_trades"): {
        "title": "DTCC SDR → Raw Trade Ingestion",
        "what": "Full OTC derivatives trade file ingested from DTCC Swap Data Repository covering all publicly reported trades.",
        "business_impact": "Creates the immutable raw trade log. Every downstream risk calculation traces back to this table. UTIs (Unique Trade Identifiers) are preserved exactly as reported.",
        "frequency": "Daily at 08:00 ET (T+1 settlement)",
        "owner": "Market Risk Data team",
    },
    ("yahoo_api_source", "bronze_yahoo_prices"): {
        "title": "Yahoo Finance → Raw Prices Ingestion",
        "what": "Daily OHLCV price data fetched from Yahoo Finance for all instruments in the trading book.",
        "business_impact": "Provides the market price series used for P&L attribution and VaR sensitivity calculations. Raw prices are unvalidated at this stage.",
        "frequency": "Daily at 17:30 ET",
        "owner": "Market Risk Data team",
    },
    ("bronze_fred_rates", "silver_rates"): {
        "title": "Raw Rates → Cleaned Rate Series",
        "what": "Outlier detection, gap-filling, and normalisation applied to the raw FRED rate series.",
        "business_impact": "3σ outlier rule removes anomalous spikes that would distort VaR calculations. Weekend and holiday gaps are forward-filled so the series aligns to the FRTB 250-day trading calendar. Yield curve monotonicity is enforced to prevent arbitrage inconsistencies in downstream pricing.",
        "frequency": "Runs immediately after bronze ingestion completes",
        "owner": "Market Risk Data team",
    },
    ("bronze_dtcc_trades", "silver_trades"): {
        "title": "Raw Trades → Validated Trade Book",
        "what": "Null-backfill, duplicate removal, FX normalisation, and FRTB risk classification applied to raw DTCC data.",
        "business_impact": "Duplicate UTI resubmissions are removed to prevent double-counting positions. Notional amounts are converted to USD using EOD FX rates, making desk-level aggregations consistent. Each trade is tagged as IMA (Internal Models Approach) or SA (Standardised Approach) based on instrument type — a key FRTB regulatory distinction.",
        "frequency": "Runs after bronze trade ingestion",
        "owner": "Market Risk Data team",
    },
    ("bronze_yahoo_prices", "silver_prices"): {
        "title": "Raw Prices → Validated Price Series",
        "what": "Corporate action adjustments, range validation, and instrument master mapping applied to raw Yahoo prices.",
        "business_impact": "Split and dividend adjustments ensure historical price comparability for the 250-day VaR lookback. Prices outside exchange tick ranges are flagged and excluded. Missing prices are interpolated using last-known-good for up to 2 business days before triggering a data quality alert.",
        "frequency": "Runs after bronze price ingestion",
        "owner": "Market Risk Data team",
    },
    # ── Silver Enrich (silver_enrich.py) ──────────────────────────────────────
    ("silver_trades", "silver_positions"): {
        "title": "Validated Trades → Asset Class Positions",
        "what": "Trade-level records aggregated to asset_class/currency level; joined with latest prices for mark-to-market and SOFR for present value discounting.",
        "business_impact": "Converts individual trade UTIs to desk-addressable position aggregates. Market value and PV figures at this stage are the inputs used by the gold layer for capital charge computation. Aggregation at asset_class/currency level reduces row count by ~200× while preserving all FRTB-required granularity.",
        "frequency": "Runs after silver_transform.py completes",
        "owner": "Quant Analytics team",
    },
    ("silver_prices", "silver_positions"): {
        "title": "Validated Prices → Mark-to-Market",
        "what": "Latest EOD adj_close per currency applied to each aggregated position for fair-value marking.",
        "business_impact": "Stale or missing prices at this stage cause positions to carry forward yesterday's value, triggering a data quality alert. Under FRTB IMA rules, positions with no valid mark for more than 2 days must be excluded from the IMA model and moved to the SA fallback.",
        "frequency": "Concurrent with position aggregation",
        "owner": "Quant Analytics team",
    },
    ("silver_rates", "silver_positions"): {
        "title": "Validated Rates → SOFR Discounting",
        "what": "SOFR (or DFF fallback) used as the flat discount rate for present value calculation on each position.",
        "business_impact": "SOFR replaced LIBOR as the USD risk-free rate in 2022. Using SOFR here ensures PV calculations are post-LIBOR compliant for GIRR capital charge inputs. A 100bp move in SOFR changes PV by ~1% on a 1-year position.",
        "frequency": "Concurrent with position aggregation",
        "owner": "Quant Analytics team",
    },
    ("bronze_risk_outputs", "silver_risk_outputs"): {
        "title": "Synthetic Risk → Cleaned Risk Outputs",
        "what": "Raw synthetic VaR/ES/P&L outputs validated, null-filled, and normalised to a consistent schema for gold consumption.",
        "business_impact": "Removes calc_date gaps and enforces consistent desk naming so gold-layer aggregations match the trade book. Any desk with missing risk figures for more than 2 consecutive days is flagged and its capital charge is frozen at the prior day's value pending restatement.",
        "frequency": "Runs after bronze_synthetic ingestion",
        "owner": "Quant Analytics team",
    },
    # ── Gold: FRTB IMA Metrics (silver_risk_outputs is the direct input) ─────
    # ── Gold: Trade Positions ──────────────────────────────────────────────────
    ("silver_positions", "gold_trade_positions"): {
        "title": "Silver Positions → Gold Trade Positions",
        "what": "Enriched silver positions promoted to gold with no additional join — all enrichment was done in silver_enrich.py.",
        "business_impact": "Gold trade_positions is the authoritative mark-to-market book of record. Downstream capital tables (ES, backtesting) trace their notional exposure back to this table via lineage. Having the join done in silver means gold computation is faster and auditable at the silver level.",
        "frequency": "Runs after silver_enrich.py completes (~10:30 ET)",
        "owner": "Quant Analytics team",
    },
    # ── Gold: FRTB IMA Metrics ─────────────────────────────────────────────────
    ("silver_risk_outputs", "gold_backtesting"): {
        "title": "Enriched Risk → VaR Back-Testing (BCBS 457 ¶351-368)",
        "what": "VaR 99% 1-day compared against hypothetical and actual P&L to count back-testing exceptions over a 250-day rolling window.",
        "business_impact": "Under FRTB IMA, VaR is used ONLY for back-testing — not for capital. Exception counts determine the traffic light zone: GREEN (0-4) = no surcharge, AMBER (5-9) = +0.75× multiplier, RED (10+) = +1.0× multiplier on top of the 3× ES floor. More than 9 exceptions in a year can increase capital requirements by up to 33%.",
        "frequency": "Daily at 11:00 ET",
        "owner": "Quant Analytics team",
    },
    ("silver_risk_outputs", "gold_es_outputs"): {
        "title": "Enriched Risk → Expected Shortfall 97.5% (BCBS 457 ¶21-34)",
        "what": "ES 97.5% computed per desk with FRTB risk class assignment and liquidity horizon scaling (ES × √LH).",
        "business_impact": "ES is the regulatory capital metric under FRTB IMA — replacing VaR. Each risk class has a different liquidity horizon: GIRR/FX/EQ/COMM = 10 days, CSR Non-Securitisation = 20 days. Scaling ES to the correct horizon is mandatory; using the wrong LH overstates or understates capital by √(LH_wrong/LH_correct).",
        "frequency": "Concurrent with back-testing",
        "owner": "Quant Analytics team",
    },
    ("silver_risk_outputs", "gold_pnl_vectors"): {
        "title": "Enriched Risk → P&L Vectors (PLAT Input, BCBS 457 ¶329)",
        "what": "Hypothetical P&L (risk-factor-only) and actual P&L (realized) vectors per desk, with unexplained component.",
        "business_impact": "pnl_unexplained = actual − hypothetical. Large unexplained P&L indicates model error or new deals not captured by the risk model. This feeds the P&L Attribution Test — if unexplained P&L is too large relative to the model's std, the desk fails PLAT and loses IMA approval.",
        "frequency": "Concurrent with ES calculation",
        "owner": "Quant Analytics team",
    },
    ("gold_pnl_vectors", "gold_plat_results"): {
        "title": "P&L Vectors → P&L Attribution Test (BCBS 457 ¶329-345)",
        "what": "Three statistical tests over a 12-month rolling window: UPL ratio (< 0.95), Spearman rank correlation (> 0.40), KS statistic (< 0.20). All three must pass.",
        "business_impact": "PLAT validates that the IMA model explains actual P&L. Failing any one test puts the desk in the PLAT amber zone for 12 months. Failing for 12 consecutive months forces the desk to SA (Standardised Approach), which typically increases capital by 20–40% for complex desks.",
        "frequency": "Daily — rolling 12-month window updated each day",
        "owner": "Model Validation team",
    },
    ("bronze_fred_rates", "gold_rfet_results"): {
        "title": "Raw Rates → RFET Observation Count (Bronze Direct Read)",
        "what": "observation_count is computed by counting distinct rate_date values per series_id directly from the raw bronze table — not from silver. RFET checks whether each FRED rate series has ≥75 real market observations in the prior 12 months OR ≥25 in the prior 90 days to qualify as a modellable risk factor under FRTB IMA.",
        "business_impact": "Reading from bronze is intentional and architecturally correct: silver forward-fills weekend and holiday gaps and removes outliers, which would inflate the count and make an ineligible factor appear eligible. A FRED rate series that fails RFET cannot be used in the IMA model and must be proxied. An ineligible GIRR factor forces SA capital charges on all trades sensitive to that factor — typically 20–40% more capital than IMA. RFET failures must be reported to the Risk Committee within 5 business days.",
        "frequency": "Daily — rolling 12-month and 90-day observation windows recalculated from raw bronze",
        "owner": "Regulatory Reporting team",
    },
    ("silver_rates", "gold_rfet_results"): {
        "title": "Validated Rates → Risk Factor Eligibility Test (BCBS 457 ¶76-80)",
        "what": "Observation counts per FRED series checked against RFET thresholds: ≥75 in 12 months OR ≥25 in 90 days.",
        "business_impact": "Risk factors that fail RFET cannot be used in IMA models and must be proxied. An ineligible GIRR factor (e.g., a tenor with sparse FRED data) forces the firm to use the SA charge for all trades sensitive to that factor. RFET failures must be reported to the Risk Committee within 5 business days.",
        "frequency": "Daily — counts over rolling 12-month and 90-day windows",
        "owner": "Regulatory Reporting team",
    },
    ("gold_es_outputs", "gold_capital_charge"): {
        "title": "ES Outputs + Back-Testing → Capital Charge (BCBS 457 ¶180-186)",
        "what": "Regulatory capital = ES 97.5% scaled × (3.0 + traffic_light_multiplier). Minimum multiplier is 3.0 (GREEN), maximum is 4.0 (RED).",
        "business_impact": "This is the number submitted to the regulator as the firm's market risk capital requirement. A desk in RED zone pays 4× ES vs 3× in GREEN — a 33% capital premium for poor back-testing performance. capital_charge_usd directly determines the firm's RWA (risk-weighted assets) reported in the Pillar 3 disclosure.",
        "frequency": "Daily after ES and back-testing are both complete",
        "owner": "Regulatory Reporting team",
    },
    ("gold_backtesting", "gold_capital_charge"): {
        "title": "Back-Testing Exceptions → Capital Multiplier",
        "what": "Traffic light zone and capital_multiplier from backtesting joined into the capital charge formula.",
        "business_impact": "The multiplier (0.00/0.75/1.00) acts as a penalty for back-testing failures. It is added to the regulatory floor of 3.0, making total multiplier range 3.0–4.0. Once a desk enters RED zone it stays there until the 250-day window clears, so the penalty can persist for up to a year.",
        "frequency": "Concurrent with ES join",
        "owner": "Regulatory Reporting team",
    },
    ("gold_backtesting", "gold_risk_summary"): {
        "title": "Back-Testing → Daily Risk Summary",
        "what": "VaR back-testing results (exception counts, traffic light zone) merged into the consolidated regulatory report.",
        "business_impact": "The risk summary is the primary output reviewed by the CRO and submitted to regulators. Back-testing status per desk must be disclosed in the Pillar 3 report. Desks with AMBER or RED status are highlighted for the Risk Committee review.",
        "frequency": "Published daily by 13:00 ET",
        "owner": "Regulatory Reporting team",
    },
    ("gold_es_outputs", "gold_risk_summary"): {
        "title": "ES Outputs → Daily Risk Summary",
        "what": "Desk-level and firm-wide ES 97.5% figures merged into the consolidated regulatory submission.",
        "business_impact": "ES is the headline capital figure. Any ES above the internal desk limit triggers an automatic alert to the CRO and CFO. The 60-day rolling average of ES is used in the FRTB capital formula, so a single spike has a lasting impact that decays over 60 trading days.",
        "frequency": "Concurrent with other risk summary joins",
        "owner": "Regulatory Reporting team",
    },
    ("gold_plat_results", "gold_risk_summary"): {
        "title": "PLAT Results → Daily Risk Summary",
        "what": "Pass/fail status for all three PLAT tests (UPL, Spearman, KS) per desk included in the risk summary.",
        "business_impact": "Regulators require PLAT results to be reported alongside capital figures. A desk showing PLAT_FAIL in the risk summary triggers a remediation clock — the firm has 12 months to fix the model before being forced to SA.",
        "frequency": "Concurrent with other risk summary joins",
        "owner": "Model Validation team",
    },
    ("gold_capital_charge", "gold_risk_summary"): {
        "title": "Capital Charge → Daily Risk Summary",
        "what": "Final capital_charge_usd and capital_multiplier per desk included in the risk summary as the regulatory submission figure.",
        "business_impact": "capital_charge_usd is the number that appears in the firm's Pillar 3 market risk capital disclosure. It determines the firm's capital adequacy ratio. An unexpected increase triggers immediate escalation to Treasury for capital planning.",
        "frequency": "Final step in the daily gold pipeline",
        "owner": "Regulatory Reporting team",
    },
}


@router.get("/nodes")
def list_nodes(
    domain: str | None = Query(None),
    layer: str | None = Query(None),
    limit: int = Query(200, le=1000),
):
    filters = ["1=1"]
    if domain:
        filters.append(f"domain = '{domain}'")
    if layer:
        filters.append(f"layer = '{layer}'")
    where = " AND ".join(filters)

    sql = f"""
        SELECT node_id, name, type, domain, layer, metadata
        FROM `{project()}.risklens_lineage.nodes`
        WHERE {where}
        ORDER BY domain, layer, name
        LIMIT {limit}
    """
    return query_rows(sql)


@router.get("/graph/{asset_id}")
def get_lineage_graph(asset_id: str, hops: int = Query(2, ge=1, le=4)):
    """
    Return nodes + edges for the subgraph within `hops` of asset_id.
    The frontend uses this to render the lineage DAG.
    """
    # Seed node IDs — start from the requested asset
    # We iteratively expand via edges up to `hops` levels
    seed_sql = f"""
        SELECT node_id FROM `{project()}.risklens_lineage.nodes`
        WHERE node_id = '{asset_id}'
    """
    seeds = [r["node_id"] for r in query_rows(seed_sql)]
    if not seeds:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Node '{asset_id}' not found")

    visited: set[str] = set(seeds)
    frontier: set[str] = set(seeds)

    for _ in range(hops):
        if not frontier:
            break
        ids = ", ".join(f"'{n}'" for n in frontier)
        expansion_sql = f"""
            SELECT DISTINCT
                CASE WHEN from_node_id IN ({ids}) THEN to_node_id ELSE from_node_id END AS neighbor
            FROM `{project()}.risklens_lineage.edges`
            WHERE from_node_id IN ({ids}) OR to_node_id IN ({ids})
        """
        neighbors = {r["neighbor"] for r in query_rows(expansion_sql)} - visited
        visited.update(neighbors)
        frontier = neighbors

    all_ids = ", ".join(f"'{n}'" for n in visited)

    nodes_sql = f"""
        SELECT node_id, name, type, domain, layer, metadata
        FROM `{project()}.risklens_lineage.nodes`
        WHERE node_id IN ({all_ids})
    """
    edges_sql = f"""
        SELECT edge_id, from_node_id, to_node_id, relationship, pipeline_job
        FROM `{project()}.risklens_lineage.edges`
        WHERE from_node_id IN ({all_ids}) AND to_node_id IN ({all_ids})
    """

    raw_edges = query_rows(edges_sql)
    for e in raw_edges:
        key = (e["from_node_id"], e["to_node_id"])
        e["story"] = EDGE_STORIES.get(key)

    return {
        "root_id": asset_id,
        "hops": hops,
        "nodes": query_rows(nodes_sql),
        "edges": raw_edges,
    }
