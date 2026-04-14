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
    ("silver_trades", "gold_trade_positions"): {
        "title": "Validated Trades → Enriched Positions",
        "what": "Trade-level positions joined with cleaned prices and rates to produce FRTB-ready position records.",
        "business_impact": "Each trade position is enriched with its current market value, FX delta, and risk sensitivities (delta, vega, curvature). The output is the primary input to both the IMA VaR engine and the SA capital charge calculation. Position quality at this stage directly determines regulatory capital accuracy.",
        "frequency": "Runs after all silver tables are ready (~10:00 ET)",
        "owner": "Quant Analytics team",
    },
    ("silver_prices", "gold_trade_positions"): {
        "title": "Validated Prices → Position Marking",
        "what": "EOD prices applied to each trade position for mark-to-market valuation.",
        "business_impact": "Provides the fair-value mark used in P&L calculation and VaR sensitivity. Stale or missing prices at this stage cause positions to carry forward yesterday's value, which must be escalated to risk oversight within 2 hours under FRTB IMA rules.",
        "frequency": "Concurrent with silver_trades join",
        "owner": "Quant Analytics team",
    },
    ("silver_rates", "gold_trade_positions"): {
        "title": "Validated Rates → Rate Sensitivity Enrichment",
        "what": "Cleaned rate series used to compute interest rate sensitivities (DV01, CS01) per position.",
        "business_impact": "DV01 (dollar value of 1bp rate move) and CS01 (credit spread sensitivity) are calculated here per desk. These sensitivities feed directly into the FRTB IMA Expected Shortfall model and the SA delta risk charge. Errors in rate sensitivity propagate to all downstream regulatory capital outputs.",
        "frequency": "Concurrent with silver_trades join",
        "owner": "Quant Analytics team",
    },
    ("gold_trade_positions", "gold_var_outputs"): {
        "title": "Enriched Positions → VaR Calculation",
        "what": "Historical Simulation VaR computed over the FRTB-mandated 250 trading day lookback window.",
        "business_impact": "Produces the 99th percentile 1-day VaR per desk and firm-wide — the primary FRTB IMA regulatory metric. A VaR breach (actual daily loss > VaR) must be reported to regulators within 24 hours. More than 4 breaches in 250 days triggers a capital multiplier increase from 3× to 4×.",
        "frequency": "Daily at 11:00 ET, submitted to Risk Committee by 12:00 ET",
        "owner": "Quant Analytics team",
    },
    ("gold_trade_positions", "gold_expected_shortfall"): {
        "title": "Enriched Positions → Expected Shortfall",
        "what": "97.5% Expected Shortfall (ES) computed per desk and aggregated firm-wide under FRTB IMA.",
        "business_impact": "ES replaces VaR as the primary risk measure under Basel IV/FRTB because it captures tail risk beyond the VaR threshold. Desk-level ES feeds the regulatory capital formula: Capital = max(ES_current, avg(ES_60days)) × liquidity horizon multiplier. A 10% increase in ES translates directly to higher regulatory capital requirements.",
        "frequency": "Concurrent with VaR calculation",
        "owner": "Quant Analytics team",
    },
    ("gold_var_outputs", "gold_risk_summary"): {
        "title": "VaR Outputs → Daily Risk Report",
        "what": "VaR, ES, P&L, and breach status aggregated into the daily regulatory risk summary.",
        "business_impact": "The final output consumed by the Risk Committee and submitted to regulators. Breach flags are compared against VaR thresholds. Report is formatted per BCBS 239 data lineage and aggregation principles, enabling full audit trail from raw DTCC trades through to reported capital figures.",
        "frequency": "Published daily by 13:00 ET",
        "owner": "Regulatory Reporting team",
    },
    ("gold_expected_shortfall", "gold_risk_summary"): {
        "title": "Expected Shortfall → Daily Risk Report",
        "what": "Desk-level and firm-wide ES figures merged into the consolidated regulatory submission.",
        "business_impact": "ES figures determine the capital adequacy ratio reported to the regulator. Any ES figure above the internal limit triggers an automatic alert to the CRO and CFO. The 60-day rolling average of ES is used in the capital formula, so a single spike has reduced but lasting impact.",
        "frequency": "Concurrent with VaR aggregation",
        "owner": "Regulatory Reporting team",
    },
    ("gold_trade_positions", "gold_pl_vectors"): {
        "title": "Enriched Positions → P&L Scenario Vectors",
        "what": "100 stress scenarios applied to each position to generate P&L attribution vectors for PLAT.",
        "business_impact": "P&L Attribution Test (PLAT) is an FRTB requirement that validates the IMA model by comparing hypothetical P&L (from risk model) against actual P&L. Failing PLAT for a desk for 12 consecutive months results in that desk being moved from IMA (lower capital) to SA (higher capital), potentially increasing capital requirements by 20–40%.",
        "frequency": "Daily after position enrichment",
        "owner": "Quant Analytics team",
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
