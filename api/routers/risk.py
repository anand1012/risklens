"""
RiskLens — Risk router: FRTB IMA gold table endpoints.

GET /risk/summary       — latest risk_summary per desk (ES + VaR + PLAT + capital)
GET /risk/capital       — capital_charge per desk (latest trade_date)
GET /risk/backtesting   — back-testing exceptions + traffic light per desk
GET /risk/plat          — P&L Attribution Test results per desk
GET /risk/rfet          — Risk Factor Eligibility Test results
GET /risk/es            — Expected Shortfall 97.5% per desk × risk class
"""

import logging

from fastapi import APIRouter, Query

from api.db.bigquery import query_rows, project

router = APIRouter(prefix="/risk", tags=["risk"])
logger = logging.getLogger(__name__)


@router.get("/summary")
def get_risk_summary(
    trade_date: str | None = Query(None, description="YYYY-MM-DD; defaults to latest"),
    desk: str | None = Query(None),
):
    """Daily risk summary: ES + VaR back-testing + PLAT + capital per desk."""
    logger.info(
        "→ get_risk_summary called",
        extra={"json_fields": {"trade_date": trade_date, "desk": desk}},
    )
    date_filter = (
        f"trade_date = '{trade_date}'"
        if trade_date
        else "trade_date = (SELECT MAX(trade_date) FROM `{p}.risklens_gold.risk_summary`)"
    )
    p = project()
    if not trade_date:
        date_filter = f"trade_date = (SELECT MAX(trade_date) FROM `{p}.risklens_gold.risk_summary`)"

    desk_filter = f"AND desk = '{desk}'" if desk else ""
    logger.debug("get_risk_summary: date_filter=%s desk_filter=%s", date_filter[:60], desk_filter)

    sql = f"""
        SELECT
            calc_date,
            desk,
            risk_class,
            liquidity_horizon,
            var_99_1d,
            var_99_10d,
            traffic_light_zone,
            exception_count_250d,
            es_975_1d,
            es_975_10d,
            es_975_scaled,
            plat_pass,
            upl_ratio,
            spearman_correlation,
            ks_statistic,
            plat_notes,
            capital_charge_usd,
            capital_multiplier,
            trade_date
        FROM `{p}.risklens_gold.risk_summary`
        WHERE {date_filter} {desk_filter}
        ORDER BY desk
    """
    rows = query_rows(sql)
    red_zones = sum(1 for r in rows if r.get("traffic_light_zone") == "RED")
    plat_fails = sum(1 for r in rows if r.get("plat_pass") is False)
    logger.info(
        "← get_risk_summary done",
        extra={"json_fields": {
            "row_count": len(rows),
            "red_zone_desks": red_zones,
            "plat_fail_desks": plat_fails,
        }},
    )
    if not rows:
        logger.warning("get_risk_summary returned 0 rows", extra={"json_fields": {"trade_date": trade_date, "desk": desk}})
    return rows


@router.get("/capital")
def get_capital_charge(
    trade_date: str | None = Query(None),
    risk_class: str | None = Query(None),
):
    """Capital charge per desk: ES × (3.0 + traffic_light_multiplier)."""
    logger.info(
        "→ get_capital_charge called",
        extra={"json_fields": {"trade_date": trade_date, "risk_class": risk_class}},
    )
    p = project()
    date_filter = (
        f"trade_date = '{trade_date}'"
        if trade_date
        else f"trade_date = (SELECT MAX(trade_date) FROM `{p}.risklens_gold.capital_charge`)"
    )
    rc_filter = f"AND risk_class = '{risk_class}'" if risk_class else ""

    sql = f"""
        SELECT
            calc_date,
            desk,
            risk_class,
            liquidity_horizon,
            es_975_1d,
            es_975_scaled,
            traffic_light_zone,
            capital_multiplier,
            regulatory_floor,
            capital_charge_usd,
            exception_count_250d,
            trade_date
        FROM `{p}.risklens_gold.capital_charge`
        WHERE {date_filter} {rc_filter}
        ORDER BY capital_charge_usd DESC
    """
    rows = query_rows(sql)
    total_capital = sum(r.get("capital_charge_usd") or 0 for r in rows if r.get("desk") != "FIRM")
    logger.info(
        "← get_capital_charge done",
        extra={"json_fields": {"row_count": len(rows), "total_capital_usd": round(total_capital, 2)}},
    )
    return rows


@router.get("/dates")
def get_available_dates(
    table: str = Query("backtesting", description="gold table name"),
):
    """Distinct calc_dates for a gold table, newest first (for date pickers)."""
    logger.info("→ get_available_dates called", extra={"json_fields": {"table": table}})
    p = project()
    allowed = {"backtesting", "es_outputs", "capital_charge", "plat_results", "risk_summary"}
    if table not in allowed:
        logger.warning("get_available_dates: invalid table requested: %s", table)
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail=f"table must be one of {sorted(allowed)}")
    sql = f"""
        SELECT DISTINCT CAST(calc_date AS STRING) AS date
        FROM `{p}.risklens_gold.{table}`
        ORDER BY 1 DESC LIMIT 120
    """
    dates = [r["date"] for r in query_rows(sql)]
    logger.info(
        "← get_available_dates done",
        extra={"json_fields": {"table": table, "date_count": len(dates), "latest": dates[0] if dates else None}},
    )
    return dates


@router.get("/trend")
def get_risk_trend(limit: int = Query(90, le=365)):
    """Daily total capital charge + ES trend across all desks (for chart)."""
    logger.info("→ get_risk_trend called", extra={"json_fields": {"limit": limit}})
    p = project()
    sql = f"""
        SELECT
            CAST(calc_date AS STRING) AS calc_date,
            SUM(capital_charge_usd)   AS total_capital_usd,
            SUM(es_975_scaled)        AS total_es_scaled,
            MAX(traffic_light_zone)   AS worst_zone
        FROM `{p}.risklens_gold.risk_summary`
        WHERE desk != 'FIRM'
        GROUP BY calc_date
        ORDER BY calc_date ASC
        LIMIT {limit}
    """
    rows = query_rows(sql)
    logger.info(
        "← get_risk_trend done",
        extra={"json_fields": {"day_count": len(rows), "limit": limit}},
    )
    return rows


@router.get("/backtesting")
def get_backtesting(
    calc_date: str | None = Query(None, description="YYYY-MM-DD calc date (preferred)"),
    trade_date: str | None = Query(None, description="pipeline run date (legacy)"),
    zone: str | None = Query(None, description="GREEN | AMBER | RED"),
):
    """VaR 99% back-testing exceptions and traffic light zone per desk."""
    logger.info(
        "→ get_backtesting called",
        extra={"json_fields": {"calc_date": calc_date, "trade_date": trade_date, "zone": zone}},
    )
    p = project()
    if calc_date:
        date_filter = f"calc_date = '{calc_date}'"
    elif trade_date:
        date_filter = f"trade_date = '{trade_date}'"
    else:
        date_filter = f"calc_date = (SELECT MAX(calc_date) FROM `{p}.risklens_gold.backtesting`)"
    zone_filter = f"AND traffic_light_zone = '{zone.upper()}'" if zone else ""
    logger.debug("get_backtesting: date_filter=%s zone_filter=%s", date_filter[:60], zone_filter)

    sql = f"""
        SELECT
            calc_date,
            desk,
            var_99_1d,
            var_99_10d,
            hypothetical_pnl,
            actual_pnl,
            hypothetical_exception,
            actual_exception,
            exception_count_250d,
            traffic_light_zone,
            capital_multiplier,
            method,
            trade_date
        FROM `{p}.risklens_gold.backtesting`
        WHERE {date_filter} {zone_filter}
        ORDER BY exception_count_250d DESC
    """
    rows = query_rows(sql)
    zone_counts = {}
    for r in rows:
        z = r.get("traffic_light_zone", "UNKNOWN")
        zone_counts[z] = zone_counts.get(z, 0) + 1
    logger.info(
        "← get_backtesting done",
        extra={"json_fields": {"row_count": len(rows), "zone_distribution": zone_counts}},
    )
    return rows


@router.get("/plat")
def get_plat_results(
    trade_date: str | None = Query(None),
    pass_only: bool = Query(False),
):
    """P&L Attribution Test results per desk (BCBS 457 ¶329-345)."""
    logger.info(
        "→ get_plat_results called",
        extra={"json_fields": {"trade_date": trade_date, "pass_only": pass_only}},
    )
    p = project()
    date_filter = (
        f"trade_date = '{trade_date}'"
        if trade_date
        else f"trade_date = (SELECT MAX(trade_date) FROM `{p}.risklens_gold.plat_results`)"
    )
    pass_filter = "AND plat_pass = TRUE" if pass_only else ""

    sql = f"""
        SELECT
            calc_date,
            desk,
            window_start_date,
            window_end_date,
            observation_count,
            hyp_pnl_mean,
            hyp_pnl_std,
            actual_pnl_mean,
            upl_ratio,
            upl_pass,
            spearman_correlation,
            spearman_pass,
            ks_statistic,
            ks_pass,
            plat_pass,
            notes,
            trade_date
        FROM `{p}.risklens_gold.plat_results`
        WHERE {date_filter} {pass_filter}
        ORDER BY plat_pass ASC, upl_ratio DESC
    """
    rows = query_rows(sql)
    fails = sum(1 for r in rows if r.get("plat_pass") is False)
    logger.info(
        "← get_plat_results done",
        extra={"json_fields": {"row_count": len(rows), "fail_count": fails}},
    )
    if fails:
        logger.warning("get_plat_results: %d desks failing PLAT", fails)
    return rows


@router.get("/rfet")
def get_rfet_results(
    rfet_date: str | None = Query(None),
    risk_class: str | None = Query(None),
    failures_only: bool = Query(False),
):
    """Risk Factor Eligibility Test results (BCBS 457 ¶76-80)."""
    logger.info(
        "→ get_rfet_results called",
        extra={"json_fields": {"rfet_date": rfet_date, "risk_class": risk_class, "failures_only": failures_only}},
    )
    p = project()
    date_filter = (
        f"rfet_date = '{rfet_date}'"
        if rfet_date
        else f"rfet_date = (SELECT MAX(rfet_date) FROM `{p}.risklens_gold.rfet_results`)"
    )
    rc_filter   = f"AND risk_class = '{risk_class}'" if risk_class else ""
    fail_filter = "AND rfet_pass = FALSE" if failures_only else ""

    sql = f"""
        SELECT
            rfet_date,
            risk_factor_id,
            risk_class,
            obs_12m_count,
            obs_90d_count,
            obs_12m_pass,
            obs_90d_pass,
            rfet_pass,
            eligible_for_ima,
            last_observation_date,
            staleness_days,
            failure_reason
        FROM `{p}.risklens_gold.rfet_results`
        WHERE {date_filter} {rc_filter} {fail_filter}
        ORDER BY rfet_pass ASC, obs_12m_count ASC
    """
    rows = query_rows(sql)
    failures = sum(1 for r in rows if r.get("rfet_pass") is False)
    logger.info(
        "← get_rfet_results done",
        extra={"json_fields": {"row_count": len(rows), "failure_count": failures}},
    )
    if failures:
        logger.warning("get_rfet_results: %d risk factors failing RFET", failures)
    return rows


@router.get("/es")
def get_es_outputs(
    trade_date: str | None = Query(None),
    risk_class: str | None = Query(None),
):
    """Expected Shortfall 97.5% per desk × risk class (BCBS 457 ¶21-34)."""
    logger.info(
        "→ get_es_outputs called",
        extra={"json_fields": {"trade_date": trade_date, "risk_class": risk_class}},
    )
    p = project()
    date_filter = (
        f"trade_date = '{trade_date}'"
        if trade_date
        else f"trade_date = (SELECT MAX(trade_date) FROM `{p}.risklens_gold.es_outputs`)"
    )
    rc_filter = f"AND risk_class = '{risk_class}'" if risk_class else ""

    sql = f"""
        SELECT
            calc_date,
            desk,
            risk_class,
            liquidity_horizon,
            es_975_1d,
            es_975_10d,
            es_975_scaled,
            trade_date
        FROM `{p}.risklens_gold.es_outputs`
        WHERE {date_filter} {rc_filter}
        ORDER BY es_975_scaled DESC
    """
    rows = query_rows(sql)
    total_es = sum(r.get("es_975_scaled") or 0 for r in rows if r.get("desk") != "FIRM")
    logger.info(
        "← get_es_outputs done",
        extra={"json_fields": {"row_count": len(rows), "total_es_scaled": round(total_es, 2)}},
    )
    return rows
