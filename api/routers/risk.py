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
        "[api] GET /risk/summary | table=risklens_gold.risk_summary | trade_date=%s | desk=%s",
        trade_date or "latest", desk or "all",
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
    amber_zones = sum(1 for r in rows if r.get("traffic_light_zone") == "AMBER")
    green_zones = sum(1 for r in rows if r.get("traffic_light_zone") == "GREEN")
    plat_fails = sum(1 for r in rows if r.get("plat_pass") is False)
    logger.info(
        "[api] ✓ GET /risk/summary | table=risklens_gold.risk_summary | rows=%d | RED=%d | AMBER=%d | GREEN=%d | plat_fails=%d | trade_date=%s",
        len(rows), red_zones, amber_zones, green_zones, plat_fails, trade_date or "latest",
        extra={"json_fields": {
            "row_count": len(rows),
            "red_zone_desks": red_zones,
            "amber_zone_desks": amber_zones,
            "green_zone_desks": green_zones,
            "plat_fail_desks": plat_fails,
        }},
    )
    if not rows:
        logger.warning("[api] GET /risk/summary returned 0 rows | table=risklens_gold.risk_summary | trade_date=%s | desk=%s", trade_date, desk,
                       extra={"json_fields": {"trade_date": trade_date, "desk": desk}})
    return rows


@router.get("/capital")
def get_capital_charge(
    trade_date: str | None = Query(None),
    risk_class: str | None = Query(None),
):
    """Capital charge per desk: ES × (3.0 + traffic_light_multiplier)."""
    logger.info(
        "[api] GET /risk/capital | table=risklens_gold.capital_charge | trade_date=%s | risk_class=%s",
        trade_date or "latest", risk_class or "all",
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
        "[api] ✓ GET /risk/capital | table=risklens_gold.capital_charge | rows=%d | total_capital_usd=$%.0f | trade_date=%s",
        len(rows), total_capital, trade_date or "latest",
        extra={"json_fields": {"row_count": len(rows), "total_capital_usd": round(total_capital, 2)}},
    )
    return rows


@router.get("/dates")
def get_available_dates(
    table: str = Query("backtesting", description="gold table name"),
):
    """Distinct calc_dates for a gold table, newest first (for date pickers)."""
    logger.info("[api] GET /risk/dates | table=risklens_gold.%s", table, extra={"json_fields": {"table": table}})
    p = project()
    allowed = {"backtesting", "es_outputs", "capital_charge", "plat_results", "risk_summary"}
    if table not in allowed:
        logger.warning("[api] GET /risk/dates: invalid table | requested=%s | allowed=%s", table, sorted(allowed))
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail=f"table must be one of {sorted(allowed)}")
    sql = f"""
        SELECT DISTINCT CAST(calc_date AS STRING) AS date
        FROM `{p}.risklens_gold.{table}`
        ORDER BY 1 DESC LIMIT 120
    """
    dates = [r["date"] for r in query_rows(sql)]
    logger.info(
        "[api] ✓ GET /risk/dates | table=risklens_gold.%s | dates=%d | latest=%s",
        table, len(dates), dates[0] if dates else None,
        extra={"json_fields": {"table": table, "date_count": len(dates), "latest": dates[0] if dates else None}},
    )
    return dates


@router.get("/trend")
def get_risk_trend(limit: int = Query(90, le=365)):
    """Daily total capital charge + ES trend across all desks (for chart)."""
    logger.info("[api] GET /risk/trend | table=risklens_gold.risk_summary | limit=%d", limit, extra={"json_fields": {"limit": limit}})
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
        "[api] ✓ GET /risk/trend | table=risklens_gold.risk_summary | days=%d | limit=%d",
        len(rows), limit,
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
        "[api] GET /risk/backtesting | table=risklens_gold.backtesting | calc_date=%s | trade_date=%s | zone=%s",
        calc_date or "latest", trade_date, zone or "all",
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
    zone_str = " | ".join(f"{z}={c}" for z, c in sorted(zone_counts.items()))
    logger.info(
        "[api] ✓ GET /risk/backtesting | table=risklens_gold.backtesting | rows=%d | zones=%s | calc_date=%s",
        len(rows), zone_str, calc_date or "latest",
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
        "[api] GET /risk/plat | table=risklens_gold.plat_results | trade_date=%s | pass_only=%s",
        trade_date or "latest", pass_only,
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
        "[api] ✓ GET /risk/plat | table=risklens_gold.plat_results | rows=%d | plat_fail=%d | plat_pass=%d | trade_date=%s",
        len(rows), fails, len(rows) - fails, trade_date or "latest",
        extra={"json_fields": {"row_count": len(rows), "fail_count": fails}},
    )
    if fails:
        logger.warning("[api] PLAT failures detected | table=risklens_gold.plat_results | failing_desks=%d | trade_date=%s", fails, trade_date)
    return rows


@router.get("/rfet")
def get_rfet_results(
    rfet_date: str | None = Query(None),
    risk_class: str | None = Query(None),
    failures_only: bool = Query(False),
):
    """Risk Factor Eligibility Test results (BCBS 457 ¶76-80)."""
    logger.info(
        "[api] GET /risk/rfet | table=risklens_gold.rfet_results | rfet_date=%s | risk_class=%s | failures_only=%s",
        rfet_date or "latest", risk_class or "all", failures_only,
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
        "[api] ✓ GET /risk/rfet | table=risklens_gold.rfet_results | rows=%d | rfet_fail=%d | rfet_pass=%d | rfet_date=%s",
        len(rows), failures, len(rows) - failures, rfet_date or "latest",
        extra={"json_fields": {"row_count": len(rows), "failure_count": failures}},
    )
    if failures:
        logger.warning("[api] RFET failures detected | table=risklens_gold.rfet_results | failing_factors=%d | rfet_date=%s", failures, rfet_date)
    return rows


@router.get("/es")
def get_es_outputs(
    trade_date: str | None = Query(None),
    risk_class: str | None = Query(None),
):
    """Expected Shortfall 97.5% per desk × risk class (BCBS 457 ¶21-34)."""
    logger.info(
        "[api] GET /risk/es | table=risklens_gold.es_outputs | trade_date=%s | risk_class=%s",
        trade_date or "latest", risk_class or "all",
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
        "[api] ✓ GET /risk/es | table=risklens_gold.es_outputs | rows=%d | total_es_scaled=$%.0f | trade_date=%s",
        len(rows), total_es, trade_date or "latest",
        extra={"json_fields": {"row_count": len(rows), "total_es_scaled": round(total_es, 2)}},
    )
    return rows
