"""
RiskLens — Risk router: FRTB IMA gold table endpoints.

GET /risk/summary       — latest risk_summary per desk (ES + VaR + PLAT + capital)
GET /risk/capital       — capital_charge per desk (latest trade_date)
GET /risk/backtesting   — back-testing exceptions + traffic light per desk
GET /risk/plat          — P&L Attribution Test results per desk
GET /risk/rfet          — Risk Factor Eligibility Test results
GET /risk/es            — Expected Shortfall 97.5% per desk × risk class
"""

from fastapi import APIRouter, Query

from api.db.bigquery import query_rows, project

router = APIRouter(prefix="/risk", tags=["risk"])


@router.get("/summary")
def get_risk_summary(
    trade_date: str | None = Query(None, description="YYYY-MM-DD; defaults to latest"),
    desk: str | None = Query(None),
):
    """Daily risk summary: ES + VaR back-testing + PLAT + capital per desk."""
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
    return query_rows(sql)


@router.get("/capital")
def get_capital_charge(
    trade_date: str | None = Query(None),
    risk_class: str | None = Query(None),
):
    """Capital charge per desk: ES × (3.0 + traffic_light_multiplier)."""
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
    return query_rows(sql)


@router.get("/backtesting")
def get_backtesting(
    trade_date: str | None = Query(None),
    zone: str | None = Query(None, description="GREEN | AMBER | RED"),
):
    """VaR 99% back-testing exceptions and traffic light zone per desk."""
    p = project()
    date_filter = (
        f"trade_date = '{trade_date}'"
        if trade_date
        else f"trade_date = (SELECT MAX(trade_date) FROM `{p}.risklens_gold.backtesting`)"
    )
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
    return query_rows(sql)


@router.get("/plat")
def get_plat_results(
    trade_date: str | None = Query(None),
    pass_only: bool = Query(False),
):
    """P&L Attribution Test results per desk (BCBS 457 ¶329-345)."""
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
    return query_rows(sql)


@router.get("/rfet")
def get_rfet_results(
    rfet_date: str | None = Query(None),
    risk_class: str | None = Query(None),
    failures_only: bool = Query(False),
):
    """Risk Factor Eligibility Test results (BCBS 457 ¶76-80)."""
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
    return query_rows(sql)


@router.get("/es")
def get_es_outputs(
    trade_date: str | None = Query(None),
    risk_class: str | None = Query(None),
):
    """Expected Shortfall 97.5% per desk × risk class (BCBS 457 ¶21-34)."""
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
    return query_rows(sql)
