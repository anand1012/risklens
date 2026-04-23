"""
Governance router — ownership and SLA health endpoints.

GET /governance/sla          SLA breaches summary
GET /governance/ownership    All asset ownership records
GET /governance/quality      Quality scores across all assets
"""

import logging

from fastapi import APIRouter, Query

from api.db.bigquery import query_rows, project

router = APIRouter(prefix="/governance", tags=["governance"])
logger = logging.getLogger(__name__)


@router.get("/sla")
def get_sla_status(breaches_only: bool = Query(False)):
    logger.info(
        "[api] GET /governance/sla | tables=risklens_catalog.sla_status+assets | breaches_only=%s",
        breaches_only,
        extra={"json_fields": {"breaches_only": breaches_only}},
    )
    where = "s.breach_flag = TRUE" if breaches_only else "1=1"
    sql = f"""
        SELECT
            a.asset_id,
            a.name,
            a.domain,
            a.layer,
            s.expected_refresh,
            s.actual_refresh,
            s.breach_flag,
            s.breach_duration_mins,
            s.checked_at
        FROM `{project()}.risklens_catalog.sla_status` s
        JOIN `{project()}.risklens_catalog.assets` a USING (asset_id)
        WHERE {where}
        ORDER BY s.breach_flag DESC, s.breach_duration_mins DESC
    """
    rows = query_rows(sql)
    breaches = sum(1 for r in rows if r.get("breach_flag"))
    logger.info(
        "[api] ✓ GET /governance/sla | tables=risklens_catalog.sla_status+assets | rows=%d | breaches=%d | on_time=%d",
        len(rows), breaches, len(rows) - breaches,
        extra={"json_fields": {
            "result_count": len(rows),
            "breach_count": breaches,
            "breaches_only": breaches_only,
        }},
    )
    if not rows:
        logger.warning("[api] GET /governance/sla returned 0 rows | breaches_only=%s", breaches_only)
    return rows


@router.get("/ownership")
def get_ownership(team: str | None = Query(None)):
    logger.info("[api] GET /governance/ownership | tables=risklens_catalog.ownership+assets | team=%s", team or "all", extra={"json_fields": {"team": team}})
    where = f"o.team = '{team}'" if team else "1=1"
    sql = f"""
        SELECT
            a.asset_id,
            a.name,
            a.domain,
            a.layer,
            o.owner_name,
            o.team,
            o.steward,
            o.email,
            o.assigned_date
        FROM `{project()}.risklens_catalog.ownership` o
        JOIN `{project()}.risklens_catalog.assets` a USING (asset_id)
        WHERE {where}
        ORDER BY o.team, o.owner_name
    """
    rows = query_rows(sql)
    unassigned = sum(1 for r in rows if not r.get("owner_name"))
    logger.info(
        "[api] ✓ GET /governance/ownership | tables=risklens_catalog.ownership+assets | rows=%d | unassigned=%d | team=%s",
        len(rows), unassigned, team or "all",
        extra={"json_fields": {"result_count": len(rows), "unassigned_count": unassigned, "team": team}},
    )
    if unassigned:
        logger.warning("[api] GET /governance/ownership: %d assets have no owner | table=risklens_catalog.ownership", unassigned)
    return rows


@router.get("/quality")
def get_quality_scores(
    freshness_status: str | None = Query(None, description="fresh | stale | critical"),
    schema_drift: bool | None = Query(None),
):
    logger.info(
        "[api] GET /governance/quality | tables=risklens_catalog.quality_scores+assets | freshness=%s | schema_drift=%s",
        freshness_status or "all", schema_drift,
        extra={"json_fields": {"freshness_status": freshness_status, "schema_drift": schema_drift}},
    )
    filters = ["1=1"]
    if freshness_status:
        filters.append(f"q.freshness_status = '{freshness_status}'")
    if schema_drift is not None:
        filters.append(f"q.schema_drift = {str(schema_drift).upper()}")
    where = " AND ".join(filters)

    sql = f"""
        SELECT
            a.asset_id,
            a.name,
            a.domain,
            a.layer,
            q.null_rate,
            q.schema_drift,
            q.freshness_status,
            q.duplicate_rate,
            q.last_checked
        FROM `{project()}.risklens_catalog.quality_scores` q
        JOIN `{project()}.risklens_catalog.assets` a USING (asset_id)
        WHERE {where}
        ORDER BY q.freshness_status DESC, q.null_rate DESC
    """
    rows = query_rows(sql)
    critical = sum(1 for r in rows if r.get("freshness_status") == "critical")
    drift_count = sum(1 for r in rows if r.get("schema_drift"))
    logger.info(
        "[api] ✓ GET /governance/quality | tables=risklens_catalog.quality_scores+assets | rows=%d | critical=%d | schema_drift=%d",
        len(rows), critical, drift_count,
        extra={"json_fields": {
            "result_count": len(rows),
            "critical_count": critical,
            "schema_drift_count": drift_count,
        }},
    )
    if critical:
        logger.warning("[api] GET /governance/quality: %d assets in critical freshness state | table=risklens_catalog.quality_scores", critical)
    return rows
