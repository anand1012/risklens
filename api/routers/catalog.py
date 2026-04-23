"""
Catalog router — data asset discovery endpoints.

GET /assets                   list all assets (with optional ?domain= ?layer= filters)
GET /assets/{asset_id}        full asset detail (asset + ownership + quality + SLA)
GET /assets/{asset_id}/schema columns for an asset
"""

import json
import logging

from fastapi import APIRouter, Query
from typing import Optional

from api.db.bigquery import query_rows, project

router = APIRouter(prefix="/assets", tags=["catalog"])
logger = logging.getLogger(__name__)


def _fix_tags(row: dict) -> dict:
    """BigQuery stores tags as a JSON string — parse it to a list."""
    if isinstance(row.get("tags"), str):
        try:
            row["tags"] = json.loads(row["tags"])
        except Exception:
            logger.warning("_fix_tags: could not parse tags JSON for asset_id=%s", row.get("asset_id"))
            row["tags"] = []
    return row


@router.get("")
def list_assets(
    domain: Optional[str] = Query(None, description="Filter by domain (risk, market_data, …)"),
    layer: Optional[str] = Query(None, description="Filter by layer (bronze, silver, gold)"),
    type: Optional[str] = Query(None, description="Filter by type (table, feed, report, model)"),
    limit: int = Query(100, le=500),
):
    logger.info(
        "[api] GET /assets | tables=risklens_catalog.assets+ownership+quality_scores+sla_status | domain=%s | layer=%s | type=%s | limit=%d",
        domain or "all", layer or "all", type or "all", limit,
        extra={"json_fields": {"domain": domain, "layer": layer, "type": type, "limit": limit}},
    )
    filters = ["1=1"]
    if domain:
        filters.append(f"a.domain = '{domain}'")
    if layer:
        filters.append(f"a.layer = '{layer}'")
    if type:
        filters.append(f"a.type = '{type}'")
    where = " AND ".join(filters)

    sql = f"""
        SELECT
            a.asset_id,
            a.name,
            a.type,
            a.domain,
            a.layer,
            a.description,
            a.tags,
            a.row_count,
            a.updated_at,
            o.owner_name,
            o.team,
            q.freshness_status,
            q.null_rate,
            s.breach_flag
        FROM `{project()}.risklens_catalog.assets` a
        LEFT JOIN `{project()}.risklens_catalog.ownership` o USING (asset_id)
        LEFT JOIN (
            SELECT asset_id, freshness_status, null_rate
            FROM `{project()}.risklens_catalog.quality_scores`
            QUALIFY ROW_NUMBER() OVER (PARTITION BY asset_id ORDER BY last_checked DESC) = 1
        ) q USING (asset_id)
        LEFT JOIN (
            SELECT asset_id, breach_flag
            FROM `{project()}.risklens_catalog.sla_status`
            QUALIFY ROW_NUMBER() OVER (PARTITION BY asset_id ORDER BY checked_at DESC) = 1
        ) s USING (asset_id)
        WHERE {where}
        ORDER BY a.domain, a.layer, a.name
        LIMIT {limit}
    """
    rows = [_fix_tags(r) for r in query_rows(sql)]
    logger.info(
        "[api] ✓ GET /assets | tables=risklens_catalog.assets+ownership+quality_scores+sla_status | rows=%d | domain=%s | layer=%s",
        len(rows), domain or "all", layer or "all",
        extra={"json_fields": {"result_count": len(rows), "domain": domain, "layer": layer}},
    )
    if not rows:
        logger.warning("[api] GET /assets returned 0 results | tables=risklens_catalog.assets | domain=%s | layer=%s | type=%s", domain, layer, type)
    return rows


@router.get("/{asset_id}")
def get_asset(asset_id: str):
    logger.info("[api] GET /assets/%s | tables=risklens_catalog.assets+ownership+quality_scores+sla_status", asset_id, extra={"json_fields": {"asset_id": asset_id}})
    sql = f"""
        SELECT
            a.*,
            o.owner_name,
            o.team,
            o.steward,
            o.email,
            q.null_rate,
            q.schema_drift,
            q.freshness_status,
            q.duplicate_rate,
            q.last_checked,
            s.expected_refresh,
            s.actual_refresh,
            s.breach_flag,
            s.breach_duration_mins
        FROM `{project()}.risklens_catalog.assets` a
        LEFT JOIN `{project()}.risklens_catalog.ownership` o USING (asset_id)
        LEFT JOIN `{project()}.risklens_catalog.quality_scores` q USING (asset_id)
        LEFT JOIN `{project()}.risklens_catalog.sla_status` s USING (asset_id)
        WHERE a.asset_id = '{asset_id}'
        LIMIT 1
    """
    rows = query_rows(sql)
    if not rows:
        logger.warning("[api] GET /assets/%s: asset not found | table=risklens_catalog.assets", asset_id, extra={"json_fields": {"asset_id": asset_id}})
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Asset '{asset_id}' not found")
    result = _fix_tags(rows[0])
    logger.info(
        "[api] ✓ GET /assets/%s | name=%s | layer=%s | domain=%s",
        asset_id, result.get("name"), result.get("layer"), result.get("domain"),
        extra={"json_fields": {"asset_id": asset_id, "name": result.get("name")}},
    )
    return result


@router.get("/{asset_id}/schema")
def get_asset_schema(asset_id: str):
    logger.info("[api] GET /assets/%s/schema | table=risklens_catalog.schema_registry", asset_id, extra={"json_fields": {"asset_id": asset_id}})
    sql = f"""
        SELECT column_name, data_type, nullable, description, sample_value
        FROM `{project()}.risklens_catalog.schema_registry`
        WHERE asset_id = '{asset_id}'
        ORDER BY column_name
    """
    rows = query_rows(sql)
    logger.info(
        "[api] ✓ GET /assets/%s/schema | table=risklens_catalog.schema_registry | columns=%d",
        asset_id, len(rows),
        extra={"json_fields": {"asset_id": asset_id, "column_count": len(rows)}},
    )
    if not rows:
        logger.warning("[api] GET /assets/%s/schema: no columns found | table=risklens_catalog.schema_registry", asset_id)
    return rows
