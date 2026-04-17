# RiskLens BigQuery Table Stats

**Project:** `risklens-frtb-2026`  
**Summary:** 42 tables across 6 datasets | 79,123 total rows | 20.245 MB total  
**Collected:** 2026-04-17T05:52:23.644190Z  

## risklens_bronze
**8 tables** | 72,193 total rows | 18.637 MB total

| Table | Rows | Size (MB) | Partition Field | Clustering | Cols | Key Columns |
|-------|------|-----------|----------------|------------|------|-------------|
| `_backup_prices_r_20260417` | 2,111 | 0.22 | `-` | - | 13 | ticker, name, asset_class, currency, date |
| `_backup_rates_r_20260417` | 1,913 | 0.142 | `-` | - | 8 | series_id, series_name, frequency, domain, date |
| `pipeline_logs_s` | 138 | 0.014 | `-` | - | 9 | job_id, job_name, run_date, status, rows_read |
| `prices_r` | 2,111 | 0.22 | `date` | ticker, asset_class, currency | 13 | ticker, name, asset_class, currency, date |
| `quarantine_r` | 84 | 0.006 | `-` | - | 5 | source_table, rejection_reason, quarantined_at, trade_date, rejected_count |
| `rates_r` | 5,606 | 0.447 | `date` | series_id, domain | 8 | series_id, series_name, frequency, domain, date |
| `risk_outputs_s` | 230 | 0.153 | `-` | - | 16 | calc_date, desk, var_99_1d, var_99_10d, es_975_1d |
| `trades_r` | 60,000 | 17.435 | `ingested_at` | asset_class | 16 | dissemination_id, original_dissemination_id, action, execution_timestamp, cleared |

## risklens_silver
**6 tables** | 3,433 total rows | 0.333 MB total

| Table | Rows | Size (MB) | Partition Field | Clustering | Cols | Key Columns |
|-------|------|-----------|----------------|------------|------|-------------|
| `_archive_prices_r_20260415` | 1,103 | 0.123 | `-` | - | 14 | ticker, name, asset_class, currency, date |
| `_archive_rates_r_20260415` | 588 | 0.048 | `-` | - | 10 | series_id, series_name, frequency, domain, date |
| `_archive_risk_outputs_s_20260415` | 325 | 0.04 | `-` | - | 15 | desk, calc_date, var_99_1d, var_99_10d, es_975_1d |
| `rates` | 1,352 | 0.113 | `date` | series_id, domain | 9 | series_id, series_name, frequency, domain, date |
| `risk_enriched` | 45 | 0.006 | `calc_date` | desk | 16 | desk, calc_date, var_99_1d, var_99_10d, es_975_1d |
| `risk_outputs` | 20 | 0.003 | `calc_date` | desk | 17 | calc_date, desk, var_99_1d, var_99_10d, es_975_1d |

## risklens_gold
**7 tables** | 443 total rows | 0.044 MB total

| Table | Rows | Size (MB) | Partition Field | Clustering | Cols | Key Columns |
|-------|------|-----------|----------------|------------|------|-------------|
| `backtesting` | 83 | 0.012 | `calc_date` | desk, traffic_light_zone | 17 | desk, calc_date, var_99_1d, var_99_10d, hypothetical_pnl |
| `capital_charge` | 131 | 0.013 | `calc_date` | desk, risk_class | 13 | calc_date, desk, risk_class, liquidity_horizon, es_975_1d |
| `es_outputs` | 83 | 0.006 | `calc_date` | desk, risk_class | 9 | calc_date, desk, risk_class, liquidity_horizon, es_975_1d |
| `plat_results` | 15 | 0.002 | `calc_date` | desk, plat_pass | 18 | calc_date, desk, window_start_date, window_end_date, observation_count |
| `pnl_vectors` | 80 | 0.007 | `calc_date` | desk, risk_class | 12 | calc_date, desk, risk_class, hypothetical_pnl, actual_pnl |
| `rfet_results` | 45 | 0.003 | `rfet_date` | risk_class, rfet_pass | 13 | rfet_date, risk_factor_id, risk_class, obs_12m_count, obs_90d_count |
| `risk_summary` | 6 | 0.001 | `calc_date` | desk, risk_class | 20 | calc_date, desk, risk_class, liquidity_horizon, var_99_1d |

## risklens_catalog
**13 tables** | 2,660 total rows | 0.225 MB total

| Table | Rows | Size (MB) | Partition Field | Clustering | Cols | Key Columns |
|-------|------|-----------|----------------|------------|------|-------------|
| `_archive_access_log_r_20260415` | 132 | 0.016 | `-` | - | 9 | job_id, job_name, run_date, status, rows_read |
| `_archive_assets_s_20260415` | 16 | 0.005 | `-` | - | 11 | asset_id, name, type, domain, layer |
| `_archive_ownership_s_20260415` | 16 | 0.001 | `-` | - | 6 | asset_id, owner_name, team, steward, email |
| `_archive_quality_scores_s_20260415` | 352 | 0.021 | `-` | - | 6 | asset_id, null_rate, schema_drift, freshness_status, duplicate_rate |
| `_archive_schema_registry_s_20260415` | 0 | 0.0 | `-` | - | 6 | asset_id, column_name, data_type, nullable, sample_value |
| `_archive_sla_status_s_20260415` | 352 | 0.03 | `-` | - | 6 | asset_id, expected_refresh, actual_refresh, breach_flag, breach_duration_mins |
| `access_log` | 589 | 0.082 | `timestamp` | page, action | 12 | event_id, page, action, detail, ip_address |
| `assets` | 21 | 0.006 | `-` | domain, layer, type | 11 | asset_id, name, type, domain, layer |
| `desk_registry` | 5 | 0.001 | `-` | risk_class, business_line | 12 | desk_id, desk_name, risk_class, business_line, approved_by |
| `ownership` | 21 | 0.002 | `-` | team | 6 | asset_id, owner_name, team, steward, email |
| `quality_scores` | 485 | 0.024 | `last_checked` | asset_id, freshness_status | 6 | asset_id, null_rate, schema_drift, freshness_status, duplicate_rate |
| `schema_registry` | 181 | 0.013 | `-` | asset_id | 6 | asset_id, column_name, data_type, nullable, description |
| `sla_status` | 490 | 0.024 | `-` | - | 6 | asset_id, expected_refresh, actual_refresh, breach_flag, breach_duration_mins |

## risklens_lineage
**4 tables** | 96 total rows | 0.009 MB total

| Table | Rows | Size (MB) | Partition Field | Clustering | Cols | Key Columns |
|-------|------|-----------|----------------|------------|------|-------------|
| `_archive_edges_s_20260415` | 16 | 0.002 | `-` | - | 6 | edge_id, from_node_id, to_node_id, relationship, pipeline_job |
| `_archive_nodes_s_20260415` | 24 | 0.002 | `-` | - | 7 | node_id, name, type, domain, layer |
| `edges` | 27 | 0.003 | `-` | relationship | 6 | edge_id, from_node_id, to_node_id, relationship, pipeline_job |
| `nodes` | 29 | 0.002 | `-` | type, domain | 7 | node_id, name, type, domain, layer |

## risklens_embeddings
**4 tables** | 298 total rows | 0.997 MB total

| Table | Rows | Size (MB) | Partition Field | Clustering | Cols | Key Columns |
|-------|------|-----------|----------------|------------|------|-------------|
| `_archive_chunks_s_20260415` | 40 | 0.013 | `-` | - | 6 | chunk_id, asset_id, text, source_type, domain |
| `_archive_vectors_s_20260415` | 40 | 0.236 | `-` | - | 2 | chunk_id, embedding |
| `chunks` | 109 | 0.106 | `-` | source_type, domain | 6 | chunk_id, asset_id, text, source_type, domain |
| `vectors` | 109 | 0.642 | `-` | - | 2 | chunk_id, embedding |

