# RiskLens BigQuery Table Stats

**Project:** `risklens-frtb-2026`  
**Summary:** 48 tables across 6 datasets | 1,508,080 total rows | 191.180 MB total  
**Collected:** 2026-04-15T05:13:21.358813Z  

## risklens_bronze
**6 tables** | 65,747 total rows | 18.330 MB total

| Table | Rows | Size (MB) | Partition Field | Clustering | Cols | Key Columns |
|-------|------|-----------|----------------|------------|------|-------------|
| `pipeline_logs_s` | 654 | 0.067 | `-` | - | 13 | job_id, job_name, run_date, status, rows_read |
| `prices_r` | 2,111 | 0.22 | `-` | - | 13 | ticker, name, asset_class, currency, date |
| `quarantine_r` | 84 | 0.006 | `-` | - | 5 | source_table, rejection_reason, quarantined_at, trade_date, rejected_count |
| `rates_r` | 1,913 | 0.142 | `-` | - | 8 | series_id, series_name, frequency, domain, date |
| `risk_outputs_s` | 985 | 0.46 | `-` | - | 16 | calc_date, desk, var_99_1d, var_99_10d, es_975_1d |
| `trades_r` | 60,000 | 17.435 | `ingested_at` | asset_class | 16 | dissemination_id, original_dissemination_id, action, execution_timestamp, cleared |

## risklens_silver
**9 tables** | 64,518 total rows | 11.278 MB total

| Table | Rows | Size (MB) | Partition Field | Clustering | Cols | Key Columns |
|-------|------|-----------|----------------|------------|------|-------------|
| `positions` | 0 | 0.0 | `processed_at` | asset_class, currency | 10 | asset_class, currency, total_notional, trade_count, mark_price |
| `prices` | 1,183 | 0.123 | `date` | ticker, asset_class, currency | 13 | ticker, name, asset_class, currency, date |
| `prices_r` | 1,103 | 0.123 | `-` | - | 14 | ticker, name, asset_class, currency, date |
| `rates` | 669 | 0.05 | `date` | series_id, domain | 9 | series_id, series_name, frequency, domain, date |
| `rates_r` | 588 | 0.048 | `-` | - | 10 | series_id, series_name, frequency, domain, date |
| `risk_enriched` | 325 | 0.041 | `calc_date` | desk | 16 | desk, calc_date, var_99_1d, var_99_10d, es_975_1d |
| `risk_outputs` | 325 | 0.049 | `calc_date` | desk | 17 | calc_date, desk, var_99_1d, var_99_10d, es_975_1d |
| `risk_outputs_s` | 325 | 0.04 | `-` | - | 15 | desk, calc_date, var_99_1d, var_99_10d, es_975_1d |
| `trades` | 60,000 | 10.804 | `execution_timestamp` | asset_class, currency | 15 | dissemination_id, original_dissemination_id, action, execution_timestamp, asset_class |

## risklens_gold
**12 tables** | 1,375,769 total rows | 160.546 MB total

| Table | Rows | Size (MB) | Partition Field | Clustering | Cols | Key Columns |
|-------|------|-----------|----------------|------------|------|-------------|
| `_archive_es_outputs_s_20260415` | 326 | 0.016 | `-` | - | 6 | calc_date, desk, es_975_1d, es_975_10d, trade_date |
| `_archive_pnl_vectors_s_20260415` | 325 | 0.02 | `-` | - | 8 | calc_date, desk, scenarios, num_scenarios, mean_pnl |
| `_archive_risk_summary_s_20260415` | 1,373,126 | 160.309 | `-` | - | 15 | calc_date, desk, var_99_1d, var_99_10d, method |
| `_archive_var_outputs_s_20260415` | 326 | 0.023 | `-` | - | 8 | calc_date, desk, var_99_1d, var_99_10d, method |
| `backtesting` | 326 | 0.043 | `calc_date` | desk, traffic_light_zone | 17 | desk, calc_date, var_99_1d, var_99_10d, hypothetical_pnl |
| `capital_charge` | 390 | 0.039 | `calc_date` | desk, risk_class | 13 | calc_date, desk, risk_class, liquidity_horizon, es_975_1d |
| `es_outputs` | 390 | 0.027 | `calc_date` | desk, risk_class | 9 | calc_date, desk, risk_class, liquidity_horizon, es_975_1d |
| `plat_results` | 5 | 0.001 | `calc_date` | desk, plat_pass | 18 | calc_date, desk, window_start_date, window_end_date, observation_count |
| `pnl_vectors` | 215 | 0.019 | `calc_date` | desk, risk_class | 12 | calc_date, desk, risk_class, hypothetical_pnl, actual_pnl |
| `rfet_results` | 14 | 0.001 | `rfet_date` | risk_class, rfet_pass | 13 | rfet_date, risk_factor_id, risk_class, obs_12m_count, obs_90d_count |
| `risk_summary` | 326 | 0.048 | `calc_date` | desk, risk_class | 20 | calc_date, desk, risk_class, liquidity_horizon, var_99_1d |
| `trade_positions` | 0 | 0.0 | `processed_at` | asset_class, currency | 11 | trade_id, currency, asset_class, underlying, notional_amount |

## risklens_catalog
**13 tables** | 1,700 total rows | 0.177 MB total

| Table | Rows | Size (MB) | Partition Field | Clustering | Cols | Key Columns |
|-------|------|-----------|----------------|------------|------|-------------|
| `access_log` | 584 | 0.081 | `timestamp` | page, action | 12 | event_id, page, action, detail, ip_address |
| `access_log_r` | 132 | 0.016 | `-` | - | 9 | job_id, job_name, run_date, status, rows_read |
| `assets` | 20 | 0.006 | `-` | domain, layer, type | 11 | asset_id, name, type, domain, layer |
| `assets_s` | 16 | 0.005 | `-` | - | 11 | asset_id, name, type, domain, layer |
| `desk_registry` | 5 | 0.001 | `-` | risk_class, business_line | 12 | desk_id, desk_name, risk_class, business_line, approved_by |
| `ownership` | 19 | 0.002 | `-` | team | 6 | asset_id, owner_name, team, steward, email |
| `ownership_s` | 16 | 0.001 | `-` | - | 6 | asset_id, owner_name, team, steward, email |
| `quality_scores` | 16 | 0.001 | `last_checked` | asset_id, freshness_status | 6 | asset_id, null_rate, schema_drift, freshness_status, duplicate_rate |
| `quality_scores_s` | 352 | 0.021 | `-` | - | 6 | asset_id, null_rate, schema_drift, freshness_status, duplicate_rate |
| `schema_registry` | 181 | 0.013 | `-` | asset_id | 6 | asset_id, column_name, data_type, nullable, description |
| `schema_registry_s` | 0 | 0.0 | `-` | - | 6 | asset_id, column_name, data_type, nullable, sample_value |
| `sla_status` | 7 | 0.0 | `-` | - | 6 | actual_refresh, asset_id, breach_duration_mins, breach_flag, checked_at |
| `sla_status_s` | 352 | 0.03 | `-` | - | 6 | asset_id, expected_refresh, actual_refresh, breach_flag, breach_duration_mins |

## risklens_lineage
**4 tables** | 96 total rows | 0.010 MB total

| Table | Rows | Size (MB) | Partition Field | Clustering | Cols | Key Columns |
|-------|------|-----------|----------------|------------|------|-------------|
| `edges` | 28 | 0.004 | `-` | relationship | 6 | edge_id, from_node_id, to_node_id, relationship, pipeline_job |
| `edges_s` | 16 | 0.002 | `-` | - | 6 | edge_id, from_node_id, to_node_id, relationship, pipeline_job |
| `nodes` | 28 | 0.002 | `-` | type, domain | 7 | node_id, name, type, domain, layer |
| `nodes_s` | 24 | 0.002 | `-` | - | 7 | node_id, name, type, domain, layer |

## risklens_embeddings
**4 tables** | 250 total rows | 0.839 MB total

| Table | Rows | Size (MB) | Partition Field | Clustering | Cols | Key Columns |
|-------|------|-----------|----------------|------------|------|-------------|
| `chunks` | 85 | 0.089 | `-` | source_type, domain | 6 | chunk_id, asset_id, text, source_type, domain |
| `chunks_s` | 40 | 0.013 | `-` | - | 6 | chunk_id, asset_id, text, source_type, domain |
| `vectors` | 85 | 0.501 | `-` | - | 2 | chunk_id, embedding |
| `vectors_s` | 40 | 0.236 | `-` | - | 2 | chunk_id, embedding |

