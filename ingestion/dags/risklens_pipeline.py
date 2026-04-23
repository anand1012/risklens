"""
RiskLens — Cloud Composer DAG
Orchestrates the full Bronze → Silver → Gold pipeline.

DAG: risklens_pipeline
Schedule: 0 6 * * 1-5  (weekdays at 6am ET)

Execution order:
  [bronze_trades, bronze_rates, bronze_prices, bronze_synthetic]  ← parallel
                              ↓
                    silver_transform   ← clean / dedup / outlier flag
                              ↓
                    silver_enrich      ← join risk_outputs × rates → risk_enriched
                              ↓
                    gold_aggregate     ← FRTB IMA metrics
                              ↓
                    trigger_indexing   ← triggers RAG index rebuild

Notes:
  - Each task submits a Spark job to an ephemeral Dataproc cluster
  - Cluster is created at DAG start, deleted at DAG end (cost control)
  - On failure: Slack alert + quarantine report emailed
  - initial_load mode: processes --days 30 (used by setup_composer.sh)
  - daily mode: processes --days 1 (normal schedule)
"""

import logging
import os
from datetime import datetime, timedelta

log = logging.getLogger("risklens_pipeline")

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule

# ── Config ────────────────────────────────────────────────────────────────────

PROJECT_ID   = Variable.get("risklens_project_id",   default_var="risklens-frtb-2026")
REGION       = Variable.get("risklens_region",        default_var="us-central1")
BUCKET       = Variable.get("risklens_bucket",        default_var=f"risklens-raw-{PROJECT_ID}")
DAYS         = int(Variable.get("risklens_days",      default_var="1"))
CLUSTER_NAME = f"risklens-dataproc-{{{{ ds_nodash }}}}"   # unique per run date

SPARK_BQ_JAR = "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.36.1.jar"

DEFAULT_ARGS = {
    "owner":            "risklens",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
    "start_date":       datetime(2026, 4, 1),
}

# ── Cluster config (single-node, ephemeral) ───────────────────────────────────

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 100},
    },
    "worker_config": {"num_instances": 0},   # single-node
    "software_config": {
        "image_version": "2.1-debian11",
        "properties": {
            "spark:spark.jars": SPARK_BQ_JAR,
        },
    },
    "gce_cluster_config": {
        "service_account": f"risklens-sa@{PROJECT_ID}.iam.gserviceaccount.com",
        "service_account_scopes": ["https://www.googleapis.com/auth/cloud-platform"],
    },
}


def make_pyspark_job(script: str, extra_args: list[str] | None = None) -> dict:
    """Build a Dataproc PySpark job definition."""
    log.debug(f"[dag] Building PySpark job definition | script={script} | cluster={CLUSTER_NAME} | project={PROJECT_ID} | extra_args={extra_args}")
    args = [
        f"--project={PROJECT_ID}",
        f"--bucket={BUCKET}",
        f"--days={DAYS}",
    ]
    if extra_args:
        args.extend(extra_args)

    job = {
        "reference":  {"project_id": PROJECT_ID},
        "placement":  {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{BUCKET}/jobs/{script}",
            "args": args,
            "jar_file_uris": [SPARK_BQ_JAR],
            "properties": {
                "spark.executor.memory":   "4g",
                "spark.driver.memory":     "4g",
                "spark.sql.adaptive.enabled": "true",
            },
        },
    }
    log.debug(f"[dag] PySpark job definition ready | script={script} | gcs_uri=gs://{BUCKET}/jobs/{script} | args={args}")
    return job


def make_silver_job() -> dict:
    """Silver job uses --date instead of --days."""
    log.debug(f"[dag] Building silver_transform job | script=silver_transform.py | cluster={CLUSTER_NAME} | date_arg={{{{ ds }}}}")
    return {
        "reference":  {"project_id": PROJECT_ID},
        "placement":  {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{BUCKET}/jobs/silver_transform.py",
            "args": [
                f"--project={PROJECT_ID}",
                f"--bucket={BUCKET}",
                "--date={{ ds }}",          # Airflow macro: execution date
            ],
            "jar_file_uris": [SPARK_BQ_JAR],
            "properties": {
                "spark.executor.memory":   "4g",
                "spark.driver.memory":     "4g",
                "spark.sql.adaptive.enabled": "true",
            },
        },
    }


def make_silver_enrich_job() -> dict:
    """Silver enrich job: joins risk_outputs × rates → risk_enriched."""
    log.debug(f"[dag] Building silver_enrich job | script=silver_enrich.py | cluster={CLUSTER_NAME} | date_arg={{{{ ds }}}}")
    return {
        "reference":  {"project_id": PROJECT_ID},
        "placement":  {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{BUCKET}/jobs/silver_enrich.py",
            "args": [
                f"--project={PROJECT_ID}",
                f"--bucket={BUCKET}",
                "--date={{ ds }}",
            ],
            "jar_file_uris": [SPARK_BQ_JAR],
            "properties": {
                "spark.executor.memory":   "4g",
                "spark.driver.memory":     "4g",
                "spark.sql.adaptive.enabled": "true",
            },
        },
    }


def make_gold_job() -> dict:
    log.debug(f"[dag] Building gold_aggregate job | script=gold_aggregate.py | cluster={CLUSTER_NAME} | date_arg={{{{ ds }}}}")
    return {
        "reference":  {"project_id": PROJECT_ID},
        "placement":  {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{BUCKET}/jobs/gold_aggregate.py",
            "args": [
                f"--project={PROJECT_ID}",
                f"--bucket={BUCKET}",
                "--date={{ ds }}",
            ],
            "jar_file_uris": [SPARK_BQ_JAR],
            "properties": {
                "spark.executor.memory":   "4g",
                "spark.driver.memory":     "4g",
                "spark.sql.adaptive.enabled": "true",
            },
        },
    }


def log_pipeline_run(**context):
    """Log pipeline run metadata to BigQuery access_log and Cloud Logging."""
    from google.cloud import bigquery
    from datetime import datetime
    import uuid

    # Set up Cloud Logging inside the callable (runs in Airflow worker context)
    try:
        import google.cloud.logging as _cloud_logging
        _cloud_logging.Client().setup_logging(
            log_level=logging.INFO,
            labels={"app": "risklens", "service": "ingestion", "layer": "orchestration", "job": "risklens_pipeline"},
        )
    except Exception:
        logging.basicConfig(level=logging.INFO)
    log = logging.getLogger("risklens_pipeline")

    ds = context["ds"]
    run_id = context.get("run_id", "unknown")
    dag_run = context.get("dag_run")
    start_time = dag_run.start_date if dag_run else None
    elapsed = (datetime.utcnow() - start_time).seconds if start_time else "unknown"
    log.info(f"[dag] Pipeline run complete | dag=risklens_pipeline | date={ds} | run_id={run_id} | elapsed_secs={elapsed} | stages=bronze(×4)→silver_transform→silver_enrich→gold_aggregate | program=risklens_pipeline.py")

    try:
        client = bigquery.Client(project=PROJECT_ID)
        event_id = str(uuid.uuid4())
        rows = [{
            "event_id":   event_id,
            "page":       "pipeline",
            "action":     "dag_run",
            "detail":     f"risklens_pipeline run for {ds} (run_id={run_id})",
            "ip_address": "internal",
            "timestamp":  datetime.utcnow().isoformat(),
        }]
        log.info(f"[dag] Writing pipeline audit log | table=risklens_catalog.access_log | event_id={event_id} | date={ds} | run_id={run_id}")
        client.insert_rows_json(
            f"{PROJECT_ID}.risklens_catalog.access_log", rows
        )
        log.info(f"[dag] ✓ Pipeline audit log written | table=risklens_catalog.access_log | event_id={event_id} | date={ds}")
    except Exception as exc:
        log.error(f"[dag] FAILED: Could not write pipeline audit log | table=risklens_catalog.access_log | date={ds} | run_id={run_id} | error={exc}", exc_info=True)


# ── DAG Definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="risklens_pipeline",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 * * 1-5",   # weekdays 6am ET
    catchup=False,
    max_active_runs=1,
    tags=["risklens", "frtb", "ingestion"],
    doc_md="""
    ## RiskLens Pipeline
    Full Bronze → Silver → Gold ingestion pipeline.
    Runs weekdays at 6am ET. Creates ephemeral Dataproc cluster,
    runs all jobs, deletes cluster.
    """,
) as dag:

    start = EmptyOperator(task_id="start")

    # ── Create Dataproc cluster ───────────────────────────────────────────────
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    # ── Bronze jobs (run in parallel) ─────────────────────────────────────────
    bronze_trades = DataprocSubmitJobOperator(
        task_id="bronze_trades",
        job=make_pyspark_job("bronze_trades.py"),
        region=REGION,
        project_id=PROJECT_ID,
    )

    bronze_rates = DataprocSubmitJobOperator(
        task_id="bronze_rates",
        job=make_pyspark_job("bronze_rates.py"),
        region=REGION,
        project_id=PROJECT_ID,
    )

    bronze_prices = DataprocSubmitJobOperator(
        task_id="bronze_prices",
        job=make_pyspark_job("bronze_prices.py"),
        region=REGION,
        project_id=PROJECT_ID,
    )

    bronze_synthetic = DataprocSubmitJobOperator(
        task_id="bronze_synthetic",
        job=make_pyspark_job("bronze_synthetic.py"),
        region=REGION,
        project_id=PROJECT_ID,
    )

    # ── Silver transform (after all bronze jobs complete) ─────────────────────
    silver = DataprocSubmitJobOperator(
        task_id="silver_transform",
        job=make_silver_job(),
        region=REGION,
        project_id=PROJECT_ID,
    )

    # ── Silver enrich (after transform — joins risk_outputs × rates) ────────────
    silver_enrich = DataprocSubmitJobOperator(
        task_id="silver_enrich",
        job=make_silver_enrich_job(),
        region=REGION,
        project_id=PROJECT_ID,
    )

    # ── Gold aggregate (after silver enrich) ──────────────────────────────────
    gold = DataprocSubmitJobOperator(
        task_id="gold_aggregate",
        job=make_gold_job(),
        region=REGION,
        project_id=PROJECT_ID,
    )

    # ── Log pipeline run ──────────────────────────────────────────────────────
    log_run = PythonOperator(
        task_id="log_pipeline_run",
        python_callable=log_pipeline_run,
        provide_context=True,
    )

    # ── Delete cluster (always runs — even if jobs fail) ──────────────────────
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule=TriggerRule.ALL_DONE,   # runs even on failure
    )

    end = EmptyOperator(task_id="end")

    # ── Dependencies ──────────────────────────────────────────────────────────
    (
        start
        >> create_cluster
        >> [bronze_trades, bronze_rates, bronze_prices, bronze_synthetic]
        >> silver
        >> silver_enrich
        >> gold
        >> log_run
        >> delete_cluster
        >> end
    )
