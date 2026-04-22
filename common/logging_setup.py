"""
RiskLens — Centralized Cloud Logging Setup

Installs Google Cloud Logging as the root Python log handler so every
logger.info/warning/error call in the API and indexing pipeline is
automatically forwarded to GCP Cloud Logging (Logs Explorer).

Falls back to basicConfig gracefully for local development when ADC /
google-cloud-logging is unavailable.

Usage:
    from common.logging_setup import setup_cloud_logging
    setup_cloud_logging()                        # call once at process start
    logger = logging.getLogger(__name__)         # then use as normal

Structured labels:
    setup_cloud_logging(labels={"service": "api", "layer": "gold"})
    → every log entry carries these as searchable fields in Logs Explorer
"""

import logging
import os


def setup_cloud_logging(
    level: int = logging.INFO,
    labels: dict | None = None,
) -> None:
    """
    Install Google Cloud Logging as the root handler.

    On GCP (GKE, Cloud Run, Dataproc, Cloud Composer) this routes all
    Python log output to Cloud Logging where it is automatically indexed,
    searchable, and available in Logs Explorer alongside infra logs.

    Locally (no GOOGLE_APPLICATION_CREDENTIALS / ADC), it falls back to a
    human-readable basicConfig format so development is unaffected.

    Args:
        level:  Python logging level to apply (default INFO).
        labels: Optional dict of key/value labels attached to every log entry.
                Shown as structured fields in Logs Explorer — ideal for
                filtering by service, layer, job, environment, etc.
                Example: {"service": "api", "env": "prod"}
    """
    # Always attach a default "app" label so all RiskLens logs are
    # identifiable even without extra labels
    merged_labels: dict = {"app": "risklens"}
    if labels:
        merged_labels.update(labels)

    try:
        import google.cloud.logging as cloud_logging  # noqa: PLC0415
        from google.cloud.logging.handlers import CloudLoggingHandler  # noqa: PLC0415

        client = cloud_logging.Client()
        client.setup_logging(log_level=level, labels=merged_labels)

    except Exception as exc:
        # Local dev fallback — google-cloud-logging not installed or no ADC
        logging.basicConfig(
            level=level,
            format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        )
        logging.getLogger(__name__).debug(
            "Cloud Logging unavailable (%s) — using local stderr handler.", exc
        )
