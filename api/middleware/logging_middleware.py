"""
RiskLens — HTTP Request/Response Logging Middleware

Logs every inbound request and its response to Cloud Logging:
  - method, path, query string
  - response status code
  - latency (ms)
  - client IP + User-Agent
  - x-session-id header (for chat correlation)

All fields are emitted as structured JSON labels so they are filterable
in Logs Explorer without text parsing.

Skips /health to avoid noise from k8s liveness/readiness probes.
"""

import logging
import os
import time

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger("risklens.http")

_SKIP_PATHS = {"/health"}


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        if request.url.path in _SKIP_PATHS:
            return await call_next(request)

        start = time.perf_counter()
        status_code = 500
        try:
            response: Response = await call_next(request)
            status_code = response.status_code
            return response
        except Exception as exc:
            logger.error(
                "Unhandled exception: %s %s → %s",
                request.method,
                request.url.path,
                exc,
                exc_info=True,
            )
            raise
        finally:
            latency_ms = round((time.perf_counter() - start) * 1000, 1)
            level = logging.WARNING if status_code >= 400 else logging.INFO
            logger.log(
                level,
                "%s %s %d  %.1fms",
                request.method,
                request.url.path,
                status_code,
                latency_ms,
                extra={
                    "json_fields": {
                        "http.method":     request.method,
                        "http.path":       request.url.path,
                        "http.query":      str(request.url.query) or None,
                        "http.status":     status_code,
                        "http.latency_ms": latency_ms,
                        "http.ip":         request.client.host if request.client else None,
                        "http.user_agent": request.headers.get("user-agent"),
                        "http.session_id": request.headers.get("x-session-id"),
                        "k8s.pod":         os.getenv("HOSTNAME",       "unknown"),
                        "k8s.namespace":   os.getenv("POD_NAMESPACE",  "unknown"),
                    }
                },
            )
