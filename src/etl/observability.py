"""Structured run events for the observability stack.

The pipeline emits machine-readable events rather than only human-readable log lines. That choice
is the difference between Logstash doing *enrichment* and Logstash doing *archaeology*: parsing
``Transform complete: 541909 -> 524876 rows (17033 dropped, 3.1%) in 10.50s`` with a grok pattern
works right up until someone rewords the message, at which point the dashboard silently loses a
metric and nobody notices for a month.

Events are written as JSON Lines to a file that Logstash tails. A file rather than a direct HTTP
shipper on purpose: if Elasticsearch is down, an ETL run must still succeed. Logging is not
important enough to fail a data load over, and a file gives free durability and backpressure —
Logstash catches up when the cluster returns.

Field names follow Elastic Common Schema (``event.dataset``, ``event.duration``, ``error.message``)
so the documents work with stock Kibana tooling instead of needing bespoke field mappings for
every panel.
"""

from __future__ import annotations

import json
import logging
import os
import socket
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

logger = logging.getLogger(__name__)

DEFAULT_EVENT_LOG = Path(__file__).resolve().parents[2] / "logs" / "pipeline-events.jsonl"

#: One id per pipeline invocation, stamped on every event it produces. This is what turns a wall
#: of log lines into "show me everything that happened during the run that failed at 03:14".
_RUN_ID = os.environ.get("PIPELINE_RUN_ID") or uuid.uuid4().hex[:12]


def run_id() -> str:
    return _RUN_ID


def _event_log_path() -> Path:
    return Path(os.environ.get("PIPELINE_EVENT_LOG", DEFAULT_EVENT_LOG))


def _base_event(stage: str, outcome: str) -> dict[str, Any]:
    return {
        # Elasticsearch's default date detection understands ISO-8601 with an offset. Emitting a
        # naive local timestamp is the classic way to end up with dashboards silently shifted by
        # the difference between the host and the cluster.
        "@timestamp": datetime.now(timezone.utc).isoformat(),
        "event": {
            "dataset": "retail.pipeline",
            "module": "etl",
            "kind": "event",
        },
        "service": {
            "name": "retail-data-pipeline",
            "environment": os.environ.get("PIPELINE_ENV", "local"),
        },
        "host": {"name": socket.gethostname()},
        "run": {"id": _RUN_ID},
        "stage": stage,
        "outcome": outcome,
    }


def emit(stage: str, outcome: str, **fields: Any) -> dict[str, Any]:
    """Append one event to the JSON Lines log.

    Never raises. An observability failure must not be able to take down the pipeline it is
    observing — that inverts the entire point of the thing.
    """
    event = _base_event(stage, outcome)
    event.update(fields)

    try:
        path = _event_log_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        # Each event is a complete line, so a crash mid-run leaves a truncated final line at
        # worst rather than a corrupt file Logstash cannot parse.
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, default=str) + "\n")
    except Exception:  # pragma: no cover - defensive by design
        logger.warning("could not write run event for stage %s", stage, exc_info=True)

    return event


@contextmanager
def observed_stage(stage: str, **context: Any) -> Iterator[dict[str, Any]]:
    """Time a stage and emit exactly one event describing how it went.

    The block receives a dict; anything put into it is merged into the event as metrics. That
    keeps the measurement in one place instead of every caller remembering to log duration, and
    guarantees a failed stage still emits — a stage that only reports on success produces a
    dashboard where failures are invisible, which is precisely backwards.

    Usage::

        with observed_stage("transform") as metrics:
            metrics["rows_in"] = len(raw)
            metrics["rows_out"] = len(clean)
    """
    started = time.perf_counter()
    metrics: dict[str, Any] = {}

    try:
        yield metrics
    except Exception as exc:
        emit(
            stage,
            "failure",
            metrics=metrics,
            error={"type": type(exc).__name__, "message": str(exc)},
            duration_ms=round((time.perf_counter() - started) * 1000, 2),
            **context,
        )
        raise
    else:
        emit(
            stage,
            "success",
            metrics=metrics,
            duration_ms=round((time.perf_counter() - started) * 1000, 2),
            **context,
        )


def emit_quality_findings(stage: str, drift: dict[str, Any], expectations: dict[str, Any]) -> None:
    """Emit one event per data-quality finding.

    One event per finding rather than one blob per run, so Kibana can aggregate on
    ``finding.column`` and answer "which column breaks most often" — a question a single nested
    document cannot answer without a scripted field.
    """
    for finding in drift.get("findings", []):
        emit(
            stage,
            "finding",
            finding={
                "category": "schema_drift",
                "kind": finding.get("kind"),
                "severity": finding.get("severity"),
                "column": finding.get("column"),
                "detail": finding.get("detail"),
            },
        )

    for failure in expectations.get("failures", []):
        emit(
            stage,
            "finding",
            finding={
                "category": "expectation",
                "kind": failure.get("expectation"),
                "severity": "breaking",
                "column": failure.get("column"),
                "detail": f"observed {failure.get('observed')}",
            },
        )
