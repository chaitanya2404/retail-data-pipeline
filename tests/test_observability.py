"""Tests for the structured run-event emitter.

These assert the properties Logstash and the index template depend on. A change that quietly
breaks one of them does not fail the pipeline — it produces documents Elasticsearch rejects, which
shows up as an empty dashboard rather than an error, so it needs to be caught here.
"""

from __future__ import annotations

import json

import pytest

from src.etl import observability


@pytest.fixture
def event_log(tmp_path, monkeypatch):
    """Redirect the event log into a temp file so tests never touch the real one."""
    path = tmp_path / "events.jsonl"
    monkeypatch.setenv("PIPELINE_EVENT_LOG", str(path))
    return path


def _read(path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


class TestEmit:
    def test_writes_one_json_line_per_event(self, event_log):
        observability.emit("extract", "success", metrics={"rows": 10})
        observability.emit("transform", "success", metrics={"rows": 8})

        events = _read(event_log)
        assert [e["stage"] for e in events] == ["extract", "transform"]

    def test_timestamp_is_timezone_aware_iso8601(self, event_log):
        # A naive local timestamp indexes without complaint and shifts every dashboard by the
        # offset between the host and the cluster — the kind of bug found weeks later.
        observability.emit("extract", "success")

        stamp = _read(event_log)[0]["@timestamp"]
        assert stamp.endswith("+00:00") or stamp.endswith("Z")

    def test_every_event_carries_the_run_id(self, event_log):
        observability.emit("extract", "success")
        observability.emit("load", "failure")

        ids = {e["run"]["id"] for e in _read(event_log)}
        # One id per invocation is what lets Kibana reconstruct a single run from a mixed index.
        assert len(ids) == 1

    def test_emit_never_raises_on_an_unwritable_path(self, monkeypatch, tmp_path):
        # Observability must not be able to take down the pipeline it observes.
        blocker = tmp_path / "blocker"
        blocker.write_text("not a directory", encoding="utf-8")
        monkeypatch.setenv("PIPELINE_EVENT_LOG", str(blocker / "events.jsonl"))

        observability.emit("extract", "success")  # must not raise

    def test_event_is_json_serialisable_with_odd_values(self, event_log):
        from datetime import date

        # default=str keeps a stray date or Decimal from killing the whole run.
        observability.emit("screen", "success", metrics={"as_of": date(2011, 12, 9)})

        assert _read(event_log)[0]["metrics"]["as_of"] == "2011-12-09"


class TestObservedStage:
    def test_success_records_duration_and_metrics(self, event_log):
        with observability.observed_stage("transform") as metrics:
            metrics["rows_in"] = 100
            metrics["rows_out"] = 90

        event = _read(event_log)[0]
        assert event["outcome"] == "success"
        assert event["metrics"] == {"rows_in": 100, "rows_out": 90}
        assert isinstance(event["duration_ms"], float)

    def test_failure_still_emits_then_reraises(self, event_log):
        # A stage that only reports on success produces a dashboard where failures are invisible,
        # which is exactly backwards.
        with pytest.raises(ValueError):
            with observability.observed_stage("load") as metrics:
                metrics["rows"] = 5
                raise ValueError("connection refused")

        event = _read(event_log)[0]
        assert event["outcome"] == "failure"
        assert event["error"]["type"] == "ValueError"
        assert "connection refused" in event["error"]["message"]
        # Metrics gathered before the failure survive — they are usually the clue.
        assert event["metrics"]["rows"] == 5

    def test_exactly_one_event_per_stage(self, event_log):
        with observability.observed_stage("screen"):
            pass

        assert len(_read(event_log)) == 1


class TestQualityFindings:
    def test_one_document_per_finding(self, event_log):
        drift = {
            "findings": [
                {
                    "kind": "missing_column",
                    "severity": "breaking",
                    "column": "Country",
                    "detail": "absent",
                },
                {
                    "kind": "new_column",
                    "severity": "additive",
                    "column": "LoyaltyTier",
                    "detail": "undeclared",
                },
            ]
        }
        expectations = {
            "failures": [
                {"expectation": "expect_not_null", "column": "UnitPrice", "observed": 0.2}
            ]
        }

        observability.emit_quality_findings("screen", drift, expectations)

        events = _read(event_log)
        # One document each, not one blob: aggregating on finding.column is how "which column
        # breaks most often" gets answered, and a nested array cannot do that without scripting.
        assert len(events) == 3
        assert {e["finding"]["column"] for e in events} == {"Country", "LoyaltyTier", "UnitPrice"}
        assert {e["outcome"] for e in events} == {"finding"}

    def test_categories_distinguish_drift_from_expectations(self, event_log):
        observability.emit_quality_findings(
            "screen",
            {"findings": [{"kind": "type_change", "severity": "breaking", "column": "Quantity"}]},
            {"failures": [{"expectation": "expect_between", "column": "Quantity"}]},
        )

        categories = {e["finding"]["category"] for e in _read(event_log)}
        assert categories == {"schema_drift", "expectation"}

    def test_clean_run_emits_nothing(self, event_log):
        observability.emit_quality_findings("screen", {"findings": []}, {"failures": []})

        assert not event_log.exists() or _read(event_log) == []
