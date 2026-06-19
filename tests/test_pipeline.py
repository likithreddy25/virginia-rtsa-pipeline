"""
tests/test_pipeline.py
Unit tests for the Virginia RTSA pipeline — no GCP credentials required.
Apache Beam is stubbed out so tests run anywhere with only:
  pip install pytest python-dateutil

Tests cover:
  - ParseAndNormalize.make_id()  — deterministic hashing
  - ParseAndNormalize.process()  — valid message → correct BigQuery row
  - ParseAndNormalize.process()  — malformed message → graceful skip (no raise)
  - FACT_SCHEMA                  — all 16 required fields present
"""
import json
import sys
import os
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# Stub apache_beam and GCP sub-modules before importing dataflow_pipeline.
# This avoids pulling in GCP credentials or C-extension dependencies in CI.
# ---------------------------------------------------------------------------
_beam_stub = MagicMock()


class _DoFn:
    """Minimal DoFn base — just needs to exist so ParseAndNormalize can inherit."""
    pass


_beam_stub.DoFn = _DoFn
sys.modules.update({
    "apache_beam": _beam_stub,
    "apache_beam.options": MagicMock(),
    "apache_beam.options.pipeline_options": MagicMock(),
    "apache_beam.io": MagicMock(),
    "apache_beam.io.gcp": MagicMock(),
    "apache_beam.io.gcp.pubsub": MagicMock(),
    "apache_beam.io.gcp.bigquery": MagicMock(),
})

# Make the repo root importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dataflow_pipeline import ParseAndNormalize, FACT_SCHEMA  # noqa: E402


# ---------------------------------------------------------------------------
# make_id
# ---------------------------------------------------------------------------

def test_make_id_is_deterministic():
    fn = ParseAndNormalize()
    assert fn.make_id("38.86", "-77.24") == fn.make_id("38.86", "-77.24")


def test_make_id_is_16_chars():
    fn = ParseAndNormalize()
    assert len(fn.make_id("a", "b", "c")) == 16


def test_make_id_differs_on_different_inputs():
    fn = ParseAndNormalize()
    assert fn.make_id("lat1", "lon1") != fn.make_id("lat2", "lon2")


# ---------------------------------------------------------------------------
# process — valid message
# ---------------------------------------------------------------------------

SAMPLE_EVENT = {
    "event_id": "EVT001",
    "timestamp": "2024-03-15T14:30:00",
    "event_type": "INCIDENT",
    "severity": "MAJOR",
    "source": "VDOT",
    "latitude": 38.86,
    "longitude": -77.24,
    "description": "Multi-vehicle collision on I-66",
}


def test_process_valid_message_returns_one_row():
    fn = ParseAndNormalize()
    msg = json.dumps(SAMPLE_EVENT).encode("utf-8")
    assert len(list(fn.process(msg))) == 1


def test_process_row_has_all_schema_fields():
    fn = ParseAndNormalize()
    msg = json.dumps(SAMPLE_EVENT).encode("utf-8")
    row = list(fn.process(msg))[0]
    schema_fields = {f["name"] for f in FACT_SCHEMA["fields"]}
    assert schema_fields == set(row.keys())


def test_process_event_type_truncated_to_100_chars():
    fn = ParseAndNormalize()
    event = {**SAMPLE_EVENT, "event_type": "X" * 200}
    row = list(fn.process(json.dumps(event).encode("utf-8")))[0]
    assert len(row["event_type"]) <= 100


def test_process_lat_lon_cast_to_float():
    fn = ParseAndNormalize()
    row = list(fn.process(json.dumps(SAMPLE_EVENT).encode("utf-8")))[0]
    assert isinstance(row["latitude"], float)
    assert isinstance(row["longitude"], float)


def test_process_ingested_at_is_populated():
    fn = ParseAndNormalize()
    row = list(fn.process(json.dumps(SAMPLE_EVENT).encode("utf-8")))[0]
    assert row["ingested_at"] is not None


# ---------------------------------------------------------------------------
# process — malformed / missing data
# ---------------------------------------------------------------------------

def test_process_invalid_json_yields_nothing():
    fn = ParseAndNormalize()
    results = list(fn.process(b"not valid json {{{"))
    assert results == []


def test_process_minimal_fields_yields_row():
    fn = ParseAndNormalize()
    minimal = {"event_id": "MIN001", "timestamp": "2024-01-01T00:00:00"}
    results = list(fn.process(json.dumps(minimal).encode("utf-8")))
    assert len(results) == 1


def test_process_null_lat_lon_handled():
    fn = ParseAndNormalize()
    event = {**SAMPLE_EVENT, "latitude": None, "longitude": None}
    row = list(fn.process(json.dumps(event).encode("utf-8")))[0]
    assert row["latitude"] is None
    assert row["longitude"] is None


# ---------------------------------------------------------------------------
# FACT_SCHEMA validation
# ---------------------------------------------------------------------------

EXPECTED_FIELDS = {
    "event_id", "location_id", "time_id", "event_type_id",
    "timestamp", "event_type", "severity", "source",
    "latitude", "longitude", "place", "description",
    "value", "nws_alert", "nws_temp", "ingested_at",
}


def test_fact_schema_has_all_16_fields():
    actual = {f["name"] for f in FACT_SCHEMA["fields"]}
    assert actual == EXPECTED_FIELDS


def test_fact_schema_event_id_is_required():
    field = next(f for f in FACT_SCHEMA["fields"] if f["name"] == "event_id")
    assert field["mode"] == "REQUIRED"


def test_fact_schema_timestamp_is_timestamp_type():
    field = next(f for f in FACT_SCHEMA["fields"] if f["name"] == "timestamp")
    assert field["type"] == "TIMESTAMP"
