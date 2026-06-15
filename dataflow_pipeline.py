"""
dataflow_pipeline.py
Apache Beam pipeline: reads from Pub/Sub → transforms → writes to BigQuery.
Run locally with DirectRunner for demo; deploy to Dataflow for production.
"""
import json, hashlib, argparse, logging
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition

logging.basicConfig(level=logging.INFO)

PROJECT_ID    = "virginia-rtsa-pipeline"
DATASET       = "virginia_rtsa"
SUBSCRIPTION  = f"projects/{PROJECT_ID}/subscriptions/virginia-traffic-sub"
BQ_TABLE      = f"{PROJECT_ID}:{DATASET}.fact_events"

# BigQuery schema for streaming inserts into fact_events
FACT_SCHEMA = {
    "fields": [
        {"name": "event_id",      "type": "STRING",    "mode": "REQUIRED"},
        {"name": "location_id",   "type": "STRING",    "mode": "NULLABLE"},
        {"name": "time_id",       "type": "STRING",    "mode": "NULLABLE"},
        {"name": "event_type_id", "type": "STRING",    "mode": "NULLABLE"},
        {"name": "timestamp",     "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "event_type",    "type": "STRING",    "mode": "NULLABLE"},
        {"name": "severity",      "type": "STRING",    "mode": "NULLABLE"},
        {"name": "source",        "type": "STRING",    "mode": "NULLABLE"},
        {"name": "latitude",      "type": "FLOAT64",   "mode": "NULLABLE"},
        {"name": "longitude",     "type": "FLOAT64",   "mode": "NULLABLE"},
        {"name": "place",         "type": "STRING",    "mode": "NULLABLE"},
        {"name": "description",   "type": "STRING",    "mode": "NULLABLE"},
        {"name": "value",         "type": "FLOAT64",   "mode": "NULLABLE"},
        {"name": "nws_alert",     "type": "STRING",    "mode": "NULLABLE"},
        {"name": "nws_temp",      "type": "STRING",    "mode": "NULLABLE"},
        {"name": "ingested_at",   "type": "TIMESTAMP", "mode": "NULLABLE"},
    ]
}


class ParseAndNormalize(beam.DoFn):
    """Parse Pub/Sub JSON message and normalize fields for BigQuery."""

    def make_id(self, *args):
        return hashlib.md5("|".join(str(a) for a in args).encode()).hexdigest()[:16]

    def process(self, message):
        try:
            rec = json.loads(message.decode("utf-8"))

            ts_raw = rec.get("timestamp", "")
            try:
                from dateutil import parser as dtparser
                ts = dtparser.parse(ts_raw)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                ts_str = ts.strftime("%Y-%m-%d %H:%M:%S UTC")
            except Exception:
                ts_str = None

            lat = rec.get("latitude")
            lon = rec.get("longitude")
            loc_id  = self.make_id(lat, lon)
            time_id = self.make_id(ts_str, rec.get("event_id", ""))
            type_id = self.make_id(rec.get("event_type"), rec.get("severity"), rec.get("source"))

            row = {
                "event_id":      rec.get("event_id") or self.make_id(ts_str, lat, lon),
                "location_id":   loc_id,
                "time_id":       time_id,
                "event_type_id": type_id,
                "timestamp":     ts_str,
                "event_type":    str(rec.get("event_type", ""))[:100],
                "severity":      str(rec.get("severity", ""))[:50],
                "source":        str(rec.get("source", ""))[:50],
                "latitude":      float(lat) if lat is not None else None,
                "longitude":     float(lon) if lon is not None else None,
                "place":         None,
                "description":   str(rec.get("description", ""))[:500],
                "value":         None,
                "nws_alert":     None,
                "nws_temp":      None,
                "ingested_at":   datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            }
            yield row
        except Exception as e:
            logging.warning(f"Failed to parse message: {e}")


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--runner", default="DirectRunner",
                        help="DirectRunner (local) or DataflowRunner (GCP)")
    known_args, pipeline_args = parser.parse_known_args()

    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).streaming = True
    options.view_as(StandardOptions).runner    = known_args.runner

    print(f"Starting pipeline with {known_args.runner}...")
    print(f"  Subscription : {SUBSCRIPTION}")
    print(f"  BigQuery sink: {BQ_TABLE}")
    print("  (Ctrl+C to stop after messages are consumed)\n")

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadPubSub"    >> ReadFromPubSub(subscription=SUBSCRIPTION)
            | "ParseNorm"     >> beam.ParDo(ParseAndNormalize())
            | "WriteBigQuery" >> WriteToBigQuery(
                BQ_TABLE,
                schema=FACT_SCHEMA,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
            )
        )


if __name__ == "__main__":
    run()
