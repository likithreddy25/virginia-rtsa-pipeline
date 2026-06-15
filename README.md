# Virginia Real-Time Safety Analytics (RTSA) — GCP Data Pipeline

End-to-end cloud data pipeline on **Google Cloud Platform** ingesting 482,285 rows from 4 heterogeneous Virginia VDOT/NWS data sources into a partitioned BigQuery star schema, with real-time streaming via Pub/Sub and Apache Beam (Dataflow).

---

## Architecture

```
On-Prem CSVs (VDOT/NWS)
        │
        ▼
Google Cloud Storage (GCS)        ← Landing zone / raw data lake
        │
        ▼
BigQuery Star Schema               ← Analytical warehouse
  ├── fact_events       (482,285 rows │ DAY-partitioned │ clustered)
  ├── dim_location      (881 rows)
  ├── dim_time          (250,664 rows)
  └── dim_event_type    (32 rows)
        │
        ▼
Pub/Sub Topic                      ← Real-time incident stream
  virginia-traffic-events
        │
        ▼
Apache Beam / Dataflow Pipeline    ← Stream processing
  Parse → Normalize → BQ Append
```

---

## Data Sources

| Source | Rows | Description |
|--------|------|-------------|
| VDOT Incidents | 12,732 | Fairfax County traffic incidents with severity |
| NWS Weather | 276,048 | Air temp, humidity, road surface, precipitation |
| VDOT Traffic Sensors | 135,844 | Vehicle speed, count, occupancy (318 sensors) |
| VDOT Road Closures | 57,661 | Permit work, utility closures with GML coordinates |
| **Total** | **482,285** | |

---

## BigQuery Schema

### `fact_events`
- Partitioned by `DATE(timestamp)` — DAY partitioning reduces scan cost
- Clustered by `event_type`, `severity` — accelerates filtered aggregations
- Fields: `event_id`, `location_id`, `time_id`, `event_type_id`, `timestamp`, `event_type`, `severity`, `source`, `latitude`, `longitude`, `place`, `description`, `value`, `nws_alert`, `nws_temp`, `ingested_at`

### Dimension Tables
- `dim_location` — lat/lon, route, direction, region
- `dim_time` — hour, day_of_week, month, year (pre-computed for fast aggregation)
- `dim_event_type` — event type, severity, source system

---

## Key Analytics Queries

**Peak traffic hours:** Saturday 7 PM highest event volume (22,591 events, avg 53 mph)

**Incident hotspots:** 12,732 incidents geo-tagged across Fairfax, VA (38.86°N, -77.24°W)

**Event distribution:** Air Temperature and Relative Humidity lead with 38,657 readings each; 318 unique traffic sensor locations

---

## Scripts

| File | Purpose |
|------|---------|
| `create_bq_schema.py` | Creates BigQuery dataset + 4 tables with partitioning/clustering |
| `load_to_bigquery.py` | Loads all 4 CSV sources into star schema via Parquet |
| `publisher.py` | Streams incidents to Pub/Sub (simulates real-time on-prem feed) |
| `dataflow_pipeline.py` | Apache Beam pipeline: Pub/Sub → normalize → BigQuery append |

---

## Setup

```bash
# 1. Install dependencies
pip install google-cloud-bigquery google-cloud-pubsub apache-beam[gcp] pandas pyarrow python-dateutil

# 2. Authenticate
gcloud auth application-default login

# 3. Enable GCP APIs
gcloud services enable bigquery.googleapis.com pubsub.googleapis.com dataflow.googleapis.com storage.googleapis.com

# 4. Create schema
python create_bq_schema.py

# 5. Load data
python load_to_bigquery.py

# 6. Stream to Pub/Sub
python publisher.py

# 7. Run Beam pipeline
python dataflow_pipeline.py
```

---

## Tech Stack

- **Google Cloud Storage** — raw data landing zone
- **BigQuery** — partitioned/clustered analytical warehouse
- **Pub/Sub** — real-time message streaming
- **Apache Beam / Dataflow** — stream processing with field normalization and BQ sink
- **Python** — pandas, pyarrow, google-cloud-bigquery, google-cloud-pubsub
- **Star Schema** — dimensional modeling (1 fact + 3 dimension tables)
