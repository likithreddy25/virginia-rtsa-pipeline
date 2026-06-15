from google.cloud import bigquery

PROJECT_ID = "virginia-rtsa-pipeline"
DATASET = "virginia_rtsa"

client = bigquery.Client(project=PROJECT_ID)

dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET}")
dataset_ref.location = "US"
client.create_dataset(dataset_ref, exists_ok=True)
print(f"Dataset {DATASET} created.")

client.create_table(bigquery.Table(
    f"{PROJECT_ID}.{DATASET}.dim_location",
    schema=[
        bigquery.SchemaField("location_id",  "STRING",  mode="REQUIRED"),
        bigquery.SchemaField("latitude",     "FLOAT64"),
        bigquery.SchemaField("longitude",    "FLOAT64"),
        bigquery.SchemaField("place",        "STRING"),
        bigquery.SchemaField("route",        "STRING"),
        bigquery.SchemaField("direction",    "STRING"),
        bigquery.SchemaField("region",       "STRING"),
    ]
), exists_ok=True)

client.create_table(bigquery.Table(
    f"{PROJECT_ID}.{DATASET}.dim_time",
    schema=[
        bigquery.SchemaField("time_id",      "STRING",  mode="REQUIRED"),
        bigquery.SchemaField("timestamp",    "TIMESTAMP"),
        bigquery.SchemaField("date",         "DATE"),
        bigquery.SchemaField("hour",         "INT64"),
        bigquery.SchemaField("day_of_week",  "STRING"),
        bigquery.SchemaField("month",        "INT64"),
        bigquery.SchemaField("year",         "INT64"),
    ]
), exists_ok=True)

client.create_table(bigquery.Table(
    f"{PROJECT_ID}.{DATASET}.dim_event_type",
    schema=[
        bigquery.SchemaField("event_type_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("event_type",    "STRING"),
        bigquery.SchemaField("severity",      "STRING"),
        bigquery.SchemaField("source",        "STRING"),
    ]
), exists_ok=True)

table = bigquery.Table(
    f"{PROJECT_ID}.{DATASET}.fact_events",
    schema=[
        bigquery.SchemaField("event_id",       "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("location_id",    "STRING"),
        bigquery.SchemaField("time_id",        "STRING"),
        bigquery.SchemaField("event_type_id",  "STRING"),
        bigquery.SchemaField("timestamp",      "TIMESTAMP"),
        bigquery.SchemaField("event_type",     "STRING"),
        bigquery.SchemaField("severity",       "STRING"),
        bigquery.SchemaField("source",         "STRING"),
        bigquery.SchemaField("latitude",       "FLOAT64"),
        bigquery.SchemaField("longitude",      "FLOAT64"),
        bigquery.SchemaField("place",          "STRING"),
        bigquery.SchemaField("description",    "STRING"),
        bigquery.SchemaField("value",          "FLOAT64"),
        bigquery.SchemaField("nws_alert",      "STRING"),
        bigquery.SchemaField("nws_temp",       "STRING"),
        bigquery.SchemaField("ingested_at",    "TIMESTAMP"),
    ]
)
table.time_partitioning = bigquery.TimePartitioning(
    type_=bigquery.TimePartitioningType.DAY,
    field="timestamp"
)
table.clustering_fields = ["event_type", "severity"]
client.create_table(table, exists_ok=True)

print("All tables created:")
print("  - dim_location")
print("  - dim_time")
print("  - dim_event_type")
print("  - fact_events (partitioned by DATE, clustered by event_type + severity)")
