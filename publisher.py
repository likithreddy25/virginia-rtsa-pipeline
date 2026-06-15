"""
publisher.py
Streams Virginia VDOT incident records to Pub/Sub, simulating a real-time
on-prem → cloud data feed for the Virginia RTSA pipeline.
"""
import json, time, hashlib
import pandas as pd
from google.cloud import pubsub_v1

PROJECT_ID = "virginia-rtsa-pipeline"
TOPIC_ID   = "virginia-traffic-events"
DATA_PATH  = "/Users/likhith/Downloads/DAEN-Capstone-GaiaViz--main/Cleaned/incidents/incidents_fairfax_timestamped.csv"
BATCH_SIZE = 100   # messages per second burst
MAX_ROWS   = 500   # cap for demo run

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

df = pd.read_csv(DATA_PATH, encoding="utf-8-sig").head(MAX_ROWS)

print(f"Streaming {len(df)} incident records to {topic_path} ...")
published = 0
futures = []

for _, row in df.iterrows():
    record = {
        "event_id":   hashlib.md5(str(row.to_dict()).encode()).hexdigest()[:16],
        "event_type": str(row.get("event_type", "")),
        "severity":   str(row.get("severity", "")),
        "latitude":   float(row["latitude"]) if pd.notna(row.get("latitude")) else None,
        "longitude":  float(row["longitude"]) if pd.notna(row.get("longitude")) else None,
        "timestamp":  str(row.get("timestamp", "")),
        "description": str(row.get("details", ""))[:200],
        "source":     "incident"
    }
    data = json.dumps(record).encode("utf-8")
    future = publisher.publish(topic_path, data=data, source="vdot-incident")
    futures.append(future)
    published += 1

    if published % BATCH_SIZE == 0:
        print(f"  Published {published}/{len(df)} messages...")
        time.sleep(0.5)

# Wait for all publishes to confirm
for f in futures:
    f.result()

print(f"\nDone! {published} messages published to Pub/Sub topic '{TOPIC_ID}'")
