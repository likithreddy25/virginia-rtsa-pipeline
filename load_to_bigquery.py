import hashlib
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timezone

PROJECT_ID = "virginia-rtsa-pipeline"
DATASET    = "virginia_rtsa"
DATA_DIR   = "/Users/likhith/Downloads/DAEN-Capstone-GaiaViz--main"

client = bigquery.Client(project=PROJECT_ID)

def make_id(*args):
    return hashlib.md5("|".join(str(a) for a in args).encode()).hexdigest()[:16]

def safe_float(val):
    try: return float(val)
    except: return None

rows_fact = []
rows_loc  = {}
rows_time = {}
rows_type = {}

# ── Load incidents ─────────────────────────────────────────────
print("Loading incidents (12K rows)...")
df = pd.read_csv(f"{DATA_DIR}/Cleaned/incidents/incidents_fairfax_timestamped.csv", encoding="utf-8-sig")
for _, r in df.iterrows():
    ts = pd.to_datetime(r.get("timestamp"), utc=True, errors="coerce")
    if pd.isnull(ts): continue
    loc_id  = make_id(r.get("latitude"), r.get("longitude"))
    time_id = make_id(str(ts), r.get("sensor_id",""))
    type_id = make_id(r.get("event_type"), r.get("severity"), "incident")
    rows_loc[loc_id]  = dict(location_id=loc_id, latitude=safe_float(r.get("latitude")),
                              longitude=safe_float(r.get("longitude")), place=None,
                              route=None, direction=None, region="Fairfax, VA")
    rows_time[time_id] = dict(time_id=time_id, timestamp=ts.isoformat(),
                               date=ts.date().isoformat(), hour=ts.hour,
                               day_of_week=ts.strftime("%A"), month=ts.month, year=ts.year)
    rows_type[type_id] = dict(event_type_id=type_id, event_type=str(r.get("event_type","")),
                               severity=str(r.get("severity","")), source="incident")
    rows_fact.append(dict(event_id=make_id(r.get("sensor_id",""), str(ts)),
                          location_id=loc_id, time_id=time_id, event_type_id=type_id,
                          timestamp=ts.isoformat(), event_type=str(r.get("event_type","")),
                          severity=str(r.get("severity","")), source="incident",
                          latitude=safe_float(r.get("latitude")),
                          longitude=safe_float(r.get("longitude")), place=None,
                          description=str(r.get("details",""))[:500], value=None,
                          nws_alert=None, nws_temp=None,
                          ingested_at=datetime.now(timezone.utc).isoformat()))
print(f"  incidents done: {len(rows_fact):,} rows so far")

# ── Load weather ───────────────────────────────────────────────
print("Loading weather (276K rows — takes ~2 min)...")
df = pd.read_csv(f"{DATA_DIR}/Cleaned/environment/weather_fairfax_timestamped.csv", encoding="utf-8-sig")
for _, r in df.iterrows():
    ts = pd.to_datetime(r.get("timestamp"), utc=True, errors="coerce")
    if pd.isnull(ts): continue
    loc_id  = make_id(r.get("latitude"), r.get("longitude"))
    time_id = make_id(str(ts), r.get("sensor_id",""))
    type_id = make_id(r.get("description"), None, "environmental")
    rows_loc[loc_id]  = dict(location_id=loc_id, latitude=safe_float(r.get("latitude")),
                              longitude=safe_float(r.get("longitude")),
                              place=None, route=str(r.get("route","")),
                              direction=str(r.get("direction","")), region="Fairfax, VA")
    rows_time[time_id] = dict(time_id=time_id, timestamp=ts.isoformat(),
                               date=ts.date().isoformat(), hour=ts.hour,
                               day_of_week=ts.strftime("%A"), month=ts.month, year=ts.year)
    rows_type[type_id] = dict(event_type_id=type_id, event_type=str(r.get("description","")),
                               severity=None, source="environmental")
    rows_fact.append(dict(event_id=make_id(r.get("sensor_id",""), str(ts)),
                          location_id=loc_id, time_id=time_id, event_type_id=type_id,
                          timestamp=ts.isoformat(), event_type=str(r.get("description","")),
                          severity=None, source="environmental",
                          latitude=safe_float(r.get("latitude")),
                          longitude=safe_float(r.get("longitude")),
                          place=str(r.get("route","")),
                          description=str(r.get("description","")),
                          value=safe_float(r.get("value")),
                          nws_alert=None, nws_temp=None,
                          ingested_at=datetime.now(timezone.utc).isoformat()))
print(f"  weather done: {len(rows_fact):,} rows so far")

# ── Load traffic ───────────────────────────────────────────────
print("Loading traffic (135K rows)...")
# File has no header — assign column names manually based on observed data
traffic_cols = ["sensor_id","sensor_id2","latitude","longitude","place","route",
                "direction","vehicle_count","vehicle_occupancy","vehicle_speed",
                "sample_period","timestamp"]
df = pd.read_csv(f"{DATA_DIR}/Ingest/Vdot/Trafficsensors/vdot_traffic_timestamped.csv",
                 encoding="utf-8-sig", header=None, names=traffic_cols)
for _, r in df.iterrows():
    ts_raw = r.get("timestamp")
    ts = pd.to_datetime(ts_raw, utc=True, errors="coerce")
    if pd.isnull(ts): continue
    loc_id  = make_id(r.get("latitude"), r.get("longitude"))
    time_id = make_id(str(ts), r.get("sensor_id",""))
    type_id = make_id("traffic_sensor", None, "traffic")
    rows_loc[loc_id]  = dict(location_id=loc_id, latitude=safe_float(r.get("latitude")),
                              longitude=safe_float(r.get("longitude")),
                              place=str(r.get("place","")), route=None,
                              direction=str(r.get("direction","")), region="Fairfax, VA")
    rows_time[time_id] = dict(time_id=time_id, timestamp=ts.isoformat(),
                               date=ts.date().isoformat(), hour=ts.hour,
                               day_of_week=ts.strftime("%A"), month=ts.month, year=ts.year)
    rows_type[type_id] = dict(event_type_id=type_id, event_type="traffic_sensor",
                               severity=None, source="traffic")
    rows_fact.append(dict(event_id=make_id(r.get("sensor_id",""), str(ts)),
                          location_id=loc_id, time_id=time_id, event_type_id=type_id,
                          timestamp=ts.isoformat(), event_type="traffic_sensor",
                          severity=None, source="traffic",
                          latitude=safe_float(r.get("latitude")),
                          longitude=safe_float(r.get("longitude")),
                          place=str(r.get("place","")),
                          description=f"speed={r.get('vehicle_speed')}, count={r.get('vehicle_count')}",
                          value=safe_float(r.get("vehicle_speed")),
                          nws_alert=None, nws_temp=None,
                          ingested_at=datetime.now(timezone.utc).isoformat()))
print(f"  traffic done: {len(rows_fact):,} rows so far")

# ── Load closures ──────────────────────────────────────────────
print("Loading road closures (58K rows)...")
df = pd.read_csv(f"{DATA_DIR}/Ingest/Vdot/Closures/vdot_closures_timestamped.csv", encoding="utf-8-sig")
for _, r in df.iterrows():
    # Column is orci:update_time in this file
    ts_raw = r.get("orci:update_time") or r.get("update_time") or r.get("collected_at")
    ts = pd.to_datetime(ts_raw, utc=True, errors="coerce")
    if pd.isnull(ts): continue
    loc_id  = make_id(r.get("from_lat"), r.get("from_lon"))
    time_id = make_id(str(ts), r.get("unique_id",""))
    type_id = make_id(r.get("event_type"), r.get("severity"), "road_closure")
    # Parse lat/lon from GML point string "lat lon" if from_lat not present
    gml = str(r.get("orci:start_point_gml:Point_gml:pos", ""))
    gml_parts = gml.split() if gml else []
    lat = safe_float(r.get("from_lat")) or (safe_float(gml_parts[0]) if len(gml_parts)>=2 else None)
    lon = safe_float(r.get("from_lon")) or (safe_float(gml_parts[1]) if len(gml_parts)>=2 else None)
    rows_loc[loc_id]  = dict(location_id=loc_id, latitude=lat, longitude=lon,
                              place=str(r.get("place") or r.get("orci:route_name","")),
                              route=str(r.get("orci:route_name","")),
                              direction=str(r.get("orci:travel_direction","")),
                              region="Fairfax, VA")
    rows_time[time_id] = dict(time_id=time_id, timestamp=ts.isoformat(),
                               date=ts.date().isoformat(), hour=ts.hour,
                               day_of_week=ts.strftime("%A"), month=ts.month, year=ts.year)
    etype = str(r.get("event_type") or r.get("orci:type_event",""))
    esev  = str(r.get("severity")   or r.get("orci:severity",""))
    rows_type[type_id] = dict(event_type_id=type_id, event_type=etype,
                               severity=esev, source="road_closure")
    rows_fact.append(dict(event_id=make_id(r.get("record_id",""), str(ts)),
                          location_id=loc_id, time_id=time_id, event_type_id=type_id,
                          timestamp=ts.isoformat(), event_type=etype,
                          severity=esev, source="road_closure",
                          latitude=lat, longitude=lon,
                          place=str(r.get("place") or r.get("orci:route_name","")),
                          description=str(r.get("orci:template_511_text",""))[:500],
                          value=None, nws_alert=None, nws_temp=None,
                          ingested_at=datetime.now(timezone.utc).isoformat()))
print(f"  closures done: {len(rows_fact):,} rows so far")

# ── Write to BigQuery ──────────────────────────────────────────
def bq_insert(table_name, rows):
    if not rows: return
    import re, tempfile, os
    df = pd.DataFrame(rows).drop_duplicates()

    # Sanitize all strings BEFORE type conversion
    def clean_str(v):
        if not isinstance(v, str): return v
        # Remove surrogates, null bytes, and all C0/C1 control chars
        v = v.encode('utf-8', errors='replace').decode('utf-8')
        v = re.sub(r'[\x00-\x1f\x7f\x80-\x9f]', ' ', v)
        return v.strip()

    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].map(lambda v: clean_str(v) if isinstance(v, str) else v)

    # Convert timestamps to tz-aware datetime (Parquet TIMESTAMP_NTZ)
    for col in ["timestamp", "ingested_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    # Write Parquet — coerce_timestamps='us' forces TIMESTAMP_MICROS logical type
    # so BigQuery reads it as TIMESTAMP, not INT64
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
        tmp_path = f.name
    df.to_parquet(tmp_path, index=False, coerce_timestamps='us', allow_truncated_timestamps=True)

    table_ref = f"{PROJECT_ID}.{DATASET}.{table_name}"
    with open(tmp_path, 'rb') as f:
        job = client.load_table_from_file(
            f, table_ref,
            job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
                source_format=bigquery.SourceFormat.PARQUET
            )
        )
    job.result()
    os.unlink(tmp_path)
    print(f"  ✓ {table_name}: {len(df):,} rows loaded")

print("\nWriting to BigQuery...")
bq_insert("dim_location",   list(rows_loc.values()))
bq_insert("dim_time",       list(rows_time.values()))
bq_insert("dim_event_type", list(rows_type.values()))
bq_insert("fact_events",    rows_fact)
print(f"\nDone! Total fact rows: {len(rows_fact):,}")
