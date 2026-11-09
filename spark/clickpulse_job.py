#!/usr/bin/env python3
"""VM3 - ClickPulse Spark job (Spark standalone + Apache Iceberg).

raw (HDFS) -> validate -> rejects table + HDFS rejects dir
           -> dedupe (latest updated_at per event_id)
           -> upsert curated Iceberg table by event_id (event_time rule)
           -> run_stats row -> Parquet snapshot of the curated table for Druid

usage: spark-submit clickpulse_job.py --day 1 --run-id manual_1
"""
import argparse, json, os, re
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Row, Window, functions as F, types as T

E = os.environ
HDFS = E.get("HDFS_URI", "hdfs://vm2:9000")
BASE = E.get("HDFS_BASE", "/clickpulse")
DB = E.get("ICEBERG_DB", "lake.clickpulse")
CURATED, REJECTS, STATS = f"{DB}.events", f"{DB}.rejects", f"{DB}.run_stats"
VALID_TYPES = {"page_view", "product_view", "add_to_cart", "remove_from_cart", "purchase"}

ap = argparse.ArgumentParser()
ap.add_argument("--day", type=int, required=True)
ap.add_argument("--run-id", default=datetime.now(timezone.utc).strftime("manual_%Y%m%dT%H%M%S"))
ap.add_argument("--raw-path", help="override (default: HDFS raw/day=N)")
ap.add_argument("--out-root", help="override root for rejects/export (default: HDFS_URI+HDFS_BASE)")
a = ap.parse_args()
a.run_id = re.sub(r"[^A-Za-z0-9_.-]", "_", a.run_id)   # safe for HDFS paths
ROOT = a.out_root or f"{HDFS}{BASE}"
RAW = a.raw_path or f"{ROOT}/raw/day={a.day}"

spark = SparkSession.builder.appName(f"clickpulse-day{a.day}").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")

# ---------------------------------------------------------------- tables
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {DB}")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {CURATED} (
    event_id STRING, user_id STRING, session_id STRING, event_type STRING,
    product_id STRING, category STRING, price DOUBLE, quantity BIGINT, device STRING,
    event_time TIMESTAMP, updated_at TIMESTAMP, event_date DATE, source_day INT)
  USING iceberg PARTITIONED BY (event_date)
  TBLPROPERTIES ('format-version'='2', 'write.merge.mode'='copy-on-write',
                 'write.update.mode'='copy-on-write', 'write.delete.mode'='copy-on-write')""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {REJECTS} (
    source_day INT, line_no BIGINT, reasons STRING, raw_line STRING, run_id STRING, rejected_at TIMESTAMP)
  USING iceberg PARTITIONED BY (source_day)""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {STATS} (
    run_id STRING, source_day INT, processed_at TIMESTAMP, raw_lines BIGINT, rejected BIGINT,
    valid BIGINT, duplicates_collapsed BIGINT, inserted BIGINT, updated BIGINT, dropped BIGINT,
    curated_rows BIGINT, curated_revenue DOUBLE, curated_purchases BIGINT) USING iceberg""")

# ---------------------------------------------------------------- validation
def parse_ts(v):
    if not isinstance(v, str) or not v.strip():
        return None
    s = v.strip().replace("Z", "+00:00").replace("z", "+00:00")
    try:
        d = datetime.fromisoformat(s)
    except ValueError:
        return None
    return (d if d.tzinfo else d.replace(tzinfo=timezone.utc)).astimezone(timezone.utc)

def num(v, integral):
    """-> (value, ok). None is allowed (e.g. page_view has no price)."""
    if v is None:
        return None, True
    if isinstance(v, bool):
        return None, False
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None, False
    if integral:
        if x != int(x):
            return None, False
        return int(x), True
    return x, True

def check(pair):
    raw, idx = pair
    reasons, rec = [], None
    try:
        o = json.loads(raw)
    except Exception:
        o = None
        reasons.append("not_readable_json")
    if o is not None and not isinstance(o, dict):
        reasons.append("not_json_object")
    elif isinstance(o, dict):
        for k in ("event_id", "user_id", "session_id"):
            v = o.get(k)
            if v is None or (isinstance(v, str) and not v.strip()):
                reasons.append(f"missing_{k}")
        if o.get("event_type") not in VALID_TYPES:
            reasons.append("invalid_event_type")
        et = parse_ts(o.get("event_time"))
        if et is None:
            reasons.append("invalid_event_time")
        price, okp = num(o.get("price"), False)
        qty, okq = num(o.get("quantity"), True)
        if not okp: reasons.append("non_numeric_price")
        if not okq: reasons.append("non_numeric_quantity")
        if price is not None and price < 0: reasons.append("negative_price")
        if qty is not None and qty < 0: reasons.append("negative_quantity")
        if not reasons:
            s = lambda k: None if o.get(k) is None else str(o.get(k))
            rec = (s("event_id"), s("user_id"), s("session_id"), o["event_type"], s("product_id"),
                   s("category"), price, qty, s("device"), et, parse_ts(o.get("updated_at")),
                   et.date(), a.day)
    return (idx + 1, raw, ";".join(reasons) if reasons else None, rec)

lines = spark.sparkContext.textFile(RAW).zipWithIndex()
checked = lines.map(check).cache()
raw_lines = checked.count()

# ---- rejects: every occurrence, separate table + separate HDFS location (overwrite this day => rerun-safe)
rej_schema = T.StructType([T.StructField("source_day", T.IntegerType()), T.StructField("line_no", T.LongType()),
                           T.StructField("reasons", T.StringType()), T.StructField("raw_line", T.StringType())])
rejects = spark.createDataFrame(checked.filter(lambda r: r[2] is not None)
                                .map(lambda r: (a.day, r[0], r[2], r[1])), rej_schema) \
    .withColumn("run_id", F.lit(a.run_id)).withColumn("rejected_at", F.current_timestamp()).cache()
n_rejected = rejects.count()
rejects.writeTo(REJECTS).overwritePartitions()
rejects.write.mode("overwrite").json(f"{ROOT}/rejects/day={a.day}")

# ---- valid records
cur_schema = spark.table(CURATED).schema
valid = spark.createDataFrame(checked.filter(lambda r: r[2] is None).map(lambda r: (r[0],) + r[3]),
                              T.StructType([T.StructField("line_no", T.LongType())] + cur_schema.fields))
n_valid = valid.count()

# ---- dedupe inside the file: identical lines collapse; conflicting versions -> latest updated_at wins
w = Window.partitionBy("event_id").orderBy(F.col("updated_at").desc_nulls_last(), F.col("line_no").desc())
staged = valid.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn", "line_no").cache()
n_staged = staged.count()

# ---- classify against the curated table
target = spark.table(CURATED).select("event_id", F.col("event_time").alias("stored_time"))
cls = staged.join(target, "event_id", "left").withColumn(
    "action", F.when(F.col("stored_time").isNull(), "insert")
               .when(F.col("event_time") > F.col("stored_time"), "update")
               .otherwise("drop")).cache()
counts = {r["action"]: r["count"] for r in cls.groupBy("action").count().collect()}
ins, upd, drp = counts.get("insert", 0), counts.get("update", 0), counts.get("drop", 0)

# ---- upsert (only rows that will actually change something => rerun creates no new snapshot)
changes = cls.filter("action in ('insert','update')").drop("stored_time", "action").localCheckpoint()  # freeze before MERGE
if ins + upd > 0:
    changes.createOrReplaceTempView("cp_changes")
    spark.sql(f"""MERGE INTO {CURATED} t USING cp_changes s
                  ON t.event_id = s.event_id
                  WHEN MATCHED AND s.event_time > t.event_time THEN UPDATE SET *
                  WHEN NOT MATCHED THEN INSERT *""")

# ---- export current state for Druid (full snapshot, overwrite)
cur = spark.table(CURATED)
export = cur.select(
    F.date_format("event_time", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("event_time_iso"),
    "event_id", "user_id", "session_id", "event_type", "product_id",
    F.coalesce("category", F.lit("Unknown")).alias("category"),
    F.coalesce("device", F.lit("unknown")).alias("device"),
    "price", "quantity",
    F.when(F.col("event_type") == "purchase",
           F.coalesce(F.col("price"), F.lit(0.0)) * F.coalesce(F.col("quantity"), F.lit(0))).otherwise(F.lit(0.0)).alias("revenue"),
    (F.col("event_type") == "purchase").cast("long").alias("is_purchase"),
    F.date_format("event_date", "yyyy-MM-dd").alias("event_date"), "source_day")
export.coalesce(1).write.mode("overwrite").parquet(f"{ROOT}/export/curated_snapshot")
tot = export.agg(F.count("*").alias("rows"), F.round(F.sum("revenue"), 2).alias("revenue"),
                 F.sum("is_purchase").alias("purchases")).first()

# ---- stats + manifest (Airflow compares Druid against this)
stats = Row(run_id=a.run_id, source_day=a.day, processed_at=datetime.now(timezone.utc), raw_lines=raw_lines,
            rejected=n_rejected, valid=n_valid, duplicates_collapsed=n_valid - n_staged, inserted=ins,
            updated=upd, dropped=drp, curated_rows=tot["rows"], curated_revenue=float(tot["revenue"] or 0),
            curated_purchases=int(tot["purchases"] or 0))
spark.createDataFrame([stats], spark.table(STATS).schema).writeTo(STATS).append()
manifest = {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in stats.asDict().items()}
spark.sparkContext.parallelize([json.dumps(manifest)], 1).saveAsTextFile(f"{ROOT}/export/_manifest_tmp_{a.run_id}")
jvm = spark._jvm; conf = spark._jsc.hadoopConfiguration()
fs = jvm.org.apache.hadoop.fs.Path(ROOT).getFileSystem(conf)
P = jvm.org.apache.hadoop.fs.Path
fs.delete(P(f"{ROOT}/export/manifest.json"), False)
fs.rename(P(f"{ROOT}/export/_manifest_tmp_{a.run_id}/part-00000"), P(f"{ROOT}/export/manifest.json"))
fs.delete(P(f"{ROOT}/export/_manifest_tmp_{a.run_id}"), True)

print("CLICKPULSE_STATS " + json.dumps(manifest))
spark.stop()
