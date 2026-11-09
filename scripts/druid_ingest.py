#!/usr/bin/env python3
"""Submit the Druid re-index of the curated snapshot and wait until Druid serves
exactly what the curated table holds (row count, revenue, purchases from
/clickpulse/export/manifest.json). Used by Airflow (VM2); also runnable by hand.

usage: python druid_ingest.py            # submit + wait + verify
       python druid_ingest.py --verify   # only compare Druid vs manifest
"""
import json, os, subprocess, sys, time
import requests

E = os.environ
ROUTER = E.get("DRUID_ROUTER", "http://vm4:8888")
DS = E.get("DRUID_DATASOURCE", "clickpulse_events")
SPEC = "/opt/clickpulse/druid/ingest_spec.json"

def manifest():
    out = subprocess.run(["hdfs", "dfs", "-cat", f"{E['HDFS_BASE']}/export/manifest.json"],
                         capture_output=True, check=True).stdout
    return json.loads(out)

def sql(q):
    r = requests.post(f"{ROUTER}/druid/v2/sql", json={"query": q}, timeout=60)
    r.raise_for_status()
    return r.json()

def druid_totals():
    try:
        row = sql(f'SELECT COUNT(*) AS "rows", ROUND(SUM(revenue), 2) AS revenue, '
                  f'SUM(is_purchase) AS purchases, COUNT(DISTINCT event_id) AS ids FROM "{DS}"')[0]
        return row
    except requests.HTTPError:
        return None   # datasource not there yet

def submit():
    spec = json.load(open(SPEC))
    spec["spec"]["dataSchema"]["dataSource"] = DS
    r = requests.post(f"{ROUTER}/druid/indexer/v1/task", json=spec, timeout=60)
    r.raise_for_status()
    tid = r.json()["task"]; print("[druid] task", tid)
    while True:
        st = requests.get(f"{ROUTER}/druid/indexer/v1/task/{tid}/status", timeout=60).json()["status"]["status"]
        if st in ("SUCCESS", "FAILED"):
            break
        time.sleep(5)
    if st != "SUCCESS":
        sys.exit(f"[druid] ingestion task {tid} {st} - see Druid console (Ingestion > Tasks)")
    print("[druid] task SUCCESS")

def wait_and_verify(timeout=300):
    m = manifest()
    want = {"rows": m["curated_rows"], "revenue": round(m["curated_revenue"], 2), "purchases": m["curated_purchases"]}
    t0 = time.time()
    while time.time() - t0 < timeout:
        got = druid_totals()
        if got and got["rows"] == want["rows"] and got["ids"] == want["rows"] \
           and round(got["revenue"] or 0, 2) == want["revenue"] and got["purchases"] == want["purchases"]:
            print(f"[druid] MIRRORS curated table: {got}")
            return
        print(f"[druid] waiting for segments... druid={got} curated={want}")
        time.sleep(10)
    sys.exit("[druid] Druid does not match the curated table")

if __name__ == "__main__":
    if "--verify" not in sys.argv:
        submit()
    wait_and_verify()
