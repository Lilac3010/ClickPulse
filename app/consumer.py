#!/usr/bin/env python3
"""VM2 - land Day N raw lines from Kafka into HDFS, unchanged.

Reads the whole topic (beginning -> current end), keeps messages whose header
source_day == N, orders them by line_no (one copy per line_no, so a retried
producer can never create extra raw lines), and writes
    /clickpulse/raw/day=N/clicks_dayN.jsonl      (overwrite -> rerun-safe)
Fails (so Airflow retries) if the line count != the producer's marker.

usage: python consumer.py --day 1
"""
import argparse, os, subprocess, sys, tempfile
from confluent_kafka import Consumer, TopicPartition

E = os.environ
ap = argparse.ArgumentParser()
ap.add_argument("--day", type=int, required=True, choices=[1, 2])
a = ap.parse_args()
topic, day = E["KAFKA_TOPIC"], str(a.day)
marker = f"{E['HDFS_BASE']}/markers/produced_day{day}"
target_dir = f"{E['HDFS_BASE']}/raw/day={day}"
target = f"{target_dir}/clicks_day{day}.jsonl"

r = subprocess.run(["hdfs", "dfs", "-cat", marker], capture_output=True)
if r.returncode != 0:
    print(f"[consumer] marker {marker} missing - run the producer first"); sys.exit(1)
expected = int(r.stdout.decode().strip())

c = Consumer({"bootstrap.servers": E["KAFKA_BOOTSTRAP"], "group.id": "clickpulse-raw-writer",
              "enable.auto.commit": False, "auto.offset.reset": "earliest"})
parts = c.list_topics(topic, timeout=15).topics[topic].partitions
ends, assign = {}, []
for p in parts:
    lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=15)
    if hi > lo:
        ends[p] = hi
        assign.append(TopicPartition(topic, p, lo))
c.assign(assign)

lines, done = {}, set()
while len(done) < len(ends):
    m = c.poll(5.0)
    if m is None:
        continue
    if m.error():
        print("[consumer] kafka error:", m.error()); continue
    h = {k: v.decode() for k, v in (m.headers() or [])}
    if h.get("source_day") == day:
        lines.setdefault(int(h["line_no"]), m.value() or b"")
    if m.offset() + 1 >= ends[m.partition()]:
        done.add(m.partition())
c.commit(offsets=[TopicPartition(topic, p, off) for p, off in ends.items()], asynchronous=False)
c.close()

if sorted(lines) != list(range(1, expected + 1)):
    print(f"[consumer] expected lines 1..{expected}, got {len(lines)} -> failing so Airflow retries")
    sys.exit(1)

with tempfile.NamedTemporaryFile(delete=False) as tmp:
    for i in range(1, expected + 1):
        tmp.write(lines[i] + b"\n")
subprocess.run(["hdfs", "dfs", "-mkdir", "-p", target_dir], check=True)
subprocess.run(["hdfs", "dfs", "-put", "-f", tmp.name, target], check=True)
os.unlink(tmp.name)
print(f"[consumer] wrote {expected} raw lines -> {E['HDFS_URI']}{target}")
