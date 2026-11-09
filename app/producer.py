#!/usr/bin/env python3
"""VM1 - publish clicks_dayN.jsonl to Kafka, every line byte-for-byte (bad lines too).

Each message carries headers source_day / line_no so the consumer can rebuild the
exact original file even if the producer is retried and some lines are sent twice.
A marker file in HDFS (/clickpulse/markers/produced_dayN = line count) makes a
rerun a no-op, so Airflow reruns do not keep piling the same file into Kafka.

usage: python producer.py --day 1 [--force]
"""
import argparse, os, subprocess, sys
from confluent_kafka import Producer

E = os.environ
ap = argparse.ArgumentParser()
ap.add_argument("--day", type=int, required=True, choices=[1, 2])
ap.add_argument("--force", action="store_true", help="re-send even if marker exists")
a = ap.parse_args()

src = os.path.join(E["DATA_DIR"], f"clicks_day{a.day}.jsonl")
marker = f"{E['HDFS_BASE']}/markers/produced_day{a.day}"

def hdfs(*args, inp=None):
    return subprocess.run(["hdfs", "dfs", *args], input=inp, capture_output=True)

if not a.force and hdfs("-test", "-e", marker).returncode == 0:
    print(f"[producer] day {a.day} already produced (marker {marker} exists) -> nothing to do")
    sys.exit(0)

errors = []
def on_delivery(err, msg):
    if err: errors.append(err)

p = Producer({"bootstrap.servers": E["KAFKA_BOOTSTRAP"], "acks": "all",
              "enable.idempotence": True, "linger.ms": 10, "client.id": "clickpulse-producer"})
n = 0
with open(src, "rb") as f:
    for n, line in enumerate(f, start=1):
        value = line[:-1] if line.endswith(b"\n") else line      # strip only the line terminator
        p.produce(E["KAFKA_TOPIC"], key=f"day{a.day}:{n}".encode(), value=value,
                  headers=[("source_day", str(a.day)), ("line_no", str(n)),
                           ("source_file", os.path.basename(src))],
                  on_delivery=on_delivery)
        p.poll(0)
left = p.flush(60)
if errors or left:
    print(f"[producer] FAILED: {len(errors)} delivery errors, {left} unflushed"); sys.exit(1)

hdfs("-mkdir", "-p", f"{E['HDFS_BASE']}/markers")
r = hdfs("-put", "-f", "-", marker, inp=f"{n}\n".encode())
if r.returncode != 0:
    print(r.stderr.decode()); sys.exit(1)
print(f"[producer] sent {n} lines of {src} to {E['KAFKA_TOPIC']} @ {E['KAFKA_BOOTSTRAP']}; marker={marker}")
