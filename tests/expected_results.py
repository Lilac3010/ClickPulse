#!/usr/bin/env python3
"""Pure-Python reference of the ClickPulse rules (no Spark). Prints the numbers the
real pipeline must produce, so the team can check Spark / Druid against them.
usage: python3 expected_results.py /path/to/ClickPulse   (folder with clicks_day1/2.jsonl)
"""
import json, sys
from collections import Counter
from datetime import datetime, timezone

VALID = {"page_view", "product_view", "add_to_cart", "remove_from_cart", "purchase"}

def ts(v):
    if not isinstance(v, str) or not v.strip(): return None
    try: d = datetime.fromisoformat(v.strip().replace("Z", "+00:00").replace("z", "+00:00"))
    except ValueError: return None
    return (d if d.tzinfo else d.replace(tzinfo=timezone.utc)).astimezone(timezone.utc)

def num(v, integral):
    if v is None: return None, True
    if isinstance(v, bool): return None, False
    try: x = float(v)
    except (TypeError, ValueError): return None, False
    if integral:
        return (int(x), True) if x == int(x) else (None, False)
    return x, True

def check(line):
    r = []
    try: o = json.loads(line)
    except Exception: return ["not_readable_json"], None
    if not isinstance(o, dict): return ["not_json_object"], None
    for k in ("event_id", "user_id", "session_id"):
        v = o.get(k)
        if v is None or (isinstance(v, str) and not v.strip()): r.append(f"missing_{k}")
    if o.get("event_type") not in VALID: r.append("invalid_event_type")
    et = ts(o.get("event_time"))
    if et is None: r.append("invalid_event_time")
    p, okp = num(o.get("price"), False); q, okq = num(o.get("quantity"), True)
    if not okp: r.append("non_numeric_price")
    if not okq: r.append("non_numeric_quantity")
    if p is not None and p < 0: r.append("negative_price")
    if q is not None and q < 0: r.append("negative_quantity")
    if r: return r, None
    o.update(price=p, quantity=q, _et=et, _ut=ts(o.get("updated_at")))
    return [], o

def run(table, path, day):
    lines = open(path, encoding="utf-8").read().split("\n")
    if lines and lines[-1] == "": lines.pop()
    rej, valid = Counter(), []
    for i, l in enumerate(lines, 1):
        reasons, o = check(l)
        if reasons: rej[";".join(reasons)] += 1
        else: valid.append((i, o))
    best = {}
    for i, o in valid:
        key = (o["_ut"] or datetime.min.replace(tzinfo=timezone.utc), i)
        if o["event_id"] not in best or key > best[o["event_id"]][0]:
            best[o["event_id"]] = (key, o)
    c = Counter()
    for eid, (_, o) in best.items():
        s = table.get(eid)
        if s is None: c["inserted"] += 1; table[eid] = o
        elif o["_et"] > s["_et"]: c["updated"] += 1; table[eid] = o
        else: c["dropped"] += 1
    rev = sum((o["price"] or 0) * (o["quantity"] or 0) for o in table.values() if o["event_type"] == "purchase")
    pur = sum(1 for o in table.values() if o["event_type"] == "purchase")
    by_date = Counter(); rev_date = Counter()
    for o in table.values():
        d = o["_et"].date().isoformat(); by_date[d] += 1
        if o["event_type"] == "purchase": rev_date[d] += (o["price"] or 0) * (o["quantity"] or 0)
    print(f"--- {path.split('/')[-1]} (day {day})")
    print(f"raw_lines={len(lines)} rejected={sum(rej.values())} valid={len(valid)} "
          f"duplicates_collapsed={len(valid)-len(best)} inserted={c['inserted']} updated={c['updated']} dropped={c['dropped']}")
    print(f"curated_rows={len(table)} curated_revenue={round(rev, 2)} curated_purchases={pur}")
    print("rejects:", dict(rej))
    print("rows by event_date:", dict(sorted(by_date.items())))
    print("revenue by event_date:", {k: round(v, 2) for k, v in sorted(rev_date.items())})

if __name__ == "__main__":
    d = sys.argv[1] if len(sys.argv) > 1 else "."
    t = {}
    run(t, f"{d}/clicks_day1.jsonl", 1)
    run(t, f"{d}/clicks_day2.jsonl", 2)
    run(t, f"{d}/clicks_day2.jsonl", 2)   # rerun Day 2
    run(t, f"{d}/clicks_day1.jsonl", 1)   # rerun Day 1 after Day 2
