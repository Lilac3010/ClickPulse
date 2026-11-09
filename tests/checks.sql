-- Run on VM3:  spark-sql -f /opt/clickpulse/tests/checks.sql
-- (uses the Iceberg catalog "lake" from spark-defaults.conf)

-- 1. One row per run: what each run did (a rerun must show inserted=0, updated=0)
SELECT run_id, source_day, raw_lines, rejected, valid, duplicates_collapsed,
       inserted, updated, dropped, curated_rows, curated_revenue, curated_purchases
FROM lake.clickpulse.run_stats ORDER BY processed_at;

-- 2. No duplicate event_ids in the curated table (must return 0 rows)
SELECT event_id, COUNT(*) c FROM lake.clickpulse.events GROUP BY event_id HAVING COUNT(*) > 1;

-- 3. Nothing invalid slipped into the curated table (all counts must be 0)
SELECT
  SUM(CASE WHEN event_type NOT IN ('page_view','product_view','add_to_cart','remove_from_cart','purchase') THEN 1 ELSE 0 END) bad_type,
  SUM(CASE WHEN price < 0 OR quantity < 0 THEN 1 ELSE 0 END) negatives,
  SUM(CASE WHEN event_id IS NULL OR user_id IS NULL OR session_id IS NULL
            OR trim(event_id)='' OR trim(user_id)='' OR trim(session_id)='' THEN 1 ELSE 0 END) missing_ids,
  SUM(CASE WHEN event_date <> to_date(event_time) THEN 1 ELSE 0 END) wrong_partition
FROM lake.clickpulse.events;

-- 4. Rejects by reason and day (every occurrence counted)
SELECT source_day, reasons, COUNT(*) n FROM lake.clickpulse.rejects GROUP BY 1,2 ORDER BY 1,3 DESC;

-- 5. Daily totals (compare with Druid / dashboard)
SELECT event_date, COUNT(*) events,
       SUM(CASE WHEN event_type='purchase' THEN 1 ELSE 0 END) purchases,
       ROUND(SUM(CASE WHEN event_type='purchase' THEN price*quantity ELSE 0 END), 2) revenue
FROM lake.clickpulse.events GROUP BY event_date ORDER BY event_date;

-- 6. Iceberg history: a no-op rerun must NOT add a snapshot to the events table
SELECT committed_at, snapshot_id, operation FROM lake.clickpulse.events.snapshots ORDER BY committed_at;

-- 7. Edge-case drill-down: events whose stored version came from Day 2 (updates / late arrivals)
SELECT event_id, event_type, event_time, event_date, price, quantity, updated_at
FROM lake.clickpulse.events WHERE source_day = 2 ORDER BY event_time;
