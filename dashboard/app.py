"""VM4 - ClickPulse dashboard (Streamlit). Every number comes from Druid SQL.
run:  streamlit run /opt/clickpulse/dashboard/app.py --server.port 8501 --server.address 0.0.0.0
"""
import os
from datetime import timedelta

import pandas as pd
import requests
import streamlit as st

SQL_URL = os.environ.get("DRUID_SQL_URL", "http://localhost:8888/druid/v2/sql")
DS = os.environ.get("DRUID_DATASOURCE", "clickpulse_events")

def q(sql, params=()):
    r = requests.post(SQL_URL, json={"query": sql, "parameters": list(params)}, timeout=30)
    if r.status_code != 200:
        st.error(r.text); st.stop()
    return pd.DataFrame(r.json())

def ts(v):
    return {"type": "TIMESTAMP", "value": v}

st.set_page_config(page_title="ClickPulse - RetailMart", layout="wide")
st.title("ClickPulse - RetailMart shopping analytics")

b = q(f'SELECT MIN(__time) AS mn, MAX(__time) AS mx FROM "{DS}"')
lo, hi = pd.to_datetime(b.mn[0]).date(), pd.to_datetime(b.mx[0]).date()
devices = q(f'SELECT DISTINCT device FROM "{DS}" ORDER BY 1').device.tolist()

c1, c2 = st.columns([2, 3])
rng = c1.date_input("Event date range (UTC)", (lo, hi), min_value=lo, max_value=hi)
sel = c2.multiselect("Device", devices, default=devices)
if not isinstance(rng, (list, tuple)) or len(rng) != 2 or not sel:
    st.info("Pick a start + end date and at least one device."); st.stop()

start, end = rng[0], rng[1] + timedelta(days=1)          # end exclusive
where = f'__time >= ? AND __time < ? AND device IN ({",".join("?" * len(sel))})'
params = [ts(f"{start} 00:00:00"), ts(f"{end} 00:00:00")] + [{"type": "VARCHAR", "value": d} for d in sel]

kpi = q(f"""SELECT COALESCE(SUM(revenue), 0) AS revenue, COALESCE(SUM(is_purchase), 0) AS purchases
            FROM "{DS}" WHERE {where} AND event_type = 'purchase'""", params)
k1, k2 = st.columns(2)
k1.metric("Revenue (sum price x quantity, purchases)", f"{kpi.revenue[0]:,.2f}")
k2.metric("Purchases", int(kpi.purchases[0]))

cat_sql = f"""SELECT category, SUM(revenue) AS revenue FROM "{DS}"
              WHERE {where} AND event_type = 'purchase' GROUP BY category ORDER BY revenue DESC"""
cat = q(cat_sql, params)
st.subheader("Revenue by category")
if cat.empty:
    st.info("No purchases for this filter.")
else:
    st.bar_chart(cat.set_index("category")["revenue"])

daily = q(f"""SELECT TIME_FLOOR(__time, 'P1D') AS "day", SUM(revenue) AS revenue, SUM(is_purchase) AS purchases
              FROM "{DS}" WHERE {where} AND event_type = 'purchase' GROUP BY 1 ORDER BY 1""", params)
with st.expander("Daily totals (useful to show late arrivals / moved events)"):
    st.dataframe(daily, use_container_width=True)
with st.expander("Druid SQL used"):
    st.code(cat_sql, language="sql")
