import streamlit as st  # 👈 Make sure this is line 1!
import psycopg2
import pandas as pd
import json
import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots

st.set_page_config(page_title="Rechaj Energy SCADA Terminal", layout="wide")

# Industrial CSS Layout Injector
st.markdown("""
<style>
    .metric-card { background-color: #1A1F2C; border: 1px solid #2D3748; border-radius: 8px; padding: 20px; }
    .metric-title { color: #A0AEC0; font-size: 0.85rem; text-transform: uppercase; font-weight: 600; }
    .metric-value { color: #FFFFFF; font-size: 1.8rem; font-weight: 700; font-family: 'Courier New', monospace; }
    .slot-box { padding: 14px; border-radius: 6px; margin-bottom: 10px; border-left: 5px solid #4A5568; }
    .slot-active { background-color: #162E22; border-left-color: #38A169; }
    .slot-standby { background-color: #1A263B; border-left-color: #3182CE; }
    .slot-empty { background-color: #1E2025; opacity: 0.6; }
</style>
""", unsafe_allow_html=True)

DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres.ehncmhxcratiyupmkzpv",
    "password": "Ngtech@19#19",
    "host": "aws-1-eu-north-1.pooler.supabase.com",
    "port": "6543"
}

# Helper to fetch the latest state from database history securely
def fetch_latest_telemetry():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        query = """
            with latest_records as (
                select distinct on (topic) topic, payload, created_at
                from telemetry_history
                order by topic, created_at desc
            )
            select topic, payload::text from latest_records;
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=10)
def query_production_history(target_date):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        edu_query = """
            SELECT created_at as timestamp, 
                   (payload->>'p_total')::float/1000.0 as p_total_kw,
                   (payload->>'energy')::float as raw_energy_wh,
                   (payload->>'sw_calculated_energy')::float as sw_energy_kwh
            FROM telemetry_history 
            WHERE topic = 'SMN/EDU' AND (created_at AT TIME ZONE 'UTC')::date = %s 
            ORDER BY created_at ASC;
        """
        df_edu = pd.read_sql_query(edu_query, conn, params=(target_date,))
        conn.close()
        return df_edu
    except Exception:
        return pd.DataFrame()

st.markdown("## 🏭 RECHAJ ENERGY | SCADA COMMAND CORE")
live_tab, historical_tab = st.tabs(["⚡ REAL-TIME CONTROL NODE", "📅 ARCHIVAL DATA EXPLORER"])

with live_tab:
    # Auto-refreshes every 5 seconds securely on the web interface
    st.fragment(run_every=5)
    def render_live_view():
        df = fetch_latest_telemetry()
        edu_data = {}
        hb_data = {}
        slots = {}
        
        if not df.empty:
            for _, row in df.iterrows():
                top = row['topic']
                pay = json.loads(row['payload'])
                if top == "SMN/EDU": edu_data = pay
                elif top == "SMN/HEARTBEAT": hb_data = pay
                elif "SLOT" in top: slots[top] = pay

        up_s = int(hb_data.get("uptime", 0))
        uptime_string = f"{up_s//86400}d {(up_s%86400)//3600}h {(up_s%3600)//60}m"
        p_kw = float(edu_data.get("p_total", 0.0)) / 1000.0

        c1, c2, c3 = st.columns(3)
        with c1: st.markdown(f"<div class='metric-card'><div class='metric-title'>Station Load</div><div class='metric-value'>{p_kw:.2f} kW</div></div>", unsafe_allow_html=True)
        with c2: st.markdown(f"<div class='metric-card'><div class='metric-title'>Calculated Energy</div><div class='metric-value'>{edu_data.get('sw_calculated_energy', 0.0)} kWh</div></div>", unsafe_allow_html=True)
        with c3: st.markdown(f"<div class='metric-card'><div class='metric-title'>Station Uptime</div><div class='metric-value'>{uptime_string}</div></div>", unsafe_allow_html=True)

        st.markdown("<br><h5>🎛️ Local Charging Grid Grid Matrix</h5>", unsafe_allow_html=True)
        cols = st.columns(4)
        for i in range(1, 5):
            slot_key = f"SMN/DCU/1/SLOT/{i}"
            with cols[i-1]:
                if slot_key in slots:
                    s = slots[slot_key]
                    st.markdown(f"<div class='slot-box slot-active'><strong>SLOT 0{i}</strong><br>SOC: {s.get('soc',0)}%<br>{s.get('bms_v',0)}V</div>", unsafe_allow_html=True)
                else:
                    st.markdown(f"<div class='slot-box slot-empty'><strong>SLOT 0{i}</strong><br>Disconnected</div>", unsafe_allow_html=True)
    render_live_view()

with historical_tab:
    selected_date = st.date_input("Select Target Query Window:", value=datetime.date.today())
    if st.button("Query Database Archives", type="primary"):
        hist_edu = query_production_history(selected_date)
        if not hist_edu.empty:
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(go.Scatter(x=hist_edu['timestamp'], y=hist_edu['p_total_kw'], name="Power Load (kW)"), secondary_y=False)
            fig.add_trace(go.Scatter(x=hist_edu['timestamp'], y=hist_edu['sw_energy_kwh'], name="SW Energy (kWh)"), secondary_y=True)
            fig.update_layout(template="plotly_dark", paper_bgcolor="#1A1F2C", plot_bgcolor="#1A1F2C")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No logs found for this date window.")