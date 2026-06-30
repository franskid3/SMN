import streamlit as st
import psycopg2
import pandas as pd
import json
import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# 1. Page Configuration & Advanced Industrial UI Styling
st.set_page_config(page_title="Rechaj Energy SCADA Terminal", layout="wide")

st.markdown("""
<style>
    body { background-color: #0E1117; color: #E2E8F0; }
    .metric-card { 
        background-color: #1A1F2C; 
        border: 1px solid #2D3748; 
        border-radius: 8px; 
        padding: 22px; 
        box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1);
    }
    .metric-title { color: #A0AEC0; font-size: 0.85rem; text-transform: uppercase; font-weight: 600; letter-spacing: 0.5px; }
    .metric-value { color: #FFFFFF; font-size: 2rem; font-weight: 700; font-family: 'Courier New', monospace; margin-top: 5px; }
    .metric-status { font-size: 0.9rem; font-weight: bold; margin-top: 5px; }
    .status-active { color: #38A169; }
    .status-offline { color: #E53E3E; }
    
    .slot-card { 
        padding: 16px; 
        border-radius: 6px; 
        margin-bottom: 12px; 
        border-left: 6px solid #4A5568;
        background-color: #1E2025;
    }
    .slot-charging { background-color: #162E22; border-left-color: #38A169; }
    .slot-standby { background-color: #1A263B; border-left-color: #3182CE; }
    .slot-fault { background-color: #3A1C1C; border-left-color: #E53E3E; }
    .slot-text { font-family: 'Courier New', monospace; font-size: 0.95rem; line-height: 1.4; color: #E2E8F0; }
</style>
""", unsafe_allow_html=True)

# 2. Secure Supabase Connection Pool
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres.ehncmhxcratiyupmkzpv",
    "password": "Ngtech@19#19",
    "host": "aws-1-eu-north-1.pooler.supabase.com",
    "port": "6543"
}

# 3. Optimized Database Fetching Functions
def fetch_latest_snapshot():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        query = """
            WITH latest_records AS (
                SELECT DISTINCT ON (topic) topic, payload, created_at
                FROM telemetry_history
                ORDER BY topic, created_at DESC
            )
            SELECT topic, payload::text FROM latest_records;
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()

def query_station_history(target_date):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        query = """
            SELECT created_at AT TIME ZONE 'UTC' as timestamp, 
                   (payload->>'p_total')::float / 1000.0 as p_total_kw,
                   (payload->>'energy')::float as raw_energy_wh,
                   (payload->>'sw_calculated_energy')::float as sw_energy_kwh
            FROM telemetry_history 
            WHERE topic = 'SMN/EDU' AND (created_at AT TIME ZONE 'UTC')::date = %s 
            ORDER BY created_at ASC;
        """
        df = pd.read_sql_query(query, conn, params=(target_date,))
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()

def query_battery_trace(target_date, target_bms_id):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        query = """
            SELECT created_at AT TIME ZONE 'UTC' as timestamp,
                   (payload->>'soc')::float as soc,
                   (payload->>'bms_v')::float as voltage,
                   (payload->>'bms_i')::float as current
            FROM telemetry_history
            WHERE topic LIKE 'SMN/DCU/%%/SLOT/%%' 
              AND payload->>'bms_id' = %s
              AND (created_at AT TIME ZONE 'UTC')::date = %s
            ORDER BY created_at ASC;
        """
        df = pd.read_sql_query(query, conn, params=(target_bms_id, target_date))
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()

def get_unique_bms_ids(target_date):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        query = """
            SELECT DISTINCT (payload->>'bms_id') as bms_id
            FROM telemetry_history
            WHERE topic LIKE 'SMN/DCU/%%/SLOT/%%' 
              AND (created_at AT TIME ZONE 'UTC')::date = %s
              AND payload->>'bms_id' IS NOT NULL;
        """
        df = pd.read_sql_query(query, conn, params=(target_date,))
        conn.close()
        return df['bms_id'].tolist()
    except Exception:
        return []

# --- APP LAYOUT HEADER ---
st.markdown("## 🏭 RECHAJ ENERGY | SCADA CORE EXECUTIVE SUITE")
st.markdown("---")

live_tab, historical_tab = st.tabs(["⚡ REAL-TIME MONITORING CONTROL", "📊 ASSET DATA TRACE DESK"])

# ==========================================
# TAB 1: REAL-TIME OPERATIONAL CONTROLS
# ==========================================
with live_tab:
    # Auto-refreshes every 4 seconds to guarantee data freshness
    st.fragment(run_every=4)
    def render_live_pipeline():
        snapshot_df = fetch_latest_snapshot()
        
        edu_payload = {}
        heartbeat_payload = {}
        active_slots = {}
        
        if not snapshot_df.empty:
            for _, row in snapshot_df.iterrows():
                top = row['topic']
                pay = json.loads(row['payload'])
                if top == "SMN/EDU": edu_payload = pay
                elif top == "SMN/HEARTBEAT": heartbeat_payload = pay
                elif "SLOT" in top: active_slots[top] = pay

        # Compute Core Metrics
        up_seconds = int(heartbeat_payload.get("uptime", 0))
        uptime_fmt = f"{up_seconds//86400}d {(up_seconds%86400)//3600}h {(up_seconds%3600)//60}m"
        p_total_kw = float(edu_payload.get("p_total", 0.0)) / 1000.0
        
        # Interlock and Generator Status Evaluation
        is_gen_active = (edu_payload.get("gen1_mask", 0) == 1 or edu_payload.get("gen2_mask", 0) == 1)
        system_status = heartbeat_payload.get("system", "OFFLINE")

        # Top Executive Information Row
        m1, m2, m3, m4 = st.columns(4)
        with m1:
            st.markdown(f"""
            <div class='metric-card'>
                <div class='metric-title'>Active Load Sum</div>
                <div class='metric-value'>{p_total_kw:.2f} kW</div>
            </div>""", unsafe_allow_html=True)
        with m2:
            st.markdown(f"""
            <div class='metric-card'>
                <div class='metric-title'>Virtual Energy Sum</div>
                <div class='metric-value'>{edu_payload.get('sw_calculated_energy', 0.0):.3f} kWh</div>
            </div>""", unsafe_allow_html=True)
        with m3:
            st.markdown(f"""
            <div class='metric-card'>
                <div class='metric-title'>System Core Heartbeat</div>
                <div class='metric-value'>{uptime_fmt}</div>
            </div>""", unsafe_allow_html=True)
        with m4:
            gen_text = "GENERATOR ON" if is_gen_active else "GRID UTILITY ACTIVE"
            gen_class = "status-active" if is_gen_active else "status-standby"
            st.markdown(f"""
            <div class='metric-card'>
                <div class='metric-title'>Power Source / State</div>
                <div class='metric-value' style='font-size:1.4rem; padding-top:8px;'>{gen_text}</div>
                <div class='metric-status {gen_class}'>Node Status: {system_status}</div>
            </div>""", unsafe_allow_html=True)

        # Redundant Audit Desk
        st.markdown("<br>", unsafe_allow_html=True)
        with st.expander("📟 Redundant Energy Meter Validation Audit"):
            a1, a2 = st.columns(2)
            with a1:
                st.metric("Hardware Energy Register (📟 HW)", f"{edu_payload.get('energy', 0)} Wh")
            with a2:
                st.metric("Software Derived Integration (💻 SW)", f"{edu_payload.get('sw_calculated_energy', 0.0)} kWh")

        # Interactive Sub-Station Slot Grid Matrix
        st.markdown("<br><h3>🔋 Distributed Charging Racks (DCU Asset Grid)</h3>", unsafe_allow_html=True)
        
        selected_rack = st.selectbox("Isolate Distribution Control Unit Rack:", ["DCU Rack Alpha (Nodes 1-12)"])
        
        grid_cols = st.columns(6)
        for idx in range(1, 13):
            slot_topic = f"SMN/DCU/1/SLOT/{idx}"
            col_target = grid_cols[(idx - 1) % 6]
            
            with col_target:
                if slot_topic in active_slots:
                    slot_data = active_slots[slot_topic]
                    status = slot_data.get("status", "STANDBY")
                    
                    card_style = "slot-charging" if "CHARGING" in status else "slot-standby"
                    if "FAULT" in status: card_style = "slot-fault"
                    
                    st.markdown(f"""
                    <div class='slot-card {card_style}'>
                        <div class='slot-text'>
                            <strong>SLOT {idx:02d}</strong><br>
                            <span style='font-size:0.8rem;'>ID: {slot_data.get('bms_id','').split('-')[-1]}</span><br>
                            <b>SOC: {slot_data.get('soc', 0)}%</b><br>
                            V: {slot_data.get('bms_v', 0.0)}V | I: {slot_data.get('bms_i', 0.0)}A
                        </div>
                    </div>""", unsafe_allow_html=True)
                else:
                    st.markdown(f"""
                    <div class='slot-card'>
                        <div class='slot-text' style='opacity:0.4;'>
                            <strong>SLOT {idx:02d}</strong><br>
                            Empty / Disconnected
                        </div>
                    </div>""", unsafe_allow_html=True)
    render_live_pipeline()

# ==========================================
# TAB 2: ARCHIVAL DATA EXPLORER & TRACE DESK
# ==========================================
with historical_tab:
    st.markdown("### 📈 Historical Analytics & Asset Inspection")
    
    c_left, c_right = st.columns([1, 2])
    with c_left:
        selected_date = st.date_input("Select Historical Query Window:", value=datetime.date.today())
        available_bms_ids = get_unique_bms_ids(selected_date)
        selected_bms_id = st.selectbox("Select Target Battery Asset Tracking String (BMS_ID):", available_bms_ids if available_bms_ids else ["No battery assets logged today"])

    # Executive Station Load Analysis Charts
    st.markdown("#### ⚡ Station Phase Load Analysis")
    hist_station_df = query_station_history(selected_date)
    
    if not hist_station_df.empty:
        fig_station = make_subplots(specs=[["..", ".."]], secondary_y=True)
        fig_station.add_trace(go.Scatter(x=hist_station_df['timestamp'], y=hist_station_df['p_total_kw'], name="Real Power Demand (kW)", line=dict(color="#3182CE", width=2)), secondary_y=False)
        fig_station.add_trace(go.Scatter(x=hist_station_df['timestamp'], y=hist_station_df['sw_energy_kwh'], name="SW Calculated Energy (kWh)", line=dict(color="#38A169", width=2, dash='dot')), secondary_y=True)
        
        fig_station.update_layout(
            template="plotly_dark", 
            paper_bgcolor="#1A1F2C", 
            plot_bgcolor="#1A1F2C",
            margin=dict(l=40, r=40, t=20, b=40),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        st.plotly_chart(fig_station, use_container_width=True)
    else:
        st.info("No station load metrics found for the selected calendar date.")

    # High-Resolution Battery Lifespan Deep Trace
    st.markdown(f"#### 🔋 Deep Battery Lifecycle Audit File [Asset: {selected_bms_id}]")
    if selected_bms_id and selected_bms_id != "No battery assets logged today":
        trace_df = query_battery_trace(selected_date, selected_bms_id)
        
        if not trace_df.empty:
            fig_trace = make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.08, subplot_titles=("State of Charge Lifecycle (%)", "Voltage Signature (V)", "Current Draw Profile (A)"))
            
            fig_trace.add_trace(go.Scatter(x=trace_df['timestamp'], y=trace_df['soc'], name="SOC (%)", line=dict(color="#4FD1C5")), row=1, col=1)
            fig_trace.add_trace(go.Scatter(x=trace_df['timestamp'], y=trace_df['voltage'], name="Voltage (V)", line=dict(color="#ED64A6")), row=2, col=1)
            fig_trace.add_trace(go.Scatter(x=trace_df['timestamp'], y=trace_df['current'], name="Current (A)", line=dict(color="#ECC94B")), row=3, col=1)
            
            fig_trace.update_layout(template="plotly_dark", paper_bgcolor="#1A1F2C", plot_bgcolor="#1A1F2C", height=500, showlegend=False, margin=dict(l=40, r=40, t=40, b=40))
            st.plotly_chart(fig_trace, use_container_width=True)
        else:
            st.info("Select a logged battery string to draw structural timeline traces.")