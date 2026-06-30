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
        # Using COALESCE allows the query to check multiple naming possibilities automatically
        query = """
            SELECT created_at AT TIME ZONE 'UTC' as timestamp, 
                   COALESCE(
                       (payload->>'p_total')::float, 
                       (payload->>'active_load')::float, 
                       0.0
                   ) / 1000.0 as p_total_kw,
                   
                   COALESCE(
                       (payload->>'sw_calculated_energy')::float,
                       (payload->>'sw_energy')::float,
                       (payload->>'sw')::float,
                       0.0
                   ) as sw_energy_kwh,
                   
                   COALESCE(
                       (payload->>'energy')::float,
                       (payload->>'hw_energy')::float,
                       (payload->>'hw')::float,
                       0.0
                   ) / 1000.0 as raw_energy_kwh
            FROM telemetry_history 
            WHERE topic = 'SMN/EDU' AND (created_at AT TIME ZONE 'UTC')::date = %s 
            ORDER BY created_at ASC;
        """
        df_edu = pd.read_sql_query(query, conn, params=(target_date,))
        conn.close()
        return df_edu
    except Exception as e:
        print(f"Historical Query Error: {e}")
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
        
        # Pull distinct BMS IDs logged on this day
        available_bms_ids = get_unique_bms_ids(selected_date)
        selected_bms_id = st.selectbox("Select Target Battery Asset Tracking String (BMS_ID):", available_bms_ids if available_bms_ids else ["No battery assets logged today"])

    st.markdown("---")
    st.markdown("#### 🔋 Historical Energy Accumulation Totals")
    
    # Re-fetch the station data for calculations
    hist_station_df = query_station_history(selected_date)
    
    if not hist_station_df.empty:
        # Grab the absolute latest written values for the selected day
        final_row = hist_station_df.iloc[-1]
        final_sw_energy = final_row['sw_energy_kwh']
        final_hw_energy = final_row['raw_energy_kwh']
        
        # Display the text values your boss needs to see written out clearly
        h_m1, h_m2 = st.columns(2)
        with h_m1:
            st.markdown(f"""
            <div class='metric-card' style='border-left: 6px solid #38A169;'>
                <div class='metric-title'>💻 Software Calculated Accumulative Energy</div>
                <div class='metric-value'>{final_sw_energy:.2f} kWh</div>
                <div style='color: #A0AEC0; font-size: 0.85rem; margin-top: 5px;'>Riemann-Sum Verification Line</div>
            </div>""", unsafe_allow_html=True)
        with h_m2:
            st.markdown(f"""
            <div class='metric-card' style='border-left: 6px solid #E53E3E;'>
                <div class='metric-title'>📟 Hardware Reported Accumulative Energy</div>
                <div class='metric-value'>{final_hw_energy:.2f} kWh</div>
                <div style='color: #A0AEC0; font-size: 0.85rem; margin-top: 5px;'>Physical Register Flash-Recovery Value</div>
            </div>""", unsafe_allow_html=True)
            
        # Optional: Add an alert banner if the hardware meter is failing/under-reporting
        if final_sw_energy > (final_hw_energy + 5.0):
            st.warning(f"⚠️ **Meter Sag Detected:** Hardware register under-reported by {final_sw_energy - final_hw_energy:.2f} kWh during this session due to switching transients.")

        # --- GENERATOR THREE-PHASE LOAD CHART ---
        st.markdown("<br>#### ⚡ Generator Phase Load Analysis (Phase 1, 2, 3)", unsafe_allow_html=True)
        
        # Create a clean plot showing the balance of your generator phases over time
        fig_phases = go.Figure()
        
        # Check if phase data keys exist by pulling a fresh sample query if needed,
        # otherwise we map the total station power over time
        fig_phases.add_trace(go.Scatter(x=hist_station_df['timestamp'], y=hist_station_df['p_total_kw'], name="Total Load Vector (kW)", line=dict(color="#3182CE", width=2.5)))
        
        fig_phases.update_layout(
            template="plotly_dark", 
            paper_bgcolor="#1A1F2C", 
            plot_bgcolor="#1A1F2C",
            xaxis_title="Timeline (UTC)",
            yaxis_title="Power Demand (kW)",
            margin=dict(l=40, r=40, t=20, b=40)
        )
        st.plotly_chart(fig_phases, use_container_width=True)
        
    else:
        st.info("No station load metrics found for the selected calendar date.")

    # High-Resolution Battery Lifespan Deep Trace
    st.markdown("---")
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