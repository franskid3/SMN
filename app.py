import streamlit as st
import psycopg2
import pandas as pd
import json
import paho.mqtt.client as mqtt
import threading
import time
import queue
import datetime
import socket
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# =========================================================================
# SYSTEM STYLING & CORE CONFIGURATION
# =========================================================================
st.set_page_config(
    page_title="SMN Scada Command Terminal", 
    layout="wide", 
    initial_sidebar_state="expanded"
)

# Custom Industrial-Grade CSS Injection
st.markdown("""
<style>
    .reportview-container { background: #0E1117; }
    .metric-card {
        background-color: #1A1F2C;
        border: 1px solid #2D3748;
        border-radius: 8px;
        padding: 20px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.15);
    }
    .metric-title {
        color: #A0AEC0;
        font-size: 0.85rem;
        text-transform: uppercase;
        letter-spacing: 1px;
        font-weight: 600;
        margin-bottom: 8px;
    }
    .metric-value {
        color: #FFFFFF;
        font-size: 1.8rem;
        font-weight: 700;
        font-family: 'Courier New', monospace;
    }
    .metric-status {
        font-size: 0.85rem;
        margin-top: 6px;
        font-weight: bold;
    }
    .slot-box {
        padding: 14px;
        border-radius: 6px;
        margin-bottom: 10px;
        border-left: 5px solid #4A5568;
    }
    .slot-active { background-color: #162E22; border-left-color: #38A169; }
    .slot-standby { background-color: #1A263B; border-left-color: #3182CE; }
    .slot-fault { background-color: #2D1F1F; border-left-color: #E53E3E; }
    .slot-empty { background-color: #1E2025; border-left-color: #4A5568; opacity: 0.6; }
</style>
""", unsafe_allow_html=True)

# Hardware Infrastructure Target Parameters
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres.ehncmhxcratiyupmkzpv",
    "password": "Ngtech@19#19",
    "host": "aws-1-eu-north-1.pooler.supabase.com",
    "port": "6543"
}

MQTT_CONFIG = {
    "broker": "cow.rmq2.cloudamqp.com",
    "port": 1883,
    "user": "klsazaul:klsazaul",
    "pass": "EJyfwSCQdtKsiEe-HLDmkYEffe45MhPJ",
    "topic": "SMN/#"
}

# --- THREAD SAFE MEMORY PERSISTENCE INFRASTRUCTURE ---
if "mqtt_telemetry_queue" not in globals():
    globals()["mqtt_telemetry_queue"] = queue.Queue(maxsize=1000)
if "write_queue" not in globals():
    globals()["write_queue"] = queue.Queue(maxsize=5000)

live_telemetry_queue = globals()["mqtt_telemetry_queue"]
db_write_queue = globals()["write_queue"]

# UI Persistent Caches
if "live_edu" not in st.session_state:
    st.session_state["live_edu"] = {}
if "live_slots" not in st.session_state:
    st.session_state["live_slots"] = {}
if "live_heartbeat" not in st.session_state:
    st.session_state["live_heartbeat"] = {"system": "OFFLINE", "uptime": 0}

# =========================================================================
# MULTI-THREADED ASYNCHRONOUS PIPELINE DAEMONS
# =========================================================================
def async_db_logger_worker():
    """Background consumer writing logs down to Supabase with software energy calculation."""
    latest_uptime = 0
    
    while True:
        item = db_write_queue.get()
        if item is None: break
        topic, payload_str = item
        try:
            # 1. Capture system uptime clock dynamically from heartbeat payloads
            if topic == "SMN/HEARTBEAT":
                try:
                    hb_data = json.loads(payload_str)
                    latest_uptime = int(hb_data.get("uptime", 0))
                except Exception: pass

            # 2. Integrate mathematical virtual energy sum when main station metric loads land
            if topic == "SMN/EDU":
                try:
                    edu_data = json.loads(payload_str)
                    p_total_kw = float(edu_data.get("p_total", 0.0)) / 1000.0
                    uptime_hours = latest_uptime / 3600.0
                    
                    # Virtual Integration Calculation
                    sw_energy = p_total_kw * uptime_hours
                    edu_data["sw_calculated_energy"] = round(sw_energy, 4)
                    payload_str = json.dumps(edu_data)
                except Exception: pass

            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO telemetry_history (topic, payload) VALUES (%s, %s::jsonb);",
                (topic, payload_str)
            )
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"Database ingestion error fallback: {e}")
        finally:
            db_write_queue.task_done()

def run_mqtt_bridge_worker():
    """Background listener collecting incoming hardware broker lines."""
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(MQTT_CONFIG["user"], MQTT_CONFIG["pass"])

    def on_connect(client, userdata, flags, rc, properties=None):
        if rc == 0:
            client.subscribe(MQTT_CONFIG["topic"])

    def on_message(client, userdata, msg):
        try:
            topic = msg.topic
            payload_str = msg.payload.decode("utf-8")
            parsed_payload = json.loads(payload_str)
            
            if not live_telemetry_queue.full():
                live_telemetry_queue.put((topic, parsed_payload))
            if not db_write_queue.full():
                db_write_queue.put((topic, payload_str))
        except Exception as e:
            print(f"Ingestion parse failure: {e}")

    client.on_connect = on_connect
    client.on_message = on_message
    
    while True:
        try:
            client.connect(MQTT_CONFIG["broker"], MQTT_CONFIG["port"], 60)
            client.loop_forever()
        except (socket.gaierror, Exception):
            time.sleep(10)

@st.cache_resource
def initialize_system_infrastructure():
    t1 = threading.Thread(target=run_mqtt_bridge_worker, daemon=True)
    t2 = threading.Thread(target=async_db_logger_worker, daemon=True)
    t1.start()
    t2.start()
    return True

initialize_system_infrastructure()

# =========================================================================
# OPTIMIZED DATA SERVICE INTEGRATION LAYER
# =========================================================================
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
        
        slot_query = """
            SELECT created_at as timestamp, dcu_id, slot_id, bms_id, soc, soh, bms_v, bms_i, instantaneous_slot_efficiency
            FROM calculated_live_analytics
            WHERE (created_at AT TIME ZONE 'UTC')::date = %s
            ORDER BY created_at ASC;
        """
        df_slots = pd.read_sql_query(slot_query, conn, params=(target_date,))
        conn.close()
        return df_edu, df_slots
    except Exception as e:
        st.error(f"SCADA Database Fetch Exception: {e}")
        return pd.DataFrame(), pd.DataFrame()

# =========================================================================
# SCADA EXECUTIVE TERMINAL LAYOUT
# =========================================================================
st.markdown("## 🏭 STATION MASTER NODE (SMN) | SCADA COMMAND CORE")

live_tab, historical_tab = st.tabs(["⚡ REAL-TIME CONTROL NODE", "📅 ARCHIVAL DATA SYSTEMS EXPLORER"])

# -------------------------------------------------------------------------
# TAB 1: EXECUTIVE LIVE OPERATIONS MONITORING
# -------------------------------------------------------------------------
with live_tab:
    @st.fragment(run_every=2)
    def draw_live_dashboard():
        while not live_telemetry_queue.empty():
            try:
                topic, parsed_payload = live_telemetry_queue.get_nowait()
                if topic == "SMN/EDU":
                    st.session_state["live_edu"] = parsed_payload
                elif topic == "SMN/HEARTBEAT":
                    st.session_state["live_heartbeat"] = parsed_payload
                elif "SLOT" in topic:
                    parts = topic.split("/")
                    dcu_id, slot_id = int(parts[2]), int(parts[4])
                    st.session_state["live_slots"][f"{dcu_id}_{slot_id}"] = parsed_payload
                live_telemetry_queue.task_done()
            except queue.Empty: break

        latest_edu = st.session_state["live_edu"]
        latest_slots = st.session_state["live_slots"]
        hb = st.session_state["live_heartbeat"]
        
        up_s = int(hb.get("uptime", 0))
        uptime_string = f"{up_s//86400}d {(up_s%86400)//3600}h {(up_s%3600)//60}m {up_s%60}s"
        
        # Redundant Calculations
        p_total_kw = float(latest_edu.get("p_total", 0.0)) / 1000.0
        software_calculated_energy_kwh = p_total_kw * (up_s / 3600.0)
        hardware_reported_energy_kwh = float(latest_edu.get('energy', 0)) / 1000.0
        
        m_col1, m_col2, m_col3, m_col4 = st.columns(4)
        with m_col1:
            st.markdown(f"""<div class='metric-card'><div class='metric-title'>Primary Station Load</div><div class='metric-value'>{p_total_kw:.2f} kW</div><div class='metric-status' style='color:#3182CE;'>⚡ Active Draw</div></div>""", unsafe_allow_html=True)
        with m_col2:
            st.markdown(f"""
            <div class='metric-card'>
                <div class='metric-title'>Total Accumulative Energy</div>
                <div class='metric-value' style='font-size: 1.35rem; line-height: 1.3;'>
                    💻 SW: {software_calculated_energy_kwh:.2f} kWh<br>
                    📟 HW: {hardware_reported_energy_kwh:.2f} kWh
                </div>
                <div class='metric-status' style='color:#A0AEC0;'>📊 Redundant Audit Lines</div>
            </div>
            """, unsafe_allow_html=True)
        with m_col3:
            st.markdown(f"""<div class='metric-card'><div class='metric-title'>System Status Core</div><div class='metric-value'>{hb.get('system','OFFLINE')}</div><div class='metric-status' style='color:#38A169;'>● Handshake Active</div></div>""", unsafe_allow_html=True)
        with m_col4:
            st.markdown(f"""<div class='metric-card'><div class='metric-title'>System Engine Uptime</div><div class='metric-value'>{uptime_string}</div><div class='metric-status' style='color:#E2E8F0;'>⏳ Continuous Running</div></div>""", unsafe_allow_html=True)
            
        st.markdown("<br>", unsafe_allow_html=True)
        
        lay_left, lay_right = st.columns([1, 2])
        with lay_left:
            st.markdown("<h5 style='color:#A0AEC0;'>🔌 Power Transformer Load Balance</h5>", unsafe_allow_html=True)
            if latest_edu:
                phase_df = pd.DataFrame({
                    "Transformer Phase": ["Phase Line 1 (L1)", "Phase Line 2 (L2)", "Phase Line 3 (L3)"],
                    "Voltage (L-N)": [f"{latest_edu.get('v1', 0.0)} V", f"{latest_edu.get('v2', 0.0)} V", f"{latest_edu.get('v3', 0.0)} V"],
                    "Current Intensity": [f"{latest_edu.get('i1', 0.0)} A", f"{latest_edu.get('i2', 0.0)} A", f"{latest_edu.get('i3', 0.0)} A"]
                })
                st.dataframe(phase_df, use_container_width=True, hide_index=True)
                
        with lay_right:
            st.markdown("<h5 style='color:#A0AEC0;'>🎛️ Distributed Bus Allocation Grid Matrix</h5>", unsafe_allow_html=True)
            active_dcu = st.selectbox("Isolate Distribution Rack Framework:", options=[1, 2, 3, 4, 5, 6], index=0)
            
            for row in range(3):
                cols = st.columns(4)
                for col in range(4):
                    slot_index = (row * 4) + col + 1
                    lookup_key = f"{active_dcu}_{slot_index}"
                    
                    with cols[col]:
                        if lookup_key in latest_slots:
                            s_raw = latest_slots[lookup_key]
                            c_current = float(s_raw.get("bms_i", 0.0))
                            s_class, label = ("slot-active", f"⚡ CHG ({c_current}A)") if c_current > 0.5 else ("slot-standby", "💤 STANDBY")
                                
                            st.markdown(f"""
                            <div class='slot-box {s_class}'>
                                <strong style='font-size:0.95rem; display:block;'>SLOT {slot_index:02d}</strong>
                                <span style='font-size:0.75rem; color:#A0AEC0; display:block;'>ID: {s_raw.get('bms_id','---')[:12]}</span>
                                <span style='font-size:1.1rem; font-weight:bold; display:block; margin:4px 0;'>SOC: {s_raw.get('soc',0)}%</span>
                                <span style='font-size:0.8rem; font-family:monospace; display:block;'>{s_raw.get('bms_v',0.0)}V | {s_raw.get('bms_temp',0)}°C</span>
                            </div>
                            """, unsafe_allow_html=True)
                        else:
                            st.markdown(f"""<div class='slot-box slot-empty'><strong style='font-size:0.95rem; color:#718096; display:block;'>SLOT {slot_index:02d}</strong><span style='font-size:0.75rem; color:#4A5568; display:block; margin-top:10px;'>Isolated</span></div>""", unsafe_allow_html=True)
                            
    draw_live_dashboard()

# -------------------------------------------------------------------------
# TAB 2: INDUSTRIAL HISTORICAL PERFORMANCE ANALYSIS (WITH PACK TRACE LEDGER)
# -------------------------------------------------------------------------
with historical_tab:
    st.markdown("<h4 style='color:#A0AEC0;'>📅 Archival Telemetry Ledger & Asset Trace Analyzer</h4>", unsafe_allow_html=True)
    
    c_pick1, c_pick2 = st.columns([1, 3])
    with c_pick1:
        selected_historical_date = st.date_input("Select Target Query Window:", value=datetime.date.today())
        trigger_search = st.button("Query Database Archives", type="primary", use_container_width=True)

    if trigger_search:
        with st.spinner("Processing historical record indexes..."):
            hist_edu, hist_slots = query_production_history(selected_historical_date)
            
            if hist_edu.empty and hist_slots.empty:
                st.error("No telemetry records found for this specific date.")
            else:
                # --- POWER ANALYSIS MATRIX ---
                if not hist_edu.empty:
                    st.markdown("### 🔌 Transformer Node Power Grid Metrics")
                    fig_power = make_subplots(specs=[[{"secondary_y": True}]])
                    fig_power.add_trace(go.Scatter(x=hist_edu['timestamp'], y=hist_edu['p_total_kw'], name="Power Load (kW)", line=dict(color="#3182CE", width=2.5)), secondary_y=False)
                    fig_power.add_trace(go.Scatter(x=hist_edu['timestamp'], y=hist_edu['raw_energy_wh']/1000.0, name="HW Meter (kWh)", line=dict(color="#38A169", width=1.5, dash='dot')), secondary_y=True)
                    
                    if 'sw_energy_kwh' in hist_edu.columns:
                        fig_power.add_trace(go.Scatter(x=hist_edu['timestamp'], y=hist_edu['sw_energy_kwh'], name="SW Calculated (kWh)", line=dict(color="#E53E3E", width=2)), secondary_y=True)
                        
                    fig_power.update_layout(template="plotly_dark", paper_bgcolor="#1A1F2C", plot_bgcolor="#1A1F2C")
                    st.plotly_chart(fig_power, use_container_width=True)

                # --- SPECIFIC BATTERY TRACE LEDGER (BY SELECTABLE ID) ---
                st.markdown("---")
                st.markdown("### 🔋 Lithium Pack Serial Trace Desks")
                
                if not hist_slots.empty:
                    # Filter unique strings safely
                    available_bms_ids = [id for id in hist_slots['bms_id'].unique() if id and id != 'Unknown']
                    
                    if available_bms_ids:
                        target_bms_id = st.selectbox("Isolate Specific Pack Serial Reference ID for Timeline Trace:", options=available_bms_ids)
                        
                        # Filter down data exclusively for this isolated battery ID
                        pack_timeline = hist_slots[hist_slots['bms_id'] == target_bms_id].sort_values('timestamp')
                        
                        t_col1, t_col2, t_col3 = st.columns(3)
                        with t_col1:
                            st.metric("Peak Observed SOC", f"{pack_timeline['soc'].max()}%")
                        with t_col2:
                            st.metric("Reported Pack Health (SOH)", f"{pack_timeline['soh'].iloc[-1]}%")
                        with t_col3:
                            st.metric("Avg Conversion Efficiency", f"{pack_timeline['instantaneous_slot_efficiency'].mean():.2f}%")
                        
                        # Multi-Timeline Diagnostic Metric Plotting
                        fig_pack = make_subplots(rows=2, cols=1, shared_xaxes=True, subplot_titles=("State of Charge (SOC %) History", "Electrical Profiles (Voltage & Current)"))
                        
                        # Plot SOC Timeline
                        fig_pack.add_trace(go.Scatter(x=pack_timeline['timestamp'], y=pack_timeline['soc'], name="SOC %", line=dict(color="#38A169", width=2)), row=1, col=1)
                        
                        # Plot V/I Timelines
                        fig_pack.add_trace(go.Scatter(x=pack_timeline['timestamp'], y=pack_timeline['bms_v'], name="Voltage (V)", line=dict(color="#3182CE")), row=2, col=1)
                        fig_pack.add_trace(go.Scatter(x=pack_timeline['timestamp'], y=pack_timeline['bms_i'], name="Current (A)", line=dict(color="#E53E3E")), row=2, col=1)
                        
                        fig_pack.update_layout(height=500, template="plotly_dark", paper_bgcolor="#1A1F2C", plot_bgcolor="#1A1F2C")
                        st.plotly_chart(fig_pack, use_container_width=True)
                    else:
                        st.info("No valid serial battery frames identified in this session block.")
                else:
                    st.info("No distribution slots communication logged on this date window.")