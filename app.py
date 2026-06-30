import streamlit as st
import psycopg2
import pandas as pd
import json
import paho.mqtt.client as mqtt
import threading
import time
import queue
import datetime
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
    /* Global Styles */
    .reportview-container { background: #0E1117; }
    
    /* Industrial Telemetry Cards */
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
    
    /* Hardware Matrix Slots */
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

if "RAW_EDU" not in globals(): globals()["RAW_EDU"] = {}
if "RAW_SLOTS" not in globals(): globals()["RAW_SLOTS"] = {}
if "RAW_HEARTBEAT" not in globals(): globals()["RAW_HEARTBEAT"] = {"system": "OFFLINE", "uptime": 0}

db_write_queue = queue.Queue(maxsize=5000)

# =========================================================================
# MULTI-THREADED ASYNCHRONOUS PIPELINE DAEMONS
# =========================================================================
def async_db_logger_worker():
    while True:
        item = db_write_queue.get()
        if item is None: break
        topic, payload_str = item
        try:
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
            print(f"Database ingestion fallback log: {e}")
        finally:
            db_write_queue.task_done()

def run_mqtt_bridge_worker():
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION1)
    client.username_pw_set(MQTT_CONFIG["user"], MQTT_CONFIG["pass"])

    def on_connect(client, userdata, flags, rc):
        client.subscribe(MQTT_CONFIG["topic"])

    def on_message(client, userdata, msg):
        try:
            topic = msg.topic
            payload_str = msg.payload.decode("utf-8")
            parsed_payload = json.loads(payload_str)
            
            if topic == "SMN/EDU":
                globals()["RAW_EDU"] = parsed_payload
            elif topic == "SMN/HEARTBEAT":
                globals()["RAW_HEARTBEAT"] = parsed_payload
            elif "SLOT" in topic:
                try:
                    parts = topic.split("/")
                    dcu_id, slot_id = int(parts[2]), int(parts[4])
                    globals()["RAW_SLOTS"][f"{dcu_id}_{slot_id}"] = parsed_payload
                except Exception: pass
            
            if not db_write_queue.full():
                db_write_queue.put((topic, payload_str))
        except Exception as e:
            print(f"Ingestion parse failure: {e}")

    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_CONFIG["broker"], MQTT_CONFIG["port"], 60)
    client.loop_forever()

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
@st.cache_data(ttl=30)
def query_production_history(target_date):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        edu_query = """
            SELECT created_at as timestamp, 
                (payload->>'p_total')::float/1000.0 as p_total_kw,
                (payload->>'energy')::float/1000.0 as energy_kwh
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
# INDUSTRIAL GRADE SCADA EXECUTIVE HEADLESS LAYOUT
# =========================================================================
st.markdown("## 🏭 STATION MASTER NODE (SMN) | SCADA COMMAND CORE")
st.markdown("<p style='color:#718096; margin-top:-15px;'>Enterprise Infrastructure Operations & Analytical Asset Ledger</p>", unsafe_allow_html=True)

live_tab, historical_tab = st.tabs(["⚡ REAL-TIME CONTROL NODE", "📅 ARCHIVAL DATA SYSTEMS EXPLORER"])

# -------------------------------------------------------------------------
# TAB 1: EXECUTIVE LIVE OPERATIONS MONITORING
# -------------------------------------------------------------------------
with live_tab:
    @st.fragment(run_every=2)
    def draw_live_dashboard():
        latest_edu = globals()["RAW_EDU"]
        latest_slots = globals()["RAW_SLOTS"]
        hb = globals()["RAW_HEARTBEAT"]
        
        # Format System Uptime Metrics
        up_s = int(hb.get("uptime", 0))
        uptime_string = f"{up_s//86400}d {(up_s%86400)//3600}h {(up_s%3600)//60}m {up_s%60}s"
        
        # 1. Top Industrial Metric Strip
        m_col1, m_col2, m_col3, m_col4 = st.columns(4)
        
        p_total_kw = float(latest_edu.get("p_total", 0.0)) / 1000.0
        with m_col1:
            st.markdown(f"""<div class='metric-card'><div class='metric-title'>Primary Station Load</div><div class='metric-value'>{p_total_kw:.2f} kW</div><div class='metric-status' style='color:#3182CE;'>⚡ Active Draw</div></div>""", unsafe_allow_html=True)
        with m_col2:
            st.markdown(f"""<div class='metric-card'><div class='metric-title'>Total Sourced Energy</div><div class='metric-value'>{(float(latest_edu.get('energy', 0))/1000.0):.1f} kWh</div><div class='metric-status' style='color:#A0AEC0;'>📊 Accumulative Registry</div></div>""", unsafe_allow_html=True)
        with m_col3:
            st.markdown(f"""<div class='metric-card'><div class='metric-title'>System Status Core</div><div class='metric-value'>{hb.get('system','OFFLINE')}</div><div class='metric-status' style='color:#38A169;'>● Node Connection Stable</div></div>""", unsafe_allow_html=True)
        with m_col4:
            st.markdown(f"""<div class='metric-card'><div class='metric-title'>System Engine Uptime</div><div class='metric-value'>{uptime_string}</div><div class='metric-status' style='color:#E2E8F0;'>⏳ Continuous Running Line</div></div>""", unsafe_allow_html=True)
            
        st.markdown("<br>", unsafe_allow_html=True)
        
        # 2. Main Transformer Phase Distribution Framework
        lay_left, lay_right = st.columns([1, 2])
        with lay_left:
            st.markdown("<h5 style='color:#A0AEC0;'>🔌 Power Transformer Load Balance</h5>", unsafe_allow_html=True)
            if latest_edu:
                # Direct Interlock Logic Output Display
                if int(latest_edu.get("gen1_mask", 0)) == 1 or str(latest_edu.get("gen1")) == "ACTIVE":
                    st.success("🔒 SOURCE INTERLOCK: AUX GENERATOR ALPHA ONLINE")
                elif int(latest_edu.get("gen2_mask", 0)) == 1 or str(latest_edu.get("gen2")) == "ACTIVE":
                    st.success("🔒 SOURCE INTERLOCK: AUX GENERATOR BETA ONLINE")
                else:
                    st.info("🔒 SOURCE INTERLOCK: UTILITY INDUSTRIAL GRID SOURCED")
                
                # High Fidelity Phasor Metric Rows
                phase_df = pd.DataFrame({
                    "Transformer Phase": ["Phase Line 1 (L1)", "Phase Line 2 (L2)", "Phase Line 3 (L3)"],
                    "Voltage (L-N)": [f"{latest_edu.get('v1')} V", f"{latest_edu.get('v2')} V", f"{latest_edu.get('v3')} V"],
                    "Current Intensity": [f"{latest_edu.get('i1')} A", f"{latest_edu.get('i2')} A", f"{latest_edu.get('i3')} A"]
                })
                st.dataframe(phase_df, use_container_width=True, hide_index=True)
            else:
                st.warning("Awaiting secure connection handshake frames from main EDU controller...")
                
        with lay_right:
            st.markdown("<h5 style='color:#A0AEC0;'>🎛️ Distributed Bus Allocation Grid Matrix (DCU Cluster Map)</h5>", unsafe_allow_html=True)
            active_dcu = st.selectbox("Isolate Hardware Distribution Rack Framework:", options=[1, 2, 3, 4, 5, 6], index=0)
            
            # Draw an explicit grid array of 12 distinct charging slot positions
            for row in range(3):
                cols = st.columns(4)
                for col in range(4):
                    slot_index = (row * 4) + col + 1
                    lookup_key = f"{active_dcu}_{slot_index}"
                    
                    with cols[col]:
                        if lookup_key in latest_slots:
                            s_raw = latest_slots[lookup_key]
                            c_fault = int(s_raw.get("chg_f", 0))
                            c_current = float(s_raw.get("bms_i", 0.0))
                            
                            if c_fault == 32:
                                s_class, label = "slot-fault", "⚠️ CRITICAL FAULT"
                            elif c_current > 0.5:
                                s_class, label = "slot-active", f"⚡ CHARGING ({c_current}A)"
                            else:
                                s_class, label = "slot-standby", "💤 STANDBY READY"
                                
                            st.markdown(f"""
                            <div class='slot-box {s_class}'>
                                <strong style='font-size:0.95rem; display:block;'>SLOT {slot_index:02d}</strong>
                                <span style='font-size:0.75rem; color:#A0AEC0; display:block;'>ID: {s_raw.get('serial',s_raw.get('bms_id','---'))[:12]}</span>
                                <span style='font-size:1.1rem; font-weight:bold; display:block; margin:4px 0;'>SOC: {s_raw.get('soc',0)}% | SOH: {s_raw.get('soh',0)}%</span>
                                <span style='font-size:0.8rem; font-family:monospace; display:block;'>{s_raw.get('bms_v',0.0)}V | {s_raw.get('bms_temp',0)}°C</span>
                                <span style='font-size:0.75rem; font-weight:bold; display:block; margin-top:5px;'>{label}</span>
                            </div>
                            """, unsafe_allow_html=True)
                        else:
                            st.markdown(f"""
                            <div class='slot-box slot-empty'>
                                <strong style='font-size:0.95rem; color:#718096; display:block;'>SLOT {slot_index:02d}</strong>
                                <span style='font-size:0.75rem; color:#4A5568; display:block; font-style:italic; margin-top:10px;'>Bus Circuit Isolated</span>
                            </div>
                            """, unsafe_allow_html=True)
                            
    draw_live_dashboard()

# -------------------------------------------------------------------------
# TAB 2: INDUSTRIAL HISTORICAL PERFORMANCE ANALYSIS
# -------------------------------------------------------------------------
with historical_tab:
    st.markdown("<h4 style='color:#A0AEC0;'>📅 Historical Cluster Session Analytics Audit</h4>", unsafe_allow_html=True)
    
    # Pre-calculated calendar offset configuration
    default_history_target = datetime.date.today() - datetime.timedelta(days=5)
    
    c_pick1, c_pick2 = st.columns([1, 3])
    with c_pick1:
        selected_historical_date = st.date_input(
            "Select Target Analytical Date Window:", 
            value=default_history_target,
            max_value=datetime.date.today()
        )
        trigger_search = st.button("Query Database Archives", type="primary", use_container_width=True)
        
    with c_pick2:
        st.markdown(
            "<div style='background-color:#1A1F2C; padding:15px; border-radius:6px; border: 1px solid #2D3748; font-size:0.85rem; color:#A0AEC0; margin-top:10px;'>"
            "<strong>Executive Audit System Instructions:</strong><br>"
            "Selecting a timestamp forces a relational query against indexed telemetry logs. "
            "Data outputs summarize net station grid efficiency, system loading metrics, and cross-sectional asset cell tracking curves."
            "</div>", unsafe_allow_html=True
        )

    if trigger_search:
        with st.spinner("Compiling database records into high-fidelity graphs..."):
            hist_edu, hist_slots = query_production_history(selected_historical_date)
            
            if hist_edu.empty and hist_slots.empty:
                st.error(f"No telemetric records located inside database archives for {selected_historical_date}.")
            else:
                # --- EXECUTIVE GRAPH 1: STATION RECONCILIATION CHARTS (PLOTLY) ---
                st.markdown("#### 🔌 Sourced Power Grid Demand Profiles")
                if not hist_edu.empty:
                    fig_power = make_subplots(specs=[[{"secondary_y": True}]])
                    
                    fig_power.add_trace(
                        go.Scatter(x=hist_edu['timestamp'], y=hist_edu['p_total_kw'], name="Active Power Demand (kW)", line=dict(color="#3182CE", width=2.5)),
                        secondary_y=False
                    )
                    fig_power.add_trace(
                        go.Scatter(x=hist_edu['timestamp'], y=hist_edu['energy_kwh'], name="Meter Energy Registry (kWh)", line=dict(color="#38A169", width=2, dash='dot')),
                        secondary_y=True
                    )
                    
                    fig_power.update_layout(
                        title_text=f"Grid Electrical Performance Load & Consumption Map ({selected_historical_date})",
                        template="plotly_dark",
                        paper_bgcolor="#1A1F2C",
                        plot_bgcolor="#1A1F2C",
                        margin=dict(l=40, r=40, t=40, b=40)
                    )
                    fig_power.update_xaxes(title_text="Operational Clock Timeline Timestamp")
                    fig_power.update_yaxes(title_text="<b>Active Demand Power Draw (kW)</b>", secondary_y=False)
                    fig_power.update_yaxes(title_text="<b>Accumulated Sourced Energy (kWh)</b>", secondary_y=True)
                    
                    st.plotly_chart(fig_power, use_container_width=True)
                else:
                    st.info("No transformer station input parameters recorded during this calendar date window.")
                
                # --- EXECUTIVE GRAPH 2: DETAILED LITHIUM ASSET PERFORMANCE CURVES ---
                st.markdown("---")
                st.markdown("#### 🔋 Cross-Sectional Lithium Array Asset Tracking")
                if not hist_slots.empty:
                    # Plotly SOC curve progression mapping
                    fig_slots = go.Figure()
                    
                    # Generate a clean series profile loop for each independent BMS Asset Serial
                    for unique_bms, group in hist_slots.groupby('bms_id'):
                        if unique_bms and unique_bms != 'Unknown':
                            fig_slots.add_trace(go.Scatter(
                                x=group['timestamp'], 
                                y=group['soc'], 
                                mode='lines',
                                name=f"Pack ID: {unique_bms[:10]}...",
                                line=dict(width=1.5)
                            ))
                            
                    fig_slots.update_layout(
                        title_text="State of Charge (SOC %) Progression Across Active Bus Slots",
                        template="plotly_dark",
                        paper_bgcolor="#1A1F2C",
                        plot_bgcolor="#1A1F2C",
                        xaxis_title="Timeline Timestamp",
                        yaxis_title="Battery Capacity Metric (SOC %)",
                        margin=dict(l=40, r=40, t=40, b=40)
                    )
                    st.plotly_chart(fig_slots, use_container_width=True)
                    
                    # Core Aggregated Summary Ledger Metrics for Executive Evaluation
                    st.markdown("##### 🔍 Pack Operational Lifecycle Metrics Ledger Summary")
                    summary_df = hist_slots.groupby('bms_id').agg({
                        'soc': ['min', 'max'],
                        'bms_v': 'max',
                        'bms_i': 'max',
                        'instantaneous_slot_efficiency': 'mean'
                    })
                    summary_df.columns = [
                        'Initial Session SOC %', 'Terminal Session SOC %', 
                        'Peak Structural Voltage (V)', 'Max Current Intensity (A)', 
                        'Mean Calculated Conversion Efficiency %'
                    ]
                    st.dataframe(summary_df.round(2), use_container_width=True)
                    
                    with st.expander("📄 Download / Audit Raw Supabase Analytics Views Log Structure"):
                        st.dataframe(hist_slots, use_container_width=True, hide_index=True)
                else:
                    st.info("No independent lithium CAN bus communication frames located on this daily operational window.")