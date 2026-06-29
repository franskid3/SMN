# import streamlit as st
# import psycopg2
# import pandas as pd
# import json
# import paho.mqtt.client as mqtt
# import threading
# import time

# # =========================================================================
# # CENTRAL RESOURCE CONFIGURATION
# # =========================================================================
# DB_CONFIG = {
#     "dbname": "postgres",
#     "user": "postgres.ehncmhxcratiyupmkzpv",
#     "password": "Ngtech@19#19",
#     "host": "aws-1-eu-north-1.pooler.supabase.com",
#     "port": "6543"
# }

# MQTT_CONFIG = {
#     "broker": "cow.rmq2.cloudamqp.com",
#     "port": 1883,
#     "user": "klsazaul:klsazaul",
#     "pass": "EJyfwSCQdtKsiEe-HLDmkYEffe45MhPJ",
#     "topic": "SMN/#"
# }

# # Force layout optimization to wide screen format
# st.set_page_config(page_title="SMN Enterprise Hub", layout="wide")

# # =========================================================================
# # AUTOMATED BACKGROUND MQTT AGENT
# # =========================================================================
# def run_mqtt_bridge_worker():
#     """Background engine running indefinitely to pipe MQTT packets straight into PostgreSQL"""
#     def on_connect(client, userdata, flags, rc):
#         client.subscribe(MQTT_CONFIG["topic"])

#     def on_message(client, userdata, msg):
#         try:
#             topic = msg.topic
#             payload_str = msg.payload.decode('utf-8')
            
#             # Direct insertion sequence into cloud database storage
#             conn = psycopg2.connect(**DB_CONFIG)
#             cur = conn.cursor()
#             query = "INSERT INTO telemetry_history (topic, payload) VALUES (%s, %s);"
#             cur.execute(query, (topic, payload_str))
#             conn.commit()
#             cur.close()
#             conn.close()
#         except Exception:
#             pass # Keep worker running silently regardless of bad incoming syntax frames

#     # Configure background worker service context instance
#     client = mqtt.Client()
#     client.username_pw_set(MQTT_CONFIG["user"], MQTT_CONFIG["pass"])
#     client.on_connect = on_connect
#     client.on_message = on_message
    
#     try:
#         client.connect(MQTT_CONFIG["broker"], MQTT_CONFIG["port"], 60)
#         client.loop_forever() # Loop blocks internally inside this separate thread line
#     except Exception:
#         time.sleep(10) # Auto fallback retry cooling pattern

# # Fire up the background listener only once per Render session activation lifecycle
# if "mqtt_thread_alive" not in st.session_state:
#     st.session_state["mqtt_thread_alive"] = True
#     bg_thread = threading.Thread(target=run_mqtt_bridge_worker, daemon=True)
#     bg_thread.start()

# # =========================================================================
# # STREAMLIT UI DATA FETCH ENGINE
# # =========================================================================
# st.title("⚡ SMN Automated Fleet & Asset Management Analytics Terminal")
# st.caption("🤖 Cloud Listener Agent Status: ACTIVE (Background Thread Logging 24/7)")
# st.markdown("---")

# @st.cache_data(ttl=1)
# def fetch_raw_data():
#     try:
#         conn = psycopg2.connect(**DB_CONFIG)
#         query = "SELECT created_at, topic, payload FROM telemetry_history ORDER BY created_at DESC LIMIT 500;"
#         df = pd.read_sql_query(query, conn)
#         conn.close()
#         return df
#     except Exception as e:
#         st.error(f"Database extraction failure: {e}")
#         return pd.DataFrame()

# df_raw = fetch_raw_data()

# if df_raw.empty:
#     st.info("Awaiting structural incoming database data streams...")
# else:
#     # =========================================================================
#     # TELEMETRY PROCESSING ENGINE
#     # =========================================================================
#     slot_records = df_raw[df_raw['topic'].str.startswith('SMN/DCU/')]
#     edu_records = df_raw[df_raw['topic'] == 'SMN/EDU']
#     heartbeat_records = df_raw[df_raw['topic'] == 'SMN/HEARTBEAT']

#     latest_slots = {}
#     battery_history = {}
#     dcu_heartbeats = {}  # Tracks the status of all 6 DCU controllers

#     # 1. Process Heartbeat Frames (Uptime & Diagnostics)
#     if not heartbeat_records.empty:
#         for _, row in heartbeat_records.iterrows():
#             try:
#                 payload = row['payload']
#                 if isinstance(payload, str):
#                     payload = json.loads(payload)
                
#                 dcu_id = int(payload.get('dcu', 1))
#                 if dcu_id not in dcu_heartbeats:
#                     uptime_sec = payload.get('uptime', 0)
#                     uptime_hours = uptime_sec / 3600.0  # Convert to standard hours representation
#                     dcu_heartbeats[dcu_id] = {
#                         "status": payload.get('system', 'UNKNOWN'),
#                         "uptime": f"{uptime_hours:.2f} Hrs",
#                         "mqtt_stat": "OK" if payload.get('mqtt') == 1 else "FAIL",
#                         "last_seen": row['created_at']
#                     }
#             except Exception:
#                 pass

#     # 2. Process DCU Slot State Parameters
#     if not slot_records.empty:
#         for _, row in slot_records.iterrows():
#             try:
#                 payload = row['payload']
#                 if isinstance(payload, str):
#                     payload = json.loads(payload)
                
#                 dcu = int(payload.get('dcu', 1))
#                 slot = int(payload.get('slot', 1))
#                 bms_id = payload.get('bms_id', 'Unknown')
#                 key = (dcu, slot)
                
#                 if key not in latest_slots:
#                     latest_slots[key] = payload
                
#                 if bms_id not in battery_history and bms_id != 'Unknown':
#                     battery_history[bms_id] = {
#                         "last_seen_dcu": dcu,
#                         "last_seen_slot": slot,
#                         "soc": payload.get('soc'),
#                         "soh": payload.get('soh'),
#                         "cycles": payload.get('cycles'),
#                         "wh_in": payload.get('wh_in', 0.0),
#                         "wh_out": payload.get('wh_out', 0.0)
#                     }
#             except Exception:
#                 pass

#     # 3. Process EDU/Generator Source Parameters with Bulletproof Cleaning
#     latest_edu = {}
#     if not edu_records.empty:
#         try:
#             raw_payload = edu_records.iloc[0]['payload']
#             if isinstance(raw_payload, str):
#                 cleaned_payload = raw_payload.replace('""', '"').strip('"')
#                 latest_edu = json.loads(cleaned_payload)
#             else:
#                 latest_edu = raw_payload
#         except Exception as e:
#             st.sidebar.error(f"EDU Parsing Error: {e}")

#     # =========================================================================
#     # USER INTERFACE LAYOUT LAYER
#     # =========================================================================
    
#     # Left Hand Sidebar Configuration
#     st.sidebar.header("🔌 Source Power Metrics")
    
#     if latest_edu:
#         g1_mask = int(latest_edu.get('gen1_mask', 0))
#         g2_mask = int(latest_edu.get('gen2_mask', 0))
        
#         if g1_mask == 1:
#             st.sidebar.success("🟢 Running on GENERATOR 1")
#         elif g2_mask == 1:
#             st.sidebar.success("🟢 Running on GENERATOR 2")
#         else:
#             st.sidebar.info("🔵 Running on UTILITY GRID")

#         p_mains_kw = float(latest_edu.get('p_total', 0.0)) / 1000.
#         st.sidebar.metric("Mains Active Draw", f"{p_mains_kw:.2f} kW")
#         st.sidebar.metric("Station Energy Counter", f"{latest_edu.get('energy', 0)} Wh")
#     else:
#         st.sidebar.warning("EDU Source Offline")

#     st.sidebar.markdown("---")
#     st.sidebar.subheader("🖥️ DCU Hub Status")
    
#     # Loop over all 6 possible DCU enclosures
#     for dcu_idx in range(1, 7):
#         if dcu_idx in dcu_heartbeats:
#             hb = dcu_heartbeats[dcu_idx]
#             st.sidebar.markdown(f"""
#             <div style='padding:8px; border-radius:5px; background-color:#F4FBF4; border-left:4px solid #2E7D32; margin-bottom:5px;'>
#                 <b style='color:#2E7D32;'>DCU {dcu_idx:02d}</b> : Dynamic Online <br>
#                 <small style='color:#555;'>Uptime: {hb['uptime']} | Status: {hb['status']}</small>
#             </div>
#             """, unsafe_allow_html=True)
#         else:
#             st.sidebar.markdown(f"""
#             <div style='padding:8px; border-radius:5px; background-color:#F9F9F9; border-left:4px solid #999; margin-bottom:5px;'>
#                 <b style='color:#777;'>DCU {dcu_idx:02d}</b> : <span style='color:#999; font-style:italic;'>Awaiting Link...</span>
#             </div>
#             """, unsafe_allow_html=True)

#     # Core Terminal Tabs Container
#     tab1, tab2, tab3 = st.tabs(["🎛️ 72-Slot Control Matrix", "🔋 Battery Asset History Ledger", "📊 Station Energy Efficiency Analysis"])

#     # --- TAB 1: CARD GRID MATRIX ---
#     with tab1:
#         selected_dcu = st.selectbox("Select Distribution Control Unit (DCU Matrix Visualizer):", options=[1, 2, 3, 4, 5, 6], index=0)
#         st.subheader(f"DCU {selected_dcu:02d} Operational Overview (Slots 01 - 12)")
        
#         for row_idx in range(3):
#             cols = st.columns(4)
#             for col_idx in range(4):
#                 slot_num = (row_idx * 4) + col_idx + 1
#                 slot_key = (selected_dcu, slot_num)
                
#                 with cols[col_idx]:
#                     if slot_key in latest_slots:
#                         data = latest_slots[slot_key]
#                         bms_id = data.get('bms_id', '---')
#                         chg_f = int(data.get('chg_f', 0))
#                         v_bms = float(data.get('bms_v', 0.0))
#                         i_bms = float(data.get('bms_i', 0.0))
                        
#                         if chg_f == 32:
#                             bg, border, status_txt, txt_color = "#FFF0F0", "#FFC0C0", "⚠️ CHARGER FAULT (32)", "#D32F2F"
#                         elif i_bms > 0.5:
#                             bg, border, status_txt, txt_color = "#F4FBF4", "#D4FFD4", "⚡ CHARGING ACTIVE", "#2E7D32"
#                         else:
#                             bg, border, status_txt, txt_color = "#F0F4FF", "#C0D0FF", "💤 STANDBY READY", "#0056B3"
                        
#                         st.markdown(f"""
#                         <div style="padding:15px; border-radius:8px; background-color:{bg}; border:2px solid {border}; margin-bottom:12px">
#                             <h4 style='margin:0;color:{txt_color};'>SLOT {slot_num:02d}</h4>
#                             <p style='margin:4px 0; font-size:11px; color:#666;'>ID: {bms_id}</p>
#                             <p style='margin:2px 0; font-weight:bold; font-size:15px;'>SOC: {data.get('soc', 0)}% | SOH: {data.get('soh', 0)}%</p>
#                             <p style='margin:2px 0; font-size:13px; font-family:monospace;'>V: {v_bms:.1f}V | I: {i_bms:.1f}A</p>
#                             <p style='margin:2px 0; font-size:12px; font-weight:bold; color:{txt_color};'>{status_txt}</p>
#                         </div>
#                         """, unsafe_allow_html=True)
#                     else:
#                         st.markdown(f"""
#                         <div style="padding:15px; border-radius:8px; background-color:#F5F5F5; border:1px dashed #CCC; margin-bottom:12px">
#                             <h4 style='margin:0;color:#999;'>SLOT {slot_num:02d}</h4>
#                             <p style='margin:8px 0; color:#AAA; font-style:italic; font-size:13px;'>Unallocated Slot</p>
#                         </div>
#                         """, unsafe_allow_html=True)

#     # --- TAB 2: ASSET LIFECYCLE LEDGER & INSPECTOR ---
#     with tab2:
#         st.header("Asset Ledger Tracker & Historical Drill-Down")
#         if not battery_history:
#             st.info("No identified battery profiles found inside database records.")
#         else:
#             ledger_data = []
#             for bms_id, metrics in battery_history.items():
#                 wh_out = float(metrics["wh_out"])
#                 wh_in = float(metrics["wh_in"])
#                 delta_eff = (wh_in / wh_out) * 100.0 if wh_out > 0 else 0.0
                
#                 ledger_data.append({
#                     "Battery BMS Serial ID": bms_id,
#                     "Current Location": f"DCU {metrics['last_seen_dcu']} / SLOT {metrics['last_seen_slot']}",
#                     "State of Charge (SOC)": f"{metrics['soc']}%",
#                     "State of Health (SOH)": f"{metrics['soh']}%",
#                     "Total Cycle Count": metrics["cycles"],
#                     "Total Energy Received (Wh_in)": wh_in,
#                     "Total Charger Emitted (Wh_out)": wh_out,
#                     "Transfer Efficiency": f"{delta_eff:.1f}%" if delta_eff > 0 else "---"
#                 })
#             st.dataframe(pd.DataFrame(ledger_data), use_container_width=True, hide_index=True)
            
#             st.markdown("---")
#             st.subheader("🔍 Battery Historical Lifecycle Inspector")
#             selected_bms = st.selectbox("Select a Battery BMS Serial ID to view historical charging curve trends:", options=list(battery_history.keys()))
            
#             if selected_bms:
#                 pack_history = []
#                 for _, row in slot_records.iterrows():
#                     try:
#                         p = row['payload']
#                         if isinstance(p, str):
#                             p = json.loads(p)
#                         if p.get('bms_id') == selected_bms:
#                             pack_history.append({
#                                 "Timestamp": row['created_at'],
#                                 "SOC (%)": int(p.get('soc', 0)),
#                                 "Voltage (V)": float(p.get('bms_v', 0.0)),
#                                 "Current (A)": float(p.get('bms_i', 0.0))
#                             })
#                     except Exception:
#                         pass
                
#                 if pack_history:
#                     df_history = pd.DataFrame(pack_history).sort_values(by="Timestamp")
#                     m1, m2 = st.columns(2)
#                     with m1:
#                         st.markdown(f"**State of Charge (SOC) Curve for {selected_bms}**")
#                         st.line_chart(data=df_history, x="Timestamp", y="SOC (%)", color="#2E7D32")
#                     with m2:
#                         st.markdown(f"**Voltage Profile (V) over Time**")
#                         st.line_chart(data=df_history, x="Timestamp", y="Voltage (V)", color="#0056B3")
                        
#                     st.markdown("**Raw Transaction Logs for this Serial Asset**")
#                     st.dataframe(df_history, use_container_width=True, hide_index=True)
#                 else:
#                     st.info(f"No historical chart profiles logged yet for serial asset: {selected_bms}")

#     # --- TAB 3: POWER CONVERSION EFFICIENCY ---
#     with tab3:
#         st.header("Operational Station Conversion Efficiencies")
#         total_dc_battery_kw = sum((float(payload.get('bms_v', 0.0)) * float(payload.get('bms_i', 0.0))) for payload in latest_slots.values()) / 1000.0
#         c1, c2, c3 = st.columns(3)
#         c1.metric("Aggregated Battery Net Draw", f"{total_dc_battery_kw:.3f} kW")
        
#         if latest_edu:
#             p_mains_kw = float(latest_edu.get('p_total', 0.0)) / 1000.0
#             c2.metric("Mains Primary Input Power", f"{p_mains_kw:.3f} kW")
#             c3.metric("End-to-End System Efficiency", f"{((total_dc_battery_kw / p_mains_kw) * 100.0):.1f} %" if p_mains_kw > 1.0 else "--- %")
#         else:
#             c2.metric("Mains Primary Input Power", "Offline")
#             c3.metric("End-to-End System Efficiency", "Offline")

# # =========================================================================
# # FULLY AUTOMATED UI REFRESH LOOP
# # =========================================================================
# @st.fragment
# def auto_refresh_loop():
#     """Forces the web browser to visually update and clear caches every 5 seconds"""
#     time.sleep(5)
#     st.rerun()

# auto_refresh_loop()


import streamlit as st
import psycopg2
import pandas as pd
import json
import paho.mqtt.client as mqtt
import threading
import time
import queue



# =========================================================================
# CENTRAL RESOURCE CONFIGURATION
# =========================================================================
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

# Force layout optimization to wide screen format
st.set_page_config(page_title="SMN Enterprise Hub", layout="wide")

# --- NEW SAFE GLOBAL CACHE OBJECTS (Bypasses session_state thread crashes) ---
if "RAW_EDU" not in globals():
    globals()["RAW_EDU"] = {}
if "RAW_SLOTS" not in globals():
    globals()["RAW_SLOTS"] = {}
if "RAW_HEARTBEAT" not in globals():
    globals()["RAW_HEARTBEAT"] = {"system": "OFFLINE", "uptime": 0}

db_write_queue = queue.Queue(maxsize=1000)

# =========================================================================
# ASYNCHRONOUS DATABASE LOGGER THREAD
# =========================================================================
def async_db_logger_worker():
    while True:
        item = db_write_queue.get()
        if item is None:
            break
        topic, payload_str = item
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO telemetry_logs (topic, payload) VALUES (%s, %s);",
                (topic, payload_str)
            )
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"Async DB Insert Lag: {e}")
        finally:
            db_write_queue.task_done()

# =========================================================================
# AUTOMATED BACKGROUND MQTT AGENT (SAFE FOR PYTHON 3.14 + PAHO MQTT V2)
# =========================================================================
def run_mqtt_bridge_worker():
    # Fix the deprecation warning by defining the explicit callback API version 
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION1)
    client.username_pw_set(MQTT_CONFIG["user"], MQTT_CONFIG["pass"])

    def on_connect(client, userdata, flags, rc):
        client.subscribe(MQTT_CONFIG["topic"])

    def on_message(client, userdata, msg):
        try:
            topic = msg.topic
            payload_str = msg.payload.decode("utf-8")
            parsed_payload = json.loads(payload_str)
            
            # Write straight to plain global Python dictionaries (Completely thread-safe and crash-proof)
            if topic == "SMN/EDU":
                globals()["RAW_EDU"] = parsed_payload
            elif topic == "SMN/HEARTBEAT":
                globals()["RAW_HEARTBEAT"] = parsed_payload
            elif topic.startswith("SMN/SLOT_"):
                try:
                    slot_id = int(topic.split("_")[-1])
                    globals()["RAW_SLOTS"][slot_id] = parsed_payload
                except ValueError:
                    pass
            
            if not db_write_queue.full():
                db_write_queue.put((topic, payload_str))
                
        except Exception as e:
            print(f"MQTT Ingestion Error: {e}")

    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_CONFIG["broker"], MQTT_CONFIG["port"], 60)
    client.loop_forever()

@st.cache_resource
def initialize_system_daemons():
    t1 = threading.Thread(target=run_mqtt_bridge_worker, daemon=True)
    t2 = threading.Thread(target=async_db_logger_worker, daemon=True)
    # NO add_script_run_ctx CALLS HERE AT ALL
    t1.start()
    t2.start()
    return True

initialize_system_daemons()

# Pull the fresh snapshots straight into your local layout frame
latest_edu = globals()["RAW_EDU"]
latest_slots = globals()["RAW_SLOTS"]
hb = globals()["RAW_HEARTBEAT"]

# =========================================================================
# PRESERVED PRETTY WEB-APP LAYOUT & PRESENTATION ENGINE
# =========================================================================
st.title("🏭 Station Master Node (SMN) Enterprise Dashboard")

# --- TOP STATUS HEADER STRIP ---
hb_seconds = int(hb.get("uptime", 0))
days = hb_seconds // 86400
hours = (hb_seconds % 86400) // 3600
minutes = (hb_seconds % 3600) // 60
seconds = hb_seconds % 60
formatted_uptime = f"{days}d {hours}h {minutes}m {seconds}s"

status_col1, status_col2, status_col3 = st.columns(3)
status_col1.metric("Node System State", str(hb.get("system", "UNKNOWN")))
status_col2.metric("Verified Node Uptime", formatted_uptime)
status_col3.metric("Live Active Lithium Slots", f"{len(latest_slots)} Units Online")

tab1, tab2, tab3 = st.tabs(["⚡ Mains Power Analyzer (EDU)", "🔋 Lithium Matrix Banks", "📊 System Efficiencies"])

# --- TAB 1: MAINS ANALYZER (EDU) ---
with tab1:
    if latest_edu:
        st.header("Primary Industrial Feed Telemetry")
        
        # Core UI Conversion Layer (Displaying kW and kWh safely from native W/Wh)
        p_total_w = float(latest_edu.get("p_total", 0.0))
        energy_wh = float(latest_edu.get("energy", 0))
        
        p_total_kw = p_total_w / 1000.0
        energy_kwh = energy_wh / 1000.0

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("System Frequency", f"{latest_edu.get('freq', 0.0):.1f} Hz")
        c2.metric("Total Load (kW)", f"{p_total_kw:.2f} kW")
        c3.metric("Accumulated Energy (kWh)", f"{energy_kwh:.2f} kWh")
        c4.metric("Average Power Factor", f"{latest_edu.get('pf', 0.00):.2f}")

        st.markdown("---")
        col_v, col_i = st.columns(2)
        with col_v:
            st.subheader("Phase Voltages (Line-to-Neutral)")
            st.info(f"**L1:** {latest_edu.get('v1')} V | **L2:** {latest_edu.get('v2')} V | **L3:** {latest_edu.get('v3')} V")
        with col_i:
            st.subheader("Current Distribution")
            st.warning(f"**I1:** {latest_edu.get('i1')} A | **I2:** {latest_edu.get('i2')} A | **I3:** {latest_edu.get('i3')} A")
            
        st.markdown("---")
        st.subheader("Generator Transfer Interlock Logic Control Flags")
        g1, g2 = st.columns(2)
        g1.metric("Generator Bank Alpha", str(latest_edu.get("gen1", "---")))
        g2.metric("Generator Bank Beta", str(latest_edu.get("gen2", "---")))
    else:
        st.info("Awaiting structural payload broadcast stream from EDU Core...")

# --- TAB 2: LITHIUM PACK MATRIX ---
with tab2:
    st.header("Lithium Bank Parallel Battery Clusters")
    if latest_slots:
        slot_records = []
        for s_id, s_data in sorted(latest_slots.items()):
            slot_records.append({
                "Slot Index": s_id,
                "Serial Tag": s_data.get("serial", "---"),
                "Voltage (V)": s_data.get("bms_v", 0.0),
                "Current (A)": s_data.get("bms_i", 0.0),
                "State of Charge (%)": f"{s_data.get('soc', 0.0)}%",
                "State of Health (%)": f"{s_data.get('soh', 0.0)}%",
                "Min Cell (V)": s_data.get("v_min", 0.0),
                "Max Cell (V)": s_data.get("v_max", 0.0),
                "BMS Temp (°C)": s_data.get("temp", 0.0),
                "Status Mask": s_data.get("status", 0)
            })
        st.dataframe(pd.DataFrame(slot_records), use_container_width=True, hide_index=True)
        
        # Preserved Analytical Sub-Charts Profile Loop
        st.markdown("---")
        st.subheader("Asset Diagnostics")
        selected_bms = st.selectbox("Select Lithium Asset Serial for Verification Logs", 
                                    options=[r["Serial Tag"] for r in slot_records if r["Serial Tag"] != "---"])
        
        if selected_bms:
            try:
                conn = psycopg2.connect(**DB_CONFIG)
                query = """
                    SELECT timestamp, payload->>'bms_v' as v, payload->>'bms_i' as i, payload->>'soc' as soc 
                    FROM telemetry_logs 
                    WHERE topic LIKE 'SMN/SLOT_%%' AND payload->>'serial' = %s 
                    ORDER BY timestamp DESC LIMIT 20;
                """
                df_history = pd.read_sql_query(query, conn, params=(str(selected_bms),))
                conn.close()
                
                if not df_history.empty:
                    df_history['v'] = pd.to_numeric(df_history['v'])
                    df_history['i'] = pd.to_numeric(df_history['i'])
                    df_history['soc'] = pd.to_numeric(df_history['soc'])
                    
                    st.markdown(f"**Historical Chart Profiles logged yet for serial asset: {selected_bms}**")
                    st.line_chart(df_history.set_index('timestamp')[['soc', 'v']])
                    
                    st.markdown(f"**Data Logs for this Serial Asset**")
                    st.dataframe(df_history, use_container_width=True, hide_index=True)
                else:
                    st.info(f"No historical chart profiles logged yet for serial asset: {selected_bms}")
            except Exception as e:
                st.error(f"Could not connect to Database historical archives: {e}")
    else:
        st.info("No Active DCU Lithium arrays transmitting on the local CAN network segments.")

# --- TAB 3: POWER CONVERSION EFFICIENCY ---
with tab3:
    st.header("Operational Station Conversion Efficiencies")
    total_dc_battery_kw = sum((float(payload.get('bms_v', 0.0)) * float(payload.get('bms_i', 0.0))) for payload in latest_slots.values()) / 1000.0
    c1, c2, c3 = st.columns(3)
    c1.metric("Aggregated Battery Net Draw", f"{total_dc_battery_kw:.3f} kW")
    
    if latest_edu:
        p_mains_kw = float(latest_edu.get('p_total', 0.0)) / 1000.0
        c2.metric("Mains Primary Input Power", f"{p_mains_kw:.3f} kW")
        c3.metric("End-to-End System Efficiency", f"{((total_dc_battery_kw / p_mains_kw) * 100.0):.1f} %" if p_mains_kw > 1.0 else "--- %")
    else:
        c2.metric("Mains Primary Input Power", "Offline")
        c3.metric("End-to-End System Efficiency", "--- %")

# Fast refresh cycle to ensure instant visibility changes
time.sleep(1.5)
st.rerun()