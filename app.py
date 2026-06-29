import streamlit as st
import psycopg2
import pandas as pd
import json

# ==========================================
# CLOUD DATABASE CONFIGURATION
# ==========================================
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres.ehncmhxcratiyupmkzpv",            # <-- Make sure this matches your new user format!
    "password": "Ngtech@19#19",
    "host": "aws-1-eu-north-1.pooler.supabase.com",     # <-- Your new European pooler host
    "port": "6543"                                      # <-- Dedicated pooling port
}

st.set_page_config(page_title="SMN Enterprise Hub", layout="wide")
st.title("⚡ SMN Industrial Fleet & Asset Management Analytics Terminal")
st.markdown("---")

# --- DATA RETRIEVAL ENGINE ---
@st.cache_data(ttl=1)  # High frequency refresh for prototype monitoring
def fetch_raw_data():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        query = "SELECT created_at, topic, payload FROM telemetry_history ORDER BY created_at DESC LIMIT 500;"
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Database extraction failure: {e}")
        return pd.DataFrame()

df_raw = fetch_raw_data()

if df_raw.empty:
    st.info("Awaiting structural incoming database data streams...")
else:
    # Separate streams based on incoming topics
    slot_records = df_raw[df_raw['topic'].str.startswith('SMN/DCU/')]
    edu_records = df_raw[df_raw['topic'] == 'SMN/EDU']
    
    # Process Latest States for All 72 Possible Slots (6 DCUs x 12 Slots)
    latest_slots = {}
    battery_history = {}  # Global ledger indexing by bms_id
    
    if not slot_records.empty:
        for _, row in slot_records.iterrows():
            try:
                payload = row['payload']
                if isinstance(payload, str):
                    payload = json.loads(payload)
                
                dcu = int(payload.get('dcu', 1))
                slot = int(payload.get('slot', 1))
                bms_id = payload.get('bms_id', 'Unknown')
                key = (dcu, slot)
                
                # Capture current snapshot state for the slot matrix
                if key not in latest_slots:
                    latest_slots[key] = payload
                
                # Build Battery Tracking History Ledger dynamically
                if bms_id not in battery_history and bms_id != 'Unknown':
                    battery_history[bms_id] = {
                        "last_seen_dcu": dcu,
                        "last_seen_slot": slot,
                        "soc": payload.get('soc'),
                        "soh": payload.get('soh'),
                        "cycles": payload.get('cycles'),
                        "wh_in": payload.get('wh_in', 0.0),
                        "wh_out": payload.get('wh_out', 0.0),
                        "temp_max": payload.get('bms_temp', 0)
                    }
            except Exception:
                pass

    # Extract EDU State Metrics
    latest_edu = {}
    if not edu_records.empty:
        try:
            latest_edu = edu_records.iloc[0]['payload']
            if isinstance(latest_edu, str):
                latest_edu = json.loads(latest_edu)
        except Exception:
            pass

    # =========================================================================
    # GLOBAL SIDEBAR: STATIONS SOURCE POWER & GENERATOR METRICS
    # =========================================================================
    st.sidebar.header("🔌 Source Power Metrics")
    if latest_edu:
        g1_active = latest_edu.get('gen1_mask', 0) == 1
        g2_active = latest_edu.get('gen2_mask', 0) == 1
        
        if g1_active:
            st.sidebar.success("🟢 Running on GENERATOR 1")
        elif g2_active:
            st.sidebar.success("🟢 Running on GENERATOR 2")
        else:
            st.sidebar.info("🔵 Running on UTILITY GRID")

        p_mains_kw = latest_edu.get('p_total', 0.0) / 1000.0
        st.sidebar.metric("Mains Active Draw", f"{p_mains_kw:.2f} kW")
        st.sidebar.metric("Station Energy Counter", f"{latest_edu.get('energy', 0)} Wh")
    else:
        st.sidebar.warning("EDU Source Offline")

    # Dynamic UI Tab Layout Configuration
    tab1, tab2, tab3 = st.tabs(["🎛️ 72-Slot Control Matrix", "🔋 Battery Asset History Ledger", "📊 Station Energy Efficiency Analysis"])

    # -------------------------------------------------------------------------
    # TAB 1: 72-SLOT CONTROLLER GRID MATRIX
    # -------------------------------------------------------------------------
    with tab1:
        selected_dcu = st.selectbox("Select Distribution Control Unit (DCU Matrix Visualizer):", options=[1, 2, 3, 4, 5, 6], index=0)
        st.subheader(f"DCU {selected_dcu:02d} Operational Overview (Slots 01 - 12)")
        
        for row_idx in range(3):
            cols = st.columns(4)
            for col_idx in range(4):
                slot_num = (row_idx * 4) + col_idx + 1
                slot_key = (selected_dcu, slot_num)
                
                with cols[col_idx]:
                    if slot_key in latest_slots:
                        data = latest_slots[slot_key]
                        bms_id = data.get('bms_id', '---')
                        chg_f = data.get('chg_f', 0)
                        v_bms = data.get('bms_v', 0.0)
                        i_bms = data.get('bms_i', 0.0)
                        
                        # Style depending on standard vs charger fault code 32 parameters
                        if chg_f == 32:
                            bg, border, status_txt, txt_color = "#FFF0F0", "#FFC0C0", "⚠️ CHARGER FAULT (32)", "#D32F2F"
                        elif i_bms > 0.5:
                            bg, border, status_txt, txt_color = "#F4FBF4", "#D4FFD4", "⚡ CHARGING ACTIVE", "#2E7D32"
                        else:
                            bg, border, status_txt, txt_color = "#F0F4FF", "#C0D0FF", "💤 STANDBY READY", "#0056B3"
                        
                        st.markdown(f"""
                        <div style="padding:15px; border-radius:8px; background-color:{bg}; border:2px solid {border}; margin-bottom:12px">
                            <h4 style='margin:0;color:{txt_color};'>SLOT {slot_num:02d}</h4>
                            <p style='margin:4px 0; font-size:11px; color:#666;'>ID: {bms_id}</p>
                            <p style='margin:2px 0; font-weight:bold; font-size:15px;'>SOC: {data.get('soc', 0)}% | SOH: {data.get('soh', 0)}%</p>
                            <p style='margin:2px 0; font-size:13px; font-family:monospace;'>V: {v_bms:.1f}V | I: {i_bms:.1f}A</p>
                            <p style='margin:2px 0; font-size:12px; font-weight:bold; color:{txt_color};'>{status_txt}</p>
                        </div>
                        """, unsafe_allow_html=True)
                    else:
                        st.markdown(f"""
                        <div style="padding:15px; border-radius:8px; background-color:#F5F5F5; border:1px dashed #CCC; margin-bottom:12px">
                            <h4 style='margin:0;color:#999;'>SLOT {slot_num:02d}</h4>
                            <p style='margin:8px 0; color:#AAA; font-style:italic; font-size:13px;'>Unallocated Slot</p>
                        </div>
                        """, unsafe_allow_html=True)

    # -------------------------------------------------------------------------
    # TAB 2: BATTERY ASSET HISTORY LEDGER
    # -------------------------------------------------------------------------
    with tab2:
        st.header("Asset Ledger Tracker (Historical Lifecycle Data across Slots)")
        if not battery_history:
            st.info("No identified battery profiles found inside the active database buffer logs.")
        else:
            # Transform global tracking dictionaries into organized tabular UI layouts
            ledger_data = []
            for bms_id, metrics in battery_history.items():
                # Delta calculations between energy supplied vs entered
                delta_eff = 0.0
                if metrics["wh_out"] > 0:
                    delta_eff = (metrics["wh_in"] / metrics["wh_out"]) * 100.0
                
                ledger_data.append({
                    "Battery BMS Serial ID": bms_id,
                    "Current Location": f"DCU {metrics['last_seen_dcu']} / SLOT {metrics['last_seen_slot']}",
                    "State of Charge (SOC)": f"{metrics['soc']}%",
                    "State of Health (SOH)": f"{metrics['soh']}%",
                    "Total Cycle Count": metrics["cycles"],
                    "Total Energy Received (Wh_in)": metrics["wh_in"],
                    "Total Charger Emitted (Wh_out)": metrics["wh_out"],
                    "Transfer Efficiency": f"{delta_eff:.1f}%" if delta_eff > 0 else "---"
                })
            
            st.dataframe(pd.DataFrame(ledger_data), use_container_width=True, hide_index=True)

    # -------------------------------------------------------------------------
    # TAB 3: STATION ENERGY EFFICIENCY ANALYSIS
    # -------------------------------------------------------------------------
    with tab3:
        st.header("Operational Station Conversion Efficiencies")
        
        # Calculate instant total power consumed strictly by the battery storage matrix blocks
        total_dc_battery_power_w = 0.0
        for key, payload in latest_slots.items():
            v = payload.get('bms_v', 0.0)
            i = payload.get('bms_i', 0.0)
            total_dc_battery_power_w += (v * i)
            
        total_dc_battery_kw = total_dc_battery_power_w / 1000.0
        
        c1, c2, c3 = st.columns(3)
        c1.metric("Aggregated Battery Net Draw", f"{total_dc_battery_kw:.3f} kW")
        
        if latest_edu:
            p_mains_kw = latest_edu.get('p_total', 0.0) / 1000.0
            c2.metric("Mains Primary Input Power", f"{p_mains_kw:.3f} kW")
            
            # Global conversion alignment ratio coefficient
            if p_mains_kw > 1.0:
                overall_efficiency = (total_dc_battery_kw / p_mains_kw) * 100.0
                c3.metric("End-to-End System Efficiency", f"{overall_efficiency:.1f} %")
            else:
                c3.metric("End-to-End System Efficiency", "--- %")
        else:
            c2.metric("Mains Primary Input Power", "Offline")
            c3.metric("End-to-End System Efficiency", "Offline")

        # Slot Thermal/Ohmic Degradation Mapping Table
        st.subheader("Individual Charging Interface Loss Index")
        loss_ledger = []
        for (dcu_id, slot_id), payload in latest_slots.items():
            wh_in = payload.get('wh_in', 0.0)
            wh_out = payload.get('wh_out', 0.0)
            loss_wh = wh_out - wh_in
            
            loss_ledger.append({
                "DCU": dcu_id,
                "Slot": slot_id,
                "Current Battery Loaded": payload.get('bms_id'),
                "Energy Out of Charger (Wh)": wh_out,
                "Energy Absorbed by Battery (Wh)": wh_in,
                "Energy Lost in Transfer (Wh)": round(loss_wh, 2)
            })
        
        if loss_ledger:
            st.dataframe(pd.DataFrame(loss_ledger).sort_values(by="Energy Lost in Transfer (Wh)", ascending=False), use_container_width=True, hide_index=True)

# Force periodic execution frame
st.button("Click to Refresh Diagnostics Engine")