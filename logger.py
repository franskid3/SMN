import paho.mqtt.client as mqtt
import psycopg2
import json
import time
import os

# Database & MQTT Configuration Metrics
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

# Persistent In-Memory State Variables
latest_uptime = 0

def on_connect(client, userdata, flags, rc, properties=None):
    print(f"Connected to CloudAMQP Broker with result code: {rc}")
    client.subscribe(MQTT_CONFIG["topic"])

def on_message(client, userdata, msg):
    global latest_uptime
    try:
        topic = msg.topic
        payload_str = msg.payload.decode("utf-8")
        parsed_payload = json.loads(payload_str)
        
        # 1. Update system uptime clock directly from heartbeats
        if topic == "SMN/HEARTBEAT":
            latest_uptime = int(parsed_payload.get("uptime", 0))

        # 2. Intercept and calculate software virtual energy sum
        if topic == "SMN/EDU":
            p_total_kw = float(parsed_payload.get("p_total", 0.0)) / 1000.0
            uptime_hours = latest_uptime / 3600.0
            sw_energy = p_total_kw * uptime_hours
            parsed_payload["sw_calculated_energy"] = round(sw_energy, 4)
            payload_str = json.dumps(parsed_payload)

        # 3. Direct Commit to Supabase
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO telemetry_history (topic, payload) VALUES (%s, %s::jsonb);",
            (topic, payload_str)
        )
        conn.commit()
        cursor.close()
        conn.close()
        print(f" Successfully logged packet from topic: {topic}")
        
    except Exception as e:
        print(f"Ingestion Pipeline Exception: {e}")

def main():
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(MQTT_CONFIG["user"], MQTT_CONFIG["pass"])
    client.on_connect = on_connect
    client.on_message = on_message

    print("Starting Rechaj Energy Asynchronous Ingestion Engine...")
    while True:
        try:
            client.connect(MQTT_CONFIG["broker"], MQTT_CONFIG["port"], 60)
            client.loop_forever()
        except Exception as e:
            print(f"Broker connection disconnected. Reconnecting in 10s... Error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()