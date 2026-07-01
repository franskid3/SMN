# import json
# import logging
# import os
# import queue
# import threading
# import time
# from http.server import BaseHTTPRequestHandler, HTTPServer
# import paho.mqtt.client as mqtt
# import psycopg2

# # --- LOGGING SETUP ---
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s",
#     handlers=[logging.StreamHandler()],
# )
# logger = logging.getLogger("RechajLogger")

# # --- CONFIGURATION SETTINGS ---
# DB_CONFIG = {
#     "dbname": "postgres",
#     "user": "postgres.ehncmhxcratiyupmkzpv",
#     "password": "Ngtech@19#19",
#     "host": "aws-1-eu-north-1.pooler.supabase.com",
#     "port": "6543",
#     "connect_timeout": 5,
# }

# MQTT_CONFIG = {
#     "broker": "cow.rmq2.cloudamqp.com",
#     "port": 1883,
#     "user": "klsazaul:klsazaul",
#     "pass": "EJyfwSCQdtKsiEe-HLDmkYEffe45MhPJ",
#     "topic": "SMN/#",
# }

# # --- THREAD-SAFE STORAGE & STATE ---
# # Message buffer queue to prevent data loss during DB downtime
# telemetry_queue = queue.Queue(maxsize=10000)

# # Thread-safe state locks
# state_lock = threading.Lock()
# latest_uptime = 0
# last_timestamp = None
# last_power_kw = 0.0
# software_energy = 0.0


# # --- HEALTH ENDPOINT FOR RENDER ---
# class HealthCheckHandler(BaseHTTPRequestHandler):
#     def do_GET(self):
#         if self.path == "/healthz" or self.path == "/":
#             self.send_response(200)
#             self.send_header("Content-type", "text/plain")
#             self.end_headers()
#             self.wfile.write(b"RECHAJ LOGGER IS ONLINE")
#         else:
#             self.send_response(404)
#             self.end_headers()

#     def log_message(self, format, *args):
#         # Keeps standard HTTP noise out of logs
#         return


# def run_health_server():
#     port = int(os.environ.get("PORT", 10000))
#     server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
#     logger.info(f"Render Health Check responder listening on port {port}...")
#     server.serve_forever()


# # --- DATABASE WORKER THREAD (Persistent & Auto-reconnect) ---
# def db_worker():
#     """Manages persistent DB connection, auto-reconnects, and processes the queue."""
#     conn = None
#     cursor = None

#     while True:
#         if conn is None or conn.closed != 0:
#             logger.info("Attempting to establish persistent PostgreSQL connection...")
#             try:
#                 conn = psycopg2.connect(**DB_CONFIG)
#                 cursor = conn.cursor()
#                 logger.info("PostgreSQL connection established successfully.")
#             except Exception as e:
#                 logger.error(
#                     f"PostgreSQL connection failed: {e}. Retrying in 5 seconds..."
#                 )
#                 time.sleep(5)
#                 continue

#         # Fetch an item from the queue (Blocks until an item is available)
#         topic, payload_str = telemetry_queue.get()

#         try:
#             cursor.execute(
#                 """
#                 INSERT INTO telemetry_history(topic, payload)
#                 VALUES(%s, %s::jsonb)
#                 """,
#                 (topic, payload_str),
#             )
#             conn.commit()
#             logger.info(f"Successfully logged packet to DB: {topic}")
#             telemetry_queue.task_done()

#         except (psycopg2.OperationalError, psycopg2.InterfaceError) as db_err:
#             logger.error(
#                 f"Database error detected: {db_err}. Re-queueing message and resetting connection."
#             )
#             # Re-queue the failed item so data is not lost
#             # We insert it back using a temporary list or direct placement if ordering allows.
#             # For strictness, we reconstruct it into the queue or just put it back.
#             try:
#                 cursor.close()
#             except Exception:
#                 pass
#             try:
#                 conn.close()
#             except Exception:
#                 pass
#             conn = None  # Triggers reconnect on next loop iteration

#             # Re-insert the data back to the front/end of the queue safely
#             if telemetry_queue.full():
#                 logger.warning("Queue full! Dropping oldest item to buffer current item.")
#                 try:
#                     telemetry_queue.get_nowait()
#                 except queue.Empty:
#                     pass
#             telemetry_queue.put((topic, payload_str))
#             telemetry_queue.task_done()
#             time.sleep(2)

#         except Exception as e:
#             logger.error(
#                 f"Unexpected error writing to DB (dropping corrupt message): {e}"
#             )
#             # Commit or rollback to clear transaction state if connection isn't dead
#             try:
#                 conn.rollback()
#             except Exception:
#                 pass
#             telemetry_queue.task_done()


# # --- MQTT INGESTION ENGINE ---
# def on_connect(client, userdata, flags, rc, properties=None):
#     if rc == 0:
#         logger.info("Connected to CloudAMQP Broker successfully.")
#         client.subscribe(MQTT_CONFIG["topic"])
#     else:
#         logger.error(f"MQTT Connection failed with result code: {rc}")


# def on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None):
#     logger.warning(f"MQTT DISCONNECTED! Reason code: {reason_code}. Reconnecting automatically...")


# def on_message(client, userdata, msg):
#     global latest_uptime, last_timestamp, last_power_kw, software_energy

#     try:
#         topic = msg.topic
#         payload_str = msg.payload.decode()
#         parsed_payload = json.loads(payload_str)

#         with state_lock:
#             # Heartbeat handling
#             if topic == "SMN/HEARTBEAT":
#                 latest_uptime = int(parsed_payload.get("uptime", 0))
#                 logger.debug(f"Heartbeat received. Uptime: {latest_uptime}")

#             # Software Energy Calculation
#             elif topic == "SMN/EDU":
#                 current_power_kw = float(parsed_payload.get("p_total", 0)) / 1000.0
#                 now = time.time()

#                 if last_timestamp is not None:
#                     delta_seconds = now - last_timestamp
#                     # Energy (kWh) = Power (kW) * Time (hours)
#                     software_energy += (last_power_kw * delta_seconds) / 3600.0

#                 last_timestamp = now
#                 last_power_kw = current_power_kw

#                 parsed_payload["sw_calculated_energy"] = round(software_energy, 4)
#                 payload_str = json.dumps(parsed_payload)

#         # Buffer data securely into the Thread-safe queue
#         if telemetry_queue.full():
#             logger.warning("Telemetry buffer queue is full! Dropping oldest packet.")
#             try:
#                 telemetry_queue.get_nowait()  # Drop oldest if overflowing
#             except queue.Empty:
#                 pass

#         telemetry_queue.put((topic, payload_str))

#     except json.JSONDecodeError:
#         logger.error(f"Malformed JSON dropped from topic {msg.topic}: {msg.payload}")
#     except Exception as e:
#         logger.error(f"Error processing incoming MQTT message: {e}", exc_info=True)


# def main():
#     # 1. Spin up the health checker thread to satisfy Render's port scan
#     web_thread = threading.Thread(
#         target=run_health_server, name="HealthServerThread", daemon=True
#     )
#     web_thread.start()

#     # 2. Start the persistent DB consumer thread
#     db_thread = threading.Thread(
#         target=db_worker, name="DatabaseWorkerThread", daemon=True
#     )
#     db_thread.start()

#     # 3. Generate a completely unique identifier so Render instances never fight
#     unique_render_id = f"Rechaj_Render_Engine_{int(time.time())}"
#     logger.info(f"Starting Ingestion Engine with Dedicated ID: {unique_render_id}")

#     # 4. Configure MQTT Client (Paho v2 compliant) with built-in auto-reconnect
#     client = mqtt.Client(
#         callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
#         client_id=unique_render_id,
#     )
#     client.username_pw_set(MQTT_CONFIG["user"], MQTT_CONFIG["pass"])
#     client.on_connect = on_connect
#     client.on_disconnect = on_disconnect
#     client.on_message = on_message

#     # Configure Paho's built-in automatic reconnection delay parameters
#     client.reconnect_delay_set(min_delay=1, max_delay=120)

#     # Infinite reconnect loop for network resilience
#     while True:
#         try:
#             client.connect(MQTT_CONFIG["broker"], MQTT_CONFIG["port"], keepalive=60)
#             # loop_forever handles automatic network reconnect loops natively
#             client.loop_forever()
#         except Exception as e:
#             logger.error(
#                 f"Broker connection drop. Retrying entire client setup in 10s... Error: {e}"
#             )
#             time.sleep(10)


# if __name__ == "__main__":
#     main()
#######################################################################################################
import os
import json
import time
import threading
from queue import Queue

import paho.mqtt.client as mqtt
import psycopg2

from http.server import BaseHTTPRequestHandler, HTTPServer

# ==========================================================
# CONFIG
# ==========================================================

DB = {
    "dbname":"postgres",
    "user":"postgres.ehncmhxcratiyupmkzpv",
    "password":"Ngtech@19#19",
    "host":"aws-1-eu-north-1.pooler.supabase.com",
    "port":"6543"
}

MQTT={
    "broker":"cow.rmq2.cloudamqp.com",
    "port":1883,
    "user":"klsazaul:klsazaul",
    "password":"EJyfwSCQdtKsiEe-HLDmkYEffe45MhPJ",
    "topic":"SMN/#"
}

# ==========================================================
# GLOBALS
# ==========================================================

db=None
cursor=None

db_queue=Queue()

latest_uptime=0

last_packet=time.time()

last_power_kw=0.0
last_timestamp=None
software_energy=0.0

mqtt_packets=0
db_inserts=0

# ==========================================================
# HEALTH SERVER
# ==========================================================

class Health(BaseHTTPRequestHandler):

    def do_GET(self):

        txt=f"""
SMN LOGGER RUNNING

Packets : {mqtt_packets}

DB Inserts : {db_inserts}

Queue : {db_queue.qsize()}

Last Packet : {round(time.time()-last_packet,1)} sec

Software Energy : {software_energy:.3f} kWh
"""

        self.send_response(200)
        self.send_header("Content-Type","text/plain")
        self.end_headers()
        self.wfile.write(txt.encode())

    def log_message(self,*args):
        return

def health_server():

    port=int(os.environ.get("PORT",10000))

    HTTPServer(
        ("0.0.0.0",port),
        Health
    ).serve_forever()

# ==========================================================
# DATABASE
# ==========================================================

def connect_db():

    global db
    global cursor

    while True:

        try:

            print("Connecting PostgreSQL...")

            db=psycopg2.connect(**DB)

            db.autocommit=True

            cursor=db.cursor()

            print("Database Connected")

            return

        except Exception as e:

            print(e)

            print("Retry DB in 5 sec")

            time.sleep(5)

def db_worker():

    global db_inserts
    global cursor
    global db

    while True:

        topic,payload=db_queue.get()

        while True:

            try:

                cursor.execute(
                    """
                    INSERT INTO telemetry_history(topic,payload)
                    VALUES(%s,%s::jsonb)
                    """,
                    (
                        topic,
                        json.dumps(payload)
                    )
                )

                db_inserts+=1

                break

            except Exception as e:

                print("DB ERROR:",e)

                try:
                    cursor.close()
                except:
                    pass

                try:
                    db.close()
                except:
                    pass

                connect_db()

# ==========================================================
# MQTT CALLBACKS
# ==========================================================

def on_connect(client,userdata,flags,rc,properties=None):

    print("MQTT Connected",rc)

    client.subscribe(MQTT["topic"])

def on_disconnect(client,userdata,disconnect_flags,reason_code,properties=None):

    print("MQTT LOST",reason_code)

def on_message(client,userdata,msg):

    global mqtt_packets
    global latest_uptime
    global last_packet

    global last_timestamp
    global last_power_kw
    global software_energy

    mqtt_packets+=1

    last_packet=time.time()

    try:

        payload=json.loads(
            msg.payload.decode()
        )

    except:

        return

    topic=msg.topic

    if topic=="SMN/HEARTBEAT":

        latest_uptime=int(
            payload.get("uptime",0)
        )

    elif topic=="SMN/EDU":

        now=time.time()

        power_kw=float(
            payload.get("p_total",0)
        )/1000.0

        if last_timestamp is not None:

            dt=now-last_timestamp

            if 0<dt<5:

                software_energy+=(
                    last_power_kw*
                    dt/
                    3600.0
                )

        last_timestamp=now

        last_power_kw=power_kw

        payload["sw_calculated_energy"]=round(
            software_energy,
            4
        )

        payload["energy_difference"]=round(
            software_energy-
            payload.get("energy",0)/1000.0,
            4
        )

    db_queue.put(
        (
            topic,
            payload
        )
    )

# ==========================================================
# WATCHDOG
# ==========================================================

def watchdog():

    global mqtt_packets
    global db_inserts
    global software_energy
    global last_packet

    old_packets=0

    while True:

        age=time.time()-last_packet

        print("\n================ LOGGER STATUS ================")
        print("MQTT Packets :",mqtt_packets)
        print("DB Inserts   :",db_inserts)
        print("Queue Size   :",db_queue.qsize())
        print("Last Packet  : %.1f sec ago"%age)
        print("Software E   : %.3f kWh"%software_energy)
        print("Runtime      : %d hr %02d min"%(
            latest_uptime//3600,
            (latest_uptime%3600)//60
        ))

        if mqtt_packets==old_packets:
            print("WARNING : No new MQTT packets.")
        else:
            print("MQTT Receiving OK")

        old_packets=mqtt_packets

        print("===============================================\n")

        time.sleep(30)

# ==========================================================
# MQTT ENGINE
# ==========================================================

def mqtt_worker():

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"Render_{int(time.time())}"
    )

    client.username_pw_set(
        MQTT["user"],
        MQTT["password"]
    )

    client.reconnect_delay_set(1, 60)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    while True:

        try:

            print("Connecting MQTT...")

            client.connect(
                MQTT["broker"],
                MQTT["port"],
                keepalive=120
            )

            client.loop_forever()

        except KeyboardInterrupt:
            raise

        except Exception as e:

            print("MQTT ERROR:", e)

            time.sleep(5)

# ==========================================================
# START THREADS
# ==========================================================

def start_threads():

    threading.Thread(
        target=health_server,
        daemon=True
    ).start()

    threading.Thread(
        target=db_worker,
        daemon=True
    ).start()

    threading.Thread(
        target=watchdog,
        daemon=True
    ).start()

# ==========================================================
# MAIN
# ==========================================================

def main():

    print("\n======================================")
    print(" SMN TELEMETRY LOGGER")
    print(" PostgreSQL + MQTT + Render")
    print("======================================\n")

    connect_db()

    start_threads()

    mqtt_worker()

# ==========================================================
# ENTRY
# ==========================================================

if __name__=="__main__":
    main()