import os
import glob
import time
import math
import serial
import sqlite3
import threading
import queue
import requests
import atexit
from datetime import datetime, date, timezone, timedelta
from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO, emit

# Optional Audio Siren Detector Import
try:
    from siren_detector import SirenDetector
    siren_detector = SirenDetector(model_path="yamnet.tflite")
except Exception as e:
    print(f"[Warning] Siren Detector failed to initialize: {e}")
    siren_detector = None

# ==========================================
# CONFIGURATION & ENVIRONMENT VARIABLES
# ==========================================
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://canlnrismlirrnfqpqmv.supabase.co")

# Read service role key or anon key strictly from environment variables
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
if not SUPABASE_KEY:
    print("[WARNING] SUPABASE_KEY environment variable is missing. Cloud sync will be disabled.")

# Fallback Station ID (used only if Supabase hardware lookup fails)
DEFAULT_STATION_ID = os.getenv("STATION_ID", "meter-01")
STATION_ID = DEFAULT_STATION_ID  # Will be dynamically resolved on boot

SECRET_KEY = os.getenv("FLASK_SECRET_KEY", "dev-key-change-in-production")
DECIBEL_THRESHOLD = float(os.getenv("DECIBEL_THRESHOLD", 85.0))
ALERT_COOLDOWN_SECONDS = int(os.getenv("ALERT_COOLDOWN_SECONDS", 5))

DB_PATH = "noise_monitor.db"
CSV_PATH = "decibel_log.csv"
COUNT_PATH = "submission_count.txt"

# Modbus RTU Command: Read decibel level from acoustic sensor
READ_DECIBELS_CMD = b'\x01\x03\x00\x00\x00\x01\x84\x0a'

# Global State & Caching
latest_reading = {
    "dBA": 0.0,
    "timestamp": None
}
cached_total_submissions = 0

# Metadata Caching with TTL (5-minute refresh window)
cached_station_metadata = {}
last_metadata_fetch_time = 0
METADATA_CACHE_TTL = 300  # seconds

# CSV Buffer in Memory (Protects SD Card from Small Constant Writes)
csv_buffer = []
csv_buffer_lock = threading.Lock()
last_csv_flush_time = time.time()
CSV_FLUSH_INTERVAL = 300  # Flush every 5 minutes or 20 records

# Task Queue with Capacity Limit (Protects RAM during network outages)
task_queue = queue.Queue(maxsize=100)

# Initialize Flask & SocketIO
app = Flask(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True
import logging
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)
app.config['SECRET_KEY'] = SECRET_KEY
socketio = SocketIO(app, cors_allowed_origins="*")

try:
    from denver_311 import submit_daily_summary_report
except ImportError:
    submit_daily_summary_report = None


# ==========================================
# HARDWARE & SUPABASE RESOLUTION LOGIC
# ==========================================
def get_cpu_serial() -> str:
    """Extracts the unique CPU serial number from /proc/cpuinfo on ARM hardware."""
    try:
        with open("/proc/cpuinfo", "r") as f:
            for line in f:
                if line.startswith("Serial"):
                    return line.split(":")[1].strip()
    except Exception as e:
        print(f"[Hardware Warning] Could not read /proc/cpuinfo: {e}")
    return "UNKNOWN_SERIAL"


def resolve_station_id() -> str:
    """
    Queries Supabase 'devices' table using the Pi's CPU serial number to dynamically 
    determine which station_id is assigned to this physical hardware unit.
    """
    global STATION_ID
    cpu_serial = get_cpu_serial()
    print(f"[Boot] Reading hardware CPU Serial: {cpu_serial}")

    if not SUPABASE_KEY:
        print(f"[Warning] Missing SUPABASE_KEY. Defaulting station_id to '{DEFAULT_STATION_ID}'.")
        return DEFAULT_STATION_ID

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }

    # Query 'devices' table by hardware_serial
    url = f"{SUPABASE_URL}/rest/v1/devices?hardware_serial=eq.{cpu_serial}&select=station_id,status"
    
    # Retry loop on boot in case Wi-Fi/network is still initializing
    for attempt in range(1, 6):
        try:
            res = requests.get(url, headers=headers, timeout=5)
            if res.status_code == 200:
                data = res.json()
                if isinstance(data, list) and len(data) > 0 and data[0].get("station_id"):
                    assigned_id = data[0]["station_id"]
                    print(f"[Boot SUCCESS] Mapped CPU Serial '{cpu_serial}' -> Station ID: '{assigned_id}'")
                    
                    # Update last_seen in Supabase
                    try:
                        patch_url = f"{SUPABASE_URL}/rest/v1/devices?hardware_serial=eq.{cpu_serial}"
                        requests.patch(patch_url, json={"last_seen": "now()"}, headers=headers, timeout=3)
                    except Exception:
                        pass
                        
                    STATION_ID = assigned_id
                    return assigned_id
                else:
                    print(f"[Boot Warning] Serial '{cpu_serial}' found in Supabase but no 'station_id' assigned.")
            else:
                print(f"[Boot Warning] Device lookup returned HTTP {res.status_code}: {res.text}")
        except Exception as e:
            print(f"[Boot Warning] Attempt {attempt}/5 failed connecting to Supabase: {e}")
        
        time.sleep(3)

    print(f"[Boot Fallback] Could not resolve station from Supabase. Falling back to STATION_ID='{DEFAULT_STATION_ID}'.")
    STATION_ID = DEFAULT_STATION_ID
    return DEFAULT_STATION_ID


def get_station_metadata(station_id=None, force_refresh=False) -> dict:
    """
    Dynamically retrieves full station metadata (location address, name, coordinates)
    from the Supabase 'stations' table with TTL caching and flexible schema field mapping.
    """
    global cached_station_metadata, last_metadata_fetch_time
    target_id = station_id if station_id else STATION_ID
    now = time.time()

    # Return cached version if still valid and refresh not forced
    if not force_refresh and cached_station_metadata and (now - last_metadata_fetch_time < METADATA_CACHE_TTL):
        return cached_station_metadata

    fallback_addr = os.getenv("DEFAULT_LOCATION_ADDRESS", "3265 Federal Blvd, Denver, CO 80211")
    fallback_meta = {
        "id": target_id,
        "station_id": target_id,
        "location_address": fallback_addr,
        "address": fallback_addr,
        "name": f"Station {target_id}"
    }

    if not SUPABASE_KEY:
        cached_station_metadata = fallback_meta
        return fallback_meta

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }

    try:
        url = f"{SUPABASE_URL}/rest/v1/stations?or=(id.eq.{target_id},station_id.eq.{target_id})&select=*"
        res = requests.get(url, headers=headers, timeout=5)
        if res.status_code == 200:
            data = res.json()
            if isinstance(data, list) and len(data) > 0:
                raw_meta = data[0]

                # Flexible field extraction supporting different Supabase column naming schemas
                resolved_address = (
                    raw_meta.get("location_address") or 
                    raw_meta.get("address") or 
                    raw_meta.get("location") or 
                    raw_meta.get("formatted_address") or 
                    fallback_addr
                )
                resolved_name = (
                    raw_meta.get("name") or 
                    raw_meta.get("station_name") or 
                    raw_meta.get("title") or 
                    f"Station {target_id}"
                )

                normalized_meta = {
                    **raw_meta,
                    "id": target_id,
                    "station_id": target_id,
                    "location_address": resolved_address,
                    "address": resolved_address,
                    "name": resolved_name
                }

                cached_station_metadata = normalized_meta
                last_metadata_fetch_time = now
                return normalized_meta
    except Exception as e:
        print(f"[Metadata Error] Failed fetching metadata for station '{target_id}': {e}")

    return cached_station_metadata or fallback_meta


# ==========================================
# MODBUS UTILITIES
# ==========================================
def verify_modbus_crc(data: bytes) -> bool:
    """Verifies Modbus RTU CRC-16 checksum to filter out electrical noise errors."""
    if len(data) < 3:
        return False
    crc = 0xFFFF
    for pos in data[:-2]:
        crc ^= pos
        for _ in range(8):
            if (crc & 0x0001) != 0:
                crc >>= 1
                crc ^= 0xA001
            else:
                crc >>= 1
    expected_crc = data[-2:]
    actual_crc = crc.to_bytes(2, byteorder='little')
    return expected_crc == actual_crc


# ==========================================
# QUEUE WORKER
# ==========================================
def task_worker():
    """Background queue worker thread that processes sync tasks sequentially."""
    while True:
        task = task_queue.get()
        try:
            task_type = task.get("type")
            if task_type == "supabase_push":
                _supabase_push_worker(
                    dba_value=task.get("dba_value"),
                    sqlite_row_id=task.get("sqlite_row_id"),
                    timestamp_str=task.get("timestamp")
                )
            elif task_type == "supabase_sync_count":
                _supabase_sync_count_worker(task.get("count"))
            elif task_type == "311_report":
                if submit_daily_summary_report:
                    try:
                        station_meta = get_station_metadata(STATION_ID)
                        loc_addr = station_meta.get("location_address") or station_meta.get("address")
                        submit_daily_summary_report(task.get("today_count"), location_address=loc_addr)
                    except Exception as e:
                        print(f"[311 Error] Failed to submit report via Queue Worker: {e}")
        except Exception as e:
            print(f"[Queue Worker Error] Unhandled task execution error: {e}")
        finally:
            task_queue.task_done()


# ==========================================
# SUPABASE QUEUE LISTENER WORKER
# ==========================================
def supabase_queue_listener():
    """
    Background worker thread that polls Supabase 'pending_reports' table for queued 311 tasks,
    fetches dynamic location metadata, executes Playwright automation, and updates DB status.
    """
    global cached_total_submissions
    print("[Queue Listener] Started listening for pending 311 report requests on Supabase...")
    
    while True:
        try:
            if not SUPABASE_KEY:
                time.sleep(10)
                continue

            headers = {
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type": "application/json",
                "Prefer": "return=representation"
            }

            url = f"{SUPABASE_URL}/rest/v1/pending_reports?status=eq.pending&order=created_at.asc&limit=5"
            res = requests.get(url, headers=headers, timeout=10)

            if res.status_code == 200:
                pending_reports = res.json()
                if isinstance(pending_reports, list) and len(pending_reports) > 0:
                    for report in pending_reports:
                        report_id = report.get("id")
                        report_station_id = report.get("station_id", STATION_ID)
                        
                        print(f"[Queue Listener] Picked up pending report ID {report_id} for station '{report_station_id}'.")

                        # Dynamically fetch station address metadata from Supabase
                        station_meta = get_station_metadata(report_station_id)
                        location_address = station_meta.get("location_address") or station_meta.get("address")

                        patch_url = f"{SUPABASE_URL}/rest/v1/pending_reports?id=eq.{report_id}"

                        # 1. Transition report status to 'processing'
                        res_proc = requests.patch(
                            patch_url,
                            json={"status": "processing"},
                            headers=headers,
                            timeout=5
                        )
                        if res_proc.status_code >= 400:
                            print(f"[PATCH Error {res_proc.status_code}] Failed setting status 'processing' for ID {report_id}: {res_proc.text}")

                        # 2. Execute 311 Report Playwright Automation with dynamic address
                        today_count = get_today_exceedance_count()
                        success = False

                        if submit_daily_summary_report:
                            try:
                                result = submit_daily_summary_report(
                                    total_count=today_count,
                                    location_address=location_address
                                )
                                success = result if result is not None else True
                            except Exception as e:
                                print(f"[Queue Listener Error] Failed to execute 311 report for ID {report_id}: {e}")
                                success = False
                        else:
                            print("[Queue Listener Warning] 'submit_daily_summary_report' function not imported; marking task completed.")
                            success = True

                        # 3. Finalize report status and sync total counts
                        if success:
                            res_comp = requests.patch(
                                patch_url,
                                json={"status": "completed"},
                                headers=headers,
                                timeout=5
                            )
                            if res_comp.status_code >= 400:
                                print(f"[PATCH Error {res_comp.status_code}] Failed setting status 'completed' for ID {report_id}: {res_comp.text}")

                            cached_total_submissions += 1
                            try:
                                with open(COUNT_PATH, "w") as f:
                                    f.write(str(cached_total_submissions))
                            except Exception as e:
                                print(f"[Queue Listener Error] Failed to write count file: {e}")

                            sync_submission_count_to_supabase(cached_total_submissions)
                            print(f"[Queue Listener] Report ID {report_id} completed successfully. Total submissions: {cached_total_submissions}")

                        else:
                            res_fail = requests.patch(
                                patch_url,
                                json={"status": "failed"},
                                headers=headers,
                                timeout=5
                            )
                            if res_fail.status_code >= 400:
                                print(f"[PATCH Error {res_fail.status_code}] Failed setting status 'failed' for ID {report_id}: {res_fail.text}")
                            print(f"[Queue Listener] Report ID {report_id} marked as failed.")

        except Exception as e:
            print(f"[Queue Listener Loop Error] {e}")

        time.sleep(5)


# ==========================================
# DATABASE HELPER (OPTIMIZED FOR SD CARDS)
# ==========================================
def init_db():
    global cached_total_submissions
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        
        cursor.execute("PRAGMA journal_mode=WAL;")
        cursor.execute("PRAGMA synchronous=NORMAL;")
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS exceedances (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                peak_dba REAL NOT NULL,
                synced INTEGER DEFAULT 0
            )
        ''')
        
        cursor.execute("PRAGMA table_info(exceedances)")
        columns = [col[1] for col in cursor.fetchall()]
        if "synced" not in columns:
            cursor.execute("ALTER TABLE exceedances ADD COLUMN synced INTEGER DEFAULT 0")
            
        conn.commit()

    cached_total_submissions = _query_total_submissions()

def log_exceedance_sqlite(timestamp_str, peak_dba):
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO exceedances (timestamp, peak_dba, synced) VALUES (?, ?, 0)",
                (timestamp_str, peak_dba)
            )
            conn.commit()
            return cursor.lastrowid
    except sqlite3.Error as e:
        print(f"[SQLite Error] Failed to log exceedance: {e}")
        return None

def mark_exceedance_synced(row_id):
    if not row_id:
        return
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE exceedances SET synced = 1 WHERE id = ?", (row_id,))
            conn.commit()
    except sqlite3.Error as e:
        print(f"[SQLite Error] Failed to mark as synced: {e}")

def _query_today_exceedance_count():
    today_prefix = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM exceedances WHERE timestamp LIKE ?",
                (f"{today_prefix}%",)
            )
            return cursor.fetchone()[0]
    except sqlite3.Error as e:
        print(f"[SQLite Error] Failed to fetch today's count: {e}")
        return 0

def get_today_exceedance_count():
    return _query_today_exceedance_count()

def query_exceedances_last_hour():
    one_hour_ago = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM exceedances WHERE timestamp >= ?", (one_hour_ago,))
            return cursor.fetchone()[0]
    except sqlite3.Error as e:
        print(f"[SQLite Error] Failed to fetch last hour count: {e}")
        return 0

def _query_total_submissions():
    if os.path.exists(COUNT_PATH):
        try:
            with open(COUNT_PATH, "r") as f:
                return int(f.read().strip() or 0)
        except ValueError:
            return 0
    return 0

def get_total_submissions():
    return cached_total_submissions


# ==========================================
# IN-MEMORY CSV LOGGING & CLOUD SYNC
# ==========================================
def flush_csv_buffer():
    global csv_buffer, last_csv_flush_time
    with csv_buffer_lock:
        if not csv_buffer:
            return
        
        file_exists = os.path.isfile(CSV_PATH)
        try:
            with open(CSV_PATH, "a") as f:
                if not file_exists:
                    f.write("Timestamp,Peak_dBA\n")
                f.writelines(csv_buffer)
            csv_buffer.clear()
            last_csv_flush_time = time.time()
        except IOError as e:
            print(f"[CSV Error] Failed to flush CSV buffer: {e}")

def log_to_csv(timestamp_str, peak_dba):
    with csv_buffer_lock:
        csv_buffer.append(f"{timestamp_str},{peak_dba:.1f}\n")
    
    if len(csv_buffer) >= 20 or (time.time() - last_csv_flush_time) >= CSV_FLUSH_INTERVAL:
        flush_csv_buffer()

atexit.register(flush_csv_buffer)

def _supabase_push_worker(dba_value, sqlite_row_id=None, timestamp_str=None):
    if not SUPABASE_KEY:
        print("[Supabase Warning] Missing SUPABASE_KEY environment variable. Push skipped.")
        return
    dbc_value = round(dba_value + (2.5 if dba_value > 70 else 1.0), 1)
    record_time = timestamp_str if timestamp_str else datetime.now(timezone.utc).isoformat()
    
    payload = {
        "station_id": STATION_ID,
        "timestamp": record_time,
        "dba": dba_value,
        "dbc": dbc_value
    }
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }
    try:
        res = requests.post(f"{SUPABASE_URL}/rest/v1/noise_logs", json=payload, headers=headers, timeout=5)
        if res.status_code in (200, 201) and sqlite_row_id:
            mark_exceedance_synced(sqlite_row_id)
    except Exception as e:
        print(f"[Supabase Sync Error] {e}")

def push_to_supabase_async(dba_value, sqlite_row_id=None, timestamp_str=None):
    item = {
        "type": "supabase_push",
        "dba_value": dba_value,
        "sqlite_row_id": sqlite_row_id,
        "timestamp": timestamp_str
    }
    try:
        task_queue.put_nowait(item)
    except queue.Full:
        if sqlite_row_id is not None:
            try:
                task_queue.get_nowait()
                task_queue.put_nowait(item)
            except (queue.Empty, queue.Full):
                pass

def _supabase_sync_count_worker(count):
    if not SUPABASE_KEY:
        return
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }
    payload = {
        "station_id": STATION_ID,
        "total_submitted": count,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    try:
        requests.post(f"{SUPABASE_URL}/rest/v1/report_stats", json=payload, headers=headers, timeout=5)
    except Exception as e:
        print(f"[Supabase Sync Error] {e}")

def sync_submission_count_to_supabase(count):
    try:
        task_queue.put_nowait({
            "type": "supabase_sync_count",
            "count": count
        })
    except queue.Full:
        pass

def resync_offline_records():
    while True:
        try:
            with sqlite3.connect(DB_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT id, peak_dba, timestamp FROM exceedances WHERE synced = 0 LIMIT 10")
                unsynced = cursor.fetchall()
                for row_id, dba, ts in unsynced:
                    _supabase_push_worker(dba, sqlite_row_id=row_id, timestamp_str=ts)
        except Exception as e:
            print(f"[Resync Error] {e}")
            
        flush_csv_buffer()
        time.sleep(60)


# ==========================================
# SENSOR READING LOOP
# ==========================================
def find_serial_port():
    ports = glob.glob("/dev/ttyUSB*") + glob.glob("/dev/ttyACM*")
    return ports[0] if ports else None

def background_sensor_loop():
    global latest_reading
    last_alert_time = 0
    last_supabase_push = 0  

    while True:
        port = find_serial_port()
        if not port:
            time.sleep(2)
            continue

        try:
            with serial.Serial(port, baudrate=9600, timeout=1) as ser:
                while True:
                    ser.write(READ_DECIBELS_CMD)
                    response = ser.read(7)
                    
                    if len(response) == 7 and verify_modbus_crc(response):
                        high_byte = response[3]
                        low_byte = response[4]
                        raw_value = (high_byte << 8) + low_byte
                        dba = raw_value / 10.0

                        timestamp_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                        latest_reading["dBA"] = dba
                        latest_reading["timestamp"] = timestamp_str

                        socketio.emit('new_reading', latest_reading)

                        current_time = time.time()

                        if dba >= DECIBEL_THRESHOLD:
                            is_siren_event = siren_detector.is_siren() if siren_detector else False

                            if is_siren_event:
                                print(f"[Suppressed Exceedance] {dba:.1f} dBA flagged as Emergency Siren. Skipping log.")
                            elif current_time - last_alert_time >= ALERT_COOLDOWN_SECONDS:
                                last_alert_time = current_time
                                last_supabase_push = current_time
                                row_id = log_exceedance_sqlite(timestamp_str, dba)
                                log_to_csv(timestamp_str, dba)
                                push_to_supabase_async(dba, sqlite_row_id=row_id, timestamp_str=timestamp_str)
                                
                                socketio.emit('exceedance_alert', {
                                    "dBA": dba,
                                    "timestamp": timestamp_str,
                                    "total_today": get_today_exceedance_count()
                                })

                        elif current_time - last_supabase_push >= 5.0:
                            last_supabase_push = current_time
                            push_to_supabase_async(dba, timestamp_str=timestamp_str)

                    time.sleep(1)
        except Exception as e:
            print(f"[Sensor Error] {e} - Reconnecting in 3 seconds...")
            time.sleep(3)


# ==========================================
# FLASK ROUTES
# ==========================================
@app.route("/")
def index():
    raw_dba = latest_reading.get("dBA", 0.0)
    try:
        current_db = float(raw_dba)
    except (ValueError, TypeError):
        current_db = 0.0

    station_meta = get_station_metadata(STATION_ID)

    return render_template(
        "index.html",
        current_db=current_db,
        threshold=DECIBEL_THRESHOLD,
        today_count=get_today_exceedance_count(),
        total_submitted=get_total_submissions(),
        exceedances_last_hour=query_exceedances_last_hour(),
        station_id=STATION_ID,
        station_metadata=station_meta
    )

@app.route("/api/current_db")
def api_current_db():
    raw_dba = latest_reading.get("dBA", 0.0)
    try:
        current_db = float(raw_dba)
    except (ValueError, TypeError):
        current_db = 0.0

    station_meta = get_station_metadata(STATION_ID)

    return jsonify({
        "current_db": current_db,
        "today_count": get_today_exceedance_count(),
        "exceedances_last_hour": query_exceedances_last_hour(),
        "station_id": STATION_ID,
        "station_name": station_meta.get("name"),
        "location_address": station_meta.get("location_address")
    })

@app.route("/api/station_info")
def api_station_info():
    """Returns complete dynamic station metadata retrieved from Supabase."""
    return jsonify({
        "station_id": STATION_ID,
        "metadata": get_station_metadata(STATION_ID)
    })

@app.route("/file_daily_summary", methods=["POST"])
def route_file_daily_summary():
    global cached_total_submissions
    today_count = get_today_exceedance_count()
    if today_count == 0:
        return jsonify({"success": False, "message": "No noise violations today to report."})

    cached_total_submissions += 1
    with open(COUNT_PATH, "w") as f:
        f.write(str(cached_total_submissions))

    sync_submission_count_to_supabase(cached_total_submissions)

    if submit_daily_summary_report:
        try:
            task_queue.put_nowait({
                "type": "311_report",
                "today_count": today_count
            })
        except queue.Full:
            pass

    return jsonify({
        "success": True,
        "message": f"Successfully queued summary report for {today_count} violations.",
        "total_submitted": cached_total_submissions
    })


# ==========================================
# MAIN ENTRY POINT
# ==========================================
if __name__ == "__main__":
    init_db()

    # 1. Dynamically lookup station ID using CPU serial on boot
    resolved_id = resolve_station_id()
    
    # 2. Pre-fetch and cache station metadata (address, coords, name) from Supabase
    get_station_metadata(resolved_id, force_refresh=True)

    # 3. Initial sync of total counts
    sync_submission_count_to_supabase(get_total_submissions())
    
    # 4. Start background threads
    worker_thread = threading.Thread(target=task_worker, daemon=True)
    worker_thread.start()
    
    queue_listener_thread = threading.Thread(target=supabase_queue_listener, daemon=True)
    queue_listener_thread.start()

    sensor_thread = threading.Thread(target=background_sensor_loop, daemon=True)
    sensor_thread.start()
    
    resync_thread = threading.Thread(target=resync_offline_records, daemon=True)
    resync_thread.start()
    
    socketio.run(app, host="0.0.0.0", port=5000, debug=False, allow_unsafe_werkzeug=True)
