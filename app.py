import os
import requests
import io
import csv
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.dialects.mysql import (
    BIGINT, DATETIME, FLOAT, INTEGER, VARCHAR, TEXT
)
from dotenv import load_dotenv
load_dotenv()
# --- Configuration ---

app = Flask(__name__)

# 1. ML API Configuration
ML_API_URL = os.getenv("API_URL")

# 2. Database Configuration
db_uri = os.getenv("DATABASE_URL")
app.config['SQLALCHEMY_DATABASE_URI'] = db_uri

# SSL Configuration for Aiven
ca_pem_path = os.path.join(os.path.dirname(__file__), 'ca.pem')

if not os.path.exists(ca_pem_path):
    print("-------------------------------------------------------")
    print(f"ERROR: ca.pem file not found at: {ca_pem_path}")
    print("Please download ca.pem from your Aiven dashboard")
    print("and place it in the same directory as app.py")
    print("-------------------------------------------------------")
    
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    'connect_args': {
        'ssl': {
            'ca': ca_pem_path
        }
    }
}
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# --- Database Model ---
# This class is an exact match for your table schema
class NetworkLog(db.Model):
    __tablename__ = 'tbl_network_log'
    
    id = db.Column(INTEGER(unsigned=True), primary_key=True)
    session_id = db.Column(BIGINT, nullable=False, index=True)
    timestamp = db.Column(DATETIME, index=True)
    lat = db.Column(FLOAT(precision=10, scale=6))
    lon = db.Column(FLOAT(precision=10, scale=6))
    altitude = db.Column(db.Float)
    indoor_outdoor = db.Column(VARCHAR(45))
    phone_heading = db.Column(db.Float)
    battery = db.Column(db.Integer)
    dls = db.Column(VARCHAR(50))
    uls = db.Column(VARCHAR(50))
    call_state = db.Column(VARCHAR(50))
    hotspot = db.Column(VARCHAR(512))
    apps = db.Column(VARCHAR(512))
    num_cells = db.Column(db.Integer)
    network = db.Column(VARCHAR(45), index=True)
    m_mcc = db.Column(db.Integer)
    m_mnc = db.Column(db.Integer)
    m_alpha_long = db.Column(VARCHAR(45), index=True)
    m_alpha_short = db.Column(VARCHAR(45))
    mci = db.Column(VARCHAR(45))
    pci = db.Column(VARCHAR(45))
    tac = db.Column(VARCHAR(45))
    earfcn = db.Column(VARCHAR(45))
    rssi = db.Column(FLOAT(precision=5, scale=2))
    rsrp = db.Column(FLOAT(precision=5, scale=2))
    rsrq = db.Column(FLOAT(precision=5, scale=2))
    sinr = db.Column(FLOAT(precision=5, scale=2))
    total_rx_kb = db.Column(VARCHAR(45))
    total_tx_kb = db.Column(VARCHAR(45))
    mos = db.Column(db.Float)
    jitter = db.Column(db.Float)
    latency = db.Column(db.Float)
    packet_loss = db.Column(db.Float)
    dl_tpt = db.Column(VARCHAR(45))
    ul_tpt = db.Column(VARCHAR(45))
    volte_call = db.Column(VARCHAR(45))
    band = db.Column(VARCHAR(64), index=True)
    cqi = db.Column(db.Float)
    bler = db.Column(VARCHAR(45))
    primary_cell_info_1 = db.Column(TEXT)
    primary_cell_info_2 = db.Column(TEXT)
    all_neigbor_cell_info = db.Column(TEXT)
    image_path = db.Column(VARCHAR(512))
    polygon_id = db.Column(db.Integer, index=True)
    primary_cell_info_3 = db.Column(TEXT)
    speed = db.Column(db.Float)
    ta = db.Column(VARCHAR(128))
    mcc = db.Column(db.Integer)
    mnc = db.Column(db.Integer)
    gps_fix_type = db.Column(VARCHAR(128))
    gps_hdop = db.Column(db.Float)
    gps_vdop = db.Column(db.Float)
    phone_antenna_gain = db.Column(VARCHAR(128))
    csi_rsrp = db.Column(db.Float)
    csi_rsrq = db.Column(db.Float)
    csi_sinr = db.Column(db.Float)
    level = db.Column(db.Integer)
    cell_id = db.Column(VARCHAR(128))
    nodeb_id = db.Column(VARCHAR(128))
    primary = db.Column(VARCHAR(128))


# CSV Header from your sample file
CSV_HEADER = [
    'timestamp_utc', 'lat', 'lon', 'network', 'technology',
    'earfcn_or_narfcn', 'pci_or_psi', 'rsrp_dbm', 'rsrq_db',
    'sinr_db', 'band_mhz', 'cell_id_global', 'ta'
]

# --- Helper Functions for Data Cleaning ---

def safe_int(value, default=None):
    """
    Tries to convert a value to an integer.
    Handles None, empty strings, and floats.
    Returns 'default' (None) on failure.
    """
    if value is None or value == '':
        return default
    try:
        # Convert floats to int, then to int
        return int(float(value))
    except (ValueError, TypeError):
        return default 

def safe_float(value, default=None):
    """
    Tries to convert a value to a float.
    Handles None, empty strings.
    Returns 'default' (None) on failure.
    """
    if value is None or value == '':
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

# --- API Endpoint ---
@app.route('/api/process-and-upload', methods=['POST'])
def process_and_upload():
    data = request.get_json()
    session_id = data.get('session_id')
    project_id = data.get('project_id')

    if not session_id or not project_id:
        return jsonify({"error": "Missing 'session_id' or 'project_id'"}), 400

    print(f"Processing session: {session_id} for project: {project_id}")

    try:
        # 1. Find data in tbl_network_log
        logs = NetworkLog.query.filter_by(session_id=int(session_id)).all()

        if not logs:
            return jsonify({"error": f"No logs found for session_id: {session_id}"}), 404

        print(f"Found {len(logs)} log entries.")



        # 2. Create the CSV file in memory
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(CSV_HEADER)
        
        for log in logs:
            timestamp_str = log.timestamp.isoformat() if log.timestamp else None
            
            # This mapping is correct based on your sample file
            writer.writerow([
                timestamp_str,
                safe_float(log.lat),
                safe_float(log.lon),
                log.network,              # network/operator (e.g., 'Airtel', 'VI', 'JIO')
                log.dls or log.uls or log.band or log.network,  # technology fallback (LTE/NR/3G)
                safe_int(log.earfcn),
                safe_int(log.pci),
                safe_int(log.rsrp),
                safe_int(log.rsrq),
                safe_int(log.sinr),
                log.band,                 # keep band as TEXT, do NOT convert
                str(log.mci),             # IMPORTANT: send as string, not int
                log.ta                    # keep raw TA value
            ])


        
        csv_data = output.getvalue()
        output.close()

        # 3. Prepare data to send to ML API
        payload_data = {
            'project_id': project_id
        }
        
        payload_files = {
            'file': (f'{session_id}_data.csv', csv_data, 'text/csv')
        }

        # 4. Send to ML prediction tool
        print(f"Forwarding CSV data to ML API...")
        response = requests.post(ML_API_URL, data=payload_data, files=payload_files)
        
        response.raise_for_status() # Error on 4xx/5xx

        # 5. Return the response from the ML tool
        return jsonify({
            "status": "success",
            "message": "Data processed and sent to ML API",
            "ml_api_response": response.json()
        }), 200

    except requests.exceptions.HTTPError as e:
        # Handle errors from the ML API call
        print(f"HTTP Error from ML API: {e}")
        return jsonify({
            "error": "ML API returned an error", 
            "details": str(e),
            "ml_api_response": e.response.text
        }), e.response.status_code
        
    except Exception as e:
        # Handle other errors
        print(f"An unexpected error occurred: {e}")
        db.session.rollback()
        return jsonify({"error": "An internal server error occurred", "details": str(e)}), 500

# --- Run the App ---
if __name__ == '__main__':
    app.run(debug=True, port=8080)