import os
import requests
import io
import csv
import re
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.dialects.mysql import (
    BIGINT, DATETIME, FLOAT, INTEGER, VARCHAR, TEXT
)
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

BASE_DIR = os.path.dirname(__file__)
OUTPUT_FOLDER = os.path.join(BASE_DIR, 'output')

# ML API URL (this API decides ML or NoML automatically)
ML_API_URL = os.getenv("API_URL")

# Database config
db_uri = os.getenv("DATABASE_URL")
app.config['SQLALCHEMY_DATABASE_URI'] = db_uri

# SSL config for Aiven
ca_pem_path = os.path.join(BASE_DIR, 'ca.pem')
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    'connect_args': {
        'ssl': {'ca': ca_pem_path}
    }
}
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# --- Database Model ---
class NetworkLog(db.Model):
    __tablename__ = 'tbl_network_log'
    
    id = db.Column(INTEGER(unsigned=True), primary_key=True)
    session_id = db.Column(BIGINT, nullable=False, index=True)
    timestamp = db.Column(DATETIME, index=True)
    lat = db.Column(FLOAT(precision=10, scale=6))
    lon = db.Column(FLOAT(precision=10, scale=6))
    band = db.Column(VARCHAR(64), index=True)
    m_alpha_long = db.Column(VARCHAR(45), index=True)
    network = db.Column(VARCHAR(45), index=True)
    earfcn = db.Column(VARCHAR(45))
    pci = db.Column(VARCHAR(45))
    rsrp = db.Column(FLOAT(precision=5, scale=2))
    rsrq = db.Column(FLOAT(precision=5, scale=2))
    sinr = db.Column(FLOAT(precision=5, scale=2))
    primary_cell_info_1 = db.Column(TEXT)
    ta = db.Column(VARCHAR(128))


# CSV Header
CSV_HEADER = [
    'timestamp_utc', 'lat', 'lon', 'network', 'technology',
    'earfcn_or_narfcn', 'pci_or_psi', 'rsrp_dbm', 'rsrq_db',
    'sinr_db', 'band_mhz', 'cell_id_global', 'ta'
]

def safe_int(value):
    try:
        return int(float(value))
    except:
        return None

def safe_float(value):
    try:
        return float(value)
    except:
        return None

def extract_mci(cell_info_str):
    if not cell_info_str:
        return None
    match = re.search(r'mCi=([0-9*]+)', cell_info_str)
    return match.group(1) if match else None


# --- MAIN API ENDPOINT ---
@app.route('/api/process-and-save', methods=['POST'])
def process_and_save():
    data = request.get_json()

    session_ids = data.get('session_ids')
    project_id_str = data.get('project_id')

    if not session_ids or not isinstance(session_ids, list):
        return jsonify({"error": "session_ids must be a list"}), 400

    if not project_id_str:
        return jsonify({"error": "project_id is required"}), 400

    project_id = safe_int(project_id_str)
    if project_id is None:
        return jsonify({"error": "Invalid project_id; must be integer"}), 400

    logs = NetworkLog.query.filter(NetworkLog.session_id.in_(session_ids)).all()
    if not logs:
        return jsonify({"error": "No logs found for given session_ids"}), 404

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    filename = f"project_{project_id}_combined_data.csv"
    file_path = os.path.join(OUTPUT_FOLDER, filename)

    # Create CSV file
    with open(file_path, 'w', newline='', encoding='utf-8') as output_file:
        writer = csv.writer(output_file)
        writer.writerow(CSV_HEADER)

        for log in logs:
            writer.writerow([
                log.timestamp.isoformat() if log.timestamp else None,
                safe_float(log.lat),
                safe_float(log.lon),
                log.m_alpha_long,
                log.network,
                safe_int(log.earfcn),
                safe_int(log.pci),
                safe_float(log.rsrp),
                safe_float(log.rsrq),
                safe_float(log.sinr),
                log.band,
                extract_mci(log.primary_cell_info_1),
                log.ta
            ])

    # --- SEND TO ML API ALWAYS ---
    if not ML_API_URL:
        return jsonify({"error": "ML_API_URL not configured in .env"}), 500

    try:
        with open(file_path, 'rb') as f:
            response = requests.post(
                ML_API_URL,
                files={'file': (filename, f, 'text/csv')},
                data={'project_id': project_id}
            )

        response.raise_for_status()

        try:
            ml_api_response = response.json()
        except:
            ml_api_response = {"raw_response": response.text}

    except requests.exceptions.RequestException as e:
        return jsonify({
            "status": "error",
            "message": "ML API request failed",
            "error": str(e)
        }), 502

    return jsonify({
        "status": "success",
        "file_path": file_path,
        "ml_api_response": ml_api_response
    }), 200


# --- RUN SERVER ---
if __name__ == '__main__':
    app.run(debug=True, port=8080, host='0.0.0.0')
