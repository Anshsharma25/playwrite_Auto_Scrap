# app.py
import os
import tempfile
import traceback
from flask import Flask, request, render_template, jsonify
import pandas as pd
from pathlib import Path
import importlib
import src.config as config
from src import main as cfe_main

app = Flask(__name__, template_folder="templates")

ALLOWED_EXT = {'.txt', '.csv', '.xlsx', '.xls'}

def _parse_uploaded_file_to_dict(path: str) -> dict:
    """
    Parse a small XLSX or TXT/CSV file and return a dict of key->value.
    For Excel: expects first sheet, first column = key, second column = value (header optional).
    For TXT/CVS: expects lines like KEY=VALUE or comma separated key,value
    """
    ext = Path(path).suffix.lower()
    data = {}
    try:
        if ext in ('.xlsx', '.xls'):
            df = pd.read_excel(path, header=None)
            for _, row in df.iterrows():
                if len(row) >= 2 and pd.notna(row[0]):
                    key = str(row[0]).strip()
                    val = '' if pd.isna(row[1]) else str(row[1]).strip()
                    data[key] = val
        else:
            # txt/csv simple parsing
            with open(path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    if '=' in line:
                        k, v = line.split('=', 1)
                        data[k.strip()] = v.strip()
                    else:
                        parts = [p.strip() for p in line.split(',') if p.strip()]
                        if len(parts) >= 2:
                            data[parts[0]] = parts[1]
    except Exception:
        traceback.print_exc()
    return data

def _parse_date(s: str):
    """Try to parse a date from common formats. Return datetime.date or None."""
    if not s:
        return None
    s = str(s).strip()
    fmts = ['%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', '%d.%m.%Y', '%m/%d/%Y']
    for f in fmts:
        try:
            return pd.to_datetime(s, format=f).date()
        except Exception:
            continue
    # fallback to pandas parsing
    try:
        return pd.to_datetime(s, dayfirst=True).date()
    except Exception:
        return None

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/run', methods=['POST'])
def run_scraper():
    """
    Accepts an optional file upload containing key/value pairs (XLSX or TXT/CSV).
    Also accepts form fields: rut, clave, from_date, to_date.
    Applies overrides in-memory (does NOT write .env), validates date range (<=30 days),
    forces HEADLESS True and runs the scraper synchronously (blocking).
    Returns JSON with 'ok' and 'output' or 'error'.
    """
    file = request.files.get('file')
    temp_path = None
    overrides = {}

    # parse uploaded file if present
    if file:
        ext = Path(file.filename).suffix.lower()
        if ext not in ALLOWED_EXT:
            return jsonify({'ok': False, 'error': 'unsupported file type'}), 400
        tmpdir = tempfile.mkdtemp()
        temp_path = os.path.join(tmpdir, file.filename)
        file.save(temp_path)
        overrides = _parse_uploaded_file_to_dict(temp_path) or {}

    # parse explicit form fields (they override file values)
    form_rut = request.form.get('rut') or request.form.get('RUT')
    form_clave = request.form.get('clave') or request.form.get('CLAVE')
    form_from = request.form.get('from_date') or request.form.get('ECF_FROM_DATE') or request.form.get('from')
    form_to = request.form.get('to_date') or request.form.get('ECF_TO_DATE') or request.form.get('to')

    if form_rut:
        overrides['RUT'] = form_rut.strip()
    if form_clave:
        overrides['CLAVE'] = form_clave.strip()
    if form_from:
        overrides['ECF_FROM_DATE'] = form_from.strip()
    if form_to:
        overrides['ECF_TO_DATE'] = form_to.strip()

    try:
        # reload config and apply overrides in-memory (no .env write)
        importlib.reload(config)
        if overrides:
            # normalize keys to expected uppercase keys
            normalized = {}
            for k, v in overrides.items():
                if not k:
                    continue
                kk = str(k).strip()
                vv = "" if v is None else str(v).strip()
                if kk.lower() == "rut":
                    normalized["RUT"] = vv
                elif kk.lower() == "clave":
                    normalized["CLAVE"] = vv
                elif kk.lower() in ("ecf_from_date", "from", "from_date"):
                    normalized["ECF_FROM_DATE"] = vv
                elif kk.lower() in ("ecf_to_date", "to", "to_date"):
                    normalized["ECF_TO_DATE"] = vv
                elif kk.lower() == "ecf_tipo":
                    # frontend is NOT supposed to override ECF_TIPO, but accept if provided intentionally
                    normalized["ECF_TIPO"] = vv
                elif kk == "MAX_PAGES":
                    normalized["MAX_PAGES"] = vv
                else:
                    # keep any other keys
                    normalized[kk] = vv
            config.override_from_dict(normalized)

        # simple required check: RUT and CLAVE must be present (frontend must supply)
        cur_rut = getattr(config, "RUT", "") or ""
        cur_clave = getattr(config, "CLAVE", "") or ""
        if not cur_rut or not cur_clave:
            return jsonify({'ok': False, 'error': 'RUT and CLAVE required (provide via file or form fields).'}), 400

        # Validate dates before launching browser
        d_from_s = getattr(config, "ECF_FROM_DATE", "") or ""
        d_to_s = getattr(config, "ECF_TO_DATE", "") or ""
        d_from = _parse_date(d_from_s)
        d_to = _parse_date(d_to_s)
        if d_from and d_to:
            delta = (d_to - d_from).days
            if delta < 0:
                return jsonify({'ok': False, 'error': 'ECF_TO_DATE is earlier than ECF_FROM_DATE'}), 400
            if delta > 30:
                return jsonify({'ok': False, 'error': 'Date range too large: max allowed is 30 days'}), 400

        # Ensure HEADLESS enforced (do not open visible browser from web server)
        config.HEADLESS = True
        os.environ['HEADLESS'] = "true"

        # Run scraper synchronously
        try:
            cfe_main.run()
        except ValueError as ve:
            # Known validation errors (login failure etc) -> return 400 with message
            return jsonify({'ok': False, 'error': str(ve)}), 400

        output_file = getattr(config, 'OUTPUT_FILE', None)
        return jsonify({'ok': True, 'output': output_file})
    except Exception as e:
        traceback.print_exc()
        return jsonify({'ok': False, 'error': 'Internal Server Error (check logs)'}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))  # Render provides PORT
    app.run(host="0.0.0.0", port=port, debug=False)
