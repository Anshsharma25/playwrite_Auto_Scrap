# # src/app.py
# from flask import Flask, request, jsonify, render_template, Response
# from types import SimpleNamespace
# from datetime import datetime
# import traceback

# import src.main as main_mod

# app = Flask(__name__, template_folder="templates", static_folder="static")


# def _iso_to_ddmmyyyy(iso_date_str):
#     """
#     Convert YYYY-MM-DD to DD/MM/YYYY. If input empty -> None.
#     """
#     if not iso_date_str:
#         return None
#     try:
#         d = datetime.fromisoformat(iso_date_str)
#         return d.strftime("%d/%m/%Y")
#     except Exception:
#         parts = iso_date_str.split("-")
#         if len(parts) >= 3:
#             return f"{parts[2]}/{parts[1]}/{parts[0]}"
#         return iso_date_str


# @app.route("/", methods=["GET"])
# def index():
#     return render_template("index.html")


# @app.route("/run", methods=["POST"])
# def run_scraper():
#     """
#     Expects form fields from the frontend:
#       - login_method: 'rut' or 'cedula'
#       - rut / clave  OR cedula / clave_cedula
#       - filter_code (mapped to ECF_TIPO)
#       - from_date (YYYY-MM-DD) optional
#       - to_date (YYYY-MM-DD) optional
#     Returns: JSON { ok: bool, output: <path> } or { ok: False, error: <msg> }
#     """
#     try:
#         fm = request.form or {}
#         login_method = fm.get("login_method")
#         if login_method == "rut":
#             rut = fm.get("rut", "").strip()
#             clave = fm.get("clave", "").strip()
#         else:
#             rut = fm.get("cedula", "").strip()
#             clave = fm.get("clave_cedula", "").strip()

#         filter_code = fm.get("filter_code", "").strip() or None
#         from_date_iso = fm.get("from_date") or None
#         to_date_iso = fm.get("to_date") or None

#         # validate dates if provided
#         if from_date_iso and to_date_iso:
#             try:
#                 d_from = datetime.fromisoformat(from_date_iso)
#                 d_to = datetime.fromisoformat(to_date_iso)
#             except Exception:
#                 return jsonify({"ok": False, "error": "Invalid date format. Use YYYY-MM-DD."}), 400
#             diff_days = (d_to - d_from).days + 1
#             if diff_days <= 0:
#                 return jsonify({"ok": False, "error": "ECF_TO_DATE must be same or after ECF_FROM_DATE."}), 400
#             if diff_days > 30:
#                 return jsonify({"ok": False, "error": "Date range too large: max allowed is 30 days."}), 400

#         # build CLI-like namespace for main.run
#         cli_args = SimpleNamespace(
#             rut=rut or None,
#             clave=clave or None,
#             tipo=filter_code or None,
#             from_date=_iso_to_ddmmyyyy(from_date_iso) if from_date_iso else None,
#             to_date=_iso_to_ddmmyyyy(to_date_iso) if to_date_iso else None,
#             max_pages=None,
#             headless=None,
#             show_browser=None
#         )

#         # Run synchronously, force headless on server
#         res = main_mod.run(cli_args=cli_args, headless_forced=True)

#         if res.get("ok"):
#             return jsonify({"ok": True, "output": res.get("output")})
#         else:
#             return jsonify({"ok": False, "error": res.get("error") or "unknown"}), 500

#     except Exception as e:
#         traceback.print_exc()
#         return jsonify({"ok": False, "error": str(e)}), 500


# if __name__ == "__main__":
#     # debug server for local dev (use gunicorn/uwsgi in production)
#     app.run(host="0.0.0.0", port=5000, debug=True)

# src/app.py
from flask import Flask, request, jsonify, render_template, Response
from types import SimpleNamespace
from datetime import datetime
import traceback
import uuid
import time

import src.main as main_mod
from src.logger import logger

app = Flask(__name__, template_folder="templates", static_folder="static")


def _iso_to_ddmmyyyy(iso_date_str):
    """
    Convert YYYY-MM-DD to DD/MM/YYYY. If input empty -> None.
    """
    if not iso_date_str:
        return None
    try:
        d = datetime.fromisoformat(iso_date_str)
        return d.strftime("%d/%m/%Y")
    except Exception:
        parts = iso_date_str.split("-")
        if len(parts) >= 3:
            return f"{parts[2]}/{parts[1]}/{parts[0]}"
        return iso_date_str


def _mask_sensitive(s: str, keep_left: int = 3, keep_right: int = 3):
    if not s:
        return "(empty)"
    s = str(s)
    if len(s) <= (keep_left + keep_right):
        return "*" * len(s)
    return s[:keep_left] + "*" * (len(s) - keep_left - keep_right) + s[-keep_right:]


def _get_client_ip():
    # Respect X-Forwarded-For if behind a proxy; otherwise remote_addr
    xff = request.headers.get("X-Forwarded-For", "")
    if xff:
        # could be comma separated list
        return xff.split(",")[0].strip()
    return request.remote_addr or "unknown"


@app.route("/", methods=["GET"])
def index():
    logger.info("Serving index.html")
    return render_template("index.html")


@app.route("/run", methods=["POST"])
def run_scraper():
    """
    Expects form fields from the frontend:
      - login_method: 'rut' or 'cedula'
      - rut / clave  OR cedula / clave_cedula
      - filter_code (mapped to ECF_TIPO)
      - from_date (YYYY-MM-DD) optional
      - to_date (YYYY-MM-DD) optional

    Logs:
      - job_id
      - timestamp start/end
      - duration
      - masked credentials
      - client IP
      - outcome and error trace if any

    Returns JSON: { ok: bool, output: <path>, job_id: <uuid>, duration_sec: <float> }
    """
    job_id = str(uuid.uuid4())
    client_ip = _get_client_ip()
    received_at = datetime.utcnow().isoformat() + "Z"

    logger.info(f"[{job_id}] Received /run request from {client_ip} at {received_at}")

    try:
        fm = request.form or {}
        login_method = fm.get("login_method")
        if login_method == "rut":
            rut = fm.get("rut", "").strip()
            clave = fm.get("clave", "").strip()
        else:
            rut = fm.get("cedula", "").strip()
            clave = fm.get("clave_cedula", "").strip()

        filter_code = fm.get("filter_code", "").strip() or None
        from_date_iso = fm.get("from_date") or None
        to_date_iso = fm.get("to_date") or None

        # Log sanitized inputs (mask secrets)
        logger.info(
            f"[{job_id}] Inputs: login_method={login_method or 'none'}, "
            f"rut={_mask_sensitive(rut)}, clave={_mask_sensitive(clave)}, "
            f"filter_code={filter_code or '(none)'}, from_date={from_date_iso or '(none)'}, to_date={to_date_iso or '(none)'}"
        )

        # validate dates if provided
        if from_date_iso and to_date_iso:
            try:
                d_from = datetime.fromisoformat(from_date_iso)
                d_to = datetime.fromisoformat(to_date_iso)
            except Exception:
                logger.info(f"[{job_id}] Date parsing failed for inputs: from={from_date_iso} to={to_date_iso}")
                return jsonify({"ok": False, "error": "Invalid date format. Use YYYY-MM-DD.", "job_id": job_id}), 400
            diff_days = (d_to - d_from).days + 1
            if diff_days <= 0:
                logger.info(f"[{job_id}] Invalid date range: from {from_date_iso} to {to_date_iso}")
                return jsonify({"ok": False, "error": "ECF_TO_DATE must be same or after ECF_FROM_DATE.", "job_id": job_id}), 400
            if diff_days > 30:
                logger.info(f"[{job_id}] Date range too large: {diff_days} days")
                return jsonify({"ok": False, "error": "Date range too large: max allowed is 30 days.", "job_id": job_id}), 400

        # build CLI-like namespace for main.run
        cli_args = SimpleNamespace(
            rut=rut or None,
            clave=clave or None,
            tipo=filter_code or None,
            from_date=_iso_to_ddmmyyyy(from_date_iso) if from_date_iso else None,
            to_date=_iso_to_ddmmyyyy(to_date_iso) if to_date_iso else None,
            max_pages=None,
            headless=None,
            show_browser=None
        )

        # Start timer
        start_ts = time.time()
        logger.info(f"[{job_id}] Starting job (headless forced).")

        # Run synchronously, force headless on server
        res = main_mod.run(cli_args=cli_args, headless_forced=True)

        duration = time.time() - start_ts
        if res.get("ok"):
            logger.info(f"[{job_id}] Job finished successfully in {duration:.2f}s. Output: {res.get('output')}")
            return jsonify({"ok": True, "output": res.get("output"), "job_id": job_id, "duration_sec": round(duration, 2)})
        else:
            logger.error(f"[{job_id}] Job failed in {duration:.2f}s. Error: {res.get('error')}")
            return jsonify({"ok": False, "error": res.get("error") or "unknown", "job_id": job_id, "duration_sec": round(duration, 2)}), 500

    except Exception as e:
        duration = time.time() - start_ts if 'start_ts' in locals() else None
        logger.exception(f"[{job_id}] Unhandled exception while processing job. duration={duration}")
        return jsonify({"ok": False, "error": str(e), "job_id": job_id}), 500


if __name__ == "__main__":
    # debug server for local dev (use gunicorn/uwsgi in production)
    logger.info("Starting Flask app (development mode)")
    app.run(host="0.0.0.0", port=5000, debug=True)

