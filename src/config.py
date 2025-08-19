# src/config.py
import os
from dotenv import load_dotenv
from pathlib import Path

# Load .env if present (optional) — runtime overrides from the Flask upload will not write .env
BASE_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = BASE_DIR / '.env'
if ENV_PATH.exists():
    load_dotenv(dotenv_path=str(ENV_PATH), override=True)
else:
    load_dotenv(override=False)

def _bool(v, default=False):
    if v is None:
        return default
    v = str(v).lower()
    return v in ("1", "true", "yes", "y", "on")

# default values (read from environment if present)
RUT = os.getenv('RUT', '')
CLAVE = os.getenv('CLAVE', '')
START_URL = os.getenv('START_URL', 'https://servicios.dgi.gub.uy/serviciosenlinea')
HEADLESS = _bool(os.getenv('HEADLESS', 'true'))
ECF_TIPO = os.getenv('ECF_TIPO', '111')
ECF_FROM_DATE = os.getenv('ECF_FROM_DATE', '')
ECF_TO_DATE = os.getenv('ECF_TO_DATE', '')
OUTPUT_FILE = os.getenv('OUTPUT_FILE', 'output/results.xlsx')
DOWNLOAD_DIR = os.getenv('DOWNLOAD_DIR', 'downloads')
MAX_PAGES = int(os.getenv('MAX_PAGES')) if os.getenv('MAX_PAGES') else None

def override_from_dict(d: dict):
    """
    Apply overrides at runtime (in-memory). Does not write .env file.
    Keys should match the uppercase config names.
    """
    for k, v in d.items():
        if v is None:
            continue
        kk = str(k).strip()
        kk_norm = kk.upper()
        os.environ[kk_norm] = str(v)
        globals()[kk_norm] = v
    # ensure HEADLESS interpreted to boolean
    if 'HEADLESS' in os.environ:
        val = os.environ.get('HEADLESS')
        globals()['HEADLESS'] = str(val).lower() in ("1", "true", "yes", "y", "on")
