# src/config.py
import os
from dotenv import load_dotenv
from pathlib import Path

# Load .env if present (optional)
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

# Root output directory (new): will contain per-RUT folders
OUTPUT_DIR = os.getenv('OUTPUT_DIR', 'output')

# Backwards-compatible single-file output path.
# If not provided explicitly, it will be placed inside OUTPUT_DIR.
_tmp_output_file = os.getenv('OUTPUT_FILE', None)
if _tmp_output_file:
    OUTPUT_FILE = _tmp_output_file
else:
    OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'results.xlsx')

RUT = os.getenv('RUT', '')
CLAVE = os.getenv('CLAVE', '')
START_URL = os.getenv('START_URL', 'https://servicios.dgi.gub.uy/serviciosenlinea')
HEADLESS = _bool(os.getenv('HEADLESS', 'true'))
ECF_TIPO = os.getenv('ECF_TIPO', '111')
ECF_FROM_DATE = os.getenv('ECF_FROM_DATE', '')
ECF_TO_DATE = os.getenv('ECF_TO_DATE', '')
# downloads folder for browser downloads (separate from OUTPUT_DIR)
DOWNLOAD_DIR = os.getenv('DOWNLOAD_DIR', 'downloads')

# MAX_PAGES safe parsing
try:
    MAX_PAGES = int(os.getenv('MAX_PAGES')) if os.getenv('MAX_PAGES') else None
except Exception:
    MAX_PAGES = None

def override_from_dict(d: dict):
    """
    Apply overrides at runtime (in-memory). Does not write .env file.
    Keys should match the uppercase config names.
    """
    global OUTPUT_DIR, OUTPUT_FILE, HEADLESS, MAX_PAGES

    for k, v in d.items():
        if v is None:
            continue
        kk = str(k).strip()
        kk_norm = kk.upper()
        os.environ[kk_norm] = str(v)
        # write into module globals so callers reading config.<NAME> get the update
        globals()[kk_norm] = v

    # If OUTPUT_DIR was set via overrides but OUTPUT_FILE was not, update OUTPUT_FILE default
    if 'OUTPUT_DIR' in os.environ and not os.environ.get('OUTPUT_FILE'):
        OUTPUT_DIR = os.environ.get('OUTPUT_DIR')
        OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'results.xlsx')

    # ensure HEADLESS interpreted to boolean
    if 'HEADLESS' in os.environ:
        val = os.environ.get('HEADLESS')
        globals()['HEADLESS'] = str(val).lower() in ("1", "true", "yes", "y", "on")

    # ensure MAX_PAGES is integer or None
    if 'MAX_PAGES' in os.environ:
        try:
            globals()['MAX_PAGES'] = int(os.environ.get('MAX_PAGES'))
        except Exception:
            globals()['MAX_PAGES'] = None
