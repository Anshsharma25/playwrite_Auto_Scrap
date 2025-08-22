# # src/main.py
# """
# Main runner for the efactura scraping job.

# Usage examples:
#     python -m src.main                 # show the browser (webview) — default
#     python -m src.main --show-browser  # explicit: show webview
#     python -m src.main --headless      # run headless (no UI)
#     python -m src.main --rut 213...    # override RUT via CLI
#     You can also set environment variables RUT, CLAVE, ECF_TIPO, ECF_FROM_DATE, ECF_TO_DATE
# """

# import argparse
# import importlib
# import os
# import sys
# import time
# from pathlib import Path

# from playwright.sync_api import sync_playwright

# from src import auth
# from src import selectors as sel

# # --- Embedded defaults you gave (will be forced into config if not provided) ---
# EMBEDDED_DEFAULTS = {
#     "RUT": "213624850018",
#     "CLAVE": "aa0000aa",
#     "ECF_TIPO": "0",
#     "ECF_FROM_DATE": "01/07/2025",
#     "ECF_TO_DATE": "30/07/2025",
# }

# '''
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Todos ==================================================================  0,
# e-Ticket =============================================================== 101
# Nota de Crédito de e-Ticket  =========================================== 102,
# Nota de Débito de e-Ticket ============================================= 103, 
# e-Factura ============================================================== 111,
# Nota de Crédito de e-Factura =========================================== 112,
# Nota de Débito de e-Factura ============================================ 113,
# e-Factura de Exportación================================================ 121,
# Nota de crédito de e-Factura de Exportación============================= 122,
# Nota de débito de e-Factura de Exportación ============================= 123,
# e-Remito de Exportación ================================================ 124,
# e-Ticket Venta por Cuenta Ajena ======================================== 131,
# Nota de Crédito de e-Ticket Venta por Cuenta Ajena ===================== 132,
# Nota de Débito de e-Ticket Venta por Cuenta Ajena -===================== 133,
# e-Factura Venta por Cuenta Ajena ======================================= 141,
# Nota de Crédito de e-Factura Venta por Cuenta Ajena ==================== 142,
# Nota de Débito de e-Factura Venta por Cuenta Ajena ===================== 143, 
# e-Boleta de entrada ==================================================== 151,
# Nota de Crédito de e-Boleta de entrada ================================= 152,
# Nota de Débito de e-Boleta de entrada=================================== 153,
# e-Remito =============================================================== 181, 
# e-Resguardo============================================================= 182
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# '''

# def parse_args():
#     p = argparse.ArgumentParser(description="Run EFactura scraping job (main).")
#     group = p.add_mutually_exclusive_group()
#     group.add_argument("--headless", action="store_true", default=False,
#                        help="Run in headless mode (no browser UI).")
#     group.add_argument("--show-browser", action="store_true", default=False,
#                        help="Show browser/webview (headed). This is the default when no flag is provided.")
#     p.add_argument("--rut", type=str, help="Override RUT")
#     p.add_argument("--clave", type=str, help="Override CLAVE")
#     p.add_argument("--tipo", type=str, help="Override ECF_TIPO")
#     p.add_argument("--from", dest="from_date", type=str, help="Override ECF_FROM_DATE")
#     p.add_argument("--to", dest="to_date", type=str, help="Override ECF_TO_DATE")
#     p.add_argument("--max-pages", dest="max_pages", type=int, default=None,
#                    help="Optional: limit pages to fetch")
#     return p.parse_args()


# def build_and_apply_overrides(cli_args):
#     """
#     Build overrides dict with priority CLI -> existing ENV -> EMBEDDED_DEFAULTS.
#     Then call src.config.override_from_dict(overrides) so config module and os.environ are updated.
#     """
#     import src.config as config
#     importlib.reload(config)

#     keys = ["RUT", "CLAVE", "ECF_TIPO", "ECF_FROM_DATE", "ECF_TO_DATE"]

#     cli_map = {
#         "RUT": cli_args.rut,
#         "CLAVE": cli_args.clave,
#         "ECF_TIPO": cli_args.tipo,
#         "ECF_FROM_DATE": cli_args.from_date,
#         "ECF_TO_DATE": cli_args.to_date,
#     }

#     overrides = {}
#     for k in keys:
#         cli_val = cli_map.get(k)
#         env_val = os.getenv(k)
#         default_val = EMBEDDED_DEFAULTS.get(k)
#         chosen = cli_val if cli_val is not None else (env_val if env_val is not None and env_val != "" else default_val)
#         # ensure chosen is string or empty string
#         overrides[k] = str(chosen) if chosen is not None else ""

#     # include MAX_PAGES if provided on CLI
#     if cli_args.max_pages is not None:
#         overrides["MAX_PAGES"] = cli_args.max_pages

#     # Apply overrides into src.config and environment
#     try:
#         config.override_from_dict(overrides)
#     except Exception:
#         # fallback: write directly into os.environ and module globals
#         for kk, vv in overrides.items():
#             if vv is None:
#                 vv = ""
#             os.environ[kk] = str(vv)
#             try:
#                 setattr(config, kk, vv)
#             except Exception:
#                 config.__dict__[kk] = vv

#     # Ensure OUTPUT_FILE / DOWNLOAD_DIR defaults exist in config
#     if not getattr(config, "OUTPUT_FILE", None):
#         config.OUTPUT_FILE = os.path.join(getattr(config, "OUTPUT_DIR", "."), "results.xlsx")
#     if not getattr(config, "DOWNLOAD_DIR", None):
#         config.DOWNLOAD_DIR = os.path.join(".", "downloads")

#     # reload to ensure consistency
#     importlib.reload(config)
#     return config


# def mask_secret(s: str) -> str:
#     if not s:
#         return "(empty)"
#     s = str(s)
#     if len(s) <= 6:
#         return "*" * len(s)
#     return s[:3] + "*" * (len(s) - 6) + s[-3:]


# def run():
#     args = parse_args()

#     # Force overrides into src.config (and env)
#     config = build_and_apply_overrides(args)

#     # Print the final runtime config that will be used
#     print("[INFO] Final runtime config being used:")
#     print(f"       RUT           = {getattr(config, 'RUT', '') or '(empty)'}")
#     print(f"       CLAVE         = {mask_secret(getattr(config, 'CLAVE', ''))}")
#     print(f"       ECF_TIPO      = {getattr(config, 'ECF_TIPO', '') or '(empty)'}")
#     print(f"       ECF_FROM_DATE = {getattr(config, 'ECF_FROM_DATE', '') or '(empty)'}")
#     print(f"       ECF_TO_DATE   = {getattr(config, 'ECF_TO_DATE', '') or '(empty)'}")
#     print(f"       OUTPUT_FILE   = {config.OUTPUT_FILE}")
#     print(f"       DOWNLOAD_DIR  = {config.DOWNLOAD_DIR}")

#     # quick sanity: abort if RUT/CLAVE empty (shouldn't happen given EMBEDDED_DEFAULTS)
#     if not getattr(config, "RUT", "") or not getattr(config, "CLAVE", ""):
#         print("[ERROR] RUT or CLAVE is empty after forced overrides. Aborting.")
#         print("        Set them with --rut/--clave or via environment variables or edit EMBEDDED_DEFAULTS in src/main.py.")
#         sys.exit(2)

#     # ensure output dirs exist
#     Path(config.DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)
#     Path(Path(config.OUTPUT_FILE).parent).mkdir(parents=True, exist_ok=True)

#     # Decide headless mode:
#     if args.headless:
#         headless = True
#     elif args.show_browser:
#         headless = False
#     else:
#         headless = False  # default to showing browser/webview

#     print(f"[INFO] Starting run. headless={headless}")

#     with sync_playwright() as p:
#         slow_mo = 50 if not headless else 0
#         browser = p.chromium.launch(headless=headless, slow_mo=slow_mo)
#         ctx = browser.new_context(accept_downloads=True)
#         page = ctx.new_page()

#         login_url = getattr(config, "START_URL", "https://servicios.dgi.gub.uy/serviciosenlinea")
#         print('[INFO] Navigating to', login_url)
#         try:
#             page.goto(login_url, wait_until='networkidle', timeout=60000)
#         except Exception as e:
#             print('[WARN] initial goto failed or timed out:', e)

#         # save quick debug screenshot on start
#         try:
#             Path('debug').mkdir(parents=True, exist_ok=True)
#             page.screenshot(path='debug/after_goto.png', full_page=True)
#             print('[INFO] Saved debug screenshot: debug/after_goto.png')
#         except Exception as e:
#             print('[WARN] Could not save screenshot:', e)

#         # perform login and navigate to Consulta de CFE recibidos
#         try:
#             final_page, final_url = auth.login_and_continue(page, post_click_wait=5, wait_for_selector=sel.SELECT_TIPO_CFE)
#             print('[INFO] Reached', final_url)
#         except ValueError:
#             print("[ERROR] Login appears to have failed (ValueError raised).")
#             print("[INFO] Look at debug/*.html and debug/*.png — especially the file created right after login.")
#             try:
#                 # keep browser open in headed mode so you can inspect
#                 if not headless:
#                     print("[INFO] Leaving browser open for inspection (headed). Press Ctrl+C to exit this script when done.")
#                     while True:
#                         time.sleep(1)
#                 else:
#                     browser.close()
#             except KeyboardInterrupt:
#                 pass
#             raise
#         except Exception as e:
#             print('[ERROR] login_and_continue failed:', e)
#             try:
#                 browser.close()
#             except Exception:
#                 pass
#             return

#         # Optionally fill the tipo/date and click consultar
#         try:
#             final_page, results_url = auth.fill_cfe_and_consult(
#                 final_page,
#                 tipo_value=config.ECF_TIPO,
#                 date_from=config.ECF_FROM_DATE,
#                 date_to=config.ECF_TO_DATE,
#                 wait_after_result=3
#             )
#             print('[INFO] Results page URL:', results_url)
#         except Exception as e:
#             print('[WARN] fill_cfe_and_consult failed:', e)

#         # Collect links from the results grid and extract fields
#         try:
#             link_selector = getattr(sel, "GRID_LINKS_SELECTOR", None)
#             parent_selector = getattr(sel, "GRID_PARENT_SELECTOR", None)
#             out = auth.collect_cfe_from_links(
#                 final_page,
#                 link_selector=link_selector,
#                 output_file=config.OUTPUT_FILE,
#                 parent_selector=parent_selector,
#                 do_post_action=False,
#                 max_pages=getattr(config, "MAX_PAGES", None)
#             )
#             print('[INFO] Extraction saved to:', out)
#         except Exception as e:
#             print('[ERROR] collect_cfe_from_links failed:', e)

#         # Optional post action (refill/next image)
#         try:
#             tipo = getattr(config, "ECF_TIPO", None)
#             d_from = getattr(config, "ECF_FROM_DATE", None)
#             d_to = getattr(config, "ECF_TO_DATE", None)
#             try:
#                 auth.go_to_consulta_and_click_next(final_page, tipo_value=tipo, date_from=d_from, date_to=d_to, wait_after_fill=2.0)
#             except Exception as e:
#                 print('[WARN] Post-collection navigation/click failed:', e)
#         except Exception:
#             pass

#         # If headed (webview shown), keep browser open so user can inspect
#         if not headless:
#             print("\n[INFO] Running in headed mode (webview visible).")
#             print("[INFO] The browser will remain open so you can inspect it. Press Ctrl+C to exit this script when you're done.")
#             try:
#                 while True:
#                     time.sleep(1)
#             except KeyboardInterrupt:
#                 print("\n[INFO] KeyboardInterrupt received. Closing browser and exiting.")
#                 try:
#                     browser.close()
#                 except Exception:
#                     pass
#                 return
#         else:
#             browser.close()
#             print('[INFO] Browser closed. Done')


# if __name__ == "__main__":
#     run()
# src/main.py
"""
Main runner for the efactura scraping job.

This refactored version **does not** contain embedded defaults for credentials/dates.
The caller (CLI or the Flask API) must provide RUT/CLAVE/ECF_TIPO/ECF_FROM_DATE/ECF_TO_DATE
via CLI args, environment variables, or by passing a cli_args-like object to run(...).

Usage:
    python -m src.main                # uses CLI args / environment
    python -m src.main --rut ...      # override via CLI
    Or call run(cli_args=SimpleNamespace(...), headless_forced=True) from other code.
"""

import argparse
import importlib
import os
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from datetime import datetime

from playwright.sync_api import sync_playwright

from src import auth
from src import selectors as sel


def parse_args():
    p = argparse.ArgumentParser(description="Run EFactura scraping job (main).")
    group = p.add_mutually_exclusive_group()
    group.add_argument("--headless", action="store_true", default=False,
                       help="Run in headless mode (no browser UI).")
    group.add_argument("--show-browser", action="store_true", default=False,
                       help="Show browser/webview (headed).")
    p.add_argument("--rut", type=str, help="Override RUT")
    p.add_argument("--clave", type=str, help="Override CLAVE")
    p.add_argument("--tipo", type=str, help="Override ECF_TIPO")
    p.add_argument("--from", dest="from_date", type=str, help="Override ECF_FROM_DATE (DD/MM/YYYY or other)")
    p.add_argument("--to", dest="to_date", type=str, help="Override ECF_TO_DATE (DD/MM/YYYY or other)")
    p.add_argument("--max-pages", dest="max_pages", type=int, default=None,
                   help="Optional: limit pages to fetch")
    return p.parse_args()


def build_and_apply_overrides(cli_args):
    """
    Build overrides dict with priority:
      CLI -> existing ENV -> no default (None)
    Then call src.config.override_from_dict(overrides) so config module and os.environ are updated.
    """
    import src.config as config
    importlib.reload(config)

    keys = ["RUT", "CLAVE", "ECF_TIPO", "ECF_FROM_DATE", "ECF_TO_DATE"]

    cli_map = {
        "RUT": getattr(cli_args, "rut", None),
        "CLAVE": getattr(cli_args, "clave", None),
        "ECF_TIPO": getattr(cli_args, "tipo", None),
        "ECF_FROM_DATE": getattr(cli_args, "from_date", None),
        "ECF_TO_DATE": getattr(cli_args, "to_date", None),
    }

    overrides = {}
    for k in keys:
        cli_val = cli_map.get(k)
        env_val = os.getenv(k)
        # NO embedded defaults here — must come from CLI or env or caller
        chosen = cli_val if cli_val is not None else (env_val if env_val is not None and env_val != "" else None)
        overrides[k] = str(chosen) if chosen is not None else None

    # include MAX_PAGES if provided on CLI-like object
    if getattr(cli_args, "max_pages", None) is not None:
        overrides["MAX_PAGES"] = cli_args.max_pages

    # Apply overrides into src.config and environment
    try:
        config.override_from_dict(overrides)
    except Exception:
        # fallback: write directly into os.environ and module globals
        for kk, vv in overrides.items():
            if vv is None:
                # remove from env if present
                if kk in os.environ:
                    try:
                        del os.environ[kk]
                    except Exception:
                        pass
                try:
                    delattr(config, kk)
                except Exception:
                    config.__dict__.pop(kk, None)
                continue
            os.environ[kk] = str(vv)
            try:
                setattr(config, kk, vv)
            except Exception:
                config.__dict__[kk] = vv

    # Ensure OUTPUT_FILE / DOWNLOAD_DIR defaults exist in config (if not provided by config)
    if not getattr(config, "OUTPUT_FILE", None):
        config.OUTPUT_FILE = os.path.join(getattr(config, "OUTPUT_DIR", "."), "results.xlsx")
    if not getattr(config, "DOWNLOAD_DIR", None):
        config.DOWNLOAD_DIR = os.path.join(".", "downloads")

    # reload to ensure consistency
    importlib.reload(config)
    return config


def mask_secret(s: str) -> str:
    if not s:
        return "(empty)"
    s = str(s)
    if len(s) <= 6:
        return "*" * len(s)
    return s[:3] + "*" * (len(s) - 6) + s[-3:]


def normalize_date_ddmmyyyy(s: str):
    """
    Accept either DD/MM/YYYY or YYYY-MM-DD and return DD/MM/YYYY.
    If none/empty -> return None.
    """
    if not s:
        return None
    s = s.strip()
    if "/" in s:
        return s
    # try ISO
    try:
        dt = datetime.fromisoformat(s)
        return dt.strftime("%d/%m/%Y")
    except Exception:
        # fallback: return original
        return s


def run(cli_args=None, headless_forced=None):
    """
    Run the scraping job.

    - cli_args: if None, CLI args will be parsed. Otherwise cli_args should be an object with:
        rut, clave, tipo, from_date, to_date, max_pages
    - headless_forced: if not None, it overrides headless selection (True/False).

    Returns: dict { ok: bool, output: path or None, error: str or None }
    """
    if cli_args is None:
        args = parse_args()
    else:
        args = cli_args

    # normalize date inputs (allow ISO from frontend)
    if getattr(args, "from_date", None):
        args.from_date = normalize_date_ddmmyyyy(args.from_date)
    if getattr(args, "to_date", None):
        args.to_date = normalize_date_ddmmyyyy(args.to_date)

    # Force overrides into src.config (and env)
    config = build_and_apply_overrides(args)

    # Print the final runtime config that will be used (mask sensitive)
    print("[INFO] Final runtime config being used:")
    print(f"       RUT           = {getattr(config, 'RUT', '') or '(empty)'}")
    print(f"       CLAVE         = {mask_secret(getattr(config, 'CLAVE', ''))}")
    print(f"       ECF_TIPO      = {getattr(config, 'ECF_TIPO', '') or '(empty)'}")
    print(f"       ECF_FROM_DATE = {getattr(config, 'ECF_FROM_DATE', '') or '(empty)'}")
    print(f"       ECF_TO_DATE   = {getattr(config, 'ECF_TO_DATE', '') or '(empty)'}")
    print(f"       OUTPUT_FILE   = {config.OUTPUT_FILE}")
    print(f"       DOWNLOAD_DIR  = {config.DOWNLOAD_DIR}")

    # quick sanity: abort if RUT/CLAVE empty (now they must be provided)
    if not getattr(config, "RUT", "") or not getattr(config, "CLAVE", ""):
        msg = ("RUT or CLAVE is empty after forced overrides. Aborting. "
               "Provide them via CLI, environment variables, or caller (frontend).")
        print("[ERROR] " + msg)
        return {"ok": False, "error": msg}

    # ensure output dirs exist
    Path(config.DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)
    Path(Path(config.OUTPUT_FILE).parent).mkdir(parents=True, exist_ok=True)

    # Decide headless mode:
    if headless_forced is not None:
        headless = bool(headless_forced)
    else:
        headless = getattr(args, "headless", False)
        if getattr(args, "show_browser", False):
            headless = False
        if not hasattr(args, "headless") and not hasattr(args, "show_browser"):
            headless = False

    print(f"[INFO] Starting run. headless={headless}")

    out_path = None
    try:
        with sync_playwright() as p:
            slow_mo = 50 if not headless else 0
            browser = p.chromium.launch(headless=headless, slow_mo=slow_mo)
            ctx = browser.new_context(accept_downloads=True)
            page = ctx.new_page()

            login_url = getattr(config, "START_URL", "https://servicios.dgi.gub.uy/serviciosenlinea")
            print('[INFO] Navigating to', login_url)
            try:
                page.goto(login_url, wait_until='networkidle', timeout=60000)
            except Exception as e:
                print('[WARN] initial goto failed or timed out:', e)

            try:
                Path('debug').mkdir(parents=True, exist_ok=True)
                page.screenshot(path='debug/after_goto.png', full_page=True)
                print('[INFO] Saved debug screenshot: debug/after_goto.png')
            except Exception as e:
                print('[WARN] Could not save screenshot:', e)

            # perform login and navigate to Consulta de CFE recibidos
            try:
                final_page, final_url = auth.login_and_continue(page, post_click_wait=5, wait_for_selector=sel.SELECT_TIPO_CFE)
                print('[INFO] Reached', final_url)
            except ValueError:
                msg = "Login appears to have failed (ValueError raised). Check debug files."
                print("[ERROR]", msg)
                try:
                    browser.close()
                except Exception:
                    pass
                return {"ok": False, "error": msg}
            except Exception as e:
                print('[ERROR] login_and_continue failed:', e)
                try:
                    browser.close()
                except Exception:
                    pass
                return {"ok": False, "error": str(e)}

            # Optionally fill the tipo/date and click consultar
            try:
                final_page, results_url = auth.fill_cfe_and_consult(
                    final_page,
                    tipo_value=config.ECF_TIPO,
                    date_from=config.ECF_FROM_DATE,
                    date_to=config.ECF_TO_DATE,
                    wait_after_result=3
                )
                print('[INFO] Results page URL:', results_url)
            except Exception as e:
                print('[WARN] fill_cfe_and_consult failed:', e)

            # Collect links from the results grid and extract fields
            try:
                link_selector = getattr(sel, "GRID_LINKS_SELECTOR", None)
                parent_selector = getattr(sel, "GRID_PARENT_SELECTOR", None)
                out = auth.collect_cfe_from_links(
                    final_page,
                    link_selector=link_selector,
                    output_file=config.OUTPUT_FILE,
                    parent_selector=parent_selector,
                    do_post_action=False,
                    max_pages=getattr(config, "MAX_PAGES", None)
                )
                out_path = out
                print('[INFO] Extraction saved to:', out)
            except Exception as e:
                print('[ERROR] collect_cfe_from_links failed:', e)

            # Optional post action
            try:
                tipo = getattr(config, "ECF_TIPO", None)
                d_from = getattr(config, "ECF_FROM_DATE", None)
                d_to = getattr(config, "ECF_TO_DATE", None)
                try:
                    auth.go_to_consulta_and_click_next(final_page, tipo_value=tipo, date_from=d_from, date_to=d_to, wait_after_fill=2.0)
                except Exception as e:
                    print('[WARN] Post-collection navigation/click failed:', e)
            except Exception:
                pass

            if not headless:
                print("\n[INFO] Running in headed mode (webview visible).")
                print("[INFO] The browser will remain open so you can inspect it. Press Ctrl+C to exit this script when you're done.")
                try:
                    while True:
                        time.sleep(1)
                except KeyboardInterrupt:
                    print("\n[INFO] KeyboardInterrupt received. Closing browser and exiting.")
                    try:
                        browser.close()
                    except Exception:
                        pass
                    return {"ok": True, "output": out_path}
            else:
                browser.close()
                print('[INFO] Browser closed. Done')

        return {"ok": True, "output": out_path}
    except Exception as e:
        print("[ERROR] Unexpected failure during run:", e)
        return {"ok": False, "error": str(e)}


if __name__ == "__main__":
    res = run()
    if not res.get("ok", False):
        print("ERROR:", res.get("error"))
        sys.exit(1)
    print("Output:", res.get("output"))
