# src/main.py
"""
Main runner for the efactura scraping job.

Usage examples:
    python -m src.main                 # show the browser (webview) — default
    python -m src.main --show-browser  # explicit: show webview
    python -m src.main --headless      # run headless (no UI)
    python -m src.main --rut 213...    # override RUT via CLI
    You can also set environment variables RUT, CLAVE, ECF_TIPO, ECF_FROM_DATE, ECF_TO_DATE
"""

import argparse
import importlib
import os
import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

from src import auth
from src import selectors as sel

# --- Embedded defaults you gave (will be forced into config if not provided) ---
EMBEDDED_DEFAULTS = {
    "RUT": "213624850018",
    "CLAVE": "aa0000aa",
    "ECF_TIPO": "111",
    "ECF_FROM_DATE": "01/07/2025",
    "ECF_TO_DATE": "20/07/2025",
}


def parse_args():
    p = argparse.ArgumentParser(description="Run EFactura scraping job (main).")
    group = p.add_mutually_exclusive_group()
    group.add_argument("--headless", action="store_true", default=False,
                       help="Run in headless mode (no browser UI).")
    group.add_argument("--show-browser", action="store_true", default=False,
                       help="Show browser/webview (headed). This is the default when no flag is provided.")
    p.add_argument("--rut", type=str, help="Override RUT")
    p.add_argument("--clave", type=str, help="Override CLAVE")
    p.add_argument("--tipo", type=str, help="Override ECF_TIPO")
    p.add_argument("--from", dest="from_date", type=str, help="Override ECF_FROM_DATE")
    p.add_argument("--to", dest="to_date", type=str, help="Override ECF_TO_DATE")
    p.add_argument("--max-pages", dest="max_pages", type=int, default=None,
                   help="Optional: limit pages to fetch")
    return p.parse_args()


def build_and_apply_overrides(cli_args):
    """
    Build overrides dict with priority CLI -> existing ENV -> EMBEDDED_DEFAULTS.
    Then call src.config.override_from_dict(overrides) so config module and os.environ are updated.
    """
    import src.config as config
    importlib.reload(config)

    keys = ["RUT", "CLAVE", "ECF_TIPO", "ECF_FROM_DATE", "ECF_TO_DATE"]

    cli_map = {
        "RUT": cli_args.rut,
        "CLAVE": cli_args.clave,
        "ECF_TIPO": cli_args.tipo,
        "ECF_FROM_DATE": cli_args.from_date,
        "ECF_TO_DATE": cli_args.to_date,
    }

    overrides = {}
    for k in keys:
        cli_val = cli_map.get(k)
        env_val = os.getenv(k)
        default_val = EMBEDDED_DEFAULTS.get(k)
        chosen = cli_val if cli_val is not None else (env_val if env_val is not None and env_val != "" else default_val)
        # ensure chosen is string or empty string
        overrides[k] = str(chosen) if chosen is not None else ""

    # include MAX_PAGES if provided on CLI
    if cli_args.max_pages is not None:
        overrides["MAX_PAGES"] = cli_args.max_pages

    # Apply overrides into src.config and environment
    try:
        config.override_from_dict(overrides)
    except Exception:
        # fallback: write directly into os.environ and module globals
        for kk, vv in overrides.items():
            if vv is None:
                vv = ""
            os.environ[kk] = str(vv)
            try:
                setattr(config, kk, vv)
            except Exception:
                config.__dict__[kk] = vv

    # Ensure OUTPUT_FILE / DOWNLOAD_DIR defaults exist in config
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


def run():
    args = parse_args()

    # Force overrides into src.config (and env)
    config = build_and_apply_overrides(args)

    # Print the final runtime config that will be used
    print("[INFO] Final runtime config being used:")
    print(f"       RUT           = {getattr(config, 'RUT', '') or '(empty)'}")
    print(f"       CLAVE         = {mask_secret(getattr(config, 'CLAVE', ''))}")
    print(f"       ECF_TIPO      = {getattr(config, 'ECF_TIPO', '') or '(empty)'}")
    print(f"       ECF_FROM_DATE = {getattr(config, 'ECF_FROM_DATE', '') or '(empty)'}")
    print(f"       ECF_TO_DATE   = {getattr(config, 'ECF_TO_DATE', '') or '(empty)'}")
    print(f"       OUTPUT_FILE   = {config.OUTPUT_FILE}")
    print(f"       DOWNLOAD_DIR  = {config.DOWNLOAD_DIR}")

    # quick sanity: abort if RUT/CLAVE empty (shouldn't happen given EMBEDDED_DEFAULTS)
    if not getattr(config, "RUT", "") or not getattr(config, "CLAVE", ""):
        print("[ERROR] RUT or CLAVE is empty after forced overrides. Aborting.")
        print("        Set them with --rut/--clave or via environment variables or edit EMBEDDED_DEFAULTS in src/main.py.")
        sys.exit(2)

    # ensure output dirs exist
    Path(config.DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)
    Path(Path(config.OUTPUT_FILE).parent).mkdir(parents=True, exist_ok=True)

    # Decide headless mode:
    if args.headless:
        headless = True
    elif args.show_browser:
        headless = False
    else:
        headless = False  # default to showing browser/webview

    print(f"[INFO] Starting run. headless={headless}")

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

        # save quick debug screenshot on start
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
            print("[ERROR] Login appears to have failed (ValueError raised).")
            print("[INFO] Look at debug/*.html and debug/*.png — especially the file created right after login.")
            try:
                # keep browser open in headed mode so you can inspect
                if not headless:
                    print("[INFO] Leaving browser open for inspection (headed). Press Ctrl+C to exit this script when done.")
                    while True:
                        time.sleep(1)
                else:
                    browser.close()
            except KeyboardInterrupt:
                pass
            raise
        except Exception as e:
            print('[ERROR] login_and_continue failed:', e)
            try:
                browser.close()
            except Exception:
                pass
            return

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
            print('[INFO] Extraction saved to:', out)
        except Exception as e:
            print('[ERROR] collect_cfe_from_links failed:', e)

        # Optional post action (refill/next image)
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

        # If headed (webview shown), keep browser open so user can inspect
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
                return
        else:
            browser.close()
            print('[INFO] Browser closed. Done')


if __name__ == "__main__":
    run()
# src/main.py
# """
# Main runner for the efactura scraping job.

# Usage examples:
#     python -m src.main                 # show the browser (webview) — default
#     python -m src.main --show-browser  # explicit: show webview
#     python -m src.main --headless      # run headless (no UI)
#     python -m src.main --rut 213...    # override RUT via CLI
# You can also set environment variables RUT, CLAVE, ECF_TIPO, ECF_FROM_DATE, ECF_TO_DATE
# Note: Frontend (Flask) will push RUT/CLAVE/ECF_FROM_DATE/ECF_TO_DATE into src.config before calling run().
# Only ECF_TIPO remains as an embedded default here.
# """

# import argparse
# import importlib
# import os
# import sys
# import time
# from pathlib import Path
# from datetime import datetime

# from playwright.sync_api import sync_playwright

# from src import auth
# from src import selectors as sel

# # --- Embedded defaults: only ECF_TIPO left as frontend will provide RUT/CLAVE/dates ---
# EMBEDDED_DEFAULTS = {
#     "ECF_TIPO": "111",
# }


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
#     Build overrides dict with priority CLI -> existing ENV -> config module -> EMBEDDED_DEFAULTS.
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
#         # in-memory module value
#         try:
#             module_val = getattr(config, k, None)
#         except Exception:
#             module_val = None
#         default_val = EMBEDDED_DEFAULTS.get(k)
#         # Priority: CLI -> ENV -> module -> EMBEDDED_DEFAULTS -> empty
#         if cli_val is not None:
#             chosen = cli_val
#         elif env_val is not None and env_val != "":
#             chosen = env_val
#         elif module_val not in (None, ""):
#             chosen = module_val
#         elif default_val is not None:
#             chosen = default_val
#         else:
#             chosen = ""
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

#     # quick sanity: abort if RUT/CLAVE empty (frontend should supply these)
#     if not getattr(config, "RUT", "") or not getattr(config, "CLAVE", ""):
#         print("[ERROR] RUT or CLAVE is empty after forced overrides. Aborting.")
#         print("        Provide RUT and CLAVE via CLI (--rut/--clave), environment variables, or via the frontend upload/form.")
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
#         headless_env = os.getenv("HEADLESS", None)
#         if headless_env is None:
#             headless_env = getattr(config, "HEADLESS", None)
#         headless = str(headless_env).lower() in ("1", "true", "yes")

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
#                 tipo_value=getattr(config, "ECF_TIPO", None),
#                 date_from=getattr(config, "ECF_FROM_DATE", None),
#                 date_to=getattr(config, "ECF_TO_DATE", None),
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
#                 do_post_action=True,
#                 max_pages=getattr(config, "MAX_PAGES", None)
#             )
#             print('[INFO] Extraction saved to:', out)
#         except Exception as e:
#             print('[ERROR] collect_cfe_from_links failed:', e)

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
