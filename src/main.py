# src/main.py — updated to always run headless and not pause
import time
from pathlib import Path
from playwright.sync_api import sync_playwright
import importlib
import os

from src import auth
from src import selectors as sel

def run():
    """
    Run the Playwright scraping job. Always runs headless (no UI) and does not pause.
    Raises ValueError for login failures propagated from auth.login_and_continue.

    Only the following environment/override keys are honored (if present):
      - RUT, CLAVE, ECF_TIPO, ECF_FROM_DATE, ECF_TO_DATE
    """
    # reload config module so runtime overrides are picked up
    import src.config as config
    importlib.reload(config)

    # Only apply these five keys from environment (if provided).
    allowed_override_keys = ['RUT', 'CLAVE', 'ECF_TIPO', 'ECF_FROM_DATE', 'ECF_TO_DATE']
    for k in allowed_override_keys:
        v = os.getenv(k)
        if v is not None:
            try:
                setattr(config, k, v)
            except Exception:
                pass

    Path(config.DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)
    Path(Path(config.OUTPUT_FILE).parent).mkdir(parents=True, exist_ok=True)

    # force headless True to prevent visible browser
    headless = True

    print(f"[INFO] Launching browser (headless={headless})")
    with sync_playwright() as p:
        slow_mo = 0
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
            # login failure (auth raised ValueError) - propagate to caller
            raise
        except Exception as e:
            print('[ERROR] login_and_continue failed:', e)
            try:
                browser.close()
            except Exception:
                pass
            return

        # Optionally fill the tipo/date and click consultar (keeps behavior as before)
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


        # Collect links from the results grid and extract fields (this now does in-place pagination & saving)
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

        browser.close()
        print('[INFO] Browser closed. Done')