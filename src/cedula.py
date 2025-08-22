# src/auth.py
"""
Authentication helpers for DGI / idUruguay flows.

Contains:
- existing `login_and_continue` (RUT-based) flow (keeps original behavior)
- new `login_with_cedula_direct` flow that navigates directly to the idUruguay login
  page and performs Usuario Gub.uy (cédula + password) authentication, then returns
  to DGI and selects the entity + clicks "Continuar".

Drop this file into `src/auth.py`. You can call `login_with_cedula_direct(page, cedula, password)`
from your runner (e.g. src/main.py). The file includes robust helpers that search frames
and attempt shadow-DOM/js fallbacks when needed. Debug HTML/PNG dumps are written on failure.
"""

import time
import traceback
import os
import urllib.parse
import csv
from typing import Optional, Tuple
from pathlib import Path
from playwright.sync_api import TimeoutError, Error

from src import selectors as sel
from src import config

# ---------------------------
# Debug / wait helpers
# ---------------------------
def _dump_debug(page, prefix="debug"):
    """Save screenshot + HTML for debugging (best-effort)."""
    try:
        ts = int(time.time())
        out_png = f"{prefix}_{ts}.png"
        out_html = f"{prefix}_{ts}.html"
        try:
            page.screenshot(path=out_png, full_page=True)
        except Exception:
            pass
        try:
            with open(out_html, "w", encoding="utf-8") as f:
                f.write(page.content())
        except Exception:
            pass
        print(f"[DEBUG] Saved debug files: {out_png} and {out_html}")
    except Exception as e:
        print("[DEBUG] Could not save debug files:", e)


def _wait_for_url_contains(page, substring, timeout=60):
    """Wait until page.url contains substring (searches current page only)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            url = getattr(page, "url", "") or ""
        except Exception:
            url = ""
        if substring in url:
            return True
        time.sleep(0.5)
    return False

# ---------------------------
# URL helpers & CSV persistence
# ---------------------------
def _canonicalize_url(url: str) -> str:
    """Normalize a URL for deduplication. Removes fragment and trailing slash."""
    if not url:
        return ""
    try:
        p = urllib.parse.urlparse(url)
        path = urllib.parse.urljoin('/', p.path).rstrip('/')
        canon = urllib.parse.urlunparse((p.scheme.lower(), p.netloc.lower(), path, "", p.query or "", ""))
        return canon
    except Exception:
        return url.strip()


def _append_row_to_csv(csv_path: str, row: dict, fieldnames: list):
    """Append a row to CSV (creates file + header if missing)."""
    write_header = not os.path.exists(csv_path)
    out_dir = os.path.dirname(csv_path) or "."
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow(row)

# ---------------------------
# Generic page/frame click helper
# ---------------------------
def _click_in_page_or_frames(page, selector, timeout=20) -> bool:
    """
    Try to click a selector on the page or within any frame.
    Returns True on success.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            el = page.query_selector(selector)
            if el:
                try:
                    el.scroll_into_view_if_needed()
                except Exception:
                    pass
                el.click()
                return True
        except Exception:
            pass
        try:
            for f in page.frames:
                try:
                    fel = f.query_selector(selector)
                    if fel:
                        try:
                            fel.scroll_into_view_if_needed()
                        except Exception:
                            pass
                        fel.click()
                        return True
                except Exception:
                    continue
        except Exception:
            pass
        time.sleep(0.5)
    return False

# ---------------------------
# Find the Continue element used in existing RUT flow
# ---------------------------
def _find_continue_element(page, timeout=30):
    """Search for the configured CONTINUE_BUTTON selector on page / frames."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            el = page.query_selector(sel.CONTINUE_BUTTON)
            if el:
                try:
                    el.scroll_into_view_if_needed()
                except Exception:
                    pass
                return el
        except Exception:
            pass
        try:
            for frame in page.frames:
                try:
                    fel = frame.query_selector(sel.CONTINUE_BUTTON)
                    if fel:
                        try:
                            fel.scroll_into_view_if_needed()
                        except Exception:
                            pass
                        return fel
                except Exception:
                    continue
        except Exception:
            pass
        time.sleep(0.5)
    return None

# ---------------------------
# Robust password setter (supports frame + shadow DOM + JS fallback)
# ---------------------------
def _set_password_in_page(page_or_frame, password: str) -> Tuple[bool, str]:
    """
    Try multiple strategies to set password input value inside page_or_frame
    (which can be a Page or a Frame). Returns (True, message) on success.
    """
    try:
        common_selectors = [
            '#password',
            'input[type="password"]',
            'input[id*="pass"]',
            'input[name*="pass"]',
            'input[aria-label*="contrase"]',
            'input[placeholder*="contrase"]',
            'input[aria-label*="password"]',
            'input[placeholder*="Password"]'
        ]
        for sel in common_selectors:
            try:
                if page_or_frame.query_selector(sel):
                    page_or_frame.fill(sel, str(password))
                    return True, f"filled by selector {sel}"
            except Exception:
                pass

        # wait briefly for any password input then fill
        try:
            el = page_or_frame.wait_for_selector('input[type="password"]', timeout=3000)
            if el:
                page_or_frame.fill('input[type="password"]', str(password))
                return True, "filled input[type=password] after wait"
        except Exception:
            pass

        # JS: search document + shadow roots and set the value
        js = r"""
        (pwd) => {
            function findAndSet(root) {
                const selectors = [
                    'input[type="password"]',
                    'input[id*="pass" i]',
                    'input[name*="pass" i]',
                    'input[aria-label*="contrase" i]',
                    'input[placeholder*="contrase" i]',
                    'input[aria-label*="password" i]',
                    'input[placeholder*="password" i]'
                ];
                for (let sel of selectors) {
                    try {
                        const el = root.querySelector(sel);
                        if (el) {
                            el.focus && el.focus();
                            el.value = pwd;
                            el.dispatchEvent(new Event('input', { bubbles: true }));
                            el.dispatchEvent(new Event('change', { bubbles: true }));
                            return {ok: true, how: sel, outer: el.outerHTML.slice(0,500)};
                        }
                    } catch (e) {}
                }
                const walker = document.createTreeWalker(root, NodeFilter.SHOW_ELEMENT, null, false);
                let node;
                while (node = walker.nextNode()) {
                    try {
                        if (node.shadowRoot) {
                            const res = findAndSet(node.shadowRoot);
                            if (res && res.ok) return res;
                        }
                    } catch (e) {}
                }
                return {ok: false};
            }
            try {
                return findAndSet(document);
            } catch (e) {
                return {ok:false, err: String(e)};
            }
        }
        """
        try:
            res = page_or_frame.evaluate(js, password)
            if isinstance(res, dict) and res.get("ok"):
                how = res.get("how", "js")
                outer = res.get("outer", "")[:300]
                return True, f"set by JS ({how}), outer={outer!s}"
        except Exception:
            pass

        return False, "not found"
    except Exception as e:
        return False, f"exception: {e}"


# ---------------------------
# Existing RUT flow (unchanged logic; kept for compatibility)
# ---------------------------
def login_and_continue(page, post_click_wait: int = 5, wait_for_selector: Optional[str] = None) -> Tuple[object, str]:
    """
    Existing login flow (RUT-based). Kept intact so existing users continue to work.
    Returns (final_page, final_url).
    """
    try:
        print("[INFO] Waiting for initial page load (networkidle)...")
        try:
            page.wait_for_load_state("networkidle", timeout=60000)
        except Exception:
            try:
                page.wait_for_load_state("load", timeout=30000)
            except Exception:
                print("[WARN] initial load didn't reach networkidle/load - continuing")

        target = None
        try:
            print("[INFO] Looking for username input on main page...")
            page.wait_for_selector(sel.USERNAME_INPUT, timeout=15000)
            target = page
            print("[INFO] Found main page login inputs.")
        except TimeoutError:
            print("[INFO] Main page inputs not found; trying iframe...")
            iframe_el = page.query_selector('iframe[src*="loginProd"]') or page.query_selector("iframe")
            if iframe_el:
                frame = iframe_el.content_frame()
                if frame:
                    target = frame
                    print("[INFO] Using iframe as target for login.")
            if not target:
                raise Exception("Login inputs not found on main page or in iframe.")

        print("[INFO] Filling username...")
        target.fill(sel.USERNAME_INPUT, str(config.RUT))
        print("[INFO] Filling password...")
        target.fill(sel.PASSWORD_INPUT, str(config.CLAVE))

        print("[INFO] Clicking login button...")
        try:
            if target.query_selector(sel.LOGIN_BUTTON_IMG):
                target.click(sel.LOGIN_BUTTON_IMG)
            elif target.query_selector('input[type="submit"]'):
                target.click('input[type="submit"]')
            elif target.query_selector('button[type="submit"]'):
                target.click('button[type="submit"]')
            else:
                try:
                    target.click('button:has-text("Ingresar")')
                except Exception:
                    try:
                        target.press(sel.PASSWORD_INPUT, "Enter")
                    except Exception:
                        pass
        except Exception:
            try:
                target.press(sel.PASSWORD_INPUT, "Enter")
            except Exception:
                pass

        time.sleep(1.5)

        # Heuristic: check if login form remains visible -> likely bad credentials
        still_has_input = False
        try:
            if page.query_selector(sel.USERNAME_INPUT):
                still_has_input = True
            else:
                for f in page.frames:
                    try:
                        if f.query_selector(sel.USERNAME_INPUT):
                            still_has_input = True
                            break
                    except Exception:
                        continue
        except Exception:
            still_has_input = False

        def _page_has_auth_error(p):
            try:
                text = ""
                try:
                    text = p.inner_text()[:2000].lower()
                except Exception:
                    try:
                        text = p.evaluate("() => document.body ? document.body.innerText : ''") or ""
                    except Exception:
                        text = ""
                if not text:
                    return False
                patterns = [
                    'clave incorrecta', 'usuario o clave', 'usuario incorrecto', 'credencial',
                    'authentication failed', 'login failed', 'wrong user', 'no autorizado', 'usuario no encontrado'
                ]
                for pat in patterns:
                    if pat in text:
                        return True
            except Exception:
                pass
            return False

        err_detected = _page_has_auth_error(page)
        if not err_detected:
            for f in page.frames:
                try:
                    if _page_has_auth_error(f):
                        err_detected = True
                        break
                except Exception:
                    continue

        if err_detected or still_has_input:
            _dump_debug(page)
            raise ValueError("Login appears to have failed — check RUT/CLAVE (login form still present or error message detected).")

        print("[INFO] Waiting for 'selecciona-entidad' in URL (up to 60s)...")
        reached = _wait_for_url_contains(page, "selecciona-entidad", timeout=60)
        print(f"[DEBUG] URL after login attempt: {getattr(page, 'url', '')}")
        if not reached:
            print("[WARN] 'selecciona-entidad' not observed; will still search for Continue button.")

        cont_el = _find_continue_element(page, timeout=30)
        if not cont_el:
            print("[WARN] Continue button not found. Dumping debug and returning current page.")
            _dump_debug(page)
            return page, getattr(page, "url", "")

        final_page = page
        final_url = getattr(page, "url", "")

        print("[INFO] Clicking Continue...")
        try:
            with page.context.expect_page(timeout=5000) as new_page_ctx:
                cont_el.click()
            new_page = new_page_ctx.value
            try:
                new_page.wait_for_load_state("load", timeout=30000)
            except Exception:
                try:
                    new_page.wait_for_load_state("networkidle", timeout=30000)
                except Exception:
                    pass
            final_page = new_page
            final_url = getattr(new_page, "url", "")
            print("[INFO] Landed on new tab after Continue:", final_url)
        except TimeoutError:
            print("[DEBUG] No new tab; waiting for same-page navigation...")
            try:
                page.wait_for_navigation(timeout=30000)
                final_page = page
                final_url = getattr(page, "url", "")
            except Exception:
                try:
                    page.wait_for_load_state("networkidle", timeout=30000)
                except Exception:
                    pass
                final_page = page
                final_url = getattr(page, "url", "")
            print("[INFO] After Continue (same page):", final_url)

        if wait_for_selector:
            try:
                final_page.wait_for_selector(wait_for_selector, timeout=post_click_wait * 1000)
            except Exception:
                print("[WARN] wait_for_selector did not appear in time.")

        # Now click 'Consulta de CFE recibidos'
        print("[INFO] Clicking 'Consulta de CFE recibidos' ...")
        try:
            with final_page.expect_navigation(timeout=30000):
                final_page.click('text="Consulta de CFE recibidos"')
            final_url = getattr(final_page, "url", "")
            print("[INFO] Landed on Consulta de CFE recibidos:", final_url)
        except Exception:
            # try frames fallback
            try:
                for f in final_page.frames:
                    try:
                        if f.query_selector('text="Consulta de CFE recibidos"'):
                            f.click('text="Consulta de CFE recibidos"')
                            time.sleep(2)
                            break
                    except Exception:
                        continue
            except Exception:
                pass

        time.sleep(post_click_wait)
        return final_page, final_url

    except ValueError:
        raise
    except Error as e:
        print("[ERROR] Playwright Error:", e)
        traceback.print_exc()
        _dump_debug(page)
        raise
    except Exception as e:
        print("[ERROR] Exception:", e)
        traceback.print_exc()
        _dump_debug(page)
        raise

# ---------------------------
# New direct idUruguay (cédula) flow
# ---------------------------
def login_with_cedula_direct(page, cedula: str, password: str, post_click_wait: int = 5, wait_for_selector: Optional[str] = None) -> Tuple[object, str]:
    """
    Navigate straight to the idUruguay login page and perform cédula/password
    authentication via 'Usuario Gub.uy', then return to DGI and click through entity selection.

    Steps:
      1. page.goto("https://mi.iduruguay.gub.uy/login")
      2. click 'Usuario Gub.uy'
      3. fill '#username' (cedula) and click continuar
      4. fill '#password' (password) robustly and click continuar
      5. wait for redirect back to DGI 'selecciona-entidad'
      6. click the DGI 'Ingresar' image (select2.png) and the 'Continuar' btn
      7. return (final_page, final_url)
    """
    try:
        print("[INFO] Starting direct iduruguay login flow...")
        id_url = "https://mi.iduruguay.gub.uy/login"
        page.goto(id_url, timeout=60000)
        try:
            page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass

        # Click Usuario Gub.uy option
        print("[INFO] Selecting 'Usuario Gub.uy' option...")
        if not _click_in_page_or_frames(page, 'button[aria-label="Usuario Gub.uy"]', timeout=8):
            _click_in_page_or_frames(page, 'text="Usuario Gub.uy"', timeout=5)
        time.sleep(0.6)

        # Fill username
        print(f"[INFO] Filling username (cedula): {cedula}")
        filled = False
        username_selectors = ['#username', 'input[aria-label*="cédula" i]', 'input[placeholder*="Ej." i]']
        for sel_usr in username_selectors:
            try:
                if page.query_selector(sel_usr):
                    page.fill(sel_usr, str(cedula))
                    filled = True
                    break
            except Exception:
                pass
        if not filled:
            for f in page.frames:
                try:
                    for sel_usr in username_selectors:
                        if f.query_selector(sel_usr):
                            f.fill(sel_usr, str(cedula))
                            filled = True
                            break
                    if filled:
                        break
                except Exception:
                    continue

        if not filled:
            _dump_debug(page)
            raise ValueError("Could not find username input for cédula on iduruguay page (direct).")

        # Click Continuar after username
        print("[INFO] Clicking Continuar after username...")
        if not _click_in_page_or_frames(page, 'button[aria-label="Continuar"]', timeout=8):
            if not _click_in_page_or_frames(page, 'button:has-text("Continuar")', timeout=4):
                _dump_debug(page)
                raise Exception("Continue button not found after username on iduruguay (direct).")

        # Wait for password step to load
        time.sleep(1.0)

        # Fill password robustly
        print("[INFO] Trying to set password...")
        pwd_set = False
        ok, msg = _set_password_in_page(page, password)
        if ok:
            pwd_set = True
            print("[DEBUG] password set on main page:", msg)
        else:
            for f in page.frames:
                try:
                    okf, msgf = _set_password_in_page(f, password)
                    if okf:
                        pwd_set = True
                        print("[DEBUG] password set in frame:", msgf)
                        break
                except Exception:
                    continue

        if not pwd_set:
            _dump_debug(page)
            raise ValueError("Could not find password input on iduruguay page (direct).")

        # Click Continuar after password
        print("[INFO] Clicking Continuar after password...")
        if not _click_in_page_or_frames(page, 'button[aria-label="Continuar"]', timeout=8):
            if not _click_in_page_or_frames(page, 'button:has-text("Continuar")', timeout=4):
                _dump_debug(page)
                raise Exception("Continue button after password not found on iduruguay (direct).")

        # Wait for redirect back to DGI (select entity)
        print("[INFO] Waiting for redirect back to DGI (selecciona-entidad)...")
        reached = _wait_for_url_contains(page, "selecciona-entidad", timeout=30)
        if not reached:
            for p in list(page.context.pages):
                try:
                    if "selecciona-entidad" in getattr(p, "url", ""):
                        page = p
                        reached = True
                        break
                except Exception:
                    continue
        time.sleep(1.5)

        # Click the DGI "Ingresar" image input (select2.png)
        print("[INFO] Clicking DGI entity 'Ingresar' image (select2.png)...")
        try:
            if not _click_in_page_or_frames(page, 'input[type="image"][src*="select2.png"]', timeout=8):
                if page.query_selector('#W0011vINGRESAR_0003'):
                    page.click('#W0011vINGRESAR_0003')
                else:
                    img_inputs = page.query_selector_all('input[type="image"]')
                    if img_inputs:
                        img_inputs[0].click()
        except Exception:
            _dump_debug(page)
            print("[WARN] Could not click specific 'Ingresar' image. Continuing (maybe already selected).")

        time.sleep(1.5)

        # Click DGI "Continuar"
        print("[INFO] Clicking DGI 'Continuar' button...")
        try:
            if not _click_in_page_or_frames(page, 'input[name="CONTINUAR"]', timeout=8):
                if not _click_in_page_or_frames(page, 'input[value="Continuar"]', timeout=4):
                    if page.query_selector('input[title="Continuar"]'):
                        page.click('input[title="Continuar"]')
                    else:
                        _click_in_page_or_frames(page, 'button:has-text("Continuar")', timeout=4)
        except Exception:
            _dump_debug(page)
            raise

        # final wait for stable load
        try:
            page.wait_for_load_state("networkidle", timeout=20000)
        except Exception:
            pass

        if wait_for_selector:
            try:
                page.wait_for_selector(wait_for_selector, timeout=post_click_wait * 1000)
            except Exception:
                print("[WARN] wait_for_selector after DGI continue did not appear in time.")

        final_page = page
        final_url = getattr(page, "url", "")
        print("[INFO] Direct iduruguay flow completed. Final URL:", final_url)
        return final_page, final_url

    except ValueError:
        raise
    except Error as e:
        print("[ERROR] Playwright Error during direct cedula flow:", e)
        traceback.print_exc()
        _dump_debug(page)
        raise
    except Exception as e:
        print("[ERROR] Exception during direct cedula flow:", e)
        traceback.print_exc()
        _dump_debug(page)
        raise
