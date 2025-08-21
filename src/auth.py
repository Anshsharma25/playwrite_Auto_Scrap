# src/auth.py
# Full updated auth module with robust download handling (page.expect_download preferred,
# threaded fallback using context.wait_for_event), plus the requested output folder/filename structure.

import time
import traceback
import os
import re
import urllib.parse
import csv
import shutil
import threading
from datetime import datetime
from typing import Optional, Tuple, List
import pandas as pd
from pathlib import Path
from playwright.sync_api import TimeoutError, Error

from src import selectors as sel
from src import config

# ---------------------------
# Debug / wait helpers
# ---------------------------
def _dump_debug(page, prefix="debug"):
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
# URL helpers & incremental persistence
# ---------------------------
def _canonicalize_url(url: str) -> str:
    """Normalize a URL for deduplication. Removes fragment, normalizes scheme/netloc casing and strips trailing slash."""
    if not url:
        return ""
    try:
        p = urllib.parse.urlparse(url)
        path = urllib.parse.urljoin('/', p.path)  # normalizes path
        path = path.rstrip('/')
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
# Login / Continue helpers
# ---------------------------
def _find_continue_element(page, timeout=30):
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


def login_and_continue(page, post_click_wait: int = 5, wait_for_selector: Optional[str] = None) -> Tuple[object, str]:
    """
    Login to the site, press Continue and then click the 'Consulta de CFE recibidos' entry.
    Returns (final_page, final_url).
    Raises ValueError on likely login failure (keeps behavior for caller to surface).
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
            # give slightly longer time for login input to appear
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
            # try pressing Enter as fallback
            try:
                target.press(sel.PASSWORD_INPUT, "Enter")
            except Exception:
                pass

        # short pause to allow any inline login error to appear
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

        # Also check page text for common failure phrases
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
        # re-raise to let caller display message
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
# Fill filters / Consult
# ---------------------------
def _click_maybe_in_frames(page, selector, timeout=2000):
    try:
        page.click(selector, timeout=timeout)
        return True
    except Exception:
        for frame in page.frames:
            try:
                frame.click(selector, timeout=timeout)
                return True
            except Exception:
                continue
    return False


def _find_element_in_page_and_frames(page, selector, timeout=5000):
    deadline = time.time() + (timeout / 1000)
    while time.time() < deadline:
        try:
            el = page.query_selector(selector)
            if el:
                return page, el
        except Exception:
            pass
        try:
            for frame in page.frames:
                try:
                    el = frame.query_selector(selector)
                    if el:
                        return frame, el
                except Exception:
                    continue
        except Exception:
            pass
        time.sleep(0.2)
    return None, None


def _set_select_value(frame_or_page, element_handle, value):
    try:
        frame_or_page.select_option(sel.SELECT_TIPO_CFE, value)
        return True
    except Exception:
        pass
    try:
        element_handle.evaluate(
            """(el, val) => {
                el.value = val;
                el.dispatchEvent(new Event('input',{bubbles:true}));
                el.dispatchEvent(new Event('change',{bubbles:true}));
                el.dispatchEvent(new Event('blur',{bubbles:true}));
                try{ if(window.gx && gx.evt && typeof gx.evt.onchange === 'function') gx.evt.onchange(el);}catch(e){}
                return true;
            }""",
            value
        )
        return True
    except Exception:
        pass
    try:
        element_handle.click()
        frame_or_page.click(f'{sel.SELECT_TIPO_CFE} >> option[value="{value}"]', timeout=2000)
        return True
    except Exception:
        pass
    return False


def _set_input_value_with_fallback(frame_or_page, element_handle, value):
    try:
        element_handle.evaluate(
            """(el, val) => {
                try{ el.focus && el.focus(); }catch(e){}
                el.value = val;
                el.dispatchEvent(new Event('input',{bubbles:true}));
                el.dispatchEvent(new Event('change',{bubbles:true}));
                el.dispatchEvent(new Event('blur',{bubbles:true}));
                try{ if(window.gx && gx.evt && typeof gx.evt.onchange === 'function') gx.evt.onchange(el); }catch(e){}
                try{ if(window.gx && gx.date && typeof gx.date.valid_date === 'function') { try{ gx.date.valid_date(el,10,'DMY',0,24,'spa',false,0);}catch(e){} } }catch(e){}
                return true;
            }""",
            value
        )
        return True
    except Exception:
        pass

    try:
        element_handle.click(timeout=2000)
        time.sleep(0.1)
        for ch in value:
            element_handle.type(ch, delay=60)
        try:
            element_handle.evaluate("(el) => { el.dispatchEvent(new Event('blur',{bubbles:true})); }")
        except Exception:
            pass
        return True
    except Exception:
        pass
    return False


def fill_cfe_and_consult(
    page,
    tipo_value: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    wait_after_result: int = 3
) -> Tuple[object, str]:
    """
    Set filters on the Consulta page and click Consultar. Returns (page, url).
    """
    try:
        tipo = tipo_value or getattr(config, "ECF_TIPO", "111")
        d_from = date_from or getattr(config, "ECF_FROM_DATE", "")
        d_to = date_to or getattr(config, "ECF_TO_DATE", "")

        print(f"[INFO] fill_cfe_and_consult: tipo={tipo}, desde={d_from}, hasta={d_to}")

        frame, el = _find_element_in_page_and_frames(page, sel.SELECT_TIPO_CFE, timeout=5000)
        if el:
            if not _set_select_value(frame, el, tipo):
                print("[WARN] Could not set tipo select by any method.")
        else:
            print("[WARN] SELECT_TIPO_CFE not found.")

        if d_from:
            f_from, el_from = _find_element_in_page_and_frames(page, sel.DATE_FROM, timeout=5000)
            if el_from:
                _set_input_value_with_fallback(f_from, el_from, d_from)
            else:
                print("[WARN] DATE_FROM not found.")

        if d_to:
            f_to, el_to = _find_element_in_page_and_frames(page, sel.DATE_TO, timeout=5000)
            if el_to:
                _set_input_value_with_fallback(f_to, el_to, d_to)
            else:
                print("[WARN] DATE_TO not found.")

        time.sleep(0.5)

        print("[INFO] Clicking Consultar...")
        try:
            with page.expect_navigation(timeout=30000):
                clicked = _click_maybe_in_frames(page, sel.BUTTON_CONSULTAR)
                if not clicked:
                    raise Exception("Could not click Consultar")
            final_url = getattr(page, "url", "")
            final_page = page
        except Exception:
            # maybe new tab
            try:
                with page.context.expect_page(timeout=5000) as ctx:
                    clicked = _click_maybe_in_frames(page, sel.BUTTON_CONSULTAR)
                    if not clicked:
                        raise Exception("Could not click Consultar (new tab attempt failed)")
                new_page = ctx.value
                try:
                    new_page.wait_for_load_state("load", timeout=30000)
                except Exception:
                    pass
                final_page = new_page
                final_url = getattr(new_page, "url", "")
            except Exception:
                # fallback click then wait
                clicked_any = _click_maybe_in_frames(page, sel.BUTTON_CONSULTAR)
                if not clicked_any:
                    _dump_debug(page)
                    return page, getattr(page, "url", "")
                try:
                    page.wait_for_load_state("networkidle", timeout=30000)
                except Exception:
                    pass
                final_page = page
                final_url = getattr(page, "url", "")

        if wait_after_result and wait_after_result > 0:
            time.sleep(wait_after_result)

        return final_page, final_url

    except Exception as e:
        print("[ERROR] Exception in fill_cfe_and_consult:", e)
        traceback.print_exc()
        _dump_debug(page)
        raise

# ---------------------------
# Grid scanning / extraction helpers
# ---------------------------
def _try_get_text(element):
    if element is None:
        return ""
    try:
        tag = element.evaluate("el => el.tagName && el.tagName.toLowerCase()")
    except Exception:
        tag = None
    try:
        if tag in ("input", "textarea"):
            return element.evaluate("el => el.value ? el.value.trim() : ''") or ""
        else:
            return element.inner_text().strip()
    except Exception:
        try:
            return element.evaluate("el => el.textContent ? el.textContent.trim() : ''") or ""
        except Exception:
            return ""


def _sanitize_fecha_emision(value: str) -> str:
    if not value:
        return ""
    v = str(value).strip()
    # keep same behavior as earlier code: remove trailing timezone/extra 5 chars if present
    return v[:-5] if len(v) > 5 else ""


def _extract_fields_from_page(p):
    mapping = {
        "Razon Social": ["#span_vDENOMINACION", '[id*="span_vDENOMINACION"]', '.ReadonlyAttribute#span_vDENOMINACION'],
        "RUT": ["#span_CTLEFACARCHEMISORDOCNRO", '[id*="CTLEFACARCHEMISORDOCNRO"]'],
        "Tipo CFE": ["#span_CTLEFACCMPTIPODESCORTA", '[id*="CTLEFACCMPTIPODESCORTA"]'],
        "Serie": ["#span_CTLEFACCFESERIE1", '[id*="CTLEFACCFESERIE1"]'],
        "Numero": ["#span_CTLEFACCFENUMERO1", '[id*="CTLEFACCFENUMERO1"]'],
        "Fecha de Emision": ["#CTLEFACCFEFIRMAFECHAHORA_dp_container", '[id*="CTLEFACCFEFIRMAFECHAHORA"]', '[id*="FECHAHORA"]'],
        "Moneda": ["#span_CTLEFACCFETIPOMONEDA", '[id*="CTLEFACCFETIPOMONEDA"]'],
        "TC": ["#span_CTLEFACCFETIPOCAMBIO", '[id*="CTLEFACCFETIPOCAMBIO"]', '[id*="TIPOCAMBIO"]'],
        "Monto No Gravado": ["#span_CTLEFACCFETOTALMONTONOGRV", '[id*="TOTALMONTONOGRV"]'],
        "Monto Exportacion y Asimilados": ["#span_CTLEFACCFETOTALMONTONOGRV", '[id*="TOTALMONTONOGRV"]'],
        "Monto Impuesto Percibido": ["#span_CTLEFACCFETOTALMNTIMPPER", '[id*="TOTALMNTIMPPER"]'],
        "Monto  IVA en suspenso": ["#span_CTLEFACCFETOTALMNTIVASUSP", '[id*="TOTALMNTIVASUSP"]'],
        "Neto Iva Tasa Basica": ["#span_CTLEFACCFETOTALMNTNETOIVATTB", '[id*="TOTALMNTNETOIVATTB"]'],
        "Neto Iva Tasa Minima": ["#span_CTLEFACCFETOTALMNTNETOIVATTM", '[id*="TOTALMNTNETOIVATTM"]'],
        "Neto Iva Otra Tasa": ["#span_CTLEFACCFETOTALMNTNETOIVATTO", '[id*="TOTALMNTNETOIVATTO"]'],
        "Monto Total": ["#span_CTLEFACCFETOTALMONTOTOTAL", '[id*="TOTALMONTOTOTAL"]'],
        "Monto Retenido": ['#span_CTLEFACCFETOTALMONTORET', '[id*="CTLEFACCFETOTALMONTORET"]', '.TextView#TEXTBLOCK64'],
        "Monto Credito Fiscal": ["#span_CTLEFACCFETOTALMONTCREDFISC", '[id*="TOTALMONTCREDFISC"]'],
        "Monto No facturable": ["#span_CTLEFACCFEMONTONOFACT", '[id*="MONTONOFACT"]'],
        "Monto Total a Pagar": ["#span_CTLEFACCFETOTALMNTAPAGAR", '[id*="TOTALMNTAPAGAR"]'],
        "Iva Tasa Basica": ["#span_CTLEFACCFETOTALIVATASABASICA", '[id*="TOTALIVATASABASICA"]'],
        "Iva Tasa Minima": ["#span_CTLEFACCFETOTALIVATASAMIN", '[id*="TOTALIVATASAMIN"]'],
        "Iva Otra Tasa": ['#span_CTLEFACCFETOTALIVAOTRATASA', '[id*="TOTALIVAOTRATASA"]'],
    }

    result = {}
    for col, selectors in mapping.items():
        found_text = ""
        for s in selectors:
            try:
                el = p.query_selector(s)
            except Exception:
                el = None
            if el:
                found_text = _try_get_text(el)
                if found_text:
                    break
        result[col] = found_text
    return result


def _collect_candidate_urls(page, parent_selector=None, link_selector=None) -> List[str]:
    urls = []
    frames_to_search = [page] + list(page.frames)
    tried = set()

    for p in frames_to_search:
        base = getattr(p, 'url', '') or getattr(page, 'url', '') or ''
        selectors = [link_selector] if link_selector else [
            f"{parent_selector} a[href]" if parent_selector else "a[href]",
            f"{parent_selector} img[src]" if parent_selector else "img[src]",
            "a[onclick]",
        ]
        for selq in selectors:
            try:
                els = p.query_selector_all(selq)
            except Exception:
                els = []
            for el in els:
                try:
                    href = el.get_attribute('href')
                except Exception:
                    href = None
                try:
                    src = el.get_attribute('src')
                except Exception:
                    src = None
                try:
                    onclick = el.get_attribute('onclick')
                except Exception:
                    onclick = None

                candidate = href or src or ''
                if candidate and not candidate.lower().startswith('javascript') and candidate.strip() != '#':
                    try:
                        absurl = urllib.parse.urljoin(base, candidate)
                    except Exception:
                        absurl = candidate
                    if absurl not in tried:
                        tried.add(absurl)
                        urls.append(absurl)
                        continue

                if onclick:
                    m = re.search(r"['\"](https?://[^'\"]+)['\"]", onclick)
                    if m:
                        absurl = m.group(1)
                        if absurl not in tried:
                            tried.add(absurl)
                            urls.append(absurl)
                            continue
                    m2 = re.search(r"open\(['\"]([^'\"]+)['\"]", onclick)
                    if m2:
                        try:
                            absurl = urllib.parse.urljoin(base, m2.group(1))
                        except Exception:
                            absurl = m2.group(1)
                        if absurl not in tried:
                            tried.add(absurl)
                            urls.append(absurl)
                            continue
    return urls


def _gather_candidate_link_elements(page, parent_selector=None, link_selector=None):
    candidates = []
    frames_to_search = [page] + list(page.frames)
    tried = set()

    if link_selector:
        for p in frames_to_search:
            try:
                els = p.query_selector_all(link_selector)
            except Exception:
                els = []
            for el in els:
                try:
                    sig = el.evaluate("el => el.outerHTML.substring(0,200)")
                except Exception:
                    sig = str(el)
                if (getattr(p, "url", None), sig) in tried:
                    continue
                tried.add((getattr(p, "url", None), sig))
                candidates.append((p, el))
        return candidates

    parent_candidates = []
    if parent_selector:
        parent_candidates.append(parent_selector)
    parent_candidates.extend([
        "div[id*='Container']", "div[id*='sector']", "table[class*='gx-region']", "div.gx-region", "div.gxp-page", "div[id^='W']"
    ])

    for p in frames_to_search:
        for pc in parent_candidates:
            try:
                parent = p.query_selector(pc)
            except Exception:
                parent = None
            if parent:
                try:
                    elems = parent.query_selector_all("a, button, img")
                except Exception:
                    elems = []
                for el in elems:
                    try:
                        sig = el.evaluate("el => el.outerHTML.substring(0,200)")
                    except Exception:
                        sig = str(el)
                    if (getattr(p, "url", None), sig) in tried:
                        continue
                    tried.add((getattr(p, "url", None), sig))
                    try:
                        href = el.get_attribute("href")
                        onclick = el.get_attribute("onclick")
                    except Exception:
                        href = onclick = None
                    try:
                        has_img = el.evaluate("el => !!el.querySelector('img')")
                    except Exception:
                        has_img = False
                    if href or onclick or has_img:
                        candidates.append((p, el))
                if candidates:
                    return candidates

    for p in frames_to_search:
        try:
            elems = p.query_selector_all("a[href], a:has(img), img[id^='vCOLDISPLAY'], button")
        except Exception:
            elems = []
        for el in elems:
            try:
                sig = el.evaluate("el => el.outerHTML.substring(0,200)")
            except Exception:
                sig = str(el)
            if (getattr(p, "url", None), sig) in tried:
                continue
            tried.add((getattr(p, "url", None), sig))
            candidates.append((p, el))
    return candidates


# ---------------------------
# click Next only (no re-fill)
# ---------------------------
def click_next_only(page) -> bool:
    candidates = [
        'input#W0127SIGUIENTE',
        'input[name="W0127SIGUIENTE"]',
        'input.Image[id="W0127SIGUIENTE"]',
        'input.Image[src*="K2BPageNext.png"]',
        'input[src*="K2BPageNext.png"]',
        'img[src*="K2BPageNext.png"]',
        'button[title*="Sig"]',
        'a[title*="Sig"]',
        'button:has-text("Siguiente")',
        'a:has-text("Siguiente")'
    ]
    clicked = False
    for sel_q in candidates:
        frame_or_page, el = _find_element_in_page_and_frames(page, sel_q, timeout=2500)
        if el:
            try:
                print(f"[INFO] Found next-button by selector '{sel_q}', clicking (no fill)...")
                try:
                    el.click()
                except Exception:
                    el.evaluate("el => el.click()")
                clicked = True
                break
            except Exception as e:
                print("[WARN] Next-button click failed for selector", sel_q, e)
                continue

    if not clicked:
        print("[WARN] Could not find/click the next image button (click_next_only).")
        try:
            _dump_debug(page, prefix="debug_next_button")
        except Exception:
            pass
    return clicked


def go_to_consulta_and_click_next(page,
                                 consulta_url: Optional[str] = "https://servicios.dgi.gub.uy/serviciosenlinea/con-clave/dgi--servicios-en-linea--otros-servicios--efactura-consulta-de-cfe-y-cfc-recibidos",
                                 tipo_value: Optional[str] = None,
                                 date_from: Optional[str] = None,
                                 date_to: Optional[str] = None,
                                 wait_after_fill: float = 2.0) -> Tuple[object, str]:
    """
    Navigate to consulta page (if needed), run fill_cfe_and_consult once to ensure grid is loaded,
    then click the Next image/button and return (final_page, url).
    """
    try:
        try:
            cur_url = getattr(page, "url", "") or ""
        except Exception:
            cur_url = ""
        if consulta_url and consulta_url not in cur_url:
            try:
                print(f"[INFO] Navigating to consulta page: {consulta_url}")
                page.goto(consulta_url, timeout=30000)
                try:
                    page.wait_for_load_state("load", timeout=20000)
                except Exception:
                    try:
                        page.wait_for_load_state("networkidle", timeout=20000)
                    except Exception:
                        pass
            except Exception as e:
                print("[WARN] Navigation to consulta URL failed or timed out:", e)

        tipo = tipo_value or getattr(config, "ECF_TIPO", "111")
        d_from = date_from or getattr(config, "ECF_FROM_DATE", "")
        d_to = date_to or getattr(config, "ECF_TO_DATE", "")

        print(f"[INFO] fill_cfe_and_consult: tipo={tipo}, desde={d_from}, hasta={d_to}")
        final_page, final_url = fill_cfe_and_consult(page, tipo_value=tipo, date_from=d_from, date_to=d_to, wait_after_result=0)

        print(f"[INFO] Waiting {wait_after_fill} seconds before clicking next image...")
        time.sleep(wait_after_fill)

        clicked = click_next_only(final_page)
        try:
            final_page.wait_for_load_state("load", timeout=10000)
        except Exception:
            try:
                final_page.wait_for_load_state("networkidle", timeout=10000)
            except Exception:
                pass

        return final_page, getattr(final_page, "url", final_url)
    except Exception as e:
        print("[ERROR] go_to_consulta_and_click_next failed:", e)
        traceback.print_exc()
        try:
            _dump_debug(page, prefix="debug_go_to_consulta_error")
        except Exception:
            pass
        raise


# ---------------------------
# New: export XLS helpers (integrated) - improved with fallback
# ---------------------------
def click_iframe_image_and_open(page, wait_seconds: int = 5):
    try:
        print("[INFO] Looking for efacConsultasMenuServFE iframe...")
        iframe_el = page.query_selector('iframe[src*="efacConsultasMenuServFE"]') or page.query_selector('iframe[id^="gxpea"]')
        if not iframe_el:
            for f in page.query_selector_all("iframe"):
                src = f.get_attribute("src") or ""
                if "efacConsultasMenuServFE" in src or "efacconsmnuservredireccion" in src:
                    iframe_el = f
                    break

        if not iframe_el:
            print("[ERROR] Target iframe not found on the page.")
            _dump_debug(page)
            return None

        frame = iframe_el.content_frame()
        if not frame:
            print("[ERROR] Could not access iframe content frame.")
            _dump_debug(page)
            return None

        print("[INFO] Got content frame. Looking for image/link inside frame...")

        selectors_to_try = [
            'a[href*="efacconsultatwebsobrecfe"]',
            'a:has(img[src*="K2BActionDisplay.gif"])',
            'a:has(img[id^="vCOLDISPLAY"])',
            'img[src*="K2BActionDisplay.gif"]',
            'img[id^="vCOLDISPLAY"]'
        ]

        anchor = None
        for selq in selectors_to_try:
            try:
                el = frame.query_selector(selq)
                if el:
                    if el.evaluate("el => el.tagName.toLowerCase()") == "img":
                        try:
                            parent = el.evaluate_handle("img => img.closest('a')")
                            if parent:
                                anchor = parent.as_element()
                        except Exception:
                            anchor = None
                    else:
                        anchor = el
                if anchor:
                    print(f"[DEBUG] Found element with selector: {selq}")
                    break
            except Exception:
                continue

        if not anchor:
            print("[ERROR] Could not find link/image inside iframe with known selectors.")
            _dump_debug(page)
            return None

        print("[INFO] Clicking the link inside iframe...")
        try:
            with page.context.expect_page(timeout=10000) as new_page_ctx:
                anchor.click()
            new_page = new_page_ctx.value
            try:
                new_page.wait_for_load_state("load", timeout=20000)
            except Exception:
                try:
                    new_page.wait_for_load_state("networkidle", timeout=20000)
                except Exception:
                    pass
            print("[SUCCESS] Link opened in a new tab:", new_page.url)
            return new_page
        except TimeoutError:
            try:
                anchor.click()
            except Exception as e:
                print("[WARN] click without new tab failed:", e)
            try:
                frame.wait_for_load_state("load", timeout=10000)
            except Exception:
                try:
                    page.wait_for_load_state("networkidle", timeout=10000)
                except Exception:
                    pass
            print("[INFO] Clicked link — no new tab detected. Current page URL:", page.url)
            time.sleep(wait_seconds)
            return page

    except Exception as e:
        print("[ERROR] Exception in click_iframe_image_and_open:", e)
        traceback.print_exc()
        _dump_debug(page)
        raise


def export_xls_and_save(page, save_dir="downloads", timeout=30000, filename_prefix: str = ""):
    """
    Find and click the EXPORTXLS element (searching page and frames),
    wait for the download and save it into save_dir. Returns saved filepath or None.

    Implements:
      - page.expect_download() (preferred)
      - fallback to context.wait_for_event('download') via a background thread if expect_download not present
    """
    try:
        selectors = [
            getattr(sel, 'EXPORT_XLS_BY_NAME', None),
            getattr(sel, 'EXPORT_XLS_BY_ID', None),
            getattr(sel, 'EXPORT_XLS_IMG', None),
            'input[name="EXPORTXLS"]',
            'input#EXPORTXLS'
        ]

        frame_or_page = None
        el = None
        for s in selectors:
            if not s:
                continue
            frame_or_page, el = _find_element_in_page_and_frames(page, s, timeout=2000)
            if el:
                print(f"[DEBUG] Found export element using selector: {s}")
                break

        # last resort: try to find by image src matching 'xls' icon
        if not el:
            frame_or_page, el = _find_element_in_page_and_frames(page, 'img[src*="xls22.png"]', timeout=2000)
            if el:
                print("[DEBUG] Found export image by src 'xls22.png'")

        if not el:
            print("[ERROR] Export element not found with known selectors. Dumping debug.")
            _dump_debug(page)
            return None

        # ensure save dir exists
        Path(save_dir).mkdir(parents=True, exist_ok=True)

        # Try preferred API: page.expect_download()
        try:
            print("[INFO] Attempting page.expect_download() to capture the download.")
            with page.expect_download(timeout=timeout) as download_ctx:
                try:
                    # try direct click first
                    try:
                        el.click()
                    except Exception:
                        try:
                            el.evaluate("el => el.click()")
                        except Exception:
                            href = None
                            try:
                                href = el.get_attribute('href')
                            except Exception:
                                href = None
                            if href:
                                page.evaluate("url => window.open(url, '_blank')", href)
                            else:
                                raise
                except Exception as e:
                    print("[ERROR] Could not click export element (expect_download path):", e)
                    return None
            download = download_ctx.value
            suggested = download.suggested_filename or "export.xls"
            ts = datetime.now().strftime("%Y%m%d%H%M%S")
            dest_name = f"{filename_prefix or ''}{ts}_{suggested}"
            dest = Path(save_dir) / dest_name
            download.save_as(str(dest))
            print(f"[SUCCESS] Download saved to: {dest}")
            return str(dest)
        except AttributeError:
            # page.expect_download not available; fall through to threaded fallback
            print("[WARN] page.expect_download() not available, falling back to threaded context.wait_for_event('download').")
        except Exception as e:
            print("[WARN] page.expect_download() attempt failed, trying fallback. Error:", e)

        # ----------------------------
        # Threaded fallback using context.wait_for_event('download')
        # ----------------------------
        ctx = page.context
        download_result = {"download": None, "error": None}

        def _wait_for_download():
            try:
                # This will block until download event or timeout
                d = ctx.wait_for_event('download', timeout=timeout)
                download_result["download"] = d
            except Exception as ex:
                download_result["error"] = ex

        waiter = threading.Thread(target=_wait_for_download, daemon=True)
        waiter.start()
        # small grace to ensure waiter thread started and is listening
        time.sleep(0.05)

        # perform click
        try:
            try:
                el.click()
            except Exception:
                try:
                    el.evaluate("el => el.click()")
                except Exception:
                    href = None
                    try:
                        href = el.get_attribute('href')
                    except Exception:
                        href = None
                    if href:
                        page.evaluate("url => window.open(url, '_blank')", href)
                    else:
                        raise
        except Exception as e:
            print("[ERROR] Could not click export element (threaded fallback):", e)
            # make sure thread finishes
            waiter.join(timeout=0.1)
            return None

        # wait for the waiter thread to capture the download (bounded by timeout)
        waiter.join(timeout=(timeout / 1000.0) + 1.0)

        if download_result.get("error"):
            print("[ERROR] download listener reported an error:", download_result["error"])
            _dump_debug(page)
            return None

        download_obj = download_result.get("download")
        if not download_obj:
            print("[ERROR] No download event captured by fallback listener. Dumping debug.")
            _dump_debug(page)
            return None

        suggested = getattr(download_obj, "suggested_filename", None) or "export.xls"
        ts = datetime.now().strftime("%Y%m%d%H%M%S")
        dest_name = f"{filename_prefix or ''}{ts}_{suggested}"
        dest = Path(save_dir) / dest_name
        try:
            download_obj.save_as(str(dest))
            print(f"[SUCCESS] Download saved to: {dest} (fallback path)")
            return str(dest)
        except Exception as e:
            print("[ERROR] Could not save download to disk (fallback):", e)
            _dump_debug(page)
            return None

    except Exception as e:
        print("[ERROR] Exception during export_xls_and_save (outer):", e)
        traceback.print_exc()
        try:
            _dump_debug(page)
        except Exception:
            pass
        return None

# ---------------------------
# process current page and append rows to CSV/Excel
# ---------------------------
def process_and_save_current_page(page, processed: set, csv_path: str, cols_order: List[str],
                                  parent_selector: Optional[str] = None, link_selector: Optional[str] = None,
                                  wait_for_new_seconds: float = 6.0) -> int:
    """
    Process the current page: wait until the page grid yields new (unprocessed) URLs or
    until wait_for_new_seconds timeout, then extract and append rows.

    Returns number of new rows added.
    """
    start = time.time()
    new_rows = 0

    # Poll until we get candidate URLs or timeout
    urls = []
    while time.time() - start < wait_for_new_seconds:
        try:
            urls = _collect_candidate_urls(page, parent_selector=parent_selector, link_selector=link_selector)
            # compute whether there are any urls that are not already processed
            unprocessed = [u for u in urls if _canonicalize_url(u) not in processed]
            if urls and unprocessed:
                urls = urls  # proceed
                break
            # If nothing processed yet (first page), accept whatever we have
            if not processed and urls:
                break
        except Exception:
            pass
        time.sleep(0.3)

    if not urls:
        # final attempt to collect fallback elements
        try:
            urls = _collect_candidate_urls(page, parent_selector=parent_selector, link_selector=link_selector)
        except Exception:
            urls = []

    fallback_elements = []
    if not urls:
        try:
            fallback_elements = _gather_candidate_link_elements(page, parent_selector=parent_selector, link_selector=link_selector)
        except Exception:
            fallback_elements = []

    # Process URL list first
    for idx, url in enumerate(urls, start=1):
        canon = _canonicalize_url(url)
        if canon in processed:
            print(f"[INFO] URL already processed; skipping: {url}")
            continue
        print(f"[INFO] Opening URL {idx}/{len(urls)}: {url}")
        try:
            new_page = None
            # Prefer to open using window.open from the original page -> preserves session and referrer
            try:
                with page.context.expect_page(timeout=8000) as new_page_ctx:
                    page.evaluate("url => window.open(url, '_blank')", url)
                new_page = new_page_ctx.value
            except Exception:
                # Fallback: create a new page in same context and goto the url
                try:
                    new_page = page.context.new_page()
                    new_page.goto(url, timeout=30000)
                except Exception:
                    try:
                        if new_page:
                            new_page.close()
                    except Exception:
                        pass
                    new_page = None

            if not new_page:
                print("[WARN] Could not open URL in a new tab; trying to open in-place via navigation on current page")
                try:
                    page.goto(url, timeout=30000)
                    extraction_target = page
                except Exception:
                    print("[ERROR] Could not navigate to URL in-place either; skipping")
                    continue
            else:
                extraction_target = new_page
                try:
                    new_page.wait_for_load_state("load", timeout=20000)
                except Exception:
                    try:
                        new_page.wait_for_load_state("networkidle", timeout=20000)
                    except Exception:
                        pass

            try:
                # If content is in a frame, find a good frame
                for f in getattr(extraction_target, 'frames', []):
                    try:
                        if f.query_selector('#span_vDENOMINACION') or f.query_selector('[id*="CTLEFACCFETOTALMONTOTOTAL"]'):
                            extraction_target = f
                            break
                    except Exception:
                        continue
            except Exception:
                pass

            data = _extract_fields_from_page(extraction_target)
            # sanitize Fecha de Emision (remove last 5 chars)
            if "Fecha de Emision" in data:
                data["Fecha de Emision"] = _sanitize_fecha_emision(data["Fecha de Emision"])
            # compatibility: if extraction used misspelled key
            if "Fecha de Emisin" in data and not data.get("Fecha de Emision"):
                data["Fecha de Emision"] = _sanitize_fecha_emision(data.get("Fecha de Emisin", ""))

            # keep source URL for dedupe/debug as requested
            data['h_source_url'] = url

            # ensure all columns exist
            for c in cols_order:
                if c not in data:
                    data[c] = ''

            try:
                _append_row_to_csv(csv_path, data, fieldnames=cols_order)
                processed.add(canon)
                new_rows += 1
                print(f"[INFO] Appended row for {url}")
            except Exception as e:
                print("[ERROR] Could not append row:", e)

            try:
                if new_page and new_page is not page:
                    new_page.close()
            except Exception:
                pass
        except Exception as e:
            print("[ERROR] Error opening URL:", url, e)
            try:
                if new_page and new_page is not page:
                    new_page.close()
            except Exception:
                pass
            continue

    # If URL list gave nothing or didn't yield new rows, process fallback element-clicks
    if new_rows == 0 and fallback_elements:
        print("[INFO] Attempting fallback element-click extraction on this page.")
        for (frm, el) in fallback_elements:
            try:
                opened_page = None
                try:
                    with page.context.expect_page(timeout=3000) as new_page_ctx:
                        el.click()
                    opened_page = new_page_ctx.value
                except Exception:
                    try:
                        el.click()
                        time.sleep(0.5)
                        opened_page = page
                    except Exception:
                        opened_page = None

                if not opened_page:
                    continue

                extraction_target = opened_page
                try:
                    for f in getattr(opened_page, 'frames', []):
                        if f.query_selector('#span_vDENOMINACION') or f.query_selector('[id*="CTLEFACCFETOTALMONTOTOTAL"]'):
                            extraction_target = f
                            break
                except Exception:
                    pass

                data = _extract_fields_from_page(extraction_target)
                # sanitize Fecha de Emision before saving
                if "Fecha de Emision" in data:
                    data["Fecha de Emision"] = _sanitize_fecha_emision(data["Fecha de Emision"])
                if "Fecha de Emisin" in data and not data.get("Fecha de Emision"):
                    data["Fecha de Emision"] = _sanitize_fecha_emision(data.get("Fecha de Emisin", ""))

                src_url = getattr(opened_page, 'url', '') or ''
                data['h_source_url'] = src_url
                canon = _canonicalize_url(src_url) if src_url else ''

                if canon and canon in processed:
                    print("[INFO] Fallback element led to already-processed page; skipping.")
                else:
                    for c in cols_order:
                        if c not in data:
                            data[c] = ''
                    try:
                        _append_row_to_csv(csv_path, data, fieldnames=cols_order)
                        if canon:
                            processed.add(canon)
                        new_rows += 1
                        print(f"[INFO] Appended fallback row (element-click path)")
                    except Exception as e:
                        print("[ERROR] Could not append fallback row:", e)

                try:
                    if opened_page is not page:
                        opened_page.close()
                except Exception:
                    pass

            except Exception as e:
                print("[WARN] Exception during fallback element processing:", e)
                continue

    # Best-effort: update Excel after processing this page
    try:
        if os.path.exists(csv_path):
            final_df = pd.read_csv(csv_path)
            try:
                xlsx_path = os.path.splitext(csv_path)[0] + ".xlsx"
                # sanitize Fecha de Emision column in final_df (defensive)
                if "Fecha de Emision" in final_df.columns:
                    final_df["Fecha de Emision"] = final_df["Fecha de Emision"].fillna("").astype(str).apply(lambda s: s[:-5] if len(s) > 5 else "")
                final_df.to_excel(xlsx_path, index=False)
                print(f"[INFO] Saved {len(final_df)} rows to {xlsx_path} (intermediate).")
            except Exception as e:
                print("[WARN] Could not save intermediate Excel:", e)
    except Exception as e:
        print("[WARN] Could not convert CSV to Excel / read CSV during intermediate save:", e)

    return new_rows


# ---------------------------
# collect_cfe_from_links: multi-page, uses click_next_only for in-place pagination
# ---------------------------
def _normalize_date_for_folder(s: str) -> str:
    """Convert various date formats into DD-MM-YYYY (best-effort). If not parseable, sanitize digits."""
    if not s:
        return ""
    s = s.strip()
    # try common formats
    patterns = [
        ("%d/%m/%Y", r"\d{2}/\d{2}/\d{4}"),
        ("%Y-%m-%d", r"\d{4}-\d{2}-\d{2}"),
        ("%d-%m-%Y", r"\d{2}-\d{2}-\d{4}"),
        ("%d.%m.%Y", r"\d{2}\.\d{2}\.\d{4}"),
        ("%m/%d/%Y", r"\d{2}/\d{2}/\d{4}"),
    ]
    for fmt, pat in patterns:
        try:
            if re.match(pat, s):
                dt = datetime.strptime(s, fmt)
                return dt.strftime("%d-%m-%Y")
        except Exception:
            continue
    # try to extract numbers and reformat dd-mm-yyyy if possible
    digits = re.findall(r"\d+", s)
    if len(digits) >= 3:
        # choose last 3 if year first
        if len(digits[0]) == 4:
            y, m, d = digits[0], digits[1], digits[2]
        else:
            d, m, y = digits[0], digits[1], digits[2]
        try:
            dt = datetime(int(y), int(m), int(d))
            return dt.strftime("%d-%m-%Y")
        except Exception:
            pass
    # fallback sanitize
    return re.sub(r"[^\d\-]", "-", s)


def _period_string(from_s: str, to_s: str) -> str:
    """
    Build period string like '2025_7_1-2025_7_31' based on config dates.
    If both empty -> 'ALL'
    """
    def parse_date(s):
        if not s:
            return None
        fmts = ["%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d.%m.%Y", "%m/%d/%Y"]
        for f in fmts:
            try:
                return datetime.strptime(s.strip(), f)
            except Exception:
                pass
        # fallback: try to extract 3 numbers
        parts = re.findall(r'\d+', s)
        if len(parts) >= 3:
            if len(parts[0]) == 4:
                y, m, d = parts[0], parts[1], parts[2]
            else:
                d, m, y = parts[0], parts[1], parts[2]
            try:
                return datetime(int(y), int(m), int(d))
            except Exception:
                return None
        return None

    d1 = parse_date(from_s)
    d2 = parse_date(to_s)
    if d1 and d2:
        return f"{d1.year}_{d1.month}_{d1.day}-{d2.year}_{d2.month}_{d2.day}"
    if d1 and not d2:
        return f"{d1.year}_{d1.month}_{d1.day}-"
    if d2 and not d1:
        return f"-{d2.year}_{d2.month}_{d2.day}"
    return "ALL"


def collect_cfe_from_links(page, link_selector: Optional[str] = None, output_file: str = "results.xlsx", parent_selector: Optional[str]=None,
                           do_post_action: bool = True, max_pages: Optional[int] = None) -> str:
    """
    Main function: find document links in the grid (or using link_selector),
    extract fields and save incrementally to CSV/Excel for each page, and paginate by clicking Next in-place.

    Saves outputs inside:
      <OUTPUT_DIR>/<PROCESS_NAME>/<RUT>/
         result.csv
         <Process>_<RUT>_Excel_TODOS_<period>[_pN].xlsx   <- downloaded export(s) (renamed)
         <Process>_<RUT>_Apertura_TODOS_<period>.xlsx    <- final output (Apertura)
    """
    print("[INFO] Starting collection (in-place pagination + immediate extraction).")

    # Detect process name from page (title) or fallback to config.PROCESS_NAME
    process_name_raw = None
    try:
        # look for the specific title span used in the UI
        sel_candidates = [
            'span.TextBlock_TitleExt#W0006TITULO',
            'span#W0006TITULO',
            'span.TextBlock_TitleExt',
            'span[id*="TITULO"]',
            'h1', 'h2'
        ]
        frames = [page] + list(page.frames)
        for p in frames:
            for s in sel_candidates:
                try:
                    el = p.query_selector(s)
                except Exception:
                    el = None
                if el:
                    try:
                        t = el.inner_text().strip()
                        if t:
                            process_name_raw = t
                            break
                    except Exception:
                        pass
            if process_name_raw:
                break
    except Exception:
        process_name_raw = None

    process_name_raw = process_name_raw or getattr(config, "PROCESS_NAME", "CFERecibidos")
    # sanitize process name to safe filesystem base (remove spaces/unsafe chars)
    process_base = re.sub(r'[^\w]+', '', process_name_raw) or getattr(config, "PROCESS_NAME", "CFERecibidos")

    output_root = getattr(config, "OUTPUT_DIR", "DGI") or "DGI"
    rut_val = str(getattr(config, "RUT", "")).strip() or "unknown_rut"
    rut_dir = os.path.join(output_root, process_base, rut_val)
    os.makedirs(rut_dir, exist_ok=True)

    period = _period_string(getattr(config, "ECF_FROM_DATE", ""), getattr(config, "ECF_TO_DATE", ""))

    csv_path = os.path.join(rut_dir, "result.csv")
    apertura_filename = f"{process_base}_{rut_val}_Apertura_TODOS_{period}.xlsx"
    apertura_path = os.path.join(rut_dir, apertura_filename)

    # load processed URLs from existing outputs (Excel or CSV) using h_source_url
    processed = set()
    if os.path.exists(apertura_path):
        try:
            existing_df = pd.read_excel(apertura_path)
            if "h_source_url" in existing_df.columns:
                for u in existing_df["h_source_url"].fillna("").astype(str):
                    processed.add(_canonicalize_url(u))
            print(f"[INFO] Loaded {len(existing_df)} existing rows from {apertura_path}.")
        except Exception as e:
            print("[WARN] Could not read existing Apertura Excel (will try CSV).", e)

    if os.path.exists(csv_path):
        try:
            existing_csv = pd.read_csv(csv_path)
            if "h_source_url" in existing_csv.columns:
                for u in existing_csv["h_source_url"].fillna("").astype(str):
                    processed.add(_canonicalize_url(u))
            print(f"[INFO] Loaded {len(existing_csv)} existing rows from {csv_path}.")
        except Exception as e:
            print("[WARN] Could not read existing CSV.", e)

    cols_order = [
        "Razon Social", "RUT", "Tipo CFE", "Serie", "Numero", "Fecha de Emision",
        "Moneda", "TC", "Monto No Gravado", "Monto Exportacion y Asimilados",
        "Monto Impuesto Percibido", "Monto  IVA en suspenso", "Neto Iva Tasa Basica",
        "Neto Iva Tasa Minima", "Neto Iva Otra Tasa", "Monto Total", "Monto Retenido",
        "Monto Credito Fiscal", "Monto No facturable", "Monto Total a Pagar",
        "Iva Tasa Basica", "Iva Tasa Minima", "Iva Otra Tasa", "h_source_url"
    ]

    rows_added = 0
    page_count = 0
    download_counter = 1

    def _move_and_rename_download(path_saved: str, idx: int = None) -> Optional[str]:
        if not path_saved or not os.path.exists(path_saved):
            return None
        ext = os.path.splitext(path_saved)[1] or ".xlsx"
        base_name = f"{process_base}_{rut_val}_Excel_TODOS_{period}"
        if idx is not None:
            base_name = f"{base_name}_p{idx}"
        dest_name = f"{base_name}{ext}"
        dest = os.path.join(rut_dir, dest_name)
        counter = 1
        final_dest = dest
        while os.path.exists(final_dest):
            final_dest = os.path.join(rut_dir, f"{base_name}_{counter}{ext}")
            counter += 1
        try:
            shutil.move(path_saved, final_dest)
            print(f"[INFO] Moved downloaded export to: {final_dest}")
            return final_dest
        except Exception as e:
            print("[WARN] Could not move downloaded export:", e)
            return path_saved

    # 1) Process current page first
    page_count += 1
    print(f"[INFO] Processing initial page (page {page_count}) ...")
    new_on_page = process_and_save_current_page(page, processed, csv_path, cols_order, parent_selector=parent_selector, link_selector=link_selector)
    rows_added += new_on_page
    print(f"[INFO] New rows from initial page: {new_on_page}")

    # Export XLS for the current page (save into rut_dir then rename)
    try:
        saved = export_xls_and_save(page, save_dir=rut_dir, filename_prefix=f"page{page_count}_")
        if saved:
            _move_and_rename_download(saved, idx=download_counter)
            download_counter += 1
    except Exception as e:
        print("[WARN] export_xls failed:", e)

    # 2) Loop: click Next (in-place) then immediately process that page
    while True:
        # decide whether to stop
        if max_pages is not None and page_count >= max_pages:
            print(f"[INFO] Reached max_pages limit ({max_pages}). Stopping pagination.")
            break
        if new_on_page == 0:
            print("[INFO] No new items found on last processed page. Stopping pagination.")
            break

        print("[INFO] Clicking Next to advance to next page (no fill) ...")
        clicked = click_next_only(page)
        if not clicked:
            print("[WARN] Could not click Next — stopping pagination.")
            break

        # small grace before scraping — process_and_save_current_page will poll for new items
        time.sleep(0.2)

        page_count += 1
        print(f"[INFO] Processing page {page_count} ...")
        new_on_page = process_and_save_current_page(page, processed, csv_path, cols_order, parent_selector=parent_selector, link_selector=link_selector)
        rows_added += new_on_page
        print(f"[INFO] New rows from page {page_count}: {new_on_page}")

        # Export XLS for this page as well (saved into rut_dir)
        try:
            saved = export_xls_and_save(page, save_dir=rut_dir, filename_prefix=f"page{page_count}_")
            if saved:
                _move_and_rename_download(saved, idx=download_counter)
                download_counter += 1
        except Exception as e:
            print("[WARN] export_xls failed:", e)

    # Final: try to write final Excel from CSV, dropping h_source_url for final report
    try:
        final_df = pd.read_csv(csv_path) if os.path.exists(csv_path) else pd.DataFrame(columns=cols_order)
        # DROP the h_source_url column for the final output as requested
        if "h_source_url" in final_df.columns:
            final_df = final_df.drop(columns=["h_source_url"])
        # sanitize Fecha de Emision column in final_df (defensive)
        if "Fecha de Emision" in final_df.columns:
            final_df["Fecha de Emision"] = final_df["Fecha de Emision"].fillna("").astype(str).apply(lambda s: s[:-1] if len(s) > 5 else "")
        try:
            final_df.to_excel(apertura_path, index=False)
            print(f"[SUCCESS] Saved {len(final_df)} rows to {apertura_path}")
            result_path = apertura_path
        except PermissionError as pe:
            ts_path = os.path.join(rut_dir, f"{process_base}_{rut_val}_Apertura_TODOS_{period}_{datetime.now().strftime('%Y%m%d%H%M%S')}.xlsx")
            try:
                final_df.to_excel(ts_path, index=False)
                print(f"[WARN] Could not overwrite {apertura_path} (Permission denied). Saved Excel to {ts_path} instead.")
                result_path = ts_path
            except Exception as e:
                print("[ERROR] Could not save Excel fallback:", e)
                print("[INFO] Leaving incremental CSV at:", csv_path)
                result_path = csv_path
    except Exception as e:
        print("[ERROR] Could not convert CSV to Excel / read CSV:", e)
        print("[INFO] Leaving incremental CSV at:", csv_path)
        result_path = csv_path

    # ---- After finishing collection, optionally navigate back to consulta and refill the same details ----
    if do_post_action:
        try:
            try:
                tipo = getattr(config, "ECF_TIPO", None)
                d_from = getattr(config, "ECF_FROM_DATE", None)
                d_to = getattr(config, "ECF_TO_DATE", None)
            except Exception:
                tipo = d_from = d_to = None

            print("[INFO] Performing post-collection action: navigate to consulta and refill filters + click next image...")
            try:
                go_to_consulta_and_click_next(page, tipo_value=tipo, date_from=d_from, date_to=d_to, wait_after_fill=2.0)
            except Exception as e:
                print("[WARN] Post-collection navigation/click failed:", e)
        except Exception:
            pass

    return result_path
