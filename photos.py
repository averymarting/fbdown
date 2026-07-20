#!/usr/bin/env python3
"""
Facebook Photos Scraper - Fixed caption extraction
"""

import os
import re
import sys
import csv
import json
import time
import queue
import datetime
import threading
import subprocess
from pathlib import Path
from urllib.parse import urljoin, urlparse, urlunparse, parse_qs
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.webp'}
MBASIC_BASE = "https://mbasic.facebook.com"
GENERIC_DESCRIPTIONS = {"", "see posts, photos and more on facebook."}

NAV_TEXT_RE = re.compile(
    r'^(like|comment|share|full size|view full size|comments?|write a comment|'
    r'see more|see less|reply|\d+[\d,.]*\s*(likes?|comments?|shares?)|'
    r'photo|options|report)$', re.I
)

WALL_SIGNALS = [
    "log into facebook", "log in to facebook", "you must log in",
    "id=\"login_form\"", "name=\"login\"", "checkpoint",
    "session has expired", "temporarily blocked",
    "not available on this browser", "get the facebook lite app",
]

# Config
PAGE_PHOTOS_URL = os.environ.get("PAGE_PHOTOS_URL", "").strip()
MAX_PAGES = int(os.environ.get("MAX_SCROLLS", "3"))
FOLDER_NAME = os.environ.get("FOLDER_NAME", "facebook_photos")
STORAGE_STATE = os.environ.get("STORAGE_STATE_FILE", "storage_state.json")
OUTPUT_DIR = os.path.join("output", FOLDER_NAME)
MAX_RETRIES = 3
RETRY_DELAYS = [5, 15, 30]
CONCURRENCY = int(os.environ.get("CONCURRENCY", "6"))

SHEET_ID = os.environ.get("SHEET_ID", "").strip()
GOOGLE_TOKEN_JSON = os.environ.get("GOOGLE_TOKEN_JSON", "").strip()
SHEET_TAB_NAME = os.environ.get("SHEET_TAB_NAME", "").strip()

MEGA_REMOTE = os.environ.get("MEGA_REMOTE", "mega")
MEGA_FOLDER_NAME = os.environ.get("MEGA_FOLDER_NAME", "").strip() or FOLDER_NAME

_log_lock = threading.Lock()
def log(msg):
    with _log_lock:
        print(msg, flush=True)

# ==================== GOOGLE SHEETS ====================
def with_retries(fn, *args, attempts=5, base_delay=5, **kwargs):
    for attempt in range(1, attempts + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if attempt == attempts: raise
            time.sleep(base_delay * (2 ** (attempt - 1)))
            log(f"Retry {attempt}/{attempts} after error: {e}")

def get_sheets_service(token_json_str):
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    info = json.loads(token_json_str)
    creds = Credentials(**{k: info.get(k) for k in ["token","refresh_token","token_uri","client_id","client_secret"] if info.get(k)})
    if not creds.valid:
        creds.refresh(Request())
    return build("sheets", "v4", credentials=creds)

def ensure_tab_and_get_id(service, sheet_id, tab_name):
    meta = with_retries(service.spreadsheets().get(spreadsheetId=sheet_id).execute)
    for s in meta.get("sheets", []):
        if s["properties"]["title"] == tab_name:
            return s["properties"]["sheetId"]
    resp = with_retries(service.spreadsheets().batchUpdate(
        spreadsheetId=sheet_id,
        body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]}
    ).execute)
    new_id = resp["replies"][0]["addSheet"]["properties"]["sheetId"]
    with_retries(service.spreadsheets().values().update(
        spreadsheetId=sheet_id, range=f"'{tab_name}'!A1:C1",
        valueInputOption="RAW", body={"values": [["filename", "caption", "source_url"]]}
    ).execute)
    return new_id

def append_rows(service, sheet_id, tab_name, rows):
    if not rows: return
    for i in range(0, len(rows), 40):
        chunk = rows[i:i+40]
        with_retries(service.spreadsheets().values().append(
            spreadsheetId=sheet_id, range=f"'{tab_name}'!A:C",
            valueInputOption="RAW", insertDataOption="INSERT_ROWS",
            body={"values": chunk}
        ).execute)
    log(f"✅ Appended {len(rows)} rows to sheet")

# ==================== BROWSER HELPERS ====================
def to_mbasic(url):
    p = urlparse(url)
    return urlunparse(p._replace(netloc="mbasic.facebook.com", scheme="https"))

def save_debug_html(html, name):
    os.makedirs("output", exist_ok=True)
    with open(os.path.join("output", name), "w", encoding="utf-8") as f:
        f.write(html)

def goto_html(page, url):
    for _ in range(MAX_RETRIES):
        try:
            page.goto(url, timeout=30000, wait_until="domcontentloaded")
            page.wait_for_timeout(1500)
            return page.content()
        except:
            time.sleep(5)
    return None

# ==================== IMPROVED CAPTION EXTRACTION ====================
def extract_caption(soup):
    # Best chance: main post text
    for tag in soup.find_all(["div", "span", "p"]):
        text = tag.get_text(" ", strip=True)
        if len(text) > 10 and not NAV_TEXT_RE.match(text) and not re.search(r'^\d+\s*(like|comment|min|hr|day)', text, re.I):
            return text

    # Fallback
    candidates = []
    for tag in soup.find_all(["div", "span", "p"]):
        text = tag.get_text(" ", strip=True)
        if len(text) > 8 and not NAV_TEXT_RE.match(text):
            candidates.append(text)
    if candidates:
        return max(candidates, key=len)

    og = soup.find("meta", property="og:description")
    if og and og.get("content"):
        txt = og["content"].strip()
        if txt.lower() not in GENERIC_DESCRIPTIONS:
            return txt
    return ""

def extract_photo_details(page, permalink):
    html = goto_html(page, permalink)
    if not html:
        return None, ""
    if len(html) < 6000:
        save_debug_html(html, "debug_photo.html")

    soup = BeautifulSoup(html, "html.parser")

    # Image URL
    image_url = None
    for a in soup.find_all("a", href=True):
        if "full size" in a.get_text(strip=True).lower():
            image_url = urljoin(MBASIC_BASE, a["href"])
            break
    if not image_url:
        og = soup.find("meta", property="og:image")
        if og: image_url = og.get("content")
    if not image_url:
        img = soup.find("img", src=True)
        if img: image_url = urljoin(MBASIC_BASE, img["src"])

    caption = extract_caption(soup)
    return image_url, caption

# ==================== LISTING ====================
def collect_photo_links(page, start_url, max_pages):
    url = to_mbasic(start_url)
    seen = set()
    links = []
    pages_done = 0
    to_visit = [url]

    while to_visit and pages_done < max_pages:
        cur = to_visit.pop(0)
        pages_done += 1
        log(f"Fetching listing page {pages_done}/{max_pages}")
        html = goto_html(page, cur)
        if not html: continue

        soup = BeautifulSoup(html, "html.parser")

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/photo.php" in href or "/photo/" in href:
                abs_url = urljoin(MBASIC_BASE, href)
                fbid = parse_qs(urlparse(abs_url).query).get("fbid", [None])[0]
                key = fbid or abs_url
                if key not in seen:
                    seen.add(key)
                    links.append((key, abs_url))

        # Next page
        for a in soup.find_all("a", href=True):
            if re.search(r'see more|more photos|next', a.get_text(strip=True), re.I):
                next_url = urljoin(MBASIC_BASE, a["href"])
                if next_url not in to_visit:
                    to_visit.append(next_url)
                break

    log(f"Found {len(links)} photo links")
    return links

# ==================== WORKER & MAIN ====================
def process_one(page, fbid, permalink):
    image_url, caption = extract_photo_details(page, permalink)
    if not image_url:
        log(f"[{fbid}] No image found")
        return None

    ext = re.search(r'\.(jpg|jpeg|png|webp)', image_url.lower())
    ext = ext.group(0) if ext else ".jpg"
    filename = f"{fbid}{ext}"
    dest = os.path.join(OUTPUT_DIR, filename)

    # Download
    try:
        resp = page.context.request.get(image_url, timeout=30000)
        if resp.ok:
            with open(dest, "wb") as f:
                f.write(resp.body())
            log(f"✅ Downloaded {filename} | Caption: {caption[:80]}...")
            return [filename, caption, permalink]
    except Exception as e:
        log(f"Download failed {fbid}: {e}")
    return None

def worker(worker_id, tasks, results, lock, storage_path):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--disable-notifications"])
        ctx = browser.new_context(storage_state=storage_path if os.path.exists(storage_path) else None)
        page = ctx.new_page()
        while True:
            try:
                item = tasks.get_nowait()
            except queue.Empty:
                break
            row = process_one(page, *item)
            if row:
                with lock:
                    results.append(row)
            tasks.task_done()
        ctx.close()
        browser.close()

def upload_all_mega():
    files = list(Path(OUTPUT_DIR).glob("*.jp*g")) + list(Path(OUTPUT_DIR).glob("*.png")) + list(Path(OUTPUT_DIR).glob("*.webp"))
    if not files: return
    log(f"Uploading {len(files)} files to Mega")
    subprocess.run(["rclone", "copy", OUTPUT_DIR, f"{MEGA_REMOTE}:{MEGA_FOLDER_NAME}", "--include", "*.{jpg,jpeg,png,webp}", "--transfers", "4"])

def main():
    if not PAGE_PHOTOS_URL:
        log("PAGE_PHOTOS_URL missing")
        sys.exit(1)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Sheets
    sheets_service = None
    tab_name = SHEET_TAB_NAME or f"fb_{datetime.datetime.utcnow().strftime('%Y%m%d')}"
    if SHEET_ID and GOOGLE_TOKEN_JSON:
        try:
            sheets_service = get_sheets_service(GOOGLE_TOKEN_JSON)
            ensure_tab_and_get_id(sheets_service, SHEET_ID, tab_name)
        except Exception as e:
            log(f"Sheet error: {e}")

    # Collect links
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(storage_state=STORAGE_STATE if os.path.exists(STORAGE_STATE) else None)
        page = ctx.new_page()
        photo_links = collect_photo_links(page, PAGE_PHOTOS_URL, MAX_PAGES)
        ctx.close()
        browser.close()

    if not photo_links:
        log("No photos found")
        return

    # Process photos
    tasks = queue.Queue()
    for item in photo_links:
        tasks.put(item)

    results = []
    lock = threading.Lock()
    threads = [threading.Thread(target=worker, args=(i, tasks, results, lock, STORAGE_STATE)) 
               for i in range(CONCURRENCY)]
    for t in threads: t.start()
    for t in threads: t.join()

    # Save results
    if results:
        csv_path = os.path.join("output", f"{FOLDER_NAME}.csv")
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerows([["filename","caption","source_url"]] + results)
        log(f"✅ Saved {len(results)} entries to CSV")

        if sheets_service:
            append_rows(sheets_service, SHEET_ID, tab_name, results)

        upload_all_mega()

if __name__ == "__main__":
    main()
