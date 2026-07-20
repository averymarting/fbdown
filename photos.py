#!/usr/bin/env python3
"""
Headless pipeline for GitHub Actions - Facebook Photos Scraper
Improved caption extraction
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
    "unsupported-interstitial", "get one of the browsers below",
]

# ---------------- config ----------------
PAGE_PHOTOS_URL = os.environ.get("PAGE_PHOTOS_URL", "").strip()
MAX_PAGES = int(os.environ.get("MAX_SCROLLS", os.environ.get("MAX_PAGES", "3")))
FOLDER_NAME = os.environ.get("FOLDER_NAME", "facebook_photos")
STORAGE_STATE = os.environ.get("STORAGE_STATE_FILE", "storage_state.json")
OUTPUT_DIR = os.path.join("output", FOLDER_NAME)
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
RETRY_DELAYS = [5, 15, 30]
CONCURRENCY = int(os.environ.get("CONCURRENCY", "6"))

# Google Sheet
SHEET_ID = os.environ.get("SHEET_ID", "").strip()
GOOGLE_TOKEN_JSON = os.environ.get("GOOGLE_TOKEN_JSON", "").strip()
SHEET_TAB_NAME = os.environ.get("SHEET_TAB_NAME", "").strip()

# Mega
MEGA_REMOTE = os.environ.get("MEGA_REMOTE", "mega").strip()
MEGA_FOLDER_NAME = os.environ.get("MEGA_FOLDER_NAME", "").strip() or FOLDER_NAME

_log_lock = threading.Lock()
def log(msg):
    with _log_lock:
        print(msg, flush=True)

def check_cookie_names(storage_state_path):
    if not os.path.exists(storage_state_path):
        log("⚠️ No storage_state.json found")
        return
    with open(storage_state_path) as f:
        state = json.load(f)
    names = sorted(c.get("name", "") for c in state.get("cookies", []) 
                   if "facebook" in c.get("domain", "") or "fbcdn" in c.get("domain", ""))
    log(f"storage_state has {len(names)} facebook cookie(s)")
    for critical in ("c_user", "xs"):
        if critical not in names:
            log(f"⚠️ MISSING critical cookie '{critical}' — likely logged out")

# ---------------- Google Sheets ----------------
def with_retries(fn, *args, attempts=5, base_delay=5, **kwargs):
    for attempt in range(1, attempts + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if attempt == attempts:
                raise
            delay = base_delay * (2 ** (attempt - 1))
            log(f"Sheets API failed ({e}), retrying in {delay}s [{attempt}/{attempts}]")
            time.sleep(delay)

# ... (keep the rest of Google Sheets functions unchanged: get_sheets_service, ensure_tab_and_get_id, append_rows)

# ---------------- Browser Helpers ----------------
def to_mbasic(url):
    parsed = urlparse(url)
    return urlunparse(parsed._replace(netloc="mbasic.facebook.com", scheme="https"))

def looks_like_wall(html):
    if not html:
        return True
    lowered = html.lower()
    return any(s in lowered for s in WALL_SIGNALS)

def save_debug_html(html, name):
    os.makedirs("output", exist_ok=True)
    path = os.path.join("output", name)
    with open(path, "w", encoding="utf-8") as f:
        f.write(html or "")
    log(f"💾 Saved debug: {path}")

def goto_html(page, url, attempts=MAX_RETRIES):
    for attempt in range(1, attempts + 1):
        try:
            page.goto(url, timeout=30000, wait_until="domcontentloaded")
            page.wait_for_timeout(1200)
            return page.content()
        except Exception as e:
            if attempt == attempts:
                log(f"Failed to load {url}: {e}")
                return None
            time.sleep(RETRY_DELAYS[min(attempt-1, len(RETRY_DELAYS)-1)])
    return None

# ---------------- Improved Caption Extraction ----------------
def extract_caption(soup):
    """Improved caption extractor for mbasic photo pages"""
    # 1. Try the most reliable post text container
    for selector in [
        'div[data-gt*="photo"], div[id^="photo"]',  # common containers
        'div > p', 'div > span', 'div[role="article"]'
    ]:
        for tag in soup.select(selector):
            text = tag.get_text(" ", strip=True)
            if len(text) > 5 and not NAV_TEXT_RE.match(text):
                return text

    # 2. Fallback: longest non-navigation text
    candidates = []
    for tag in soup.find_all(["div", "span", "p"]):
        text = tag.get_text(" ", strip=True)
        if not text or len(text) < 5 or NAV_TEXT_RE.match(text):
            continue
        if re.match(r'^\d+\s*(min|hr|hrs|day|days|week|weeks)', text, re.I):
            continue
        candidates.append(text)

    if candidates:
        return max(candidates, key=len)

    # 3. Last resort: og:description
    og = soup.find("meta", property="og:description")
    if og and og.get("content"):
        text = og["content"].strip()
        if text.lower() not in GENERIC_DESCRIPTIONS:
            return text

    return ""

def extract_photo_details(page, permalink_url):
    html = goto_html(page, permalink_url)
    if not html:
        return None, ""

    soup = BeautifulSoup(html, "html.parser")

    # Save debug on first failure or randomly for inspection
    if len(html) < 5000:
        save_debug_html(html, "debug_photo_page.html")

    # --- Image URL ---
    image_url = None
    for a in soup.find_all("a", href=True):
        if re.search(r'view full size|full size', a.get_text(strip=True), re.I):
            image_url = urljoin(MBASIC_BASE, a["href"])
            break

    if not image_url:
        og_img = soup.find("meta", property="og:image")
        if og_img and og_img.get("content"):
            image_url = og_img["content"]

    if not image_url:
        img = soup.find("img", src=True)
        if img:
            image_url = urljoin(MBASIC_BASE, img["src"])

    # --- Caption ---
    caption = extract_caption(soup)

    return image_url, caption

# ---------------- Rest of the script (unchanged except small improvements) ----------------
def ext_from_url(image_url, default=".jpg"):
    m = re.search(r'\.(jpg|jpeg|png|webp)(\?|$)', image_url.lower())
    return f".{m.group(1)}" if m else default

def download_image(page, image_url, dest_path):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = page.context.request.get(image_url, timeout=30000)
            if resp.ok:
                with open(dest_path, "wb") as f:
                    f.write(resp.body())
                return True
        except Exception as e:
            log(f"Download error: {e}")
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAYS[min(attempt-1, len(RETRY_DELAYS)-1)])
    return False

def process_one(page, fbid, permalink):
    try:
        image_url, caption = extract_photo_details(page, permalink)
    except Exception as e:
        log(f"[{fbid}] scrape error: {e}")
        return None

    if not image_url:
        log(f"[{fbid}] no image URL found")
        return None

    ext = ext_from_url(image_url)
    filename = f"{fbid}{ext}"
    dest_path = os.path.join(OUTPUT_DIR, filename)

    if not download_image(page, image_url, dest_path):
        log(f"[{fbid}] download failed")
        return None

    log(f"[{fbid}] ✅ {filename} | Caption: {caption[:80]}{'...' if len(caption)>80 else ''}")
    return [filename, caption, permalink]

# Worker, upload, and main functions remain the same
# (I kept them unchanged for brevity — just replace the extract functions above)

# ... [paste the rest of your original script from worker() onwards] ...

if __name__ == "__main__":
    main()
