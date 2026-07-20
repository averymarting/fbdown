#!/usr/bin/env python3
"""
Headless pipeline for GitHub Actions (no GUI):
  1. Scrape a Facebook profile "photos" page with Playwright (headless Chromium)
  2. For each photo, grab the full-res image + its caption text
  3. Download the images locally, named after each photo's fbid
  4. Write "file name" + "caption" rows into a specific tab of a Google Sheet
     (the tab is created automatically if it doesn't already exist)
  5. Upload the downloaded images to a Mega.nz folder via rclone

Credential files, written by the workflow from GitHub Secrets:
  - storage_state.json        -> Facebook Playwright storage_state (cookies + origins)
  - GOOGLE_TOKEN_JSON env var -> Google OAuth token (installed-app style: token,
    refresh_token, token_uri, client_id, client_secret, scopes)

    IMPORTANT: unlike the reels pipeline (which only reads sheets), this script
    WRITES to the sheet, so GOOGLE_TOKEN_JSON must have been generated with the
    "https://www.googleapis.com/auth/spreadsheets" scope (not the read-only one).

Mega.nz credentials are handled entirely by rclone's own config file
(~/.config/rclone/rclone.conf), written from the RCLONE_CONF GitHub Secret.
This script never sees the Mega password.

SECURITY NOTE: never hardcode or print the contents of GOOGLE_TOKEN_JSON,
storage_state.json, or rclone.conf. They are read from disk/env only.
"""
import os
import re
import sys
import csv
import json
import time
import subprocess
from pathlib import Path

from playwright.sync_api import sync_playwright

IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.webp'}

# ---- config from environment / workflow inputs ----
PHOTOS_URL        = os.environ.get("PHOTOS_URL", "").strip()
MAX_SCROLLS        = int(os.environ.get("MAX_SCROLLS", "5"))
FOLDER_NAME        = os.environ.get("FOLDER_NAME", "facebook_photos")
STORAGE_STATE      = os.environ.get("STORAGE_STATE_FILE", "storage_state.json")
OUTPUT_DIR         = os.path.join("output", FOLDER_NAME)
BASE_SLEEP         = float(os.environ.get("BASE_SLEEP", "1.5"))
MAX_RETRIES        = int(os.environ.get("MAX_RETRIES", "3"))
RETRY_DELAYS       = [10, 30, 60]

# Google Sheet destination
SHEET_ID           = os.environ.get("SHEET_ID", "").strip()
TAB_NAME            = os.environ.get("TAB_NAME", "").strip()
GOOGLE_TOKEN_JSON  = os.environ.get("GOOGLE_TOKEN_JSON", "").strip()

# Mega / rclone config
MEGA_REMOTE        = os.environ.get("MEGA_REMOTE", "mega").strip()
MEGA_FOLDER_NAME   = os.environ.get("MEGA_FOLDER_NAME", "").strip() or FOLDER_NAME

# UI chrome text we never want to mistake for a caption
CAPTION_BLOCKLIST = {
    "like", "comment", "share", "reply", "see more", "see translation",
    "most relevant", "write a comment", "all reactions", "public", "friends",
}


def log(msg):
    print(msg, flush=True)


# ---------------- Google Sheets ----------------
def get_sheets_service(token_json_str):
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build

    info = json.loads(token_json_str)
    creds = Credentials(
        token=info.get("token"),
        refresh_token=info.get("refresh_token"),
        token_uri=info.get("token_uri", "https://oauth2.googleapis.com/token"),
        client_id=info.get("client_id"),
        client_secret=info.get("client_secret"),
        scopes=info.get("scopes") or ["https://www.googleapis.com/auth/spreadsheets"],
    )
    if not creds.valid:
        creds.refresh(Request())
    return build("sheets", "v4", credentials=creds)


def ensure_tab_exists(service, sheet_id, tab_name):
    meta = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
    titles = [s["properties"]["title"] for s in meta.get("sheets", [])]
    if tab_name in titles:
        log(f"tab '{tab_name}' already exists")
        return
    log(f"creating tab '{tab_name}'")
    service.spreadsheets().batchUpdate(
        spreadsheetId=sheet_id,
        body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]},
    ).execute()
    # header row
    service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=f"'{tab_name}'!A1:B1",
        valueInputOption="RAW",
        body={"values": [["File Name", "Caption"]]},
    ).execute()


def append_rows(service, sheet_id, tab_name, rows):
    if not rows:
        return
    service.spreadsheets().values().append(
        spreadsheetId=sheet_id,
        range=f"'{tab_name}'!A:B",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()
    log(f"wrote {len(rows)} row(s) to tab '{tab_name}'")


# ---------------- scraping (Playwright) ----------------
def collect_photo_links(page, url, max_scrolls):
    """Scroll the profile photos grid and collect unique photo permalink URLs."""
    page.goto(url, timeout=60000)
    page.wait_for_timeout(6000)

    try:
        page.locator("div[aria-label='Close'], div[role='button']").first.click(timeout=3000)
        page.wait_for_timeout(2000)
    except Exception:
        pass

    seen = set()
    links = []
    no_progress = 0
    scroll_num = 0
    last_height = page.evaluate("document.body.scrollHeight")

    log(f"   scrolling up to {max_scrolls} time(s) to collect photo links...")
    while scroll_num < max_scrolls and no_progress < 5:
        scroll_num += 1
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(2500)

        hrefs = page.eval_on_selector_all(
            "a[href*='/photo.php'], a[href*='/photo/?']",
            "els => els.map(e => e.href)"
        )
        new_added = 0
        for href in hrefs:
            m = re.search(r'fbid=(\d+)', href)
            if not m:
                continue
            fbid = m.group(1)
            if fbid not in seen:
                seen.add(fbid)
                links.append((fbid, href.split('&set=')[0]))
                new_added += 1

        cur_height = page.evaluate("document.body.scrollHeight")
        if new_added == 0 and cur_height == last_height:
            no_progress += 1
        else:
            no_progress = 0
        last_height = cur_height
        log(f"   scroll {scroll_num}/{max_scrolls} -> {len(links)} photo(s) (+{new_added})")

    return links


def extract_caption(page):
    """Best-effort caption extraction. FB's markup is obfuscated and changes
    often, so this grabs the first substantial dir='auto' text block and
    filters out obvious UI chrome. Tune the selector list below if captions
    come back empty or wrong for your page's current layout."""
    try:
        texts = page.eval_on_selector_all(
            "div[dir='auto'], span[dir='auto']",
            "els => els.map(e => e.innerText.trim()).filter(t => t.length > 0)"
        )
    except Exception:
        texts = []

    for t in texts:
        low = t.lower().strip()
        if low in CAPTION_BLOCKLIST:
            continue
        if re.match(r'^\d+[hdwm]$', low):          # "3h", "2d" timestamps
            continue
        if re.match(r'^[\d,]+$', low):              # bare reaction counts
            continue
        if len(t) < 3:
            continue
        return t
    return ""


def extract_image_url(page):
    """Pick the largest / highest-res <img> on the photo page (the actual
    photo, not thumbnails or profile pictures)."""
    try:
        srcs = page.eval_on_selector_all(
            "img",
            """els => els
                .filter(e => e.naturalWidth > 400 && e.src && e.src.includes('scontent'))
                .sort((a, b) => (b.naturalWidth * b.naturalHeight) - (a.naturalWidth * a.naturalHeight))
                .map(e => e.src)"""
        )
    except Exception:
        srcs = []
    return srcs[0] if srcs else None


def download_image(context, url, dest_path):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = context.request.get(url, timeout=30000)
            if resp.ok:
                dest_path.write_bytes(resp.body())
                return True
            log(f"   download failed (status {resp.status}), attempt {attempt}")
        except Exception as e:
            log(f"   download error: {e}, attempt {attempt}")
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAYS[min(attempt - 1, len(RETRY_DELAYS) - 1)])
    return False


# ---------------- upload (Mega.nz via rclone) ----------------
def upload_all_mega():
    if not MEGA_FOLDER_NAME:
        log("no MEGA_FOLDER_NAME provided, skipping upload")
        return

    files = [f for f in sorted(Path(OUTPUT_DIR).iterdir())
             if f.is_file() and f.suffix.lower() in IMAGE_EXTENSIONS]

    if not files:
        log("no image files found to upload")
        return

    dest = f"{MEGA_REMOTE}:{MEGA_FOLDER_NAME}"
    log(f"uploading {len(files)} image(s) to Mega folder '{dest}' via rclone")

    args = [
        "rclone", "copy", OUTPUT_DIR, dest,
        "--include", "*.{jpg,jpeg,png,webp}",
        "--transfers", "4",
        "--retries", "3",
        "--low-level-retries", "5",
        "-v",
    ]
    proc = subprocess.run(args, capture_output=True, text=True)
    if proc.stdout:
        log(proc.stdout[-4000:])
    if proc.returncode != 0:
        if proc.stderr:
            log(proc.stderr[-4000:])
        log(f"rclone upload failed (exit {proc.returncode})")
    else:
        log("upload to Mega complete")


# ---------------- main ----------------
def main():
    if not PHOTOS_URL:
        log("PHOTOS_URL is required, exiting")
        sys.exit(1)
    if not SHEET_ID or not TAB_NAME:
        log("SHEET_ID and TAB_NAME are both required, exiting")
        sys.exit(1)
    if not GOOGLE_TOKEN_JSON:
        log("GOOGLE_TOKEN_JSON is required, exiting")
        sys.exit(1)

    sheets_service = get_sheets_service(GOOGLE_TOKEN_JSON)
    ensure_tab_exists(sheets_service, SHEET_ID, TAB_NAME)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    rows = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--disable-notifications"])
        state_arg = STORAGE_STATE if os.path.exists(STORAGE_STATE) else None
        context = browser.new_context(storage_state=state_arg, viewport={"width": 1400, "height": 1000})
        page = context.new_page()

        photo_links = collect_photo_links(page, PHOTOS_URL, MAX_SCROLLS)
        log(f"total unique photos found: {len(photo_links)}")

        for idx, (fbid, url) in enumerate(photo_links, 1):
            log(f"[{idx}/{len(photo_links)}] opening {url}")
            try:
                page.goto(url, timeout=60000)
                page.wait_for_timeout(3000)
            except Exception as e:
                log(f"   failed to open photo page: {e}")
                continue

            img_url = extract_image_url(page)
            caption = extract_caption(page)

            if not img_url:
                log("   no image found, skipping")
                continue

            ext = ".jpg"
            m = re.search(r'\.(jpg|jpeg|png|webp)', img_url.lower())
            if m:
                ext = "." + m.group(1)
            filename = f"photo_{fbid}{ext}"
            dest_path = Path(OUTPUT_DIR) / filename

            ok = download_image(context, img_url, dest_path)
            if not ok:
                log(f"   giving up on {filename}")
                continue

            log(f"   saved {filename} | caption: {caption[:60]!r}")
            rows.append([filename, caption])
            time.sleep(BASE_SLEEP)

        context.close()
        browser.close()

    if rows:
        append_rows(sheets_service, SHEET_ID, TAB_NAME, rows)
    else:
        log("no rows to write, nothing downloaded")

    # also keep a local CSV copy as a run artifact
    csv_path = os.path.join("output", f"{FOLDER_NAME}.csv")
    with open(csv_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["File Name", "Caption"])
        w.writerows(rows)
    log(f"local csv saved: {csv_path}")

    upload_all_mega()


if __name__ == "__main__":
    main()
