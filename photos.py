#!/usr/bin/env python3
"""
Headless pipeline for GitHub Actions (no GUI):
  1. Scrape a Facebook Page's Photos tab with Playwright (headless Chromium)
     -> collect each photo's image URL + its caption text
  2. Download every photo (requests, using the Playwright session's cookies)
  3. Log filename + caption into a (new or existing) tab of a Google Sheet
  4. Upload downloaded images to a Mega.nz folder via rclone

Credential files, written by the workflow from GitHub Secrets:
  - storage_state.json        -> Facebook Playwright storage_state (cookies + origins)
  - GOOGLE_TOKEN_JSON env var -> Google OAuth token (installed-app style: token,
    refresh_token, token_uri, client_id, client_secret, scopes)
    NOTE: writing a new tab + rows requires the FULL
    "https://www.googleapis.com/auth/spreadsheets" scope, not spreadsheets.readonly.

Mega.nz credentials are handled entirely by rclone's own config file
(~/.config/rclone/rclone.conf), which the workflow writes from the
RCLONE_CONF GitHub Secret. This script never sees the Mega password.

All run parameters come from environment variables set by the workflow_dispatch inputs.

SECURITY NOTE: never hardcode or print the contents of GOOGLE_TOKEN_JSON,
storage_state.json, or rclone.conf. They are read from disk/env only.
"""
import os
import re
import sys
import csv
import json
import time
import datetime
import subprocess
from pathlib import Path

import requests
from playwright.sync_api import sync_playwright

IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.webp'}

# ---- config from environment / workflow inputs ----
PAGE_PHOTOS_URL   = os.environ.get("PAGE_PHOTOS_URL", "").strip()
MAX_SCROLLS        = int(os.environ.get("MAX_SCROLLS", "3"))
FOLDER_NAME        = os.environ.get("FOLDER_NAME", "facebook_photos")
STORAGE_STATE      = os.environ.get("STORAGE_STATE_FILE", "storage_state.json")
OUTPUT_DIR         = os.path.join("output", FOLDER_NAME)
MAX_RETRIES        = int(os.environ.get("MAX_RETRIES", "3"))
RETRY_DELAYS       = [10, 30, 60]
MAX_CONSEC_FAIL    = 5
COOLDOWN_SLEEP     = 90
PER_PHOTO_SLEEP    = int(os.environ.get("PER_PHOTO_SLEEP", "2"))

# Google Sheet destination
SHEET_ID           = os.environ.get("SHEET_ID", "").strip()
GOOGLE_TOKEN_JSON  = os.environ.get("GOOGLE_TOKEN_JSON", "").strip()
SHEET_TAB_NAME     = os.environ.get("SHEET_TAB_NAME", "").strip()

# Mega / rclone config
MEGA_REMOTE        = os.environ.get("MEGA_REMOTE", "mega").strip()
MEGA_FOLDER_NAME   = os.environ.get("MEGA_FOLDER_NAME", "").strip() or FOLDER_NAME


def log(msg):
    print(msg, flush=True)


# ---------------- Google Sheets (write) ----------------
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


def ensure_tab_and_get_id(service, sheet_id, tab_name):
    """Creates the tab if it doesn't exist yet. Returns the sheetId (int)."""
    meta = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
    for s in meta.get("sheets", []):
        if s["properties"]["title"] == tab_name:
            return s["properties"]["sheetId"]

    log(f"tab '{tab_name}' not found, creating it")
    resp = service.spreadsheets().batchUpdate(
        spreadsheetId=sheet_id,
        body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]},
    ).execute()
    new_sheet_id = resp["replies"][0]["addSheet"]["properties"]["sheetId"]

    # header row
    service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=f"'{tab_name}'!A1:C1",
        valueInputOption="RAW",
        body={"values": [["filename", "caption", "source_url"]]},
    ).execute()
    return new_sheet_id


def append_rows(service, sheet_id, tab_name, rows):
    """rows: list of [filename, caption, source_url]"""
    if not rows:
        return
    service.spreadsheets().values().append(
        spreadsheetId=sheet_id,
        range=f"'{tab_name}'!A:C",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()
    log(f"appended {len(rows)} row(s) to tab '{tab_name}'")


# ---------------- scraping (Playwright) ----------------
def collect_photo_links(page, url, max_scrolls):
    page.goto(url, timeout=60000)
    page.wait_for_timeout(8000)

    try:
        page.locator("div[aria-label='Close'], div[role='button']").first.click(timeout=3000)
        page.wait_for_timeout(2000)
    except Exception:
        pass

    seen = set()
    links = []
    scroll_num = 0
    no_progress = 0
    prev_count = 0
    last_height = page.evaluate("document.body.scrollHeight")

    log(f"   scrolling up to {max_scrolls} time(s)...")

    while scroll_num < max_scrolls and no_progress < 5:
        scroll_num += 1
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(3000)
        for _ in range(6):
            page.evaluate("window.scrollBy(0, 900)")
            page.wait_for_timeout(700)
        page.wait_for_timeout(3000)

        cur_height = page.evaluate("document.body.scrollHeight")
        hrefs = page.eval_on_selector_all("a[href*='/photo']", "els => els.map(e => e.href)")

        new_added = 0
        for href in hrefs:
            if not href:
                continue
            clean = re.sub(r'&?set=[^&]*', '', href)
            clean = re.sub(r'&?type=[^&]*', '', clean)
            fbid_match = re.search(r'fbid=(\d+)', clean)
            key = fbid_match.group(1) if fbid_match else clean
            if key not in seen:
                seen.add(key)
                links.append((key, href))
                new_added += 1

        if len(links) == prev_count and cur_height == last_height:
            no_progress += 1
        else:
            no_progress = 0
        prev_count = len(links)
        last_height = cur_height

        log(f"   scroll {scroll_num}/{max_scrolls} -> {len(links)} photos (+{new_added})")

    return links


def extract_photo_details(page, fbid, permalink_url):
    """Visit a single photo permalink and pull the full-res image URL + caption."""
    page.goto(permalink_url, timeout=60000)
    page.wait_for_timeout(3500)

    # full-res image: og:image meta tag is the most reliable source
    image_url = None
    try:
        image_url = page.eval_on_selector(
            "meta[property='og:image']", "el => el.content"
        )
    except Exception:
        pass
    if not image_url:
        try:
            image_url = page.eval_on_selector(
                "img[data-visualcompletion='media-vc-image']", "el => el.src"
            )
        except Exception:
            pass

    # caption: prefer the real og:description / meta description, fall back to
    # the auto-alt text on the image, fall back to empty string
    caption = None
    try:
        caption = page.eval_on_selector(
            "meta[property='og:description']", "el => el.content"
        )
    except Exception:
        pass
    if not caption:
        try:
            caption = page.eval_on_selector(
                "img[data-visualcompletion='media-vc-image']", "el => el.alt"
            )
        except Exception:
            pass
    caption = (caption or "").strip()

    return image_url, caption


# ---------------- download ----------------
def build_requests_session(storage_state_path):
    sess = requests.Session()
    sess.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    })
    if os.path.exists(storage_state_path):
        with open(storage_state_path) as f:
            state = json.load(f)
        for c in state.get("cookies", []):
            domain = c.get("domain", "")
            if "facebook.com" in domain or "fbcdn.net" in domain:
                sess.cookies.set(c.get("name", ""), c.get("value", ""), domain=domain)
    return sess


def download_image(sess, image_url, dest_path):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = sess.get(image_url, timeout=30)
            if r.status_code == 200 and r.content:
                with open(dest_path, "wb") as f:
                    f.write(r.content)
                return True
            log(f"   http {r.status_code} for {image_url}")
        except Exception as e:
            log(f"   download error: {e}")
        if attempt < MAX_RETRIES:
            delay = RETRY_DELAYS[min(attempt - 1, len(RETRY_DELAYS) - 1)]
            time.sleep(delay)
    return False


def ext_from_url(image_url, default=".jpg"):
    m = re.search(r'\.(jpg|jpeg|png|webp)(\?|$)', image_url.lower())
    return f".{m.group(1)}" if m else default


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
    if not PAGE_PHOTOS_URL:
        log("PAGE_PHOTOS_URL is required, exiting")
        sys.exit(1)

    tab_name = SHEET_TAB_NAME or f"Photos_{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

    sheets_service = None
    if SHEET_ID and GOOGLE_TOKEN_JSON:
        try:
            sheets_service = get_sheets_service(GOOGLE_TOKEN_JSON)
            ensure_tab_and_get_id(sheets_service, SHEET_ID, tab_name)
            log(f"logging results to sheet tab '{tab_name}'")
        except Exception as e:
            log(f"could not set up Google Sheet tab: {e}")
            sheets_service = None
    else:
        log("SHEET_ID / GOOGLE_TOKEN_JSON not both set, skipping sheet logging")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    sess = build_requests_session(STORAGE_STATE)

    sheet_rows = []
    csv_rows = []
    consec_fail = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--disable-notifications"])
        state_arg = STORAGE_STATE if os.path.exists(STORAGE_STATE) else None
        context = browser.new_context(storage_state=state_arg, viewport={"width": 1400, "height": 1000})
        page = context.new_page()

        log(f"scraping {PAGE_PHOTOS_URL}")
        photo_links = collect_photo_links(page, PAGE_PHOTOS_URL, MAX_SCROLLS)
        log(f"found {len(photo_links)} photo permalink(s)")

        for i, (fbid, permalink) in enumerate(photo_links, 1):
            if consec_fail >= MAX_CONSEC_FAIL:
                log(f"cooling down after {consec_fail} consecutive failures...")
                time.sleep(COOLDOWN_SLEEP)
                consec_fail = 0

            log(f"[{i}/{len(photo_links)}] {permalink}")
            try:
                image_url, caption = extract_photo_details(page, fbid, permalink)
            except Exception as e:
                log(f"   scrape error: {e}")
                consec_fail += 1
                continue

            if not image_url:
                log("   no image URL found, skipping")
                consec_fail += 1
                continue

            ext = ext_from_url(image_url)
            filename = f"{fbid}{ext}"
            dest_path = os.path.join(OUTPUT_DIR, filename)

            if download_image(sess, image_url, dest_path):
                consec_fail = 0
                sheet_rows.append([filename, caption, permalink])
                csv_rows.append([filename, caption, permalink])
            else:
                consec_fail += 1
                log(f"   gave up on {permalink}")

            time.sleep(PER_PHOTO_SLEEP)

        if state_arg:
            context.storage_state(path=STORAGE_STATE)
        context.close()
        browser.close()

    # local csv backup regardless of sheet outcome
    if csv_rows:
        csv_path = os.path.join("output", f"{FOLDER_NAME}.csv")
        with open(csv_path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["filename", "caption", "source_url"])
            w.writerows(csv_rows)
        log(f"local csv saved: {csv_path} ({len(csv_rows)} rows)")

    if sheets_service and sheet_rows:
        try:
            append_rows(sheets_service, SHEET_ID, tab_name, sheet_rows)
        except Exception as e:
            log(f"failed to append rows to sheet: {e}")

    if not csv_rows:
        log("nothing downloaded, skipping Mega upload")
        return

    upload_all_mega()


if __name__ == "__main__":
    main()
