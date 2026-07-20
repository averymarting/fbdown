#!/usr/bin/env python3
"""
Headless pipeline for GitHub Actions (no GUI, no headless browser):
  1. Scrape a Facebook Page's Photos tab via mbasic.facebook.com (server-rendered
     plain HTML -- fast, and og:description on this version actually contains
     the real post caption instead of the client-rendered auto-alt-text)
  2. Download every photo (parallelized with a thread pool)
  3. Log filename + caption into a (new or existing) tab of a Google Sheet
  4. Upload downloaded images to a Mega.nz folder via rclone

Credential files, written by the workflow from GitHub Secrets:
  - storage_state.json        -> Facebook Playwright storage_state (cookies + origins)
                                  (still used here just as a cookie source for requests)
  - GOOGLE_TOKEN_JSON env var -> Google OAuth token (installed-app style: token,
    refresh_token, token_uri, client_id, client_secret, scopes)
    NOTE: writing a new tab + rows requires the FULL
    "https://www.googleapis.com/auth/spreadsheets" scope, not spreadsheets.readonly.

Mega.nz credentials are handled entirely by rclone's own config file
(~/.config/rclone/rclone.conf), which the workflow writes from the
RCLONE_CONF GitHub Secret. This script never sees the Mega password.

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
from urllib.parse import urljoin, urlparse, urlunparse, parse_qs
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.webp'}
MBASIC_BASE = "https://mbasic.facebook.com"

GENERIC_DESCRIPTIONS = {
    "", "see posts, photos and more on facebook.",
}

NAV_TEXT_RE = re.compile(
    r'^(like|comment|share|full size|view full size|comments?|write a comment|'
    r'see more|see less|reply|\d+[\d,.]*\s*(likes?|comments?|shares?)|'
    r'photo|options|report)$', re.I
)

# ---- config from environment / workflow inputs ----
PAGE_PHOTOS_URL   = os.environ.get("PAGE_PHOTOS_URL", "").strip()
MAX_PAGES         = int(os.environ.get("MAX_SCROLLS", os.environ.get("MAX_PAGES", "3")))
FOLDER_NAME       = os.environ.get("FOLDER_NAME", "facebook_photos")
STORAGE_STATE     = os.environ.get("STORAGE_STATE_FILE", "storage_state.json")
OUTPUT_DIR        = os.path.join("output", FOLDER_NAME)
MAX_RETRIES       = int(os.environ.get("MAX_RETRIES", "3"))
RETRY_DELAYS      = [5, 15, 30]
CONCURRENCY       = int(os.environ.get("CONCURRENCY", "6"))

# Google Sheet destination
SHEET_ID          = os.environ.get("SHEET_ID", "").strip()
GOOGLE_TOKEN_JSON = os.environ.get("GOOGLE_TOKEN_JSON", "").strip()
SHEET_TAB_NAME    = os.environ.get("SHEET_TAB_NAME", "").strip()

# Mega / rclone config
MEGA_REMOTE       = os.environ.get("MEGA_REMOTE", "mega").strip()
MEGA_FOLDER_NAME  = os.environ.get("MEGA_FOLDER_NAME", "").strip() or FOLDER_NAME


def log(msg):
    print(msg, flush=True)


# ---------------- Google Sheets (write, retried/chunked) ----------------
def with_retries(fn, *args, attempts=5, base_delay=5, **kwargs):
    last_err = None
    for attempt in range(1, attempts + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            last_err = e
            if attempt == attempts:
                break
            delay = base_delay * (2 ** (attempt - 1))
            log(f"   Sheets API call failed ({e}), retrying in {delay}s [{attempt}/{attempts}]")
            time.sleep(delay)
    raise last_err


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
    meta = with_retries(service.spreadsheets().get(spreadsheetId=sheet_id).execute)
    for s in meta.get("sheets", []):
        if s["properties"]["title"] == tab_name:
            return s["properties"]["sheetId"]

    log(f"tab '{tab_name}' not found, creating it")
    resp = with_retries(
        service.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]},
        ).execute
    )
    new_sheet_id = resp["replies"][0]["addSheet"]["properties"]["sheetId"]

    with_retries(
        service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=f"'{tab_name}'!A1:C1",
            valueInputOption="RAW",
            body={"values": [["filename", "caption", "source_url"]]},
        ).execute
    )
    return new_sheet_id


def append_rows(service, sheet_id, tab_name, rows, chunk_size=40):
    if not rows:
        return
    total = 0
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i + chunk_size]
        with_retries(
            service.spreadsheets().values().append(
                spreadsheetId=sheet_id,
                range=f"'{tab_name}'!A:C",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": chunk},
            ).execute
        )
        total += len(chunk)
        log(f"   appended {total}/{len(rows)} row(s) so far")
    log(f"appended {total} row(s) to tab '{tab_name}'")


# ---------------- HTTP session (cookies from storage_state) ----------------
def build_session(storage_state_path):
    sess = requests.Session()
    sess.headers.update({
        "User-Agent": "Mozilla/5.0 (Linux; Android 12) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0 Mobile Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    })
    if os.path.exists(storage_state_path):
        with open(storage_state_path) as f:
            state = json.load(f)
        count = 0
        for c in state.get("cookies", []):
            domain = c.get("domain", "")
            if "facebook.com" in domain or "fbcdn.net" in domain:
                sess.cookies.set(c.get("name", ""), c.get("value", ""), domain=domain)
                count += 1
        log(f"loaded {count} facebook cookie(s) into session")
    else:
        log("no storage_state.json found -- requests will be unauthenticated")
    return sess


def to_mbasic(url):
    """Rewrite a www./web./m. facebook.com URL to its mbasic.facebook.com equivalent."""
    parsed = urlparse(url)
    return urlunparse(parsed._replace(netloc="mbasic.facebook.com", scheme="https"))


def looks_like_login_wall(html):
    if not html:
        return True
    lowered = html.lower()
    signals = [
        "log into facebook", "log in to facebook", "you must log in",
        "id=\"login_form\"", "name=\"login\"", "checkpoint",
        "session has expired", "temporarily blocked",
    ]
    return any(s in lowered for s in signals)


def save_debug_html(html, name):
    os.makedirs("output", exist_ok=True)
    path = os.path.join("output", name)
    with open(path, "w", encoding="utf-8") as f:
        f.write(html or "")
    log(f"   saved debug html to {path}")


def get_html(sess, url, attempts=MAX_RETRIES):
    last_err = None
    for attempt in range(1, attempts + 1):
        try:
            r = sess.get(url, timeout=25)
            if r.status_code == 200:
                return r.text
            last_err = f"http {r.status_code}"
        except Exception as e:
            last_err = str(e)
        if attempt < attempts:
            time.sleep(RETRY_DELAYS[min(attempt - 1, len(RETRY_DELAYS) - 1)])
    log(f"   failed to fetch {url}: {last_err}")
    return None


# ---------------- listing (mbasic photos tab, paginated) ----------------
def find_photo_hrefs(soup):
    found = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/photo.php" in href or "/photo/" in href:
            found.append(href)
    return found


def find_album_hrefs(soup):
    found = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/media_set" in href or re.search(r'set=a\.', href):
            found.append(href)
    return found


def collect_photo_links(sess, start_url, max_pages):
    url = to_mbasic(start_url)
    seen = set()
    links = []
    checked_login_wall = False

    pages_to_visit = [url]
    albums_to_visit = []
    visited = set()
    pages_done = 0

    while pages_to_visit and pages_done < max_pages:
        cur = pages_to_visit.pop(0)
        if cur in visited:
            continue
        visited.add(cur)
        pages_done += 1

        log(f"   fetching listing page {pages_done}/{max_pages}: {cur}")
        html = get_html(sess, cur)

        if not checked_login_wall:
            checked_login_wall = True
            save_debug_html(html, "debug_listing_page1.html")
            if looks_like_login_wall(html):
                log("   !! this looks like a login wall / checkpoint page, not the photos tab.")
                log("   !! your storage_state.json cookies are likely expired or incomplete "
                     "(only a handful of cookies loaded -- need c_user, xs, fr, datr at minimum).")
                log("   !! re-export storage_state.json from a fresh, fully logged-in Facebook "
                     "session and re-run. see output/debug_listing_page1.html for what was returned.")

        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")

        new_added = 0
        for href in find_photo_hrefs(soup):
            abs_url = urljoin(MBASIC_BASE, href)
            qs = parse_qs(urlparse(abs_url).query)
            fbid = qs.get("fbid", [None])[0]
            key = fbid or abs_url
            if key not in seen:
                seen.add(key)
                links.append((key, abs_url))
                new_added += 1

        # sk=photos on mbasic often shows albums, not a flat photo list -- queue them up
        for href in find_album_hrefs(soup):
            abs_url = urljoin(MBASIC_BASE, href)
            if abs_url not in visited and abs_url not in albums_to_visit:
                albums_to_visit.append(abs_url)

        log(f"   page: +{new_added} photo(s), total {len(links)}, "
            f"{len(albums_to_visit)} album(s) queued")

        # pagination within the current page (see more / next)
        next_href = None
        for a in soup.find_all("a", href=True):
            text = a.get_text(strip=True)
            if re.search(r'see more|more photos|^next$', text, re.I):
                next_href = a["href"]
                break
        if next_href:
            pages_to_visit.insert(0, urljoin(MBASIC_BASE, next_href))
        elif albums_to_visit:
            pages_to_visit.append(albums_to_visit.pop(0))
        else:
            log("   no further pagination or album link found, stopping")

    return links


# ---------------- photo detail (caption + image URL) ----------------
def extract_photo_details(sess, permalink_url):
    html = get_html(sess, permalink_url)
    if not html:
        return None, ""
    soup = BeautifulSoup(html, "html.parser")

    # --- image URL ---
    image_url = None
    for a in soup.find_all("a", href=True):
        if re.search(r'view full size|full size', a.get_text(strip=True), re.I):
            candidate = urljoin(MBASIC_BASE, a["href"])
            image_url = candidate
            break
    if not image_url:
        og_image = soup.find("meta", property="og:image")
        if og_image and og_image.get("content"):
            image_url = og_image["content"]
    if not image_url:
        img_tag = soup.find("img", src=True)
        if img_tag:
            image_url = urljoin(MBASIC_BASE, img_tag["src"])

    # --- caption ---
    caption = ""
    og_desc = soup.find("meta", property="og:description")
    if og_desc and og_desc.get("content"):
        text = og_desc["content"].strip()
        if text.lower() not in GENERIC_DESCRIPTIONS:
            caption = text

    if not caption:
        # fall back: scan visible text blocks for the longest non-nav chunk
        candidates = []
        for tag in soup.find_all(["div", "span", "p"]):
            text = tag.get_text(" ", strip=True)
            if not text or len(text) < 3:
                continue
            if NAV_TEXT_RE.match(text):
                continue
            if re.match(r'^\d+\s*(min|hr|hrs|day|days|week|weeks)s?\s*(ago)?$', text, re.I):
                continue
            candidates.append(text)
        if candidates:
            caption = max(candidates, key=len)

    return image_url, caption


# ---------------- download ----------------
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
            time.sleep(RETRY_DELAYS[min(attempt - 1, len(RETRY_DELAYS) - 1)])
    return False


def ext_from_url(image_url, default=".jpg"):
    m = re.search(r'\.(jpg|jpeg|png|webp)(\?|$)', image_url.lower())
    return f".{m.group(1)}" if m else default


def process_one(sess, fbid, permalink):
    try:
        image_url, caption = extract_photo_details(sess, permalink)
    except Exception as e:
        log(f"   [{fbid}] scrape error: {e}")
        return None
    if not image_url:
        log(f"   [{fbid}] no image URL found, skipping")
        return None

    ext = ext_from_url(image_url)
    filename = f"{fbid}{ext}"
    dest_path = os.path.join(OUTPUT_DIR, filename)

    if not download_image(sess, image_url, dest_path):
        log(f"   [{fbid}] gave up downloading")
        return None

    log(f"   [{fbid}] ok -> {filename}")
    return [filename, caption, permalink]


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
    sess = build_session(STORAGE_STATE)

    log(f"scraping {PAGE_PHOTOS_URL} via mbasic")
    photo_links = collect_photo_links(sess, PAGE_PHOTOS_URL, MAX_PAGES)
    log(f"found {len(photo_links)} photo permalink(s)")

    if not photo_links:
        log("no photos found, exiting")
        return

    rows = []
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        futures = {
            pool.submit(process_one, sess, fbid, permalink): fbid
            for fbid, permalink in photo_links
        }
        done = 0
        for future in as_completed(futures):
            done += 1
            result = future.result()
            if result:
                rows.append(result)
            if done % 10 == 0 or done == len(futures):
                log(f"progress: {done}/{len(futures)} processed, {len(rows)} succeeded")

    if rows:
        csv_path = os.path.join("output", f"{FOLDER_NAME}.csv")
        with open(csv_path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["filename", "caption", "source_url"])
            w.writerows(rows)
        log(f"local csv saved: {csv_path} ({len(rows)} rows)")

    if sheets_service and rows:
        try:
            append_rows(sheets_service, SHEET_ID, tab_name, rows)
        except Exception as e:
            log(f"failed to append rows to sheet: {e}")

    if not rows:
        log("nothing downloaded, skipping Mega upload")
        return

    upload_all_mega()


if __name__ == "__main__":
    main()
