#!/usr/bin/env python3
"""
Headless pipeline for GitHub Actions:
  1. Scrape a Facebook Page's Photos tab via mbasic.facebook.com, driven by a
     real headless Chromium (Playwright) -- mbasic's server-rendered HTML gives
     accurate captions (og:description) and is light/fast to parse, but plain
     `requests` traffic gets bounced by Facebook's bot check regardless of
     User-Agent, so a real browser engine is required to fetch it.
  2. A small pool of async workers shares ONE browser process (each worker
     just gets its own lightweight context/page) and processes photos
     concurrently.
  3. Log filename + caption into a (new or existing) tab of a Google Sheet.
  4. Upload downloaded images to a Mega.nz folder via rclone.

Credential files, written by the workflow from GitHub Secrets:
  - storage_state.json        -> Facebook Playwright storage_state (cookies + origins)
  - GOOGLE_TOKEN_JSON env var -> Google OAuth token (needs full
    "https://www.googleapis.com/auth/spreadsheets" scope to create tabs/write rows)

Mega.nz credentials are handled entirely by rclone's own config file
(~/.config/rclone/rclone.conf), written by the workflow from the RCLONE_CONF secret.
This script never sees the Mega password.

SECURITY NOTE: never hardcode or print the contents of GOOGLE_TOKEN_JSON,
storage_state.json, or rclone.conf. They are read from disk/env only.

RECOVERY MODE (replaces the old separate push_csv_to_sheet.py):
  If a run's images/Mega upload succeeded but the Sheets append step failed
  (e.g. transient SSL error), you don't need to re-scrape. Just run:

      python photos.py --push-csv

  which reads the CSV that was already saved to output/<FOLDER_NAME>.csv (or
  the path in CSV_PATH) and appends it to the sheet, skipping scraping,
  downloading, and the Mega upload entirely. Uses the same SHEET_ID /
  SHEET_TAB_NAME / GOOGLE_TOKEN_JSON env vars as a normal run.
"""
import os
import re
import sys
import csv
import json
import time
import asyncio
import datetime
import subprocess
from pathlib import Path
from urllib.parse import urljoin, urlparse, urlunparse, parse_qs

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.webp'}
MBASIC_BASE = "https://mbasic.facebook.com"

GENERIC_DESCRIPTIONS = {
    "", "see posts, photos and more on facebook.",
}

# Facebook's own auto-generated alt-text for photos it can't/won't render on
# mbasic (e.g. "May be an image of one or more people and text", "No photo
# description available."). These show up as literal visible text on mbasic
# and used to get scooped up (and even concatenated together) by the caption
# fallback scan below -- explicitly exclude them.
ALT_TEXT_RE = re.compile(
    r'^(may be (a|an)\b.*|no photo description available\.?)$', re.I
)

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

# ---- config from environment / workflow inputs ----
PAGE_PHOTOS_URL   = os.environ.get("PAGE_PHOTOS_URL", "").strip()
MAX_PAGES         = int(os.environ.get("MAX_SCROLLS", os.environ.get("MAX_PAGES", "3")))
FOLDER_NAME       = os.environ.get("FOLDER_NAME", "facebook_photos")
STORAGE_STATE     = os.environ.get("STORAGE_STATE_FILE", "storage_state.json")
OUTPUT_DIR        = os.path.join("output", FOLDER_NAME)
MAX_RETRIES       = int(os.environ.get("MAX_RETRIES", "3"))
RETRY_DELAYS      = [5, 15, 30]
# One shared browser process now backs every worker (see worker() below), so
# concurrency is no longer gated by how many Chromium processes you can
# afford to spawn -- bumped the default up accordingly. Override via env.
CONCURRENCY       = int(os.environ.get("CONCURRENCY", "8"))
# Fixed settle time after each page load. Was 800ms on every single request;
# mbasic is server-rendered so it rarely needs that long.
POST_LOAD_WAIT_MS = int(os.environ.get("POST_LOAD_WAIT_MS", "300"))

# Google Sheet destination
SHEET_ID          = os.environ.get("SHEET_ID", "").strip()
GOOGLE_TOKEN_JSON = os.environ.get("GOOGLE_TOKEN_JSON", "").strip()
SHEET_TAB_NAME    = os.environ.get("SHEET_TAB_NAME", "").strip()

# Mega / rclone config
MEGA_REMOTE       = os.environ.get("MEGA_REMOTE", "mega").strip()
MEGA_FOLDER_NAME  = os.environ.get("MEGA_FOLDER_NAME", "").strip() or FOLDER_NAME

# Recovery mode
CSV_PATH          = os.environ.get("CSV_PATH", os.path.join("output", f"{FOLDER_NAME}.csv")).strip()


def log(msg):
    print(msg, flush=True)


def check_cookie_names(storage_state_path):
    if not os.path.exists(storage_state_path):
        log("no storage_state.json found")
        return
    with open(storage_state_path) as f:
        state = json.load(f)
    names = sorted(
        c.get("name", "") for c in state.get("cookies", [])
        if "facebook.com" in c.get("domain", "") or "fbcdn.net" in c.get("domain", "")
    )
    log(f"storage_state has {len(names)} facebook cookie(s): {', '.join(names)}")
    for critical in ("c_user", "xs"):
        if critical not in names:
            log(f"   !! MISSING critical cookie '{critical}' -- session will likely be logged out")


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


# ---------------- recovery mode: push an already-saved CSV ----------------
def push_csv_only(csv_path):
    if not (SHEET_ID and SHEET_TAB_NAME and GOOGLE_TOKEN_JSON):
        log("SHEET_ID, SHEET_TAB_NAME and GOOGLE_TOKEN_JSON are all required for --push-csv")
        sys.exit(1)
    if not os.path.exists(csv_path):
        log(f"csv not found at {csv_path}")
        sys.exit(1)

    rows = []
    with open(csv_path, newline="") as f:
        reader = csv.reader(f)
        next(reader, None)  # header
        for r in reader:
            if r:
                rows.append(r)

    log(f"loaded {len(rows)} row(s) from {csv_path}")

    service = get_sheets_service(GOOGLE_TOKEN_JSON)
    ensure_tab_and_get_id(service, SHEET_ID, SHEET_TAB_NAME)
    append_rows(service, SHEET_ID, SHEET_TAB_NAME, rows)


# ---------------- browser-driven fetch helpers ----------------
def to_mbasic(url):
    parsed = urlparse(url)
    return urlunparse(parsed._replace(netloc="mbasic.facebook.com", scheme="https"))


_wall_checked = False


def looks_like_wall(html):
    if not html:
        return True
    lowered = html.lower()
    return any(s in lowered for s in WALL_SIGNALS)


def save_debug_html_once(html, name):
    global _wall_checked
    if _wall_checked:
        return
    _wall_checked = True
    os.makedirs("output", exist_ok=True)
    path = os.path.join("output", name)
    with open(path, "w", encoding="utf-8") as f:
        f.write(html or "")
    log(f"   saved debug html to {path}")
    if looks_like_wall(html):
        log("   !! response looks like a login wall / bot-check / unsupported-browser page.")
        log("   !! see output/debug_listing_page1.html for what was actually returned.")


async def goto_html(page, url, attempts=MAX_RETRIES):
    last_err = None
    for attempt in range(1, attempts + 1):
        try:
            await page.goto(url, timeout=30000, wait_until="domcontentloaded")
            if POST_LOAD_WAIT_MS:
                await page.wait_for_timeout(POST_LOAD_WAIT_MS)
            return await page.content()
        except Exception as e:
            last_err = e
        if attempt < attempts:
            await asyncio.sleep(RETRY_DELAYS[min(attempt - 1, len(RETRY_DELAYS) - 1)])
    log(f"   failed to load {url}: {last_err}")
    return None


# ---------------- listing (mbasic photos tab, paginated, albums) ----------------
def find_photo_hrefs(soup):
    return [a["href"] for a in soup.find_all("a", href=True)
            if "/photo.php" in a["href"] or "/photo/" in a["href"]]


def find_album_hrefs(soup):
    return [a["href"] for a in soup.find_all("a", href=True)
            if "/media_set" in a["href"] or re.search(r'set=a\.', a["href"])]


async def collect_photo_links(page, start_url, max_pages):
    url = to_mbasic(start_url)
    seen = set()
    links = []
    visited = set()
    albums_to_visit = []
    pages_to_visit = [url]
    pages_done = 0
    first_page = True

    while pages_to_visit and pages_done < max_pages:
        cur = pages_to_visit.pop(0)
        if cur in visited:
            continue
        visited.add(cur)
        pages_done += 1

        log(f"   fetching listing page {pages_done}/{max_pages}: {cur}")
        html = await goto_html(page, cur)

        if first_page:
            first_page = False
            save_debug_html_once(html, "debug_listing_page1.html")

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

        for href in find_album_hrefs(soup):
            abs_url = urljoin(MBASIC_BASE, href)
            if abs_url not in visited and abs_url not in albums_to_visit:
                albums_to_visit.append(abs_url)

        log(f"   page: +{new_added} photo(s), total {len(links)}, "
            f"{len(albums_to_visit)} album(s) queued")

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
def extract_caption(soup):
    """Pull the real photo caption, not Facebook's auto alt-text.

    og:description is the most reliable source when present. When it's
    missing/generic, fall back to scanning the page -- but only over LEAF
    text nodes (tags with no nested div/span/p), because scanning parent
    containers lets their text (which is the concatenation of every child's
    text, including unrelated thumbnail alt-text elsewhere on the page)
    masquerade as one giant "candidate" that wins on raw length.
    """
    og_desc = soup.find("meta", property="og:description")
    if og_desc and og_desc.get("content"):
        text = og_desc["content"].strip()
        if text.lower() not in GENERIC_DESCRIPTIONS and not ALT_TEXT_RE.match(text):
            return text

    candidates = []
    for tag in soup.find_all(["div", "span", "p"]):
        if tag.find(["div", "span", "p"]):
            continue  # not a leaf -- skip to avoid swallowing sibling/child junk
        text = tag.get_text(" ", strip=True)
        if not text or len(text) < 3:
            continue
        if NAV_TEXT_RE.match(text) or ALT_TEXT_RE.match(text):
            continue
        if text.lower() in GENERIC_DESCRIPTIONS:
            continue
        if re.match(r'^\d+\s*(min|hr|hrs|day|days|week|weeks)s?\s*(ago)?$', text, re.I):
            continue
        candidates.append(text)

    if candidates:
        return max(candidates, key=len)
    return ""


async def extract_photo_details(page, permalink_url):
    html = await goto_html(page, permalink_url)
    if not html:
        return None, ""
    soup = BeautifulSoup(html, "html.parser")

    image_url = None
    for a in soup.find_all("a", href=True):
        if re.search(r'view full size|full size', a.get_text(strip=True), re.I):
            image_url = urljoin(MBASIC_BASE, a["href"])
            break
    if not image_url:
        og_image = soup.find("meta", property="og:image")
        if og_image and og_image.get("content"):
            image_url = og_image["content"]
    if not image_url:
        img_tag = soup.find("img", src=True)
        if img_tag:
            image_url = urljoin(MBASIC_BASE, img_tag["src"])

    caption = extract_caption(soup)
    return image_url, caption


def ext_from_url(image_url, default=".jpg"):
    m = re.search(r'\.(jpg|jpeg|png|webp)(\?|$)', image_url.lower())
    return f".{m.group(1)}" if m else default


async def download_image(page, image_url, dest_path):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = await page.context.request.get(image_url, timeout=30000)
            if resp.ok:
                body = await resp.body()
                with open(dest_path, "wb") as f:
                    f.write(body)
                return True
            log(f"   http {resp.status} for {image_url}")
        except Exception as e:
            log(f"   download error: {e}")
        if attempt < MAX_RETRIES:
            await asyncio.sleep(RETRY_DELAYS[min(attempt - 1, len(RETRY_DELAYS) - 1)])
    return False


async def process_one(page, fbid, permalink):
    try:
        image_url, caption = await extract_photo_details(page, permalink)
    except Exception as e:
        log(f"   [{fbid}] scrape error: {e}")
        return None
    if not image_url:
        log(f"   [{fbid}] no image URL found, skipping")
        return None

    ext = ext_from_url(image_url)
    filename = f"{fbid}{ext}"
    dest_path = os.path.join(OUTPUT_DIR, filename)

    if not await download_image(page, image_url, dest_path):
        log(f"   [{fbid}] gave up downloading")
        return None

    log(f"   [{fbid}] ok -> {filename}")
    return [filename, caption, permalink]


# ---------------- worker pool: ONE shared browser, many lightweight pages ----------------
async def worker(worker_id, browser, storage_state_path, task_queue, results):
    state_arg = storage_state_path if os.path.exists(storage_state_path) else None
    context = await browser.new_context(storage_state=state_arg, viewport={"width": 1000, "height": 900})
    page = await context.new_page()

    while True:
        try:
            fbid, permalink = task_queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        row = await process_one(page, fbid, permalink)
        if row:
            results.append(row)

    await context.close()


async def run_scrape():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--disable-notifications"])

        # listing pass: one page off the shared browser
        log(f"scraping {PAGE_PHOTOS_URL} via mbasic")
        state_arg = STORAGE_STATE if os.path.exists(STORAGE_STATE) else None
        listing_context = await browser.new_context(storage_state=state_arg, viewport={"width": 1000, "height": 900})
        listing_page = await listing_context.new_page()
        photo_links = await collect_photo_links(listing_page, PAGE_PHOTOS_URL, MAX_PAGES)
        await listing_context.close()

        log(f"found {len(photo_links)} photo permalink(s)")
        if not photo_links:
            await browser.close()
            return []

        # detail + download pass: shared browser, N lightweight contexts
        task_queue = asyncio.Queue()
        for fbid, permalink in photo_links:
            task_queue.put_nowait((fbid, permalink))

        results = []
        workers = [
            asyncio.create_task(worker(i, browser, STORAGE_STATE, task_queue, results))
            for i in range(min(CONCURRENCY, len(photo_links)))
        ]
        await asyncio.gather(*workers)

        await browser.close()
        return results


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
    if "--push-csv" in sys.argv:
        push_csv_only(CSV_PATH)
        return

    if not PAGE_PHOTOS_URL:
        log("PAGE_PHOTOS_URL is required, exiting")
        sys.exit(1)

    check_cookie_names(STORAGE_STATE)

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

    results = asyncio.run(run_scrape())

    if results:
        csv_path = os.path.join("output", f"{FOLDER_NAME}.csv")
        with open(csv_path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["filename", "caption", "source_url"])
            w.writerows(results)
        log(f"local csv saved: {csv_path} ({len(results)} rows)")

    if sheets_service and results:
        try:
            append_rows(sheets_service, SHEET_ID, tab_name, results)
        except Exception as e:
            log(f"failed to append rows to sheet: {e}")

    if not results:
        log("nothing downloaded, skipping Mega upload")
        return

    upload_all_mega()


if __name__ == "__main__":
    main()
