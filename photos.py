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

# Debugging: when true, saves a screenshot + full HTML dump for every photo
# page visited (or just the failures if DEBUG_ONLY_FAILURES=true) into
# output/debug/. Turn on when things silently fail so you can see exactly
# what Playwright was looking at.
DEBUG_MODE          = os.environ.get("DEBUG_MODE", "true").strip().lower() in ("1", "true", "yes")
DEBUG_ONLY_FAILURES = os.environ.get("DEBUG_ONLY_FAILURES", "false").strip().lower() in ("1", "true", "yes")
DEBUG_DIR           = os.path.join("output", "debug")

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
    """Scroll the profile photos grid and collect unique photo permalink URLs.

    IMPORTANT: we deliberately keep the FULL href, including any
    &set=...&type=3 suffix. Testing showed that photo.php URLs with
    &set=...&type=3 reliably render the post caption, while the bare
    ?fbid=... form often renders a stripped lazy layout where the
    caption span never mounts. Stripping the &set= part (as the
    previous version did) was the main cause of missing/garbage
    captions.
    """
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
        # small incremental scrolls with pauses trigger FB's lazy-loading far
        # more reliably than a single jump to the bottom
        for _ in range(6):
            page.evaluate("window.scrollBy(0, 900)")
            page.wait_for_timeout(700)
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
                # keep the full href (with &set=...&type=3 if present)
                links.append((fbid, href))
                new_added += 1

        cur_height = page.evaluate("document.body.scrollHeight")
        if new_added == 0 and cur_height == last_height:
            no_progress += 1
        else:
            no_progress = 0
        last_height = cur_height
        log(f"   scroll {scroll_num}/{max_scrolls} -> {len(links)} photo(s) (+{new_added})")

    return links


def save_debug(page, fbid, tag=""):
    """Dump a screenshot + full HTML of the current page state for inspection.
    Also logs the resolved URL/title so login/checkpoint walls are obvious."""
    if not DEBUG_MODE:
        return
    os.makedirs(DEBUG_DIR, exist_ok=True)
    suffix = f"_{tag}" if tag else ""
    png_path = os.path.join(DEBUG_DIR, f"{fbid}{suffix}.png")
    html_path = os.path.join(DEBUG_DIR, f"{fbid}{suffix}.html")
    try:
        page.screenshot(path=png_path, full_page=True, timeout=15000)
    except Exception as e:
        log(f"   [debug] screenshot failed: {e}")
    try:
        html_path_obj = Path(html_path)
        html_path_obj.write_text(page.content(), encoding="utf-8")
    except Exception as e:
        log(f"   [debug] html dump failed: {e}")
    log(f"   [debug] resolved url: {page.url}")
    try:
        log(f"   [debug] page title: {page.title()}")
    except Exception:
        pass


def looks_like_login_wall(page):
    url = page.url.lower()
    if "login" in url or "checkpoint" in url or "recover" in url:
        return True
    try:
        body_text = page.eval_on_selector("body", "e => e.innerText").lower()
    except Exception:
        return False
    login_markers = [
        "log in to facebook", "you must log in", "log into facebook",
        "you'll need to log in", "enter your password",
    ]
    return any(m in body_text for m in login_markers)


def _clean_caption_text(t):
    """Shared filter used by extract_caption. Returns cleaned text or
    None if this fragment should be rejected (chrome, link preview,
    emoji-only, mention-only, page/profile name label, etc.)."""
    raw = t.strip()
    low = raw.lower()

    if low in CAPTION_BLOCKLIST:
        return None
    if re.match(r'^\d+[hdwm]$', low):                 # "3h", "2d" timestamps
        return None
    if re.match(r'^[\d,]+$', low):                     # bare reaction counts
        return None
    if len(raw) < 3:
        return None

    # reject pure link-preview text, e.g. "https://t.co/Z05KVF8YzC"
    if re.match(r'^https?://\S+$', raw):
        return None

    # reject page/profile name labels -- these are short (<=3 words,
    # <25 chars), title-cased, and reappear verbatim across every photo
    # on the same profile. This is what was producing the "Coco system"
    # bug: the page/account display name was being picked up as if it
    # were the caption.
    if len(raw.split()) <= 3 and raw.istitle() and len(raw) < 25:
        return None

    return raw


def extract_caption(page):
    """Robust caption extraction with multiple fallback strategies.

    Strategy order:
      1. Wait briefly for known caption containers to mount -- they are
         often lazy-rendered, which is why fbid-only URLs (no &set=...)
         sometimes came back with the caption missing or replaced by
         unrelated short UI text (e.g. the page name).
      2. Try FB's known caption/message containers first
         (data-ad-preview, data-ad-comet-preview, post_message testid,
         and the specific span/div structure FB uses for post text).
      3. Fallback: scan all dir='auto' blocks, filter out chrome/links/
         page-name labels, and pick the LONGEST remaining candidate --
         the first dir='auto' block on the page is very often a nav or
         header element, not the caption.
    """
    # give the caption container a moment to mount
    try:
        page.wait_for_selector(
            "[data-ad-preview], [data-ad-comet-preview], "
            "div[data-testid='post_message'], "
            "div.xyinxu5, div[dir='auto']",
            timeout=8000,
        )
    except Exception:
        pass
    # extra settle time specifically helps the lazy fbid-only layout
    page.wait_for_timeout(1500)

    # ---- Strategy 1: known caption containers ----
    priority_selectors = [
        "[data-ad-preview='message']",
        "[data-ad-comet-preview='message']",
        "div[data-testid='post_message']",
        "div.xyinxu5",
    ]
    for sel in priority_selectors:
        try:
            texts = page.eval_on_selector_all(
                sel,
                "els => els.map(e => e.innerText.trim()).filter(t => t.length > 0)"
            )
        except Exception:
            texts = []
        for t in texts:
            cleaned = _clean_caption_text(t)
            if cleaned:
                return cleaned

    # ---- Strategy 2: generic dir='auto' scan, pick the longest valid one ----
    try:
        texts = page.eval_on_selector_all(
            "div[dir='auto'], span[dir='auto']",
            "els => els.map(e => e.innerText.trim()).filter(t => t.length > 0)"
        )
    except Exception:
        texts = []

    candidates = []
    for t in texts:
        cleaned = _clean_caption_text(t)
        if cleaned:
            candidates.append(cleaned)

    if candidates:
        return max(candidates, key=len)

    return ""


def extract_image_url(page):
    """Pick the full-res photo <img> on the photo page (not thumbnails or
    profile pictures). Tries FB's own "this is the main photo" marker first,
    then falls back to the largest loaded image on the page. Actively waits
    for images to finish loading first, since naturalWidth is 0 until then."""

    # give lazy-loaded images a chance to actually finish loading
    try:
        page.wait_for_function(
            """() => {
                const imgs = Array.from(document.querySelectorAll('img'));
                return imgs.some(e => e.naturalWidth > 400 && e.complete);
            }""",
            timeout=15000,
        )
    except Exception:
        pass  # fall through and try anyway; save_debug will show why

    # 1) FB tags the actual full-size photo with this attribute
    try:
        srcs = page.eval_on_selector_all(
            "img[data-visualcompletion='media-vc-image']",
            "els => els.map(e => e.src).filter(Boolean)"
        )
        if srcs:
            return srcs[0]
    except Exception:
        pass

    # 2) fallback: largest fully-loaded image referencing FB's CDN
    try:
        srcs = page.eval_on_selector_all(
            "img",
            """els => els
                .filter(e => e.complete && e.naturalWidth > 400 &&
                             e.src && (e.src.includes('scontent') || e.src.includes('fbcdn')))
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
                # bumped from 3000 -- the lazy fbid-only layout needs more
                # settle time before the caption span mounts
                page.wait_for_timeout(5000)
            except Exception as e:
                log(f"   failed to open photo page: {e}")
                continue

            if looks_like_login_wall(page):
                log("   !! this looks like a login/checkpoint wall -- storage_state "
                    "cookies are likely missing or expired. saving debug and skipping.")
                save_debug(page, fbid, tag="loginwall")
                continue

            if not DEBUG_ONLY_FAILURES:
                save_debug(page, fbid)

            img_url = extract_image_url(page)
            caption = extract_caption(page)

            if not img_url:
                log("   no image found, skipping")
                if DEBUG_ONLY_FAILURES:
                    save_debug(page, fbid, tag="noimage")
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
