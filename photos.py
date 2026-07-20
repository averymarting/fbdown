#!/usr/bin/env python3
"""
Headless pipeline for GitHub Actions (no GUI) -- FAST/CONCURRENT version.

  1. Scrape a Facebook profile "photos" page with Playwright (headless Chromium)
  2. For each photo, grab the full-res image + its caption text
     -- multiple photo pages are processed CONCURRENTLY (separate tabs,
        one shared browser) instead of one at a time
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

WHAT CHANGED FOR SPEED (vs the sequential version):
  - Photo pages are now scraped CONCURRENTLY using several tabs on one
    shared browser, controlled by CONCURRENCY (default 4). This is the
    biggest win: most of the per-photo time was idle network wait, which
    parallelizes well.
  - DEBUG_MODE now defaults to "false" and DEBUG_ONLY_FAILURES defaults
    to "true" -- full-page screenshots + HTML dumps are slow and you
    don't need them for every successful photo anymore.
  - Fonts and known analytics/tracker requests are blocked at the
    network layer to speed up page loads. Images and stylesheets are
    left alone (images are exactly what we need to load, and blocking
    stylesheets risks breaking layout-dependent selectors).
  - Fixed sleeps were trimmed in favor of targeted wait_for_selector /
    wait_for_function calls, so we don't wait longer than necessary.
  - Image downloads for a batch of photos happen concurrently with
    Playwright's request API, not queued after each other.
"""
import os
import re
import sys
import csv
import json
import time
import asyncio
import subprocess
from pathlib import Path

from playwright.async_api import async_playwright

IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.webp'}

# ---- config from environment / workflow inputs ----
PHOTOS_URL          = os.environ.get("PHOTOS_URL", "").strip()
MAX_SCROLLS          = int(os.environ.get("MAX_SCROLLS", "5"))
FOLDER_NAME          = os.environ.get("FOLDER_NAME", "facebook_photos")
STORAGE_STATE        = os.environ.get("STORAGE_STATE_FILE", "storage_state.json")
OUTPUT_DIR           = os.path.join("output", FOLDER_NAME)
BASE_SLEEP           = float(os.environ.get("BASE_SLEEP", "0.5"))
MAX_RETRIES          = int(os.environ.get("MAX_RETRIES", "3"))
RETRY_DELAYS         = [5, 15, 30]

# concurrency: how many photo pages to process at once. Each one opens
# its own tab on the shared browser context. 3-6 is a reasonable range;
# going much higher increases the odds of Facebook rate-limiting /
# showing checkpoint walls.
CONCURRENCY          = int(os.environ.get("CONCURRENCY", "4"))

# Google Sheet destination
SHEET_ID             = os.environ.get("SHEET_ID", "").strip()
TAB_NAME             = os.environ.get("TAB_NAME", "").strip()
GOOGLE_TOKEN_JSON    = os.environ.get("GOOGLE_TOKEN_JSON", "").strip()

# Mega / rclone config
MEGA_REMOTE          = os.environ.get("MEGA_REMOTE", "mega").strip()
MEGA_FOLDER_NAME     = os.environ.get("MEGA_FOLDER_NAME", "").strip() or FOLDER_NAME

# Debugging: OFF by default now for speed. Set DEBUG_MODE=true to dump
# screenshot+HTML for every photo, or leave DEBUG_ONLY_FAILURES=true
# (default) to only dump when something actually goes wrong.
DEBUG_MODE           = os.environ.get("DEBUG_MODE", "false").strip().lower() in ("1", "true", "yes")
DEBUG_ONLY_FAILURES  = os.environ.get("DEBUG_ONLY_FAILURES", "true").strip().lower() in ("1", "true", "yes")
DEBUG_DIR            = os.path.join("output", "debug")

# request URL fragments we block to speed up page loads -- fonts and
# known analytics/tracker endpoints add load time but nothing we need
BLOCKED_RESOURCE_TYPES = {"font"}
BLOCKED_URL_SUBSTRINGS = (
    "google-analytics.com", "googletagmanager.com", "doubleclick.net",
    "facebook.com/tr/", "connect.facebook.net/en_US/fbevents",
)

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


# ---------------- scraping (Playwright, async) ----------------
async def block_unneeded_requests(route, request):
    if request.resource_type in BLOCKED_RESOURCE_TYPES:
        await route.abort()
        return
    url = request.url
    if any(s in url for s in BLOCKED_URL_SUBSTRINGS):
        await route.abort()
        return
    await route.continue_()


async def collect_photo_links(page, url, max_scrolls):
    """Scroll the profile photos grid and collect unique photo permalink URLs.

    Keeps the FULL href (including &set=...&type=3) -- that form
    reliably renders the caption, while the bare ?fbid=... form
    sometimes doesn't.
    """
    await page.goto(url, timeout=60000)
    await page.wait_for_timeout(4000)

    try:
        await page.locator("div[aria-label='Close'], div[role='button']").first.click(timeout=3000)
        await page.wait_for_timeout(1000)
    except Exception:
        pass

    seen = set()
    links = []
    no_progress = 0
    scroll_num = 0
    last_height = await page.evaluate("document.body.scrollHeight")

    log(f"   scrolling up to {max_scrolls} time(s) to collect photo links...")
    while scroll_num < max_scrolls and no_progress < 5:
        scroll_num += 1
        for _ in range(6):
            await page.evaluate("window.scrollBy(0, 900)")
            await page.wait_for_timeout(500)
        await page.wait_for_timeout(1500)

        hrefs = await page.eval_on_selector_all(
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
                links.append((fbid, href))
                new_added += 1

        cur_height = await page.evaluate("document.body.scrollHeight")
        if new_added == 0 and cur_height == last_height:
            no_progress += 1
        else:
            no_progress = 0
        last_height = cur_height
        log(f"   scroll {scroll_num}/{max_scrolls} -> {len(links)} photo(s) (+{new_added})")

    return links


async def save_debug(page, fbid, tag=""):
    if not DEBUG_MODE:
        return
    os.makedirs(DEBUG_DIR, exist_ok=True)
    suffix = f"_{tag}" if tag else ""
    png_path = os.path.join(DEBUG_DIR, f"{fbid}{suffix}.png")
    html_path = os.path.join(DEBUG_DIR, f"{fbid}{suffix}.html")
    try:
        await page.screenshot(path=png_path, full_page=True, timeout=15000)
    except Exception as e:
        log(f"   [debug] screenshot failed: {e}")
    try:
        Path(html_path).write_text(await page.content(), encoding="utf-8")
    except Exception as e:
        log(f"   [debug] html dump failed: {e}")
    log(f"   [debug] resolved url: {page.url}")
    try:
        log(f"   [debug] page title: {await page.title()}")
    except Exception:
        pass


async def looks_like_login_wall(page):
    url = page.url.lower()
    if "login" in url or "checkpoint" in url or "recover" in url:
        return True
    try:
        body_text = (await page.eval_on_selector("body", "e => e.innerText")).lower()
    except Exception:
        return False
    login_markers = [
        "log in to facebook", "you must log in", "log into facebook",
        "you'll need to log in", "enter your password",
    ]
    return any(m in body_text for m in login_markers)


# JS helper: walks an element's children and builds its text the way a
# human would read it, but substitutes emoji <img alt="..."> elements
# with their actual emoji character instead of skipping them (which is
# what plain innerText does -- FB renders emoji as images, not text).
_EMOJI_AWARE_TEXT_JS = """
function emojiAwareText(el) {
    let result = '';
    for (const node of el.childNodes) {
        if (node.nodeType === Node.TEXT_NODE) {
            result += node.textContent;
        } else if (node.nodeType === Node.ELEMENT_NODE) {
            if (node.tagName === 'IMG' && node.alt) {
                result += node.alt;
            } else {
                result += emojiAwareText(node);
            }
        }
    }
    return result;
}
"""


def _looks_like_scrambled_timestamp(text):
    """Detect FB's per-character-span obfuscation used on timestamps
    (e.g. 'posted 3h ago' rendered as one <span> per letter, reassembled
    visually via CSS positioning). innerText/emojiAwareText joins these
    in raw DOM order with newlines between them, producing garbage like
    't\\nd\\nn\\no\\nr\\no\\nS...'. Heuristic: mostly single-character
    lines when split on newline.
    """
    lines = [l for l in text.split('\n') if l.strip() != '']
    if len(lines) < 6:
        return False
    single_char_lines = sum(1 for l in lines if len(l.strip()) <= 1)
    return (single_char_lines / len(lines)) > 0.6


def _clean_caption_text(t):
    # collapse the newline-per-character artifacts from emojiAwareText
    # (real captions may still have legit single '\n' line breaks, so
    # we only fully reject via _looks_like_scrambled_timestamp below;
    # here we just normalize whitespace for the other checks)
    raw = t.strip()
    low = raw.lower()

    if _looks_like_scrambled_timestamp(raw):
        return None
    if low in CAPTION_BLOCKLIST:
        return None
    if re.match(r'^\d+[hdwm]$', low):
        return None
    if re.match(r'^[\d,]+$', low):
        return None
    if len(raw) < 3:
        return None
    if re.match(r'^https?://\S+$', raw):
        return None
    if len(raw.split()) <= 3 and raw.istitle() and len(raw) < 25:
        return None

    # normalize: collapse runs of whitespace/newlines from mixed
    # text+emoji nodes into single spaces for a clean final caption
    normalized = re.sub(r'\s+', ' ', raw).strip()
    if len(normalized) < 3:
        return None
    return normalized


async def extract_caption(page):
    try:
        await page.wait_for_selector(
            "[data-ad-preview], [data-ad-comet-preview], "
            "div[data-testid='post_message'], "
            "div.xyinxu5, div[dir='auto']",
            timeout=6000,
        )
    except Exception:
        pass
    await page.wait_for_timeout(800)

    priority_selectors = [
        "[data-ad-preview='message']",
        "[data-ad-comet-preview='message']",
        "div[data-testid='post_message']",
        "div.xyinxu5",
    ]
    for sel in priority_selectors:
        try:
            texts = await page.eval_on_selector_all(
                sel,
                _EMOJI_AWARE_TEXT_JS + """
                (els) => els.map(e => emojiAwareText(e).trim()).filter(t => t.length > 0)
                """
            )
        except Exception:
            texts = []
        for t in texts:
            cleaned = _clean_caption_text(t)
            if cleaned:
                return cleaned

    try:
        texts = await page.eval_on_selector_all(
            "div[dir='auto'], span[dir='auto']",
            _EMOJI_AWARE_TEXT_JS + """
            (els) => els.map(e => emojiAwareText(e).trim()).filter(t => t.length > 0)
            """
        )
    except Exception:
        texts = []

    candidates = [c for c in (_clean_caption_text(t) for t in texts) if c]
    if candidates:
        return max(candidates, key=len)
    return ""


async def extract_image_url(page):
    try:
        await page.wait_for_function(
            """() => {
                const imgs = Array.from(document.querySelectorAll('img'));
                return imgs.some(e => e.naturalWidth > 400 && e.complete);
            }""",
            timeout=12000,
        )
    except Exception:
        pass

    try:
        srcs = await page.eval_on_selector_all(
            "img[data-visualcompletion='media-vc-image']",
            "els => els.map(e => e.src).filter(Boolean)"
        )
        if srcs:
            return srcs[0]
    except Exception:
        pass

    try:
        srcs = await page.eval_on_selector_all(
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


async def download_image(context, url, dest_path):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = await context.request.get(url, timeout=30000)
            if resp.ok:
                dest_path.write_bytes(await resp.body())
                return True
            log(f"   download failed (status {resp.status}), attempt {attempt}")
        except Exception as e:
            log(f"   download error: {e}, attempt {attempt}")
        if attempt < MAX_RETRIES:
            await asyncio.sleep(RETRY_DELAYS[min(attempt - 1, len(RETRY_DELAYS) - 1)])
    return False


async def process_photo(context, fbid, url, idx, total, sem):
    """Scrape one photo page + download its image. Runs inside a
    semaphore so only CONCURRENCY of these run at once."""
    async with sem:
        log(f"[{idx}/{total}] opening {url}")
        page = await context.new_page()
        await page.route("**/*", block_unneeded_requests)
        try:
            try:
                await page.goto(url, timeout=60000)
                await page.wait_for_timeout(2500)
            except Exception as e:
                log(f"   failed to open photo page: {e}")
                return None

            if await looks_like_login_wall(page):
                log("   !! login/checkpoint wall -- storage_state cookies likely "
                    "missing or expired. saving debug and skipping.")
                await save_debug(page, fbid, tag="loginwall")
                return None

            if not DEBUG_ONLY_FAILURES:
                await save_debug(page, fbid)

            img_url = await extract_image_url(page)
            caption = await extract_caption(page)

            if not img_url:
                log("   no image found, skipping")
                if DEBUG_ONLY_FAILURES:
                    await save_debug(page, fbid, tag="noimage")
                return None

            ext = ".jpg"
            m = re.search(r'\.(jpg|jpeg|png|webp)', img_url.lower())
            if m:
                ext = "." + m.group(1)
            filename = f"photo_{fbid}{ext}"
            dest_path = Path(OUTPUT_DIR) / filename

            ok = await download_image(context, img_url, dest_path)
            if not ok:
                log(f"   giving up on {filename}")
                return None

            log(f"   saved {filename} | caption: {caption[:60]!r}")
            if BASE_SLEEP:
                await asyncio.sleep(BASE_SLEEP)
            return [filename, caption]
        finally:
            await page.close()


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
async def async_main():
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

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--disable-notifications"])
        state_arg = STORAGE_STATE if os.path.exists(STORAGE_STATE) else None
        context = await browser.new_context(storage_state=state_arg, viewport={"width": 1400, "height": 1000})

        link_page = await context.new_page()
        await link_page.route("**/*", block_unneeded_requests)
        photo_links = await collect_photo_links(link_page, PHOTOS_URL, MAX_SCROLLS)
        await link_page.close()
        log(f"total unique photos found: {len(photo_links)}")

        sem = asyncio.Semaphore(CONCURRENCY)
        tasks = [
            process_photo(context, fbid, url, idx, len(photo_links), sem)
            for idx, (fbid, url) in enumerate(photo_links, 1)
        ]
        results = await asyncio.gather(*tasks)
        rows = [r for r in results if r is not None]

        await context.close()
        await browser.close()

    if rows:
        append_rows(sheets_service, SHEET_ID, TAB_NAME, rows)
    else:
        log("no rows to write, nothing downloaded")

    csv_path = os.path.join("output", f"{FOLDER_NAME}.csv")
    with open(csv_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["File Name", "Caption"])
        w.writerows(rows)
    log(f"local csv saved: {csv_path}")

    upload_all_mega()


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
