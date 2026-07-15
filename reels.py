#!/usr/bin/env python3
"""
Headless pipeline for GitHub Actions (no GUI):
  1. Scrape Facebook Reels tab URL(s) with Playwright (headless Chromium)
  2. Download found reels with yt-dlp
  3. Upload downloaded videos to a Google Drive folder

Reads two credential files, written by the workflow from two GitHub Secrets:
  - token.json           -> Google Drive OAuth token (same shape used by gdrive_uploader.py)
  - storage_state.json   -> Facebook Playwright storage_state (cookies + origins)

All run parameters come from environment variables set by the workflow_dispatch inputs.
"""
import os
import re
import sys
import csv
import json
import time
import mimetypes
import subprocess
from pathlib import Path

from playwright.sync_api import sync_playwright

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

VIDEO_EXTENSIONS = {'.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v'}

# ---- config from environment / workflow inputs ----
REEL_URLS       = [u.strip() for u in os.environ.get("REEL_URLS", "").splitlines()
                    if u.strip() and not u.strip().startswith("#")]
MAX_SCROLLS      = int(os.environ.get("MAX_SCROLLS", "2"))
FOLDER_NAME      = os.environ.get("FOLDER_NAME", "facebook_reels")
DRIVE_FOLDER_ID  = os.environ.get("DRIVE_FOLDER_ID", "").strip()
TOKEN_FILE       = os.environ.get("TOKEN_FILE", "token.json")
STORAGE_STATE    = os.environ.get("STORAGE_STATE_FILE", "storage_state.json")
COOKIES_TXT      = "fb_cookies.txt"
OUTPUT_DIR       = os.path.join("output", FOLDER_NAME)
BASE_SLEEP       = int(os.environ.get("BASE_SLEEP", "3"))
MAX_RETRIES      = int(os.environ.get("MAX_RETRIES", "3"))
RETRY_DELAYS     = [10, 30, 60]
MAX_CONSEC_FAIL  = 5
COOLDOWN_SLEEP   = 90


def log(msg):
    print(msg, flush=True)


# ---------------- cookies (for yt-dlp) ----------------
def storage_state_to_netscape(json_path, out_path):
    with open(json_path) as f:
        state = json.load(f)
    written = 0
    with open(out_path, "w", newline="\n") as f:
        f.write("# Netscape HTTP Cookie File\n\n")
        for c in state.get("cookies", []):
            domain = c.get("domain", "")
            if "facebook.com" not in domain and "fbcdn.net" not in domain:
                continue
            domain_flag = "TRUE" if domain.startswith(".") else "FALSE"
            secure_flag = "TRUE" if c.get("secure", False) else "FALSE"
            expiry = int(c.get("expires", 0) or 0) or int(time.time()) + 31536000
            f.write(f"{domain}\t{domain_flag}\t{c.get('path','/')}\t{secure_flag}\t"
                    f"{expiry}\t{c.get('name','')}\t{c.get('value','')}\n")
            written += 1
    return written


# ---------------- scraping (Playwright) ----------------
def scrape_reels(page, url, seen, max_scrolls):
    page.goto(url, timeout=60000)
    page.wait_for_timeout(8000)

    try:
        page.locator("div[aria-label='Close'], div[role='button']").first.click(timeout=3000)
        page.wait_for_timeout(3000)
    except Exception:
        pass

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
        page.wait_for_timeout(4000)

        cur_height = page.evaluate("document.body.scrollHeight")
        hrefs = page.eval_on_selector_all("a[href*='/reel/']", "els => els.map(e => e.href)")

        new_added = 0
        for href in hrefs:
            if href:
                clean = re.sub(r'\?.*$', '', href).rstrip('/')
                if re.search(r'/reel/[0-9a-zA-Z]{10,}', clean) and clean not in seen:
                    seen.add(clean)
                    links.append(clean)
                    new_added += 1

        if len(links) == prev_count and cur_height == last_height:
            no_progress += 1
        else:
            no_progress = 0
        prev_count = len(links)
        last_height = cur_height

        log(f"   scroll {scroll_num}/{max_scrolls} -> {len(links)} reels (+{new_added})")

    return links


# ---------------- download (yt-dlp) ----------------
def download_links(links, cookies_txt):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    archive_path = os.path.join("output", f"{FOLDER_NAME}_archive.txt")
    failed = []
    consec_fail = 0

    for i, link in enumerate(links, 1):
        if consec_fail >= MAX_CONSEC_FAIL:
            log(f"cooling down after {consec_fail} consecutive failures...")
            time.sleep(COOLDOWN_SLEEP)
            consec_fail = 0

        sleep_time = BASE_SLEEP + min(consec_fail * 5, 30)
        time.sleep(sleep_time)

        args = ["yt-dlp"]
        if cookies_txt:
            args += ["--cookies", cookies_txt]
        args += [
            "-f", "best[ext=mp4]/bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio/best",
            "--merge-output-format", "mp4",
            "--force-ipv4", "--geo-bypass",
            "--retries", "5", "--fragment-retries", "5",
            "--download-archive", archive_path,
            "--output", os.path.join(OUTPUT_DIR, "%(id)s.%(ext)s"),
            "--no-keep-video", "--no-keep-fragments",
            link,
        ]

        succeeded = False
        for attempt in range(1, MAX_RETRIES + 1):
            log(f"[{i}/{len(links)}] downloading {link}" + (f" (retry {attempt})" if attempt > 1 else ""))
            proc = subprocess.run(args, capture_output=True, text=True)
            tail = proc.stdout[-2000:] if proc.stdout else ""
            if tail:
                log(tail)
            already = "has already been recorded" in proc.stdout or "already been downloaded" in proc.stdout
            if proc.returncode == 0 or already:
                succeeded = True
                break
            if attempt < MAX_RETRIES:
                delay = RETRY_DELAYS[min(attempt - 1, len(RETRY_DELAYS) - 1)]
                log(f"   failed (exit {proc.returncode}), retrying in {delay}s")
                time.sleep(delay)

        if succeeded:
            consec_fail = 0
        else:
            consec_fail += 1
            failed.append(link)
            log(f"   gave up on {link}")

    if failed:
        failed_csv = os.path.join("output", f"{FOLDER_NAME}_failed.csv")
        with open(failed_csv, "w", newline="") as f:
            w = csv.writer(f)
            for l in failed:
                w.writerow([l])
        log(f"failed links saved to {failed_csv}")

    return failed


# ---------------- upload (Google Drive) ----------------
def load_drive_creds(token_file):
    with open(token_file) as f:
        info = json.load(f)
    creds = Credentials(
        token=info.get("token"),
        refresh_token=info.get("refresh_token"),
        token_uri=info.get("token_uri", "https://oauth2.googleapis.com/token"),
        client_id=info.get("client_id"),
        client_secret=info.get("client_secret"),
        scopes=info.get("scopes", ["https://www.googleapis.com/auth/drive"]),
    )
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return creds


def upload_file(service, fp: Path, folder_id: str):
    mime, _ = mimetypes.guess_type(str(fp))
    mime = mime or "application/octet-stream"
    file_meta = {"name": fp.name, "parents": [folder_id]}
    media = MediaFileUpload(str(fp), mimetype=mime, resumable=True, chunksize=8 * 1024 * 1024)
    request = service.files().create(body=file_meta, media_body=media, fields="id")
    response = None
    while response is None:
        _, response = request.next_chunk()
    return response


def upload_all(folder_id):
    if not folder_id:
        log("no DRIVE_FOLDER_ID provided, skipping upload")
        return
    creds = load_drive_creds(TOKEN_FILE)
    service = build("drive", "v3", credentials=creds)

    files = [f for f in sorted(Path(OUTPUT_DIR).iterdir())
             if f.is_file() and f.suffix.lower() in VIDEO_EXTENSIONS]
    log(f"uploading {len(files)} video(s) to Drive folder {folder_id}")
    for i, fp in enumerate(files, 1):
        try:
            log(f"[{i}/{len(files)}] uploading {fp.name}")
            upload_file(service, fp, folder_id)
            log(f"   done: {fp.name}")
        except Exception as e:
            log(f"   error uploading {fp.name}: {e}")


# ---------------- main ----------------
def main():
    if not REEL_URLS:
        log("no REEL_URLS provided, exiting")
        sys.exit(1)

    cookies_txt = None
    if os.path.exists(STORAGE_STATE):
        count = storage_state_to_netscape(STORAGE_STATE, COOKIES_TXT)
        log(f"converted {count} facebook cookies for yt-dlp")
        cookies_txt = COOKIES_TXT if count else None

    all_links = []
    seen = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--disable-notifications"])
        state_arg = STORAGE_STATE if os.path.exists(STORAGE_STATE) else None
        context = browser.new_context(storage_state=state_arg, viewport={"width": 1400, "height": 1000})
        page = context.new_page()

        for idx, url in enumerate(REEL_URLS, 1):
            log(f"[{idx}/{len(REEL_URLS)}] scraping {url}")
            links = scrape_reels(page, url, seen, MAX_SCROLLS)
            all_links.extend(links)
            log(f"   found {len(links)} new reels (total {len(all_links)})")

        # refresh local storage_state copy (cookies may have rotated); not persisted back to the secret
        if state_arg:
            context.storage_state(path=STORAGE_STATE)

        context.close()
        browser.close()

    if not all_links:
        log("no reels found, exiting")
        return

    os.makedirs("output", exist_ok=True)
    csv_path = os.path.join("output", f"{FOLDER_NAME}.csv")
    with open(csv_path, "w", newline="") as f:
        w = csv.writer(f)
        for l in all_links:
            w.writerow([l])
    log(f"master csv saved: {csv_path} ({len(all_links)} reels)")

    download_links(all_links, cookies_txt)
    upload_all(DRIVE_FOLDER_ID)


if __name__ == "__main__":
    main()
