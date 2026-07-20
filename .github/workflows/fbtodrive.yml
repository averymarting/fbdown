#!/usr/bin/env python3
"""
Recovery/backfill script: pushes a local filename,caption,source_url CSV
(the kind photos.py always saves to output/<folder_name>.csv, even when the
live Sheets write fails) into a Google Sheet tab, creating the tab if needed.

Use this when a run's images/Mega upload succeeded but the Sheets append
step failed (e.g. transient SSL error) -- no need to re-scrape.

Usage (env vars, same as photos.py):
  SHEET_ID=...           Google Sheet ID
  SHEET_TAB_NAME=...     Tab to create/append to
  GOOGLE_TOKEN_JSON=...  OAuth token JSON (needs full spreadsheets scope)
  CSV_PATH=...           Path to the CSV (default: output/facebook_photos.csv)

  python push_csv_to_sheet.py
"""
import os
import csv
import json
import sys
import time


def log(msg):
    print(msg, flush=True)


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


def ensure_tab(service, sheet_id, tab_name):
    meta = with_retries(service.spreadsheets().get(spreadsheetId=sheet_id).execute)
    for s in meta.get("sheets", []):
        if s["properties"]["title"] == tab_name:
            return
    log(f"tab '{tab_name}' not found, creating it")
    with_retries(
        service.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]},
        ).execute
    )
    with_retries(
        service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=f"'{tab_name}'!A1:C1",
            valueInputOption="RAW",
            body={"values": [["filename", "caption", "source_url"]]},
        ).execute
    )


def append_rows(service, sheet_id, tab_name, rows, chunk_size=40):
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


def main():
    sheet_id = os.environ.get("SHEET_ID", "").strip()
    tab_name = os.environ.get("SHEET_TAB_NAME", "").strip()
    token_json = os.environ.get("GOOGLE_TOKEN_JSON", "").strip()
    csv_path = os.environ.get("CSV_PATH", "output/facebook_photos.csv").strip()

    if not (sheet_id and tab_name and token_json):
        log("SHEET_ID, SHEET_TAB_NAME and GOOGLE_TOKEN_JSON are all required")
        sys.exit(1)
    if not os.path.exists(csv_path):
        log(f"csv not found at {csv_path}")
        sys.exit(1)

    rows = []
    with open(csv_path, newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        for r in reader:
            if r:
                rows.append(r)

    log(f"loaded {len(rows)} row(s) from {csv_path}")

    service = get_sheets_service(token_json)
    ensure_tab(service, sheet_id, tab_name)
    append_rows(service, sheet_id, tab_name, rows)


if __name__ == "__main__":
    main()
