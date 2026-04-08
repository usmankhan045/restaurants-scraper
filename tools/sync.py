"""
WF-06: Google Sheets Sync
Pushes the blender CSV output to a Google Sheet.
Supports both .csv and .json blender outputs.
"""

import csv
import json
import logging
import math
import os
import sys
import time

import gspread
from google.oauth2.service_account import Credentials

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("sync")


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_data(filepath: str) -> list[list]:
    """
    Load rows from the blender output file.
    Returns a list of rows (first row = headers).
    """
    if not os.path.exists(filepath):
        log.warning(f"File not found: {filepath}")
        return []

    try:
        if filepath.endswith(".csv"):
            with open(filepath, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                rows   = list(reader)
            log.info(f"Loaded {len(rows)} rows from CSV (including header)")
            return rows

        elif filepath.endswith(".json"):
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list) and data:
                if isinstance(data[0], dict):
                    headers = list(data[0].keys())
                    rows    = [headers] + [[str(item.get(h, "")) for h in headers] for item in data]
                    log.info(f"Loaded {len(rows)} rows from JSON (including header)")
                    return rows
                else:
                    return data  # already a list of lists
            return []

    except Exception as exc:
        log.error(f"Failed to load data from {filepath}: {exc}")
        return []

    return []


# ---------------------------------------------------------------------------
# Google Sheets helpers
# ---------------------------------------------------------------------------

def col_letter(n: int) -> str:
    """Convert a 1-indexed column number to a spreadsheet letter (A, B, ..., Z, AA...)."""
    result = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def push_to_sheet(
    worksheet: gspread.Worksheet,
    data: list[list],
    chunk_size: int = 1000,
):
    """Push data to the worksheet in chunks to avoid API timeouts."""
    total_rows = len(data)
    max_cols   = max((len(row) for row in data), default=1)

    # Resize worksheet to fit all data + buffer
    try:
        new_rows = max(1000, total_rows + 200)
        new_cols = max(26, max_cols + 3)
        worksheet.resize(rows=new_rows, cols=new_cols)
        log.info(f"Resized worksheet to {new_rows}×{new_cols}")
    except Exception as exc:
        log.warning(f"Could not resize worksheet: {exc}")

    # Clear existing content first
    try:
        worksheet.clear()
        log.info("Cleared existing sheet content")
    except Exception as exc:
        log.warning(f"Could not clear worksheet: {exc}")

    num_chunks = math.ceil(total_rows / chunk_size)
    log.info(f"Pushing {total_rows} rows in {num_chunks} chunk(s) of {chunk_size}")

    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx   = min((i + 1) * chunk_size, total_rows)
        chunk     = data[start_idx:end_idx]

        start_row = start_idx + 1  # 1-indexed
        end_row   = start_idx + len(chunk)
        end_col   = col_letter(max_cols)
        cell_range = f"A{start_row}:{end_col}{end_row}"

        log.info(f"Chunk {i+1}/{num_chunks} → {cell_range} ({len(chunk)} rows)")

        # Ensure every row has the same number of columns (pad with empty strings)
        padded_chunk = [
            row + [""] * (max_cols - len(row)) if len(row) < max_cols else row[:max_cols]
            for row in chunk
        ]

        worksheet.batch_update([{
            "range":  cell_range,
            "values": padded_chunk,
        }])

        if i < num_chunks - 1:
            time.sleep(1.5)  # avoid hitting Sheets API rate limit (60 writes/min)

    log.info("All chunks pushed successfully.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    creds_json     = os.environ.get("GCP_CREDENTIALS")
    sheet_id       = os.environ.get("GOOGLE_SHEET_ID")
    worksheet_name = os.environ.get("WORKSHEET_NAME", "Restaurants")
    blender_file   = os.environ.get("BLENDER_OUTPUT", "blender_output.csv")

    # Validate required env vars
    if not creds_json:
        log.error("GCP_CREDENTIALS environment variable is not set.")
        sys.exit(1)
    if not sheet_id:
        log.error("GOOGLE_SHEET_ID environment variable is not set.")
        sys.exit(1)

    # Authenticate
    try:
        credentials = Credentials.from_service_account_info(
            json.loads(creds_json),
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ],
        )
        gc         = gspread.authorize(credentials)
        spreadsheet = gc.open_by_key(sheet_id)
        log.info(f"Authenticated. Opening sheet: {spreadsheet.title}")
    except json.JSONDecodeError:
        log.error("GCP_CREDENTIALS is not valid JSON.")
        sys.exit(1)
    except Exception as exc:
        log.exception(f"Authentication or sheet open failed: {exc}")
        sys.exit(1)

    # Get or create worksheet
    try:
        worksheet = spreadsheet.worksheet(worksheet_name)
        log.info(f"Using existing worksheet: {worksheet_name}")
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=20)
        log.info(f"Created new worksheet: {worksheet_name}")

    # Load data
    data = load_data(blender_file)
    if not data:
        log.warning(f"No data to sync from {blender_file}. Exiting.")
        return

    log.info(f"Loaded {len(data)} rows (including header) from {blender_file}")

    # Push to sheet
    push_to_sheet(worksheet, data)

    total_records = len(data) - 1  # subtract header row
    log.info(f"Sync complete. {total_records:,} restaurant records pushed to Google Sheets.")
    print(f"\nSync complete: {total_records:,} records → sheet '{worksheet_name}'")


if __name__ == "__main__":
    main()