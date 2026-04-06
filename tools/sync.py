import os
import json
import logging
import math
import sys
import time

import gspread
from google.oauth2.service_account import Credentials

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def get_blender_data(filepath="blender_output.json"):
    """Reads the blender output file."""
    if not os.path.exists(filepath):
        logging.warning(f"File {filepath} not found.")
        return []

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            if filepath.endswith(".json"):
                data = json.load(f)
                if isinstance(data, list) and data and isinstance(data[0], dict):
                    # Flatten dict into rows
                    headers = list(data[0].keys())
                    rows = [headers]
                    for item in data:
                        rows.append([item.get(h, "") for h in headers])
                    return rows
                elif isinstance(data, list):
                    return data
            else:
                import csv
                reader = csv.reader(f)
                return list(reader)
    except Exception as e:
        logging.error(f"Error reading data: {e}")
    return []

def main():
    creds_json = os.environ.get("GCP_CREDENTIALS")
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    worksheet_name = os.environ.get("WORKSHEET_NAME", "Sheet1")
    blender_file = os.environ.get("BLENDER_OUTPUT", "blender_output.json")

    if not creds_json or not sheet_id:
        logging.error("Missing GCP_CREDENTIALS or GOOGLE_SHEET_ID environment variables.")
        sys.exit(1)

    try:
        credentials = Credentials.from_service_account_info(
            json.loads(creds_json),
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
        )
        gc = gspread.authorize(credentials)
        sh = gc.open_by_key(sheet_id)
        
        try:
            worksheet = sh.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            worksheet = sh.add_worksheet(title=worksheet_name, rows=1000, cols=20)
            
    except Exception as e:
        logging.error(f"Failed to authenticate or open sheet: {e}")
        sys.exit(1)

    data = get_blender_data(blender_file)
    if not data:
        logging.info("No data to push.")
        return

    total_rows = len(data)
    chunk_size = 1000
    logging.info(f"Loaded {total_rows} rows from {blender_file}. Pushing in chunks of {chunk_size}...")

    def colnum_string(n):
        string = ""
        while n > 0:
            n, remainder = divmod(n - 1, 26)
            string = chr(65 + remainder) + string
        return string

    num_chunks = math.ceil(total_rows / chunk_size)
    
    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx = min((i + 1) * chunk_size, total_rows)
        chunk = data[start_idx:end_idx]

        start_row = start_idx + 1 # Google sheets are 1-indexed
        end_row = start_idx + len(chunk)
        
        max_cols = max(len(row) for row in chunk) if chunk else 1
        end_col = colnum_string(max_cols)

        cell_range = f"A{start_row}:{end_col}{end_row}"

        logging.info(f"Pushing chunk {i+1}/{num_chunks} (Range: {cell_range}, Rows: {len(chunk)})")
        
        # Use batch_update to push 1000 rows at a time to prevent timeouts
        worksheet.batch_update([{
            'range': cell_range,
            'values': chunk
        }])
        
        # Adding a sleep to prevent API timeouts / rate limits
        if i < num_chunks - 1:
            time.sleep(1.5)

    logging.info("Sync completed successfully.")

if __name__ == "__main__":
    main()
