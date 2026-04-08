"""
WF-00: ZIP Code Geocoder
Converts all German PLZ codes to lat/lon coordinates natively via Geonames.
Runs in seconds instead of hours. Output is committed to the repo.

Usage:
    python tools/Geocoder.py

Output: data/zip_coords.json
"""

import argparse
import io
import json
import sys
import zipfile
from pathlib import Path

import requests

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from tools.utils import CustomLogger
from config import VALIDATION_ZIP_CODES

log = CustomLogger("geocoder")

PLZ_MASTER_PATH = ROOT / "data" / "plz_master.json"
OUTPUT_PATH     = ROOT / "data" / "zip_coords.json"
DATA_DIR        = ROOT / "data"

GEONAMES_URL = "https://download.geonames.org/export/zip/DE.zip"
GEONAMES_FILENAME = "DE.txt"

def load_existing_cache() -> dict:
    if OUTPUT_PATH.exists():
        with OUTPUT_PATH.open("r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_cache(cache: dict):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)

def build_geonames_db() -> dict:
    """Download and build an in-memory dictionary of {plz: {lat, lon}} from Geonames."""
    log.info("Downloading Geonames DB...", url=GEONAMES_URL)
    resp = requests.get(GEONAMES_URL, timeout=30)
    resp.raise_for_status()
    log.info("Download complete. Parsing coordinates...")
    
    db = {}
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        with zf.open(GEONAMES_FILENAME) as f:
            content = f.read().decode("utf-8")
            
    for line in content.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) >= 11:
            plz = parts[1].strip()
            lat_str = parts[9].strip()
            lon_str = parts[10].strip()
            if len(plz) == 5 and plz.isdigit() and lat_str and lon_str:
                try:
                    db[plz] = {"lat": float(lat_str), "lon": float(lon_str)}
                except ValueError:
                    pass
    return db

def run(zip_codes: list[str], resume: bool = False):
    cache = load_existing_cache() if resume else {}
    already_done = set(cache.keys())
    
    to_process = [z for z in zip_codes if z not in already_done]
    total = len(zip_codes)
    remaining = len(to_process)
    
    if remaining == 0:
        log.info("All ZIPs are already cached.")
        print("Done. All ZIPs already cached.")
        return

    log.info(f"Geocoding {remaining} ZIPs (skipping {total - remaining} already cached)")
    
    geonames_db = build_geonames_db()
    
    failed = []
    for zip_code in to_process:
        if zip_code in geonames_db:
            cache[zip_code] = geonames_db[zip_code]
        else:
            failed.append(zip_code)
            
    save_cache(cache)
    
    log.info(
        "Geocoding complete",
        total_zips=total,
        resolved=len(cache),
        failed=len(failed),
        output=str(OUTPUT_PATH),
    )
    if failed:
        log.warning(f"Failed ZIPs ({len(failed)}) - likely non-geographic PO Boxes.")

    print(f"\nDone. {len(cache):,} / {total:,} ZIPs resolved in seconds.")
    print(f"Output: {OUTPUT_PATH}")
    if failed:
        print(f"Failed ({len(failed)}): {failed[:10]}{'...' if len(failed)>10 else ''}")

def main():
    parser = argparse.ArgumentParser(description="WF-00: ZIP → lat/lon geocoder (via Geonames)")
    parser.add_argument("--test",   action="store_true", help="Use validation ZIPs only")
    parser.add_argument("--resume", action="store_true", help="Skip ZIPs already in output file")
    args = parser.parse_args()

    if args.test:
        log.info("TEST MODE: using validation ZIPs", codes=VALIDATION_ZIP_CODES)
        run(list(VALIDATION_ZIP_CODES), resume=args.resume)
        return

    if not PLZ_MASTER_PATH.exists():
        print(f"ERROR: {PLZ_MASTER_PATH} not found. Run tools/plz_gen.py first.")
        sys.exit(1)

    with PLZ_MASTER_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)
    zip_codes = data.get("codes", [])

    if not zip_codes:
        print("ERROR: plz_master.json contains no codes.")
        sys.exit(1)

    print(f"\nTotal ZIPs: {len(zip_codes):,}")
    print(f"Speed: Fast (using in-memory Geonames DB)")
    print(f"Output: {OUTPUT_PATH}")

    if not args.resume:
        confirm = input("\nProceed? (yes/no): ").strip().lower()
        if confirm != "yes":
            print("Aborted.")
            sys.exit(0)

    run(zip_codes, resume=args.resume)

if __name__ == "__main__":
    main()