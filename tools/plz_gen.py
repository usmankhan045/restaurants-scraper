"""
WF-01: PLZ Generator
Fetches all ~8,200 unique German 5-digit postal codes from Geonames
and saves them to data/plz_master.json.

Usage:
    python tools/plz_gen.py
"""

import io
import json
import sys
import time
import zipfile
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------

ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data"
OUTPUT_PATH = DATA_DIR / "plz_master.json"

GEONAMES_URL = "https://download.geonames.org/export/zip/DE.zip"
GEONAMES_FILENAME = "DE.txt"   # file inside the zip

MAX_RETRIES = 3
RETRY_DELAY = 5          # seconds between retries
REQUEST_TIMEOUT = 30     # seconds

# ---------------------------------------------------------------------------
# Bootstrap utils (import after path is set)
# ---------------------------------------------------------------------------

sys.path.insert(0, str(ROOT))
from tools.utils import CustomLogger

log = CustomLogger("plz_gen")


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def download_with_retry(url: str) -> bytes:
    """Download URL content with up to MAX_RETRIES attempts."""
    last_exc: Exception | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info(f"Downloading Geonames DE.zip (attempt {attempt}/{MAX_RETRIES})", url=url)
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            log.info(
                "Download complete",
                bytes=len(response.content),
                status_code=response.status_code,
            )
            return response.content

        except requests.exceptions.ConnectionError as exc:
            last_exc = exc
            log.warning(
                f"Connection error on attempt {attempt}",
                error=str(exc),
                retry_in=RETRY_DELAY if attempt < MAX_RETRIES else 0,
            )
        except requests.exceptions.Timeout as exc:
            last_exc = exc
            log.warning(
                f"Request timed out after {REQUEST_TIMEOUT}s (attempt {attempt})",
                retry_in=RETRY_DELAY if attempt < MAX_RETRIES else 0,
            )
        except requests.exceptions.HTTPError as exc:
            last_exc = exc
            status = exc.response.status_code if exc.response else "unknown"
            log.warning(
                f"HTTP {status} error (attempt {attempt})",
                error=str(exc),
                retry_in=RETRY_DELAY if attempt < MAX_RETRIES else 0,
            )
        except Exception as exc:
            last_exc = exc
            log.warning(
                f"Unexpected error on attempt {attempt}",
                error=str(exc),
            )

        if attempt < MAX_RETRIES:
            log.info(f"Waiting {RETRY_DELAY}s before retry...")
            time.sleep(RETRY_DELAY)

    # All retries exhausted
    log.error(
        "All download attempts failed. Source may be unreachable.",
        url=url,
        attempts=MAX_RETRIES,
        last_error=str(last_exc),
        diagnosis=(
            "Check network connectivity, firewall rules, or whether "
            f"{url} is reachable. "
            "Alternative mirror: https://raw.githubusercontent.com/datasets/postal-codes-germany/main/data/postal-codes-de.csv"
        ),
    )
    raise RuntimeError(
        f"Failed to download {url} after {MAX_RETRIES} attempts. "
        f"Last error: {last_exc}"
    ) from last_exc


def parse_postal_codes(raw_bytes: bytes) -> list[str]:
    """
    Unzip the Geonames archive and parse unique 5-digit PLZ from DE.txt.

    Geonames TSV columns (0-indexed):
        0  country_code
        1  postal_code
        2  place_name
        ... (more geo fields)
    """
    with zipfile.ZipFile(io.BytesIO(raw_bytes)) as zf:
        if GEONAMES_FILENAME not in zf.namelist():
            available = zf.namelist()
            log.error("Expected file not found in archive", expected=GEONAMES_FILENAME, found=available)
            raise FileNotFoundError(
                f"{GEONAMES_FILENAME} not in zip. Found: {available}"
            )

        with zf.open(GEONAMES_FILENAME) as f:
            content = f.read().decode("utf-8")

    codes: set[str] = set()
    skipped = 0

    for line_no, line in enumerate(content.splitlines(), start=1):
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < 2:
            skipped += 1
            continue
        plz = parts[1].strip()
        if len(plz) == 5 and plz.isdigit():
            codes.add(plz)
        else:
            skipped += 1

    sorted_codes = sorted(codes)
    log.info(
        "Parsed postal codes",
        total_unique=len(sorted_codes),
        lines_skipped=skipped,
        sample=sorted_codes[:5],
    )
    return sorted_codes


def save_output(codes: list[str]) -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "source": GEONAMES_URL,
        "fetched_at": __import__("datetime").datetime.utcnow().isoformat() + "Z",
        "total": len(codes),
        "codes": codes,
    }
    with OUTPUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    log.info("Saved PLZ master list", path=str(OUTPUT_PATH), total=len(codes))
    return OUTPUT_PATH


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    log.info("=== WF-01: PLZ Generator started ===")
    try:
        raw = download_with_retry(GEONAMES_URL)
        codes = parse_postal_codes(raw)
        out = save_output(codes)
        log.info("=== WF-01 complete ===", output=str(out), total_codes=len(codes))
        print(f"\nDone. {len(codes):,} unique PLZ codes saved to {out}")
    except Exception as exc:
        log.exception("WF-01 failed with unhandled exception", exc=exc)
        print(f"\nFATAL: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
