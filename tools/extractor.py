"""
WF-04: Extractor
Enriches restaurant records produced by scout_api.py with any missing fields.
Strategy:
  - Wolt records: already enriched during scout (v4 API). This stage only fills
    gaps (phone still N/A, address still N/A) via a fresh v4 call.
  - Uber records: tries to GET the store page for phone number (very rarely
    available) and verifies URL is live.

No Selenium. Pure HTTP only. Works on GitHub Actions without any browser deps.
"""

import argparse
import json
import random
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from config import MAX_RETRIES
from tools.logger import ScraperLogger
from tools.state_manager import StateManager

# ---------------------------------------------------------------------------
# Shared HTTP helpers
# ---------------------------------------------------------------------------

WOLT_VENUE_V4_URL = "https://restaurant-api.wolt.com/v4/venues/slug/{slug}"

_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_WOLT_HEADERS = {
    "User-Agent":      _BROWSER_UA,
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "de-DE,de;q=0.9",
    "Referer":         "https://wolt.com/",
    "Origin":          "https://wolt.com",
}

_GENERIC_HEADERS = {
    "User-Agent":      _BROWSER_UA,
    "Accept":          "text/html,application/xhtml+xml,*/*",
    "Accept-Language": "de-DE,de;q=0.9",
}

# German phone number pattern (covers +49 and 0-prefixed formats)
_PHONE_RE = re.compile(
    r"(?<!\d)(\+49[\s\-\./]?(?:\d[\s\-\./]?){9,12}|0\d{2,4}[\s\-\./]?\d{3,8}[\s\-\./]?\d{0,5})(?!\d)"
)

# Email pattern
_EMAIL_RE = re.compile(
    r"(?<!\S)([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})(?!\S)"
)

# Blacklisted domains for email/phone (platform-owned, not the restaurant's)
_EMAIL_BLACKLIST = {"uber.com", "ubereats.com", "wolt.com", "support.uber.com"}


def _clean_phone(raw: str) -> str:
    """Normalise a German phone number string."""
    cleaned = re.sub(r"[\s\-\(\)\/\.]", "", raw).strip()
    return cleaned if len(cleaned) >= 7 else raw.strip()


def _clean_email(raw: str) -> str:
    domain = raw.split("@")[-1].lower()
    if domain in _EMAIL_BLACKLIST:
        return "N/A"
    return raw.strip().lower()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Wolt enrichment via v4 API
# ---------------------------------------------------------------------------

def _wolt_slug_from_url(url: str) -> str:
    """Extract slug from a Wolt URL like .../restaurant/my-restaurant-slug"""
    parts = url.rstrip("/").split("/")
    return parts[-1] if parts else ""


def enrich_wolt(record: dict, log: ScraperLogger) -> dict:
    """
    Re-fetch the v4 venue endpoint to fill missing phone/address.
    Only called when phone or address is still N/A.
    """
    slug = _wolt_slug_from_url(record.get("url", ""))
    if not slug:
        return record

    bound = log.bind(record.get("zip_code", ""))
    url   = WOLT_VENUE_V4_URL.format(slug=slug)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=_WOLT_HEADERS, timeout=12)
            if resp.status_code == 429:
                time.sleep(10 * attempt)
                continue
            if resp.status_code != 200:
                break

            results = resp.json().get("results", [])
            if not results:
                break
            detail = results[0]

            # Fill gaps only — don't overwrite data we already have
            if record.get("phone") in ("N/A", "", None):
                phone = detail.get("public_phone") or detail.get("phone")
                if phone:
                    record["phone"] = _clean_phone(str(phone))

            if record.get("address") in ("N/A", "", None):
                addr = detail.get("address")
                if addr:
                    record["address"] = addr.strip()

            if record.get("rating") in ("N/A", "", None):
                rating_obj = detail.get("rating") or {}
                if isinstance(rating_obj, dict):
                    score  = rating_obj.get("score")
                    volume = rating_obj.get("volume")
                    if score:
                        record["rating"]  = str(score)
                        record["reviews"] = str(volume or "N/A")

            bound.debug(
                action="WOLT_ENRICH",
                status="OK",
                message=f"Enriched slug={slug}",
            )
            return record

        except requests.exceptions.Timeout:
            bound.warning(action="WOLT_ENRICH", message=f"Timeout attempt {attempt}")
            time.sleep(5)
        except Exception as exc:
            bound.warning(action="WOLT_ENRICH", message=f"Error: {exc}")
            break

    return record


# ---------------------------------------------------------------------------
# Uber Eats — try to scrape phone/email from the public store page
# ---------------------------------------------------------------------------

def enrich_uber(record: dict, log: ScraperLogger) -> dict:
    """
    Attempt to GET the Uber Eats store page for phone and email.
    Uber Eats rarely exposes this, but when it does it's in a <script> tag
    or in the rendered HTML as plain text.
    This is best-effort — failure is acceptable.
    """
    url   = record.get("url", "")
    bound = log.bind(record.get("zip_code", ""))

    if not url or record.get("phone") not in ("N/A", "", None):
        return record  # already have phone

    try:
        resp = requests.get(
            url,
            headers=_GENERIC_HEADERS,
            timeout=12,
            allow_redirects=True,
        )
        if resp.status_code != 200:
            return record

        text = resp.text

        # Try to find phone in page text
        m_phone = _PHONE_RE.search(text)
        if m_phone:
            cleaned = _clean_phone(m_phone.group(1))
            record["phone"] = cleaned
            bound.debug(action="UBER_ENRICH", message=f"Found phone={cleaned}")

        # Try to find email
        if record.get("email") in ("N/A", "", None):
            m_email = _EMAIL_RE.search(text)
            if m_email:
                cleaned = _clean_email(m_email.group(1))
                if cleaned != "N/A":
                    record["email"] = cleaned
                    bound.debug(action="UBER_ENRICH", message=f"Found email={cleaned}")

    except Exception:
        pass  # enrichment failure is non-fatal

    return record


# ---------------------------------------------------------------------------
# Main extractor class
# ---------------------------------------------------------------------------

class Extractor:
    def __init__(self, worker_id: str, log: ScraperLogger, state: StateManager):
        self.worker_id = worker_id
        self.log       = log
        self.state     = state
        self._out_path = ROOT / ".tmp" / f"extracted_{worker_id}.jsonl"
        self._out_path.parent.mkdir(parents=True, exist_ok=True)

    def run(self, input_file: Path) -> dict:
        records = self._load_records(input_file)
        if not records:
            self.log.warning(
                action="EXTRACTOR_RUN",
                message=f"Input file empty or missing: {input_file}",
            )
            return {}

        urls      = [r["url"] for r in records]
        remaining = [r for r in records if r["url"] in set(self.state.pending(urls))]

        self.log.info(
            action="EXTRACTOR_RUN",
            message=f"Processing {len(remaining)}/{len(records)} records",
        )

        for record in remaining:
            self._process_record(record)

        return self.state.summary()

    def _load_records(self, path: Path) -> list[dict]:
        records = []
        if not path.exists():
            return records
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
        return records

    def _process_record(self, record: dict):
        url      = record.get("url", "")
        platform = record.get("platform", "wolt")
        bound    = self.log.bind(record.get("zip_code", ""))

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                enriched = self._enrich(record, platform)
                enriched["extracted_at"]   = _now_iso()
                enriched["extract_source"] = f"{platform}_api_v2"

                # Ensure all expected fields exist with N/A fallback
                for field in ("phone", "email", "address", "rating", "reviews"):
                    if not enriched.get(field):
                        enriched[field] = "N/A"

                with self._out_path.open("a", encoding="utf-8") as f:
                    f.write(json.dumps(enriched, ensure_ascii=False) + "\n")

                self.state.mark_completed(url)
                bound.info(
                    action="EXTRACT_DONE",
                    status="OK",
                    message=f"[{platform}] {enriched.get('name', '')} | phone={enriched.get('phone')}",
                )
                return

            except Exception as exc:
                if attempt == MAX_RETRIES:
                    bound.error(
                        action="EXTRACT_FAIL",
                        message=f"All retries exhausted: {exc}",
                        url=url,
                    )
                    self._write_failure(record)
                    self.state.mark_failed(url, str(exc))
                else:
                    bound.warning(
                        action="EXTRACT_RETRY",
                        message=f"Attempt {attempt} failed: {exc}",
                    )
                    time.sleep(3 * attempt)

    def _enrich(self, record: dict, platform: str) -> dict:
        """
        Wolt v4 API is no longer publicly accessible (returns 404).
        Address is already captured during discovery. Phone/email are
        not exposed by Wolt's public API. For Uber, attempt page scrape.
        """
        if platform == "uber":
            needs_enrich = record.get("phone") in ("N/A", "", None)
            if needs_enrich:
                enriched = enrich_uber(record, self.log)
                time.sleep(random.uniform(0.5, 1.2))
                return enriched

        # Wolt: pass through as-is (address already populated from discovery)
        return record

    def _write_failure(self, record: dict):
        failure = {
            **record,
            "extracted_at":   _now_iso(),
            "extract_source": "failed",
            "phone":          record.get("phone", "N/A"),
            "email":          record.get("email", "N/A"),
            "address":        record.get("address", "N/A"),
        }
        with self._out_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(failure, ensure_ascii=False) + "\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(description="WF-04: Extractor — enrich restaurant records")
    p.add_argument("--worker-id", required=True)
    args = p.parse_args()

    wid   = args.worker_id
    log   = ScraperLogger(worker_id=wid)
    state = StateManager(f"ext_{wid}")

    # Locate the correct shard assigned to this matrix worker
    idx        = wid.split("-")[-1]
    input_file = ROOT / ".tmp" / f"results_ext-{idx}.jsonl"

    log.info(
        action="EXTRACTOR_START",
        message=f"Worker {wid} processing {input_file}",
    )

    ext     = Extractor(worker_id=wid, log=log, state=state)
    summary = ext.run(input_file)

    log.info(action="EXTRACTOR_DONE", message=str(summary))
    print(f"\nDone: {summary}")


if __name__ == "__main__":
    main()