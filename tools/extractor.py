"""
WAT Framework — WF-04: Legal Data Extractor
Hydration State Interception Architecture
"""

import argparse
import json
import re
import requests
import sys
import time
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from selenium.webdriver.common.by import By

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from config import MAX_RETRIES, HEADLESS
from tools.browser import BrowserDriver
from tools.logger import ScraperLogger
from tools.state_manager import StateManager

# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------

OWNER_RE = re.compile(
    r"(?i)(?:Firma|Inhaber|Gesch[äa]ftsf[üu]hrer|Vertreten[ \t]+durch|Name[ \t]+des[ \t]+Vertretungsberechtigten)[\s:\"\'\\]+([A-ZÄÖÜ][a-zäöüß]+(?:[ \t][A-ZÄÖÜ][a-zäöüß&0-9]+){0,4})"
)

_SUFFIX = (
    r"(?:GmbH(?:\s*&\s*Co\.?\s*KG)?|"
    r"UG\s*\(?haftungsbeschr[äa]nkt\)?|"
    r"AG|KG|OHG|GbR|e\.K\.|e\.V\.|"
    r"Ltd\.|LLC|SE|S\.à\s*r\.l\.)"
)
ENTITY_RE = re.compile(
    r"([A-ZÄÖÜ0-9][A-Za-z0-9ÄÖÜäöüß\s&,.\-\']{1,60}\s*" + _SUFFIX + r")",
    re.MULTILINE,
)

PHONE_RE = re.compile(
    r"(?:"
    r"(?:\+49|00\s?49)[\s.\-]?(?:\(0\)[\s.\-]?)?"
    r"|0"
    r")"
    r"\d{2,5}"
    r"[\s.\-/]?"
    r"\d{3,12}"
    r"(?:[\s.\-/]?\d{1,6})?",
    re.MULTILINE,
)

EMAIL_RE = re.compile(
    r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b"
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()

def _detect_platform(url: str) -> str:
    u = url.lower()
    if "wolt.com" in u:
        return "wolt"
    if "ubereats.com" in u:
        return "ubereats"
    return "generic"

# ---------------------------------------------------------------------------
# LegalExtractor
# ---------------------------------------------------------------------------

class LegalExtractor:
    """
    Visits restaurant URLs and extracts legal / Impressum data.
    """
    def __init__(
        self,
        worker_id: str,
        browser: BrowserDriver,
        log: ScraperLogger,
        state: StateManager,
    ):
        self.worker_id = worker_id
        self.browser   = browser
        self.log       = log
        self.state     = state

        self._out_path = ROOT / ".tmp" / f"extracted_{worker_id}.jsonl"
        self._out_path.parent.mkdir(parents=True, exist_ok=True)

    def run(self, records: list[dict]) -> dict:
        pending = [r for r in records if not self.state.is_completed(r["url"])]
        skipped = len(records) - len(pending)

        self.log.info(
            action="EXTRACT_RUN_START",
            message=f"{len(pending)} URLs to extract ({skipped} already done)",
            pending=len(pending),
            skipped=skipped,
        )

        for record in pending:
            self._process_with_retry(record)

        summary = self.state.summary()
        self.log.info(action="EXTRACT_RUN_DONE", message=str(summary), **summary)
        return summary

    def _process_with_retry(self, record: dict):
        url   = record["url"]
        bound = self.log.bind(record.get("zip_code", ""))

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                result = self._process_record(record, bound)
                self._save_record(result)
                self.state.mark_completed(url, metadata={
                    "extract_status": result["extract_status"],
                    "impressum_source": result.get("impressum_source", ""),
                    "has_owner": bool(result.get("owner") and result.get("owner") != "Hidden by Platform"),
                    "has_phone": bool(result.get("phone")),
                })
                return

            except Exception as exc:
                bound.retry(
                    action="EXTRACT_RETRY",
                    attempt=attempt,
                    message=f"Attempt {attempt}/{MAX_RETRIES}: {exc}",
                    url=url,
                )
                if attempt < MAX_RETRIES:
                    try:
                        self.browser._restart()
                    except Exception:
                        pass
                    time.sleep(random.uniform(3.0, 6.0))
                else:
                    bound.error(
                        action="EXTRACT_FAILED",
                        message=f"All {MAX_RETRIES} attempts exhausted for {url}",
                        url=url,
                    )
                    self.state.mark_failed(url, error=str(exc))
                    self._save_record(self._failure_record(record, str(exc)))

    def _process_record(self, record: dict, bound) -> dict:
        url      = record["url"]
        platform = _detect_platform(url)
        self.state.mark_in_progress(url)

        bound.info(action="EXTRACT_START", status="TRY",
                   message=f"[{platform}] {record.get('name', '?')}", url=url)

        # ==========================================
        # 1. ANTIGRAVITY FAST-PATH (HTTP BYPASS)
        # ==========================================
        try:
            fast_resp = requests.get(
                url, 
                timeout=5,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
                    "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7"
                }
            )
            if fast_resp.status_code == 200:
                fast_text = fast_resp.text
                fields = self._parse_legal_fields(fast_text)
                
                if fields.get("owner") not in ["", None, "Hidden by Platform"] or fields.get("email") or fields.get("address"):
                    bound.info(action="FAST_PATH_HIT", status="SUCCESS", message="Extracted via HTTP bypass")
                    fields["impressum_url"] = url
                    fields["impressum_source"] = "http_hydration"
                    fields["raw_snippet"] = fast_text[:600]
                    fields["extract_status"] = "ok"
                    
                    return {
                        **record,
                        "extracted_at": _now_iso(),
                        **fields,
                    }
        except Exception as e:
            bound.debug(action="FAST_PATH_ERROR", status="SKIP", message=str(e))

        # ==========================================
        # 2. SLOW-PATH: SELENIUM (The Deep Hunt)
        # ==========================================
        bound.info(action="SLOW_PATH", status="TRY", message="Fallback to Selenium")
        
        if hasattr(self.browser.sb, "uc_open_with_reconnect"):
            try:
                self.browser.sb.uc_open_with_reconnect(url, 4)
                ok = True
            except Exception:
                ok = self.browser.open(url, gdpr=True)
        else:
            ok = self.browser.open(url, gdpr=True)

        if not ok:
            raise RuntimeError(f"browser.open() failed for {url}")

        if platform == "wolt":
            time.sleep(2.0)
            self.browser.sb.driver.execute_script("document.querySelectorAll('[data-test-id=\\'venue-info-button\\'], button').forEach(b => { if(b.innerText.includes('Info')) b.click(); })")
            time.sleep(1.5)
        elif platform == "ubereats":
            time.sleep(2.0)
            self.browser.sb.driver.execute_script("document.querySelectorAll('button').forEach(b => { if(b.innerText.includes('Mehr Info') || b.innerText.includes('More info')) b.click(); })")
            time.sleep(1.5)
        else:
            time.sleep(2.5)

        page_source = self.browser.sb.get_page_source()
        fields = self._parse_legal_fields(page_source)

        has_data = fields.get("owner") not in ["", None, "Hidden by Platform"] or fields.get("legal_entity") not in ["", None, "Hidden by Platform"] or fields.get("email")

        fields["impressum_url"]    = url
        fields["impressum_source"] = "selenium_fallback"
        fields["raw_snippet"]      = page_source[:600] if page_source else ""
        fields["extract_status"]   = "ok" if has_data else "not_found"

        result = {
            **record,
            "extracted_at": _now_iso(),
            **fields,
        }

        bound.info(
            action="EXTRACT_DONE",
            status=fields["extract_status"],
            message=f"owner={fields.get('owner') or '-'} | entity={fields.get('legal_entity') or '-'} | phone={fields.get('phone') or '-'}",
            url=url,
        )
        return result

    def _parse_legal_fields(self, text: str) -> dict:
        if not text:
            return {"owner": "", "legal_entity": "", "phone": "", "email": "", "address": ""}

        fields = {
            "owner":        self._extract_owner(text),
            "legal_entity": self._extract_entity(text),
            "phone":        self._extract_phone(text),
            "email":        self._extract_email(text),
            "address":      self._extract_address(text),
        }

        banned_entities = ["uber portier", "wolt enterprises", "uber eats", "wolt.com", "uber.com"]
        banned_emails = ["support@uber.com", "support@wolt.com", "hilfe@uber.com", "privacy@uber.com"]
        
        if fields.get("phone"):
            if fields["phone"].startswith("+31") or fields["phone"].startswith("0031") or fields["phone"].startswith("+358"):
                fields["phone"] = None

        if fields.get("email"):
            if any(banned in fields["email"].lower() for banned in banned_emails):
                fields["email"] = None

        if fields.get("owner"):
            if any(banned in fields["owner"].lower() for banned in banned_entities):
                fields["owner"] = None

        if fields.get("legal_entity"):
            if any(banned in fields["legal_entity"].lower() for banned in banned_entities):
                fields["legal_entity"] = None

        if not fields.get("email") and not fields.get("owner") and not fields.get("legal_entity"):
            fields["owner"] = "Hidden by Platform"
            fields["legal_entity"] = "Hidden by Platform"

        return fields

    def _extract_owner(self, text: str) -> str:
        m = OWNER_RE.search(text)
        return _clean(m.group(1)) if m else ""

    def _extract_entity(self, text: str) -> str:
        m_firma = re.search(r"(?i)Firma[\s:]+([^\n\"\'\\]+)", text)
        if m_firma:
            return _clean(m_firma.group(1))

        standalone = re.compile(
            r"^([A-ZÄÖÜ0-9][A-Za-z0-9ÄÖÜäöüß\s&,.\-\']{1,60}\s*" + _SUFFIX + r")\s*$",
            re.MULTILINE,
        )
        m = standalone.search(text)
        if m:
            return _clean(m.group(1))

        m = ENTITY_RE.search(text)
        return _clean(m.group(1)) if m else ""

    def _extract_phone(self, text: str) -> str:
        m = PHONE_RE.search(text)
        if not m:
            return ""
        raw = m.group(0).strip()
        return re.sub(r"[\s.\-]{2,}", " ", raw).strip()

    def _extract_email(self, text: str) -> str:
        m = EMAIL_RE.search(text)
        return m.group(0).lower() if m else ""

    def _extract_address(self, text: str) -> str:
        m_uber = re.search(r"([A-ZÄÖÜ][A-Za-zÄÖÜäöüß\.\- ]{2,35}?,?\s*\d{1,4}[a-zA-Z]?[,\s]+[A-ZÄÖÜ][A-Za-zÄÖÜäöüß\.\- ]{2,30}?[,\s]+\d{5})(?!\d)", text)
        if m_uber: return m_uber.group(1).replace('"', '').replace('\\', '').strip()
        m_std = re.search(r"([A-ZÄÖÜ][A-Za-zÄÖÜäöüß\.\- ]{2,35}?\s+\d{1,4}[a-zA-Z]?\s*,?\s*\d{5}\s+[A-ZÄÖÜ][A-Za-zÄÖÜäöüß\.\- ]{2,30})(?![A-Za-z])", text)
        if m_std: return m_std.group(1).replace('"', '').replace('\\', '').strip()
        return ""

    def _save_record(self, record: dict):
        with self._out_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    @staticmethod
    def _failure_record(record: dict, error: str) -> dict:
        return {
            **record,
            "extracted_at":    _now_iso(),
            "extract_status":  "failed",
            "impressum_url":   "",
            "impressum_source": "",
            "owner":           "",
            "legal_entity":    "",
            "phone":           "",
            "email":           "",
            "address":         "",
            "raw_snippet":     "",
            "error":           error,
        }

# ---------------------------------------------------------------------------
# I/O helpers & CLI
# ---------------------------------------------------------------------------

def _load_results(path: Path) -> list[dict]:
    records = []
    with path.open("r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as exc:
                print(f"  WARN: skipping malformed line {lineno}: {exc}", file=sys.stderr)
    return records

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="WF-04: Extract legal data (Impressum) from restaurant URLs."
    )
    source = p.add_mutually_exclusive_group(required=True)
    source.add_argument("--worker-id", metavar="ID", help="Worker ID")
    source.add_argument("--url", metavar="URL", help="Single URL")
    p.add_argument("--results-file", metavar="PATH", help="Input file path")
    p.add_argument("--no-headless", action="store_true", help="Show browser")
    return p.parse_args()

def main():
    args = parse_args()

    if args.url:
        wid     = "debug"
        records = [{"url": args.url, "name": "debug", "zip_code": "", "status": "open"}]
    else:
        wid = args.worker_id
        results_path = Path(args.results_file) if args.results_file \
            else ROOT / ".tmp" / f"results_{wid}.jsonl"

        if not results_path.exists():
            print(f"ERROR: results file not found: {results_path}", file=sys.stderr)
            sys.exit(1)

        records = _load_results(results_path)
        if not records:
            print("INFO: results file empty.", file=sys.stderr)
            sys.exit(0)

    headless = not args.no_headless and HEADLESS
    log      = ScraperLogger(worker_id=wid)
    state    = StateManager(f"extract_{wid}")

    log.info(
        action="EXTRACTOR_INIT",
        message=f"Worker {wid} | {len(records)} records | headless={headless}",
        record_count=len(records),
        headless=headless,
    )

    with BrowserDriver(worker_id=wid, headless=headless, logger=log) as browser:
        extractor = LegalExtractor(
            worker_id=wid,
            browser=browser,
            log=log,
            state=state,
        )
        summary = extractor.run(records)

    log.info(action="EXTRACTOR_DONE", message=str(summary), **summary)
    print(f"\nDone. Summary: {summary}")
    out = ROOT / ".tmp" / f"extracted_{wid}.jsonl"
    print(f"Output → {out}")

if __name__ == "__main__":
    main()
