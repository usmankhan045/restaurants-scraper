import argparse
import json
import random
import sys
import time
import re
import requests
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from config import HEADLESS, MAX_RETRIES
from tools.browser import BrowserDriver
from tools.logger import ScraperLogger
from tools.state_manager import StateManager

# The API-Safe Regex: Looks for JSON keys as well as HTML text
OWNER_RE = re.compile(
    r"(?i)(?:Firma|Inhaber|Gesch[äa]ftsf[üu]hrer|Vertreten[ \t]+durch|Name[ \t]+des[ \t]+Vertretungsberechtigten)[\s:\"\'\\]+([A-ZÄÖÜ][a-zäöüß]+(?:[ \t][A-ZÄÖÜ][a-zäöüß&0-9\.\-]+){0,4})"
)

class LegalExtractor:
    def __init__(self, worker_id: str, browser: BrowserDriver, log: ScraperLogger, state: StateManager):
        self.worker_id = worker_id
        self.browser = browser
        self.log = log
        self.state = state
        self._out_path = ROOT / ".tmp" / f"extracted_{worker_id}.jsonl"
        self._out_path.parent.mkdir(parents=True, exist_ok=True)

    def run(self, input_file: Path) -> dict:
        records = []
        if input_file.exists():
            with input_file.open("r", encoding="utf-8") as f:
                for line in f:
                    if line.strip(): records.append(json.loads(line))
        else:
            self.log.warning(action="RUN", message="Input file does not exist, shard is empty.")
            return {}
                    
        remaining = [r for r in records if r["url"] in self.state.pending([x["url"] for x in records])]
        for rec in remaining:
            self._process_with_retry(rec)
        return self.state.summary()

    def _process_with_retry(self, record: dict):
        url = record["url"]
        bound = self.log.bind(url)
        for attempt in range(1, 4):
            try:
                res = self._process_record(record, bound)
                with self._out_path.open("a", encoding="utf-8") as f:
                    f.write(json.dumps(res, ensure_ascii=False) + "\n")
                self.state.mark_completed(url)
                return
            except Exception as e:
                if attempt == 3:
                    self.state.mark_failed(url, str(e))
                    with self._out_path.open("a", encoding="utf-8") as f:
                        f.write(json.dumps(self._failure_record(record), ensure_ascii=False) + "\n")
                time.sleep(2)

    def _process_record(self, record: dict, bound) -> dict:
        url = record["url"]
        platform = "wolt" if "wolt" in url else "uber"
        
        bound.info(action="EXTRACT_START", status="TRY", message=f"[{platform}] {record.get('name', '')}")

        extracted_text = ""
        source = "none"

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Accept": "application/json, text/html, */*",
            "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8"
        }

        # -------------------------------------------------------------
        # 1. API INTERCEPTION (Fast Path)
        # -------------------------------------------------------------
        try:
            if platform == "wolt":
                # Intercept Wolt Backend API
                slug = url.rstrip("/").split("/")[-1]
                api_url = f"https://restaurant-api.wolt.com/v3/venues/slug/{slug}"
                resp = requests.get(api_url, headers=headers, timeout=5)
                if resp.status_code == 200:
                    extracted_text = json.dumps(resp.json(), ensure_ascii=False)
                    source = "wolt_api"
            elif platform == "uber":
                # Intercept Uber React Hydration State
                resp = requests.get(url, headers=headers, timeout=5)
                if resp.status_code == 200:
                    text = resp.text
                    m = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', text)
                    if not m:
                        m = re.search(r'<script type="application/json" id="main-initial-state">(.*?)</script>', text)
                    if m:
                        try:
                            # Dump to clear unicode escapes
                            json_data = json.loads(m.group(1))
                            extracted_text = json.dumps(json_data, ensure_ascii=False)
                            source = "uber_api"
                        except:
                            extracted_text = text
                    else:
                        extracted_text = text
        except Exception:
            pass

        fields = self._parse_legal_fields(extracted_text)

        # -------------------------------------------------------------
        # 2. SELENIUM FALLBACK (If Cloudflare blocked the API)
        # -------------------------------------------------------------
        if not fields.get("owner") and not fields.get("address"):
            try:
                if hasattr(self.browser.sb, "uc_open_with_reconnect"):
                    self.browser.sb.uc_open_with_reconnect(url, 4)
                else:
                    self.browser.open(url, gdpr=True)
                
                time.sleep(3)
                
                if platform == "wolt":
                    self.browser.sb.driver.execute_script("document.querySelectorAll('[data-test-id=\\'venue-info-button\\'], button').forEach(b => { if(b.innerText.includes('Info')) b.click(); })")
                    time.sleep(2)
                elif platform == "uber":
                    self.browser.sb.driver.execute_script("document.querySelectorAll('button').forEach(b => { if(b.innerText.includes('Mehr Info') || b.innerText.includes('More info')) b.click(); })")
                    time.sleep(2)
                    
                extracted_text = self.browser.sb.get_page_source()
                fields = self._parse_legal_fields(extracted_text)
                source = "selenium_fallback"
            except:
                pass

        # -------------------------------------------------------------
        # 3. DATA GUARD & CLEANSING
        # -------------------------------------------------------------
        banned = ["uber portier", "wolt enterprises", "uber eats"]
        
        if fields.get("owner"):
            if any(b in fields["owner"].lower() for b in banned):
                fields["owner"] = "Hidden by Platform"
        
        if fields.get("email"):
            if "uber.com" in fields["email"] or "wolt.com" in fields["email"]:
                fields["email"] = "N/A"

        if not fields.get("owner"): fields["owner"] = "N/A"
        if not fields.get("address"): fields["address"] = "N/A"
        if not fields.get("email"): fields["email"] = "N/A"
        if not fields.get("phone"): fields["phone"] = "N/A"

        bound.info(action="EXTRACT_DONE", status="ok" if fields["owner"] != "N/A" else "not_found", message=f"owner={fields['owner']} | source={source}")

        return {
            **record,
            "extracted_at": _now_iso(),
            "impressum_source": source,
            **fields
        }

    def _parse_legal_fields(self, text: str) -> dict:
        if not text: return {}
        fields = {}
        
        # 1. Owner Regex
        m_owner = OWNER_RE.search(text)
        if m_owner:
            fields["owner"] = m_owner.group(1).replace('"', '').replace('\\', '').strip()
            
        m_firma = re.search(r'"Firma"\s*:\s*"([^"]+)"', text)
        if m_firma: fields["owner"] = m_firma.group(1).replace('\\', '').replace('"', '').strip()
            
        # 2. Email Regex
        m_email = re.search(r"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})", text)
        if m_email: fields["email"] = m_email.group(1)
            
        # 3. Phone Regex
        m_phone = re.search(r"(?:\+49|0)[1-9][0-9 \-\(\)]{6,14}", text)
        if m_phone: fields["phone"] = m_phone.group(0).strip()
            
        # 4. Address Regex
        m_addr1 = re.search(r"([A-ZÄÖÜ][A-Za-zÄÖÜäöüß\.\- ]{2,35}?,?\s*\d{1,4}[a-zA-Z]?[,\s]+[A-ZÄÖÜ][A-Za-zÄÖÜäöüß\.\- ]{2,30}?[,\s]+\d{5})(?!\d)", text)
        m_addr2 = re.search(r"([A-ZÄÖÜ][A-Za-zÄÖÜäöüß\.\- ]{2,35}?\s+\d{1,4}[a-zA-Z]?\s*,?\s*\d{5}\s+[A-ZÄÖÜ][A-Za-zÄÖÜäöüß\.\- ]{2,30})(?![A-Za-z])", text)
        
        if m_addr1: fields["address"] = m_addr1.group(1).replace('"', '').replace('\\', '').strip()
        elif m_addr2: fields["address"] = m_addr2.group(1).replace('"', '').replace('\\', '').strip()
        
        return fields

    def _failure_record(self, record):
        return {**record, "extracted_at": _now_iso(), "owner": "N/A", "address": "N/A", "email": "N/A", "phone": "N/A", "impressum_source": "failed"}

def _now_iso(): return datetime.now(timezone.utc).isoformat()

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--worker-id", required=True)
    args = p.parse_args()
    
    wid = args.worker_id
    log = ScraperLogger(worker_id=wid)
    state = StateManager(f"ext_{wid}")
    
    # Locate the correct shard file assigned to this matrix worker
    idx = wid.split("-")[-1]
    input_file = ROOT / ".tmp" / f"results_ext-{idx}.jsonl"
    
    with BrowserDriver(worker_id=wid, headless=HEADLESS, logger=log) as browser:
        ext = LegalExtractor(worker_id=wid, browser=browser, log=log, state=state)
        summary = ext.run(input_file)
        
    log.info(action="EXTRACTOR_DONE", message=str(summary))

if __name__ == "__main__":
    main()