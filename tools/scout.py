"""
WAT Framework — WF-03: Unified Restaurant Scout (Production Grade)
Scrapes restaurant metadata from Wolt.com and UberEats.com.

Features:
    - Human-mimicry typing and interaction delays.
    - Smart-scroll logic with "Show More" button detection.
    - Platform-aware selection engine.
    - Atomic State Management (Resumes exactly where it left off).
    - Per-worker JSONL logging (No file locks required).

Usage:
    python tools/scout.py --chunk-file .tmp/chunks/chunk_01.json --worker-id scout-wolt-1 --platform wolt
    python tools/scout.py --chunk-file .tmp/chunks/chunk_01.json --worker-id scout-uber-1 --platform uber
"""

import argparse
import json
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict

from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from config import MAX_RETRIES, REQUEST_DELAY_MIN, REQUEST_DELAY_MAX, HEADLESS
from tools.browser import BrowserDriver
from tools.logger import ScraperLogger
from tools.state_manager import StateManager

# ---------------------------------------------------------------------------
# Timing & Human Mimicry Constants
# ---------------------------------------------------------------------------

KEYPRESS_DELAY_MS   = (45, 160)    # character delay
KEYPRESS_BURST_PROB = 0.15         # probability of a "thinking" pause
KEYPRESS_BURST_MS   = (200, 550)   # longer pause range

SCROLL_STALE_TIMEOUT = 4.0         # seconds to wait for new cards
SCROLL_PAUSE         = (1.2, 2.4)  # random pause after scroll
SHOW_MORE_MAX        = 40          # safety cap on pagination
POST_LOAD_WAIT       = (2.5, 4.5)  # wait after address selection

# ---------------------------------------------------------------------------
# Platform Selector Matrix
# ---------------------------------------------------------------------------

PLATFORM_CONFIG = {
    "wolt": {
        "url": "https://wolt.com/de/discovery",
        "selectors": {
            "address_input": [
                "input[data-test-id='address-input']",  # 2024 Update
                "[data-test-id='front-page-address-input']",
                "[data-test-id='address-search-input']",
                "input[placeholder*='Adresse']",
                "//input[@type='text'][contains(@aria-label,'Adresse')]"
            ],
            "address_suggestion": [
                "[data-test-id='address-suggestion-item']",
                "[data-test-id='autocomplete-suggestion-item']",
                "//ul[@data-test-id='address-suggestions']/li[1]",
                "//li[@role='option'][1]"
            ],
            "venue_card": [
                "[data-test-id='venue-card']",
                "a[href*='/restaurant/']",
                "a[href*='/venue/']",
                "[class*='VenueCard']"
            ],
            "venue_name": [
                "[data-test-id='venue-card-header']",
                "h3", "h2",
                "[class*='VenueCard__name']"
            ],
            "venue_closed": [
                "[data-test-id='venue-card-closed-badge']",
                "[class*='closed']",
                "//span[contains(text(),'geschlossen')]"
            ],
            "zero_results": [
                "[data-test-id='discovery-no-venues']",
                "//h1[contains(.,'keine')]",
                "//p[contains(.,'no restaurants')]"
            ],
            "show_more": [
                "[data-test-id='venue-list-loadmore-btn']",
                "//button[contains(.,'Mehr anzeigen')]",
                "//button[contains(.,'show more')]"
            ]
        }
    },
    "uber": {
        "url": "https://www.ubereats.com/de",
        "selectors": {
            "address_input": [
                "input#location-typeahead-home-input",
                "input[placeholder*='adresse']",
                "//input[contains(@aria-label,'adresse')]"
            ],
            "address_suggestion": [
                "li[role='option']",
                "div[data-testid='location-typeahead-home-suggestion']",
                "//ul/li[1]"
            ],
            "venue_card": [
                "div[data-testid='store-card']",
                "a[href*='/restaurant/']",
                "a[href*='/store/']"
            ],
            "venue_name": [
                "h3",
                "div[data-testid='store-card-title']",
                "div[data-testid='restaurant-name']"
            ],
            "venue_closed": [
                "//span[contains(.,'Geschlossen')]",
                "[data-testid='closed-badge']",
                "[class*='Closed']"
            ],
            "zero_results": [
                "//h1[contains(.,'Keine Ergebnisse')]",
                "//div[contains(.,'keine Restaurants')]"
            ],
            "show_more": [
                "//button[contains(.,'Mehr anzeigen')]",
                "//button[contains(.,'Load more')]"
            ]
        }
    }
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _is_xpath(sel: str) -> bool:
    return sel.startswith("//") or sel.startswith("(//")

# ---------------------------------------------------------------------------
# Scout Engine
# ---------------------------------------------------------------------------

class ProductionScout:
    """
    High-fidelity scout for finding unique restaurant URLs.
    Includes explicit state persistence and human interaction loops.
    """

    def __init__(
        self,
        platform_key: str,
        worker_id: str,
        browser: BrowserDriver,
        log: ScraperLogger,
        state: StateManager,
        save_closed: bool = False,
    ):
        self.platform_name = platform_key
        self.cfg           = PLATFORM_CONFIG[platform_key]
        self.worker_id     = worker_id
        self.browser       = browser
        self.log           = log
        self.state         = state
        self.save_closed   = save_closed

        self._out_path = ROOT / ".tmp" / f"results_{worker_id}.jsonl"
        self._out_path.parent.mkdir(parents=True, exist_ok=True)

    def run(self, zip_codes: list[str]) -> dict:
        """Main entry loop for processing assigned ZIP chunks."""
        remaining = self.state.pending(zip_codes)
        total = len(zip_codes)
        
        self.log.info(
            action="RUN_START",
            message=f"Starting {self.platform_name} scout | {len(remaining)} ZIPs pending",
            total=total,
            worker_id=self.worker_id,
        )

        for zip_code in remaining:
            self._process_zip_with_retry(zip_code)

        return self.state.summary()

    # ------------------------------------------------------------------
    # ZIP-level Orchestration
    # ------------------------------------------------------------------

    def _process_zip_with_retry(self, zip_code: str):
        bound = self.log.bind(zip_code)
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self._process_zip(zip_code, bound)
                return
            except Exception as exc:
                bound.retry(
                    action="ZIP_RETRY",
                    attempt=attempt,
                    message=f"Attempt {attempt}/{MAX_RETRIES} failed: {exc}",
                )
                if attempt < MAX_RETRIES:
                    try:
                        self.browser._restart()
                    except: pass
                    time.sleep(random.uniform(5.0, 10.0))
                else:
                    bound.error(action="ZIP_FAILED", message=f"Exhausted all retries")
                    self.state.mark_failed(zip_code, error=str(exc))

    def _process_zip(self, zip_code: str, bound):
        self.state.mark_in_progress(zip_code)

        # 1. Navigation & Address Entry
        ok = self._enter_location(zip_code, bound)
        if not ok:
            raise RuntimeError(f"Failed to set location to {zip_code}")

        # 2. Results Check
        time.sleep(random.uniform(*POST_LOAD_WAIT))
        if self._is_zero_results():
            bound.info(action="ZERO_RESULTS", status="SKIP", message="No venues found")
            self.state.mark_completed(zip_code, metadata={"count": 0, "zero": True})
            return

        # 3. Smart Pagination & Scrolling
        self._exhaustive_scroll(bound)

        # 4. Data Extraction
        raw_records = self._extract_all_visible_cards(zip_code)
        
        # 5. Filtering (Open vs Closed)
        if not self.save_closed:
            records = [r for r in raw_records if r["status"] == "open"]
        else:
            records = raw_records

        # 6. Persistence
        self._save_records(records)
        
        self.state.mark_completed(zip_code, metadata={"count": len(records)})
        bound.info(action="ZIP_DONE", message=f"Extracted {len(records)} unique URLs")

    # ------------------------------------------------------------------
    # Interaction Layer (Human Mimicry)
    # ------------------------------------------------------------------

    def _enter_location(self, zip_code: str, bound) -> bool:
        """Navigates to home and types ZIP code with human-like delays."""
        self.browser.open(self.cfg["url"], gdpr=True)
        
        input_el = self._find_first(self.cfg["selectors"]["address_input"], timeout=15)
        if not input_el:
            bound.error(action="FIND_INPUT", message="Address bar not found")
            return False

        try:
            input_el.clear()
        except: pass

        # Type ZIP character by character
        for char in zip_code:
            input_el.send_keys(char)
            delay = random.randint(*KEYPRESS_DELAY_MS)
            if random.random() < KEYPRESS_BURST_PROB:
                delay += random.randint(*KEYPRESS_BURST_MS)
            time.sleep(delay / 1000.0)

        time.sleep(random.uniform(1.2, 2.0))
        
        # Select first suggestion
        suggestion = self._find_first(self.cfg["selectors"]["address_suggestion"], timeout=5)
        if suggestion:
            try:
                ActionChains(self.browser.sb.driver).move_to_element(suggestion).click().perform()
                return True
            except: pass

        # Fallback to Enter key
        input_el.send_keys(Keys.RETURN)
        return True

    def _exhaustive_scroll(self, bound):
        """Scrolls and clicks 'Show More' until no new content appears."""
        last_count = 0
        stale_since = None
        clicks = 0

        while clicks < SHOW_MORE_MAX:
            current_count = len(self._get_card_elements())
            
            if current_count > last_count:
                last_count = current_count
                stale_since = None
                self.browser.scroll_to_bottom()
                time.sleep(random.uniform(*SCROLL_PAUSE))
            else:
                if stale_since is None:
                    stale_since = time.time()
                elif time.time() - stale_since >= SCROLL_STALE_TIMEOUT:
                    # Attempt pagination click
                    btn = self._find_first(self.cfg["selectors"]["show_more"], timeout=2)
                    if btn and btn.is_displayed():
                        clicks += 1
                        self.browser.sb.driver.execute_script("arguments[0].click();", btn)
                        stale_since = None
                        time.sleep(2.0)
                        continue
                    break # truly finished
            
            time.sleep(1.0)

    # ------------------------------------------------------------------
    # Extraction Layer
    # ------------------------------------------------------------------

    def _extract_all_visible_cards(self, zip_code: str) -> List[Dict]:
        cards = self._get_card_elements()
        results = []
        seen_urls = set()

        for card in cards:
            try:
                # Get URL
                url = ""
                try:
                    url = card.get_attribute("href") or card.find_element(By.TAG_NAME, "a").get_attribute("href")
                except: continue
                
                if not url or not any(x in url for x in ["restaurant", "venue", "store"]):
                    continue
                
                clean_url = url.split("?")[0].split("#")[0].rstrip("/")
                if clean_url in seen_urls:
                    continue
                seen_urls.add(clean_url)

                # Get Name
                name = "Unknown"
                for sel in self.cfg["selectors"]["venue_name"]:
                    try:
                        el = card.find_element(By.XPATH if _is_xpath(sel) else By.CSS_SELECTOR, sel)
                        if el.text:
                            name = el.text.strip()
                            break
                    except: continue

                results.append({
                    "name": name,
                    "url": clean_url,
                    "platform": self.platform_name,
                    "zip_code": zip_code,
                    "status": "closed" if self._is_closed(card) else "open",
                    "scraped_at": _now_iso(),
                    "worker_id": self.worker_id
                })
            except: continue

        return results

    def _is_closed(self, card_el) -> bool:
        for sel in self.cfg["selectors"]["venue_closed"]:
            try:
                if card_el.find_elements(By.XPATH if _is_xpath(sel) else By.CSS_SELECTOR, sel):
                    return True
            except: continue
        return False

    def _is_zero_results(self) -> bool:
        for sel in self.cfg["selectors"]["zero_results"]:
            try:
                if self.browser.sb.driver.find_elements(By.XPATH if _is_xpath(sel) else By.CSS_SELECTOR, sel):
                    return True
            except: continue
        return False

    def _get_card_elements(self):
        for sel in self.cfg["selectors"]["venue_card"]:
            try:
                els = self.browser.sb.driver.find_elements(By.XPATH if _is_xpath(sel) else By.CSS_SELECTOR, sel)
                if els: return els
            except: continue
        return []

    def _find_first(self, selectors: List[str], timeout: int = 8):
        deadline = time.time() + timeout
        while time.time() < deadline:
            for s in selectors:
                try:
                    el = self.browser.sb.driver.find_element(By.XPATH if _is_xpath(s) else By.CSS_SELECTOR, s)
                    if el.is_displayed(): return el
                except: continue
            time.sleep(0.5)
        return None

    def _save_records(self, records: List[Dict]):
        if not records: return
        with self._out_path.open("a", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")

# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description="Unified Production Scout")
    p.add_argument("--chunk-file", required=True, help="Path to ZIP chunk JSON")
    p.add_argument("--worker-id", required=True, help="Unique ID for this worker instance")
    p.add_argument("--platform", choices=["wolt", "uber"], required=True, help="Target platform")
    p.add_argument("--save-closed", action="store_true", help="Include closed restaurants")
    return p.parse_args()

def main():
    args = parse_args()
    log  = ScraperLogger(args.worker_id)
    state = StateManager(f"scout_{args.worker_id}")
    
    with open(args.chunk_file, "r") as f:
        data = json.load(f)
        zips = data.get("codes", [])

    log.info(
        action="SCOUT_INIT",
        message=f"Worker {args.worker_id} | Platform: {args.platform} | {len(zips)} ZIPs",
    )

    with BrowserDriver(worker_id=args.worker_id, headless=HEADLESS, logger=log) as browser:
        scout = ProductionScout(
            platform_key=args.platform,
            worker_id=args.worker_id,
            browser=browser,
            log=log,
            state=state,
            save_closed=args.save_closed
        )
        summary = scout.run(zips)

    log.info(action="SCOUT_DONE", message=f"Run complete: {summary}")

if __name__ == "__main__":
    main()