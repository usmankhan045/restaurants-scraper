import argparse
import json
import random
import sys
import time
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from config import MAX_RETRIES, REQUEST_DELAY_MIN, REQUEST_DELAY_MAX, HEADLESS
from tools.browser import BrowserDriver
from tools.logger import ScraperLogger
from tools.state_manager import StateManager

KEYPRESS_DELAY_MS   = (50, 180)
KEYPRESS_BURST_PROB = 0.12
KEYPRESS_BURST_MS   = (250, 600)
SCROLL_STALE_TIMEOUT = 12.0        # 12s buffer for slow Uber loading
SCROLL_PAUSE         = (1.5, 2.5)  
SHOW_MORE_MAX        = 40          
POST_LOAD_WAIT       = (3.0, 5.0)  

PLATFORM_CONFIG = {
    "wolt": {
        "url": "https://wolt.com/de/discovery",
        "selectors": {
            "address_input": ["input[data-test-id='address-input']", "[data-test-id='front-page-address-input']", "[data-test-id='address-search-input']", "input[placeholder*='Adresse']"],
            "address_suggestion": ["[data-test-id='address-suggestion-item']", "[data-test-id='autocomplete-suggestion-item']", "//ul[@data-test-id='address-suggestions']/li[1]"],
            "venue_card": ["[data-test-id='venue-card']", "a[href*='/restaurant/']", "a[href*='/venue/']"],
            "venue_name": ["[data-test-id='venue-card-header']", "[data-test-id='venue-name']", "h3", "h2"],
            "venue_closed": ["[data-test-id='venue-card-closed-badge']", "[class*='closed']", "[class*='unavailable']"],
            "zero_results": ["[data-test-id='discovery-no-venues']", "[data-test-id='empty-venue-list']"],
            "show_more": ["[data-test-id='venue-list-loadmore-btn']", "//button[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'mehr anzeigen')]"]
        }
    },
    "uber": {
        "url": "https://www.ubereats.com/de",
        "selectors": {
            "address_input": ["input#location-typeahead-home-input", "input[placeholder*='adresse']"],
            "address_suggestion": ["li[role='option']", "div[data-testid='location-typeahead-home-suggestion']", "//ul/li[1]"],
            "venue_card": ["div[data-testid='store-card']", "a[href*='/restaurant/']", "a[href*='/store/']"],
            "venue_name": ["h3", "div[data-testid='store-card-title']"],
            "venue_closed": ["//span[contains(.,'Geschlossen')]", "[data-testid='closed-badge']"],
            "zero_results": ["//h1[contains(.,'Keine Ergebnisse')]"],
            "show_more": ["//button[contains(.,'Mehr anzeigen')]"]
        }
    }
}

def _now_iso() -> str: return datetime.now(timezone.utc).isoformat()
def _is_xpath(sel: str) -> bool: return sel.startswith("//") or sel.startswith("(//")

class UnifiedScout:
    def __init__(self, platform: str, worker_id: str, browser: BrowserDriver, log: ScraperLogger, state: StateManager, save_closed: bool = False):
        self.platform = platform
        self.cfg = PLATFORM_CONFIG[platform]
        self.worker_id = worker_id
        self.browser = browser
        self.log = log
        self.state = state
        self.save_closed = save_closed
        self._out_path = ROOT / ".tmp" / f"results_{worker_id}.jsonl"
        self._out_path.parent.mkdir(parents=True, exist_ok=True)

    def run(self, zip_codes: list[str]) -> dict:
        remaining = self.state.pending(zip_codes)
        self.log.info(action="RUN_START", message=f"Platform: {self.platform.upper()} | {len(remaining)} ZIPs to process")
        for zip_code in remaining: self._process_zip_with_retry(zip_code)
        return self.state.summary()

    def _process_zip_with_retry(self, zip_code: str):
        bound = self.log.bind(zip_code)
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self._process_zip(zip_code, bound)
                return
            except Exception as exc:
                bound.retry(action="ZIP_RETRY", attempt=attempt, message=f"Failed: {exc}")
                if attempt < MAX_RETRIES:
                    try: self.browser._restart()
                    except: pass
                    time.sleep(random.uniform(3.0, 6.0))
                else:
                    bound.error(action="ZIP_FAILED", message="All attempts exhausted")
                    self.state.mark_failed(zip_code, error=str(exc))

    def _process_zip(self, zip_code: str, bound):
        self.state.mark_in_progress(zip_code)
        if not self._navigate_to_zip(zip_code, bound): raise RuntimeError("Navigation failed")
        time.sleep(random.uniform(*POST_LOAD_WAIT))
        if self._is_zero_results():
            bound.info(action="ZERO_RESULTS", message="No restaurants in area")
            self.state.mark_completed(zip_code, metadata={"count": 0, "zero_results": True})
            return
        self._smart_scroll(zip_code, bound)
        raw_records = self._extract_all_cards(zip_code)
        records = raw_records if self.save_closed else [r for r in raw_records if r["status"] == "open"]
        self._save_records(records)
        bound.info(action="EXTRACT", message=f"Extracted {len(records)} active restaurants", raw_count=len(records))
        self.state.mark_completed(zip_code, metadata={"count": len(records)})

    def _navigate_to_zip(self, zip_code: str, bound) -> bool:
        target_url = self.cfg["url"]
        bound.info(action="NAVIGATE", status="TRY", message=f"Opening {target_url}")
        
        try:
            if hasattr(self.browser.sb, "uc_open_with_reconnect"):
                self.browser.sb.uc_open_with_reconnect(target_url, 4)
            else:
                self.browser.open(target_url, gdpr=True)
        except:
            self.browser.open(target_url, gdpr=True)

        input_el = self._find_first(self.cfg["selectors"]["address_input"], timeout=20)
        
        # Safely attempt captcha click ONLY if Cloudflare is blocking the input field
        if not input_el and self.platform == "wolt" and hasattr(self.browser.sb, "uc_gui_handle_captcha"):
            try:
                self.browser.sb.uc_gui_handle_captcha()
                input_el = self._find_first(self.cfg["selectors"]["address_input"], timeout=10)
            except: pass

        if not input_el:
            bound.error(action="FIND_INPUT", message="Address input not found (Likely blocked by Cloudflare/Turnstile)")
            return False
            
        try: input_el.clear()
        except: pass
        
        self._type_zip_human(input_el, zip_code, bound)
        time.sleep(random.uniform(1.0, 2.0))
        
        if not self._click_suggestion(bound):
            bound.warning(action="SUGGESTION", message="No suggestion found, pressing Enter")
            try:
                input_el.send_keys(Keys.RETURN)
                time.sleep(random.uniform(1.5, 2.5))
            except: return False
            
        bound.info(action="NAVIGATE", status="OK", message="ZIP entered")
        return True

    def _type_zip_human(self, element, zip_code: str, bound):
        driver = self.browser.sb.driver
        for char in zip_code:
            delay = random.randint(*KEYPRESS_DELAY_MS)
            if random.random() < KEYPRESS_BURST_PROB: delay += random.randint(*KEYPRESS_BURST_MS)
            actions = ActionChains(driver)
            actions.move_to_element(element).send_keys_to_element(element, char).perform()
            time.sleep(delay / 1000.0)

    def _click_suggestion(self, bound) -> bool:
        for sel in self.cfg["selectors"]["address_suggestion"]:
            try:
                els = self.browser.sb.driver.find_elements(By.XPATH, sel) if _is_xpath(sel) else self.browser.sb.driver.find_elements(By.CSS_SELECTOR, sel)
                if els:
                    ActionChains(self.browser.sb.driver).move_to_element(els[0]).click().perform()
                    return True
            except: continue
        return False

    def _is_zero_results(self) -> bool:
        for sel in self.cfg["selectors"]["zero_results"]:
            try:
                els = self.browser.sb.driver.find_elements(By.XPATH, sel) if _is_xpath(sel) else self.browser.sb.driver.find_elements(By.CSS_SELECTOR, sel)
                if els and any(e.is_displayed() for e in els): return True
            except: continue
        return False

    def _smart_scroll(self, zip_code: str, bound):
        bound.info(action="SCROLL_START", message="Starting smart scroll")
        last_count, stale_since, show_more_clicks = 0, None, 0
        while True:
            current_count = self._count_cards()
            if current_count > last_count:
                bound.debug(action="SCROLL", message=f"Cards: {last_count} → {current_count}")
                last_count = current_count
                stale_since = None
            else:
                if stale_since is None: stale_since = time.time()
                elif time.time() - stale_since >= SCROLL_STALE_TIMEOUT:
                    if show_more_clicks < SHOW_MORE_MAX and self._click_show_more():
                        show_more_clicks += 1
                        stale_since = None
                        time.sleep(random.uniform(1.5, 2.5))
                        continue
                    bound.info(action="SCROLL_DONE", message=f"Completed: {last_count} cards")
                    break
            self.browser.scroll_to_bottom()
            time.sleep(random.uniform(*SCROLL_PAUSE))

    def _count_cards(self) -> int:
        for sel in self.cfg["selectors"]["venue_card"]:
            try:
                els = self.browser.sb.driver.find_elements(By.XPATH, sel) if _is_xpath(sel) else self.browser.sb.driver.find_elements(By.CSS_SELECTOR, sel)
                if els: return len(els)
            except: continue
        return 0

    def _click_show_more(self) -> bool:
        for sel in self.cfg["selectors"]["show_more"]:
            try:
                els = self.browser.sb.driver.find_elements(By.XPATH, sel) if _is_xpath(sel) else self.browser.sb.driver.find_elements(By.CSS_SELECTOR, sel)
                visible = [e for e in els if e.is_displayed() and e.is_enabled()]
                if visible:
                    self.browser.sb.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", visible[0])
                    time.sleep(0.5)
                    visible[0].click()
                    return True
            except: continue
        return False

    def _extract_all_cards(self, zip_code: str) -> list[dict]:
        card_elements = []
        for sel in self.cfg["selectors"]["venue_card"]:
            try:
                els = self.browser.sb.driver.find_elements(By.XPATH, sel) if _is_xpath(sel) else self.browser.sb.driver.find_elements(By.CSS_SELECTOR, sel)
                if els: card_elements = els; break
            except: continue
            
        seen_urls = set()
        records = []
        for card_el in card_elements:
            rec = self._extract_single_card(card_el, zip_code)
            if rec and rec["url"] and rec["url"] not in seen_urls:
                seen_urls.add(rec["url"])
                records.append(rec)
        return records

    def _extract_single_card(self, card_el, zip_code: str) -> Optional[dict]:
        try:
            url = ""
            
            # THE FIX: Check if the card itself is the link (Uber Eats format)
            try:
                href = card_el.get_attribute("href") or ""
                if href and href.startswith("http"): url = href
            except: pass
            
            # Fallback: Check for nested links inside the card (Wolt format)
            if not url:
                try:
                    for a in card_el.find_elements(By.TAG_NAME, "a"):
                        h = a.get_attribute("href") or ""
                        if any(s in h for s in ("/restaurant/", "/venue/", "/delivery/", "/store/")):
                            url = h
                            break
                except: pass
                
            if not url: return None
            clean_url = url.split("?")[0].split("#")[0].rstrip("/")

            name = "Unknown"
            for sel in self.cfg["selectors"]["venue_name"]:
                try:
                    els = card_el.find_elements(By.XPATH, sel) if _is_xpath(sel) else card_el.find_elements(By.CSS_SELECTOR, sel)
                    if els and els[0].text.strip(): name = els[0].text.strip(); break
                except: continue

            text = card_el.text.strip()
            rating, reviews = "N/A", "N/A"
            m_rat = re.search(r"(?<!\d)([1-9][.,]\d|10[.,]0)(?!\d)", text)
            if m_rat: rating = m_rat.group(1).replace(",", ".")
            m_rev = re.search(r"\(([\d.,kK+]+)\)", text)
            if m_rev: reviews = m_rev.group(1)

            is_closed = False
            for sel in self.cfg["selectors"]["venue_closed"]:
                try:
                    els = card_el.find_elements(By.XPATH, sel) if _is_xpath(sel) else card_el.find_elements(By.CSS_SELECTOR, sel)
                    if els: is_closed = True; break
                except: continue

            return {
                "name": name, "url": clean_url, "platform": self.platform,
                "status": "closed" if is_closed else "open",
                "rating": rating, "reviews": reviews,
                "zip_code": zip_code, "scraped_at": _now_iso(), "worker_id": self.worker_id
            }
        except: return None

    def _find_first(self, selectors: list[str], timeout: float = 8.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            for sel in selectors:
                try:
                    els = self.browser.sb.driver.find_elements(By.XPATH, sel) if _is_xpath(sel) else self.browser.sb.driver.find_elements(By.CSS_SELECTOR, sel)
                    vis = [e for e in els if e.is_displayed()]
                    if vis: return vis[0]
                except: continue
            time.sleep(0.5)
        return None

    def _save_records(self, records: list[dict]):
        if records:
            with self._out_path.open("a", encoding="utf-8") as f:
                for r in records: f.write(json.dumps(r, ensure_ascii=False) + "\n")

def parse_args():
    p = argparse.ArgumentParser()
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--chunk-file")
    g.add_argument("--zip")
    p.add_argument("--worker-id", required=True)
    p.add_argument("--platform", required=True, choices=["wolt", "uber"])
    p.add_argument("--save-closed", action="store_true")
    p.add_argument("--no-headless", action="store_true")
    return p.parse_args()

def main():
    args = parse_args()
    wid, plat = args.worker_id, args.platform
    log, state = ScraperLogger(worker_id=wid), StateManager(f"scout_{wid}")
    zips = [args.zip.strip()] if args.zip else json.load(open(args.chunk_file, "r", encoding="utf-8")).get("codes", [])
    headless = not args.no_headless and HEADLESS

    log.info(action="SCOUT_INIT", message=f"Starting {plat} scout for {len(zips)} ZIPs")

    with BrowserDriver(worker_id=wid, headless=headless, logger=log) as browser:
        scout = UnifiedScout(platform=plat, worker_id=wid, browser=browser, log=log, state=state, save_closed=args.save_closed)
        summary = scout.run(zips)

    log.info(action="SCOUT_DONE", message=str(summary))

if __name__ == "__main__": main()