"""
WAT Framework — WF-03: Wolt Restaurant Scout
Scrapes restaurant name, URL, and open/closed status from Wolt.com
for every German ZIP code assigned to this worker.

Concurrency model:
    20 parallel instances, each writing to its own JSONL file.
    No shared file — zero write collisions by design.

Usage:
    # Process a full chunk (typical production use)
    python tools/scout.py --chunk-file .tmp/chunks/chunk_01.json --worker-id worker-01

    # Single ZIP (development / debugging)
    python tools/scout.py --zip 10115 --worker-id dev

    # Include closed restaurants in output
    python tools/scout.py --chunk-file .tmp/chunks/chunk_01.json --worker-id worker-01 --save-closed

Output:
    .tmp/results_{worker_id}.jsonl  — one JSON record per restaurant per line
    .tmp/checkpoint_scout_{worker_id}.json — progress checkpoint (resume on restart)
"""

import argparse
import json
import random
import sys
import time
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

# ---------------------------------------------------------------------------
# Timing constants
# ---------------------------------------------------------------------------

KEYPRESS_DELAY_MS   = (50, 180)    # per-character delay range (milliseconds)
KEYPRESS_BURST_PROB = 0.12         # probability of a longer "thinking" pause
KEYPRESS_BURST_MS   = (250, 600)   # range for those longer pauses

SCROLL_STALE_TIMEOUT = 3.0         # seconds of no new cards before stopping
SCROLL_PAUSE         = (0.9, 1.6)  # random pause after each scroll action
SHOW_MORE_MAX        = 25          # safety cap on "Show More" clicks
POST_LOAD_WAIT       = (1.8, 3.2)  # wait after ZIP suggestion is clicked

# ---------------------------------------------------------------------------
# Wolt selectors
# Organised as priority-ordered lists — first match wins.
# CSS selectors are tried before XPath (faster in DOM).
# ---------------------------------------------------------------------------

# Wolt discovery / address input
WOLT_URL = "https://wolt.com/de/discovery"

ADDRESS_INPUT = [
    "[data-test-id='front-page-address-input']",
    "[data-test-id='address-search-input']",
    "input[placeholder*='Adresse']",
    "input[placeholder*='adresse']",
    "input[placeholder*='Lieferadresse']",
    "//input[contains(@placeholder,'Adresse')]",
    "//input[contains(@placeholder,'adresse')]",
    "//input[@type='text'][contains(@aria-label,'Adresse')]",
]

ADDRESS_SUGGESTION = [
    "[data-test-id='address-suggestion-item']",
    "[data-test-id='autocomplete-suggestion-item']",
    "[data-test-id='address-item']",
    "//ul[@data-test-id='address-suggestions']/li[1]",
    "//ul[@role='listbox']/li[1]",
    "//div[@role='option'][1]",
    "//li[@role='option'][1]",
]

# Venue card root element (each instance = 1 restaurant)
VENUE_CARD = [
    "[data-test-id='venue-card']",
    "a[href*='/restaurant/']",
    "a[href*='/venue/']",
    "[class*='VenueCard']",
    "[class*='venue-card']",
]

# Selectors tried *within* a card element to extract the name
VENUE_NAME = [
    "[data-test-id='venue-card-header']",
    "[data-test-id='venue-name']",
    "h3",
    "h2",
    "[class*='VenueCard__name']",
    "[class*='venue-card__name']",
    "[class*='venueName']",
    "//h3",
]

# Selectors tried *within* a card to detect "closed" state
VENUE_CLOSED = [
    "[data-test-id='venue-card-closed-badge']",
    "[data-test-id='closed-badge']",
    "[class*='closed']",
    "[class*='Closed']",
    "[class*='unavailable']",
    "//span[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'geschlossen')]",
    "//span[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'closed')]",
]

# Page-level "no restaurants in this area" indicators
ZERO_RESULTS = [
    "[data-test-id='discovery-no-venues']",
    "[data-test-id='empty-venue-list']",
    "[data-test-id='no-results']",
    "//h1[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'keine')]",
    "//h2[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'keine')]",
    "//p[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'keine restaurant')]",
    "//p[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'no restaurants')]",
]

# "Show More" / "Mehr anzeigen" button
SHOW_MORE = [
    "[data-test-id='venue-list-loadmore-btn']",
    "[data-test-id='load-more-btn']",
    "//button[normalize-space(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'))='mehr anzeigen']",
    "//button[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'mehr anzeigen')]",
    "//button[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'show more')]",
    "//button[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'load more')]",
    "//a[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'mehr anzeigen')]",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_xpath(sel: str) -> bool:
    return sel.startswith("//") or sel.startswith("(//")


# ---------------------------------------------------------------------------
# WoltScout
# ---------------------------------------------------------------------------

class WoltScout:
    """
    Scrapes restaurant listings from Wolt.com for a list of German ZIP codes.

    Parameters
    ----------
    worker_id   : str  — unique worker name, drives filenames and logs
    browser     : BrowserDriver — already-constructed driver (caller manages context)
    log         : ScraperLogger
    state       : StateManager — checkpoint keyed on worker_id
    save_closed : bool — include closed restaurants in output (default False)
    """

    def __init__(
        self,
        worker_id: str,
        browser: BrowserDriver,
        log: ScraperLogger,
        state: StateManager,
        save_closed: bool = False,
    ):
        self.worker_id = worker_id
        self.browser   = browser
        self.log       = log
        self.state     = state
        self.save_closed = save_closed

        self._out_path = ROOT / ".tmp" / f"results_{worker_id}.jsonl"
        self._out_path.parent.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(self, zip_codes: list[str]) -> dict:
        """
        Process all ZIP codes in zip_codes, skipping already-completed ones.
        Returns the StateManager summary dict when finished.
        """
        remaining = self.state.pending(zip_codes)
        total = len(zip_codes)
        skipped = total - len(remaining)

        self.log.info(
            action="RUN_START",
            message=f"{len(remaining)} ZIPs to process ({skipped} already done)",
            total=total,
            remaining=len(remaining),
            worker_id=self.worker_id,
        )

        for zip_code in remaining:
            self._process_zip_with_retry(zip_code)

        summary = self.state.summary()
        self.log.info(
            action="RUN_COMPLETE",
            message=str(summary),
            worker_id=self.worker_id,
        )
        return summary

    # ------------------------------------------------------------------
    # ZIP-level orchestration
    # ------------------------------------------------------------------

    def _process_zip_with_retry(self, zip_code: str):
        """Retry wrapper around _process_zip with MAX_RETRIES attempts."""
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
                    # Restart browser between retries for a clean slate
                    try:
                        self.browser._restart()
                    except Exception:
                        pass
                    time.sleep(random.uniform(3.0, 6.0))
                else:
                    bound.error(
                        action="ZIP_FAILED",
                        message=f"All {MAX_RETRIES} attempts exhausted",
                    )
                    self.state.mark_failed(zip_code, error=str(exc))

    def _process_zip(self, zip_code: str, bound):
        """
        Full scraping pipeline for a single ZIP code.
        Raises on unrecoverable failure — caller handles retries.
        """
        self.state.mark_in_progress(zip_code)

        # 1. Navigate to Wolt and enter the ZIP code
        ok = self._navigate_to_zip(zip_code, bound)
        if not ok:
            raise RuntimeError("Navigation / address-entry failed")

        # 2. Short settle wait for the listing to render
        time.sleep(random.uniform(*POST_LOAD_WAIT))

        # 3. Check for zero-results state before scrolling
        if self._is_zero_results():
            bound.info(
                action="ZERO_RESULTS",
                status="SKIP",
                message="No restaurants available in this area",
            )
            self.state.mark_completed(zip_code, metadata={"count": 0, "zero_results": True})
            return

        # 4. Smart scroll to load all cards
        self._smart_scroll(zip_code, bound)

        # 5. Extract all visible cards
        raw_records = self._extract_all_cards(zip_code)
        bound.info(
            action="EXTRACT",
            message=f"Extracted {len(raw_records)} cards (incl. closed)",
            raw_count=len(raw_records),
        )

        # 6. Data Guard — filter closed unless --save-closed
        if not self.save_closed:
            open_records = [r for r in raw_records if r["status"] == "open"]
            closed_count = len(raw_records) - len(open_records)
            if closed_count:
                bound.info(
                    action="FILTER_CLOSED",
                    message=f"Dropped {closed_count} closed restaurants",
                    dropped=closed_count,
                )
            records = open_records
        else:
            records = raw_records

        # 7. Persist to per-worker JSONL
        self._save_records(records)

        self.state.mark_completed(
            zip_code,
            metadata={"count": len(records), "closed_found": len(raw_records) - len(records)},
        )
        bound.info(
            action="ZIP_DONE",
            status="OK",
            message=f"Saved {len(records)} restaurants",
            saved=len(records),
        )

    # ------------------------------------------------------------------
    # Navigation — open Wolt and enter the ZIP code
    # ------------------------------------------------------------------

    def _navigate_to_zip(self, zip_code: str, bound) -> bool:
        """
        1. Open Wolt discovery page (handles GDPR automatically via BrowserDriver).
        2. Locate the address input field.
        3. Type the ZIP code with human-like keypress delays.
        4. Wait for autocomplete dropdown and click the first suggestion.
        Returns True on success.
        """
        bound.info(action="NAVIGATE", status="TRY", message=f"Opening {WOLT_URL}")
        ok = self.browser.open(WOLT_URL, gdpr=True)
        if not ok:
            bound.error(action="NAVIGATE", message="browser.open() failed")
            return False

        # Locate address input
        input_el = self._find_first(ADDRESS_INPUT, timeout=12)
        if input_el is None:
            bound.error(action="FIND_INPUT", message="Address input not found on page")
            return False

        # Clear any pre-filled text and type ZIP character by character
        try:
            input_el.clear()
        except Exception:
            pass

        self._type_zip_human(input_el, zip_code, bound)

        # Wait for suggestion dropdown
        time.sleep(random.uniform(0.8, 1.4))

        # Click the first suggestion
        clicked = self._click_suggestion(bound)
        if not clicked:
            bound.warning(
                action="SUGGESTION",
                status="FALLBACK",
                message="No suggestion found — pressing Enter as fallback",
            )
            try:
                input_el.send_keys(Keys.RETURN)
                time.sleep(random.uniform(1.5, 2.5))
            except Exception:
                return False

        bound.info(action="NAVIGATE", status="OK", message="ZIP entered and suggestion selected")
        return True

    def _type_zip_human(self, element, zip_code: str, bound):
        """
        Send ZIP code keystrokes one character at a time with randomised delays
        to mimic realistic human typing behaviour.
        """
        driver = self.browser.sb.driver
        bound.debug(
            action="TYPE_ZIP",
            message=f"Typing {zip_code} ({len(zip_code)} chars)",
        )
        for char in zip_code:
            delay_ms = random.randint(*KEYPRESS_DELAY_MS)
            if random.random() < KEYPRESS_BURST_PROB:
                delay_ms += random.randint(*KEYPRESS_BURST_MS)

            actions = ActionChains(driver)
            actions.move_to_element(element)
            actions.send_keys_to_element(element, char)
            actions.perform()
            time.sleep(delay_ms / 1000.0)

    def _click_suggestion(self, bound) -> bool:
        """Click the first address autocomplete suggestion. Returns True if clicked."""
        for sel in ADDRESS_SUGGESTION:
            try:
                if _is_xpath(sel):
                    els = self.browser.sb.driver.find_elements(By.XPATH, sel)
                else:
                    els = self.browser.sb.driver.find_elements(By.CSS_SELECTOR, sel)

                if els:
                    target = els[0]
                    ActionChains(self.browser.sb.driver).move_to_element(target).click().perform()
                    time.sleep(random.uniform(0.4, 0.8))
                    bound.debug(
                        action="SUGGESTION",
                        status="CLICKED",
                        message=f"Clicked via selector: {sel[:60]}",
                    )
                    return True
            except Exception:
                continue
        return False

    # ------------------------------------------------------------------
    # Zero-results detection
    # ------------------------------------------------------------------

    def _is_zero_results(self) -> bool:
        """Return True if Wolt is showing a 'no restaurants in area' state."""
        for sel in ZERO_RESULTS:
            try:
                if _is_xpath(sel):
                    els = self.browser.sb.driver.find_elements(By.XPATH, sel)
                else:
                    els = self.browser.sb.driver.find_elements(By.CSS_SELECTOR, sel)
                if els and any(e.is_displayed() for e in els):
                    return True
            except Exception:
                continue
        return False

    # ------------------------------------------------------------------
    # Smart scroll — loads all cards before extraction
    # ------------------------------------------------------------------

    def _smart_scroll(self, zip_code: str, bound):
        """
        Scroll down in a loop until no new venue cards have appeared for
        SCROLL_STALE_TIMEOUT seconds. If stale, attempt a 'Show More' click
        before giving up. Caps Show More at SHOW_MORE_MAX for safety.
        """
        bound.info(action="SCROLL_START", message="Starting smart scroll")
        last_count       = 0
        stale_since: Optional[float] = None
        show_more_clicks = 0

        while True:
            current_count = self._count_cards()

            if current_count > last_count:
                bound.debug(
                    action="SCROLL",
                    status="PROGRESS",
                    message=f"Cards: {last_count} → {current_count}",
                    count=current_count,
                )
                last_count  = current_count
                stale_since = None  # reset — new content arrived

            else:
                # No new cards this cycle
                if stale_since is None:
                    stale_since = time.time()
                elif time.time() - stale_since >= SCROLL_STALE_TIMEOUT:
                    # Stale for 3 s — try "Show More" before giving up
                    if show_more_clicks < SHOW_MORE_MAX:
                        clicked = self._click_show_more()
                        if clicked:
                            show_more_clicks += 1
                            stale_since = None
                            bound.debug(
                                action="SHOW_MORE",
                                status="CLICKED",
                                message=f"Show More click #{show_more_clicks}",
                            )
                            time.sleep(random.uniform(1.2, 2.2))
                            continue

                    # Nothing else to load
                    bound.info(
                        action="SCROLL_DONE",
                        status="OK",
                        message=f"Completed: {last_count} cards, {show_more_clicks} Show More clicks",
                        total_cards=last_count,
                        show_more_clicks=show_more_clicks,
                    )
                    break

            # Scroll down a bit and pause
            self.browser.scroll_to_bottom()
            time.sleep(random.uniform(*SCROLL_PAUSE))

    def _count_cards(self) -> int:
        """Return the number of venue card elements currently in the DOM."""
        for sel in VENUE_CARD:
            try:
                if _is_xpath(sel):
                    els = self.browser.sb.driver.find_elements(By.XPATH, sel)
                else:
                    els = self.browser.sb.driver.find_elements(By.CSS_SELECTOR, sel)
                if els:
                    return len(els)
            except Exception:
                continue
        return 0

    def _click_show_more(self) -> bool:
        """Attempt to click a 'Show More' / 'Mehr anzeigen' button. Returns True if clicked."""
        for sel in SHOW_MORE:
            try:
                if _is_xpath(sel):
                    els = self.browser.sb.driver.find_elements(By.XPATH, sel)
                else:
                    els = self.browser.sb.driver.find_elements(By.CSS_SELECTOR, sel)

                visible = [e for e in els if e.is_displayed() and e.is_enabled()]
                if visible:
                    self.browser.sb.driver.execute_script(
                        "arguments[0].scrollIntoView({block:'center'});", visible[0]
                    )
                    time.sleep(0.3)
                    visible[0].click()
                    return True
            except Exception:
                continue
        return False

    # ------------------------------------------------------------------
    # Card extraction
    # ------------------------------------------------------------------

    def _extract_all_cards(self, zip_code: str) -> list[dict]:
        """
        Extract restaurant data from all venue cards currently in the DOM.
        Returns a list of dicts: {name, url, status, zip_code, scraped_at}.
        Deduplicates by URL.
        """
        card_elements = self._get_card_elements()
        seen_urls: set[str] = set()
        records: list[dict] = []

        for card_el in card_elements:
            rec = self._extract_single_card(card_el, zip_code)
            if rec is None:
                continue
            url = rec["url"]
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            records.append(rec)

        return records

    def _get_card_elements(self) -> list:
        """Return WebElement list for all venue cards using first matching selector."""
        for sel in VENUE_CARD:
            try:
                if _is_xpath(sel):
                    els = self.browser.sb.driver.find_elements(By.XPATH, sel)
                else:
                    els = self.browser.sb.driver.find_elements(By.CSS_SELECTOR, sel)
                if els:
                    return els
            except Exception:
                continue
        return []

    def _extract_single_card(self, card_el, zip_code: str) -> Optional[dict]:
        """
        Extract name, URL, and open/closed status from a single WebElement.
        Returns None if the element is not a valid restaurant card.
        """
        try:
            # --- URL ---
            url = self._extract_url_from_card(card_el)
            if not url:
                return None
            # Only keep proper restaurant / venue paths
            if not any(seg in url for seg in ("/restaurant/", "/venue/", "/delivery/")):
                return None

            # --- Name ---
            name = self._extract_name_from_card(card_el)

            # --- Closed status ---
            is_closed = self._is_card_closed(card_el)

            return {
                "name":       name,
                "url":        url,
                "status":     "closed" if is_closed else "open",
                "zip_code":   zip_code,
                "scraped_at": _now_iso(),
                "worker_id":  self.worker_id,
            }
        except Exception:
            return None

    def _extract_url_from_card(self, card_el) -> str:
        """Try to find a restaurant URL from the card element or its anchor child."""
        # The card itself might be an <a>
        try:
            href = card_el.get_attribute("href") or ""
            if href and href.startswith("http"):
                return href
        except Exception:
            pass

        # Look for nested <a> tags
        try:
            anchors = card_el.find_elements(By.TAG_NAME, "a")
            for a in anchors:
                href = a.get_attribute("href") or ""
                if href and any(
                    seg in href for seg in ("/restaurant/", "/venue/", "/delivery/")
                ):
                    return href
        except Exception:
            pass

        return ""

    def _extract_name_from_card(self, card_el) -> str:
        """Try each VENUE_NAME selector within the card element."""
        for sel in VENUE_NAME:
            try:
                if _is_xpath(sel):
                    els = card_el.find_elements(By.XPATH, sel)
                else:
                    els = card_el.find_elements(By.CSS_SELECTOR, sel)
                for el in els:
                    text = el.text.strip()
                    if text:
                        return text
            except Exception:
                continue

        # Last resort: card's own text (first non-empty line)
        try:
            for line in card_el.text.splitlines():
                line = line.strip()
                if line:
                    return line
        except Exception:
            pass

        return ""

    def _is_card_closed(self, card_el) -> bool:
        """Return True if any closed-indicator is found within the card."""
        for sel in VENUE_CLOSED:
            try:
                if _is_xpath(sel):
                    els = card_el.find_elements(By.XPATH, sel)
                else:
                    els = card_el.find_elements(By.CSS_SELECTOR, sel)
                if els:
                    return True
            except Exception:
                continue

        # Check aria-disabled on the card root
        try:
            if card_el.get_attribute("aria-disabled") == "true":
                return True
        except Exception:
            pass

        # Check data-unavailable attribute
        try:
            if card_el.get_attribute("data-unavailable") == "true":
                return True
        except Exception:
            pass

        return False

    # ------------------------------------------------------------------
    # Selector utilities
    # ------------------------------------------------------------------

    def _find_first(self, selectors: list[str], timeout: float = 8.0):
        """
        Try each selector in order and return the first WebElement found
        within *timeout* seconds total. Returns None if all fail.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            for sel in selectors:
                try:
                    if _is_xpath(sel):
                        els = self.browser.sb.driver.find_elements(By.XPATH, sel)
                    else:
                        els = self.browser.sb.driver.find_elements(By.CSS_SELECTOR, sel)
                    visible = [e for e in els if e.is_displayed()]
                    if visible:
                        return visible[0]
                except Exception:
                    continue
            time.sleep(0.5)
        return None

    # ------------------------------------------------------------------
    # Persistence — per-worker JSONL, no shared file, no lock needed
    # ------------------------------------------------------------------

    def _save_records(self, records: list[dict]):
        """
        Append records to this worker's JSONL file.
        One JSON object per line — safe for concurrent workers because
        each writes to its own file (results_{worker_id}.jsonl).
        """
        if not records:
            return
        with self._out_path.open("a", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="WF-03: Scout Wolt for restaurant URLs by German ZIP code."
    )
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--chunk-file",
        metavar="PATH",
        help="Path to a chunk JSON file produced by tools/chunker.py",
    )
    group.add_argument(
        "--zip",
        metavar="XXXXX",
        help="Single ZIP code (for testing / debugging)",
    )
    p.add_argument(
        "--worker-id",
        required=True,
        metavar="ID",
        help="Unique worker identifier, e.g. worker-01 or 1",
    )
    p.add_argument(
        "--save-closed",
        action="store_true",
        help="Include closed restaurants in output (default: open only)",
    )
    p.add_argument(
        "--no-headless",
        action="store_true",
        help="Show the browser window (useful for debugging selector issues)",
    )
    return p.parse_args()


def load_zip_codes(args: argparse.Namespace) -> list[str]:
    if args.zip:
        return [args.zip.strip()]

    chunk_path = Path(args.chunk_file)
    if not chunk_path.exists():
        print(f"ERROR: chunk file not found: {chunk_path}", file=sys.stderr)
        sys.exit(1)

    with chunk_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    codes = data.get("codes", [])
    if not codes:
        print("ERROR: chunk file contains no codes", file=sys.stderr)
        sys.exit(1)

    return codes


def main():
    args    = parse_args()
    wid     = args.worker_id
    log     = ScraperLogger(worker_id=wid)
    state   = StateManager(f"scout_{wid}")
    zips    = load_zip_codes(args)
    headless = not args.no_headless and HEADLESS

    log.info(
        action="SCOUT_INIT",
        message=f"Worker {wid} | {len(zips)} ZIPs | headless={headless} | save_closed={args.save_closed}",
        zip_count=len(zips),
        headless=headless,
        save_closed=args.save_closed,
    )

    with BrowserDriver(worker_id=wid, headless=headless, logger=log) as browser:
        scout = WoltScout(
            worker_id=wid,
            browser=browser,
            log=log,
            state=state,
            save_closed=args.save_closed,
        )
        summary = scout.run(zips)

    log.info(action="SCOUT_DONE", message=str(summary), **summary)
    print(f"\nDone. Summary: {summary}")
    print(f"Results → {ROOT / '.tmp' / f'results_{wid}.jsonl'}")


if __name__ == "__main__":
    main()
