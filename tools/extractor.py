"""
WAT Framework — WF-04: Legal Data Extractor
Visits each restaurant URL produced by scout.py and extracts legal data:
    - Impressum / Legal page URL
    - Owner / Director name  (hardened regex)
    - Legal entity name      (GmbH, UG, AG, KG …)
    - German phone number
    - E-mail address

Platform-aware pipeline:
    Wolt      → scans footer / About-section for Impressum links
    Uber Eats → opens Store Info side-drawer first; switches into
                any iframe found before reading legal text

Retry policy:
    Each URL gets MAX_RETRIES attempts.  On every failure the browser
    is fully restarted (fresh fingerprint) before the next try.
    After all attempts the URL is marked 'failed' in the StateManager.

Concurrency:
    Same model as scout.py — each worker writes to its own
    .tmp/extracted_{worker_id}.jsonl.  No shared file, no locking.

Usage:
    # Process this worker's scout results (typical)
    python tools/extractor.py --worker-id worker-01

    # Explicit input file
    python tools/extractor.py --results-file .tmp/results_worker-01.jsonl --worker-id worker-01

    # Single URL (debugging)
    python tools/extractor.py --url "https://wolt.com/de/deu/berlin/restaurant/pizza" --worker-id dev

Output:
    .tmp/extracted_{worker_id}.jsonl
    .tmp/checkpoint_extract_{worker_id}.json
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
# Timing
# ---------------------------------------------------------------------------

PAGE_LOAD_WAIT  = (2.0, 3.5)   # wait after navigation before hunting links
DRAWER_WAIT     = (1.5, 2.5)   # wait for Uber Eats side-drawer to open
IFRAME_WAIT     = 1.2           # fixed pause after switching iframe context
SCROLL_WAIT     = (0.8, 1.3)   # pause after scroll during link hunt

# ---------------------------------------------------------------------------
# Regex patterns — compiled once at module load
# ---------------------------------------------------------------------------

# Owner / Director  (provided spec — hardened)
# \s inside the name group is replaced with [ \t] so the match never
# bleeds across a line-break into the next keyword (e.g. "Vertreten durch").
OWNER_RE = re.compile(
    r"(?i)"
    r"(?:Inhaber|Gesch[äa]ftsf[üu]hrer|Vertreten[ \t]+durch|Representative|Director)"
    r"[ \t]*:?[ \t]*"
    r"([A-ZÄÖÜ][a-zäöüß]+(?:[ \t][A-ZÄÖÜ][a-zäöüß]+){1,2})"
)

# Legal entity — standalone line ending with a recognised German legal suffix
# Also catches inline forms like "von der Pizza GmbH."
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

# German phone — handles +49, 0049, and leading 0; optional formatting chars
PHONE_RE = re.compile(
    r"(?:"
    r"(?:\+49|00\s?49)[\s.\-]?(?:\(0\)[\s.\-]?)?"   # international prefix
    r"|0"                                              # or national leading 0
    r")"
    r"\d{2,5}"                                        # area code
    r"[\s.\-/]?"
    r"\d{3,12}"                                       # subscriber number
    r"(?:[\s.\-/]?\d{1,6})?",                        # optional extension
    re.MULTILINE,
)

# E-mail
EMAIL_RE = re.compile(
    r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b"
)

# Keywords that make a link a candidate for Impressum hunting
_IMPRESSUM_KW = frozenset([
    "impressum", "legal", "rechtlich", "store info", "store-info",
    "storeinfo", "rechtliche hinweise", "über uns", "about us",
    "restaurant info",
])

# ---------------------------------------------------------------------------
# Platform selectors
# ---------------------------------------------------------------------------

# ── Wolt ──────────────────────────────────────────────────────────────────

# Regions of the page most likely to contain legal links
WOLT_LEGAL_REGIONS = [
    "[data-test-id='venue-about']",
    "[data-test-id='legal-notice']",
    "[data-test-id='venue-footer']",
    "[class*='LegalInfo']",
    "[class*='AboutSection']",
    "footer",
]

# ── Uber Eats ─────────────────────────────────────────────────────────────

UBER_STORE_INFO_TRIGGER = [
    "[data-testid='store-info']",
    "[data-testid='store-info-button']",
    "[data-testid='legal-disclosure-link']",
    "//button[normalize-space()='Store Info']",
    "//a[normalize-space()='Store Info']",
    "//button[contains(text(),'Store Info')]",
    "//a[contains(text(),'Store Info')]",
    "//li[.//span[contains(text(),'Store Info')]]",
    "//button[contains(text(),'Über diesen')]",
    "//a[contains(text(),'Über diesen')]",
    "//button[contains(text(),'Restaurant Info')]",
]

UBER_DRAWER_CONTAINER = [
    "[data-testid='store-info-modal']",
    "[data-testid='store-info-sheet']",
    "[data-testid='legal-disclosure-sheet']",
    "[role='dialog']",
    "[role='complementary'][aria-label*='Info']",
    "[class*='SideSheet']",
    "[class*='StoreInfo']",
    "[class*='BottomSheet']",
]

# ── Generic Impressum link selectors (both platforms) ─────────────────────

IMPRESSUM_LINK_SELECTORS = [
    # href-based (fastest)
    "a[href*='impressum']",
    "a[href*='legal']",
    "a[href*='store-info']",
    "a[href*='storeinfo']",
    "a[href*='rechtlich']",
    # text-based XPath (case-insensitive via translate)
    "//a[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ',"
    "'abcdefghijklmnopqrstuvwxyz'),'impressum')]",
    "//a[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ',"
    "'abcdefghijklmnopqrstuvwxyz'),'legal')]",
    "//a[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ',"
    "'abcdefghijklmnopqrstuvwxyz'),'rechtlich')]",
    "//a[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ',"
    "'abcdefghijklmnopqrstuvwxyz'),'store info')]",
    "//a[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ',"
    "'abcdefghijklmnopqrstuvwxyz'),'über uns')]",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_xpath(sel: str) -> bool:
    return sel.startswith("//") or sel.startswith("(//")


def _clean(text: str) -> str:
    """Collapse whitespace and strip."""
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

    Parameters
    ----------
    worker_id   : str            — unique worker name
    browser     : BrowserDriver  — caller manages the context
    log         : ScraperLogger
    state       : StateManager   — checkpoint keyed by restaurant URL
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

    # ──────────────────────────────────────────────────────────────────────
    # Public entry point
    # ──────────────────────────────────────────────────────────────────────

    def run(self, records: list[dict]) -> dict:
        """
        Process every record that hasn't been completed yet.
        Records must contain at least 'url', 'name', and 'zip_code'.
        Returns StateManager summary.
        """
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

    # ──────────────────────────────────────────────────────────────────────
    # Retry wrapper
    # ──────────────────────────────────────────────────────────────────────

    def _process_with_retry(self, record: dict):
        """
        Up to MAX_RETRIES attempts per URL.
        Between attempts: full browser restart for a fresh fingerprint + session.
        """
        url   = record["url"]
        bound = self.log.bind(record.get("zip_code", ""))

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                result = self._process_record(record, bound)
                self._save_record(result)
                self.state.mark_completed(url, metadata={
                    "extract_status": result["extract_status"],
                    "impressum_source": result.get("impressum_source", ""),
                    "has_owner": bool(result.get("owner")),
                    "has_phone": bool(result.get("phone")),
                })
                return  # success — stop retrying

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
                    # Write a failure record so the downstream pipeline knows
                    self._save_record(self._failure_record(record, str(exc)))

    # ──────────────────────────────────────────────────────────────────────
    # Core pipeline (single attempt)
    # ──────────────────────────────────────────────────────────────────────

    def _process_record(self, record: dict, bound) -> dict:
        url      = record["url"]
        platform = _detect_platform(url)
        self.state.mark_in_progress(url)

        bound.info(action="EXTRACT_START", status="TRY",
                   message=f"[{platform}] {record.get('name', '?')}", url=url)

        # ==========================================
        # 🚀 ANTIGRAVITY FAST-PATH (HTTP BYPASS)
        # ==========================================
        # Attempt a raw HTTP GET first. If Cloudflare allows it and the data is SSR'd
        # (like Wolt), we bypass the 8-second browser routine entirely.
        try:
            fast_resp = requests.get(
                url, 
                timeout=4,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
                    "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7"
                }
            )
            if fast_resp.status_code == 200:
                fast_text = _clean(fast_resp.text)
                if _has_legal_signal(fast_text):
                    fields = self._parse_legal_fields(fast_text)
                    if fields.get("owner") or fields.get("legal_entity"):
                        bound.info(action="FAST_PATH_HIT", status="SUCCESS", message="Extracted via 0.5s HTTP bypass")
                        fields["impressum_url"] = url
                        fields["impressum_source"] = "http_fast_path"
                        fields["raw_snippet"] = fast_text[:600]
                        fields["extract_status"] = "ok"
                        
                        return {
                            **record,
                            "extracted_at": _now_iso(),
                            **fields,
                        }
        except Exception:
            pass # Silently fallback to Selenium

        # ==========================================
        # 🐢 SLOW-PATH: SELENIUM (For Uber Eats / Blocked Pages)
        # ==========================================
        if hasattr(self.browser.sb, "uc_open_with_reconnect"):
            try:
                self.browser.sb.uc_open_with_reconnect(url, 4)
                ok = True
            except Exception:
                ok = self.browser.open(url, gdpr=True)
        else:
            ok = self.browser.open(url, gdpr=True)

        if not ok:
            raise RuntimeError(f"browser.open() failed for {url} (Likely blocked by Cloudflare)")

        time.sleep(random.uniform(*PAGE_LOAD_WAIT))

        impressum_text, impressum_url, source = self._hunt_impressum(url, platform, bound)

        fields = self._parse_legal_fields(impressum_text)
        fields["impressum_url"]    = impressum_url
        fields["impressum_source"] = source
        fields["raw_snippet"]      = impressum_text[:600] if impressum_text else ""
        fields["extract_status"]   = "ok" if impressum_text else "not_found"

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

    # ──────────────────────────────────────────────────────────────────────
    # Impressum hunt — platform-aware
    # ──────────────────────────────────────────────────────────────────────

    def _hunt_impressum(
        self, page_url: str, platform: str, bound
    ) -> tuple[str, str, str]:
        """
        Try to locate and read Impressum / legal text.

        Returns (text, impressum_url, source_label).
        All three are empty strings when nothing is found.
        """
        # ── Strategy A: current page might already contain inline legal text ──
        inline = self._read_inline_legal_text(platform)
        if inline:
            bound.debug(action="IMPRESSUM_HUNT", status="INLINE",
                        message="Found inline legal text on restaurant page")
            return inline, page_url, "inline"

        # ── Strategy B: Uber Eats side-drawer (iframes included) ─────────────
        if platform == "ubereats":
            text, src = self._hunt_ubereats_drawer(page_url, bound)
            if text:
                return text, page_url, src

        # ── Strategy C: scan page for a dedicated Impressum link ─────────────
        link_url = self._find_impressum_link(platform, bound)
        if link_url:
            text = self._fetch_impressum_page(link_url, bound)
            if text:
                return text, link_url, "link"

        bound.warning(action="IMPRESSUM_HUNT", status="NOT_FOUND",
                      message="No Impressum / legal text located")
        return "", "", ""

    # ── Inline legal text detection ───────────────────────────────────────

    def _read_inline_legal_text(self, platform: str) -> str:
        """
        Check known on-page containers for embedded legal text.
        Returns the text if any Impressum-like keyword is present.
        """
        driver = self.browser.sb.driver
        regions = WOLT_LEGAL_REGIONS if platform == "wolt" else []

        for sel in regions:
            try:
                els = driver.find_elements(By.CSS_SELECTOR, sel)
                for el in els:
                    t = el.text
                    if _has_legal_signal(t):
                        return _clean(t)
            except Exception:
                continue

        # Also scan full page text quickly
        try:
            body = driver.find_element(By.TAG_NAME, "body").text
            if _has_legal_signal(body) and _has_owner_signal(body):
                return _clean(body)
        except Exception:
            pass

        return ""

    # ── Uber Eats drawer + iframe ─────────────────────────────────────────

    def _hunt_ubereats_drawer(
        self, page_url: str, bound
    ) -> tuple[str, str]:
        """
        Attempt to open the Uber Eats Store Info side-drawer and extract
        legal text from it.  Switches into iframes when present.
        Returns (text, source_label).
        """
        # 1. Click the Store Info trigger button/link
        clicked = self._click_uber_store_info(bound)
        if not clicked:
            return "", ""

        time.sleep(random.uniform(*DRAWER_WAIT))

        # 2. Locate the drawer container
        drawer_el = self._find_first_element(UBER_DRAWER_CONTAINER, timeout=6.0)
        if drawer_el is None:
            bound.debug(action="UBER_DRAWER", status="NOT_FOUND",
                        message="Drawer container not located after click")
            return "", ""

        # 3. Look for iframes inside the drawer first
        text = self._read_iframes_in_element(drawer_el, bound)
        if text:
            return text, "ubereats_drawer_iframe"

        # 4. Read the drawer's own text
        try:
            text = _clean(drawer_el.text)
            if _has_legal_signal(text):
                return text, "ubereats_drawer"
        except Exception:
            pass

        # 5. Fallback: check ALL iframes on the full page
        text = self._read_all_page_iframes(bound)
        if text:
            return text, "ubereats_iframe"

        return "", ""

    def _click_uber_store_info(self, bound) -> bool:
        """Click the Store Info trigger. Returns True if clicked."""
        driver = self.browser.sb.driver
        for sel in UBER_STORE_INFO_TRIGGER:
            try:
                if _is_xpath(sel):
                    els = driver.find_elements(By.XPATH, sel)
                else:
                    els = driver.find_elements(By.CSS_SELECTOR, sel)

                visible = [e for e in els if e.is_displayed() and e.is_enabled()]
                if visible:
                    driver.execute_script(
                        "arguments[0].scrollIntoView({block:'center'});", visible[0]
                    )
                    time.sleep(0.4)
                    visible[0].click()
                    bound.debug(action="UBER_DRAWER", status="TRIGGER_CLICKED",
                                message=f"Clicked via: {sel[:60]}")
                    return True
            except Exception:
                continue
        return False

    def _read_iframes_in_element(self, container_el, bound) -> str:
        """
        Find <iframe> tags within container_el, switch into each, read body text,
        and switch back.  Returns the first match containing legal signals.
        """
        driver = self.browser.sb.driver
        try:
            iframes = container_el.find_elements(By.TAG_NAME, "iframe")
        except Exception:
            return ""

        for iframe in iframes:
            text = self._switch_and_read_iframe(iframe, driver, bound)
            if text:
                return text

        return ""

    def _read_all_page_iframes(self, bound) -> str:
        """
        Scan every <iframe> on the current page for legal content.
        This is the Uber Eats catch-all fallback.
        """
        driver = self.browser.sb.driver
        try:
            iframes = driver.find_elements(By.TAG_NAME, "iframe")
        except Exception:
            return ""

        bound.debug(action="IFRAME_SCAN", status="START",
                    message=f"Scanning {len(iframes)} page iframes")

        for iframe in iframes:
            text = self._switch_and_read_iframe(iframe, driver, bound)
            if text:
                return text

        return ""

    def _switch_and_read_iframe(self, iframe_el, driver, bound) -> str:
        """
        Switch into an iframe, read its body text, switch back.
        Returns text if it contains legal signals, else empty string.
        """
        try:
            src = iframe_el.get_attribute("src") or "(no src)"
            driver.switch_to.frame(iframe_el)
            time.sleep(IFRAME_WAIT)

            try:
                text = _clean(driver.find_element(By.TAG_NAME, "body").text)
            except Exception:
                text = ""

            driver.switch_to.default_content()

            if text and _has_legal_signal(text):
                bound.debug(action="IFRAME_HIT", status="OK",
                            message=f"Legal content in iframe: {src[:80]}")
                return text

        except Exception as exc:
            # Always restore context — even on error
            try:
                driver.switch_to.default_content()
            except Exception:
                pass
            bound.debug(action="IFRAME_SKIP", status="ERROR",
                        message=str(exc)[:80])

        return ""

    # ── Generic Impressum link scanner ────────────────────────────────────

    def _find_impressum_link(self, platform: str, bound) -> str:
        """
        Scan the current page for a link that leads to Impressum / legal content.
        Returns the absolute URL if found, else empty string.
        """
        driver = self.browser.sb.driver

        # Scroll to bottom so footer links become visible
        try:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(random.uniform(*SCROLL_WAIT))
        except Exception:
            pass

        # Try each selector in order
        for sel in IMPRESSUM_LINK_SELECTORS:
            try:
                if _is_xpath(sel):
                    els = driver.find_elements(By.XPATH, sel)
                else:
                    els = driver.find_elements(By.CSS_SELECTOR, sel)

                for el in els:
                    href = el.get_attribute("href") or ""
                    text = (el.text or "").lower()
                    # Validate that the link actually points to legal content
                    if _is_legal_href(href) or _is_legal_text(text):
                        bound.debug(action="IMPRESSUM_LINK", status="FOUND",
                                    message=f"Found via [{sel[:50]}]: {href[:80]}")
                        return href
            except Exception:
                continue

        # Last resort: JavaScript — collect all <a> hrefs and filter
        try:
            links: list[dict] = driver.execute_script(
                "return Array.from(document.querySelectorAll('a[href]'))"
                ".map(a => ({href: a.href, text: a.textContent.trim().toLowerCase()}))"
                ".slice(0, 200);"   # cap to avoid huge payloads
            )
            for link in (links or []):
                href = link.get("href", "")
                text = link.get("text", "")
                if _is_legal_href(href) or _is_legal_text(text):
                    bound.debug(action="IMPRESSUM_LINK", status="JS_FOUND",
                                message=f"Found via JS scan: {href[:80]}")
                    return href
        except Exception:
            pass

        return ""

    def _fetch_impressum_page(self, url: str, bound) -> str:
        """
        Navigate to the Impressum URL, read the page body text, and return it.
        """
        bound.info(action="FETCH_IMPRESSUM", status="TRY", message=url)
        ok = self.browser.open(url, gdpr=True)
        if not ok:
            bound.warning(action="FETCH_IMPRESSUM", status="FAIL",
                          message=f"Failed to open {url}")
            return ""

        time.sleep(random.uniform(*PAGE_LOAD_WAIT))

        # Check for iframes first — some Impressum pages embed content in iframes
        text = self._read_all_page_iframes(bound)
        if text:
            return text

        try:
            body_text = _clean(
                self.browser.sb.driver.find_element(By.TAG_NAME, "body").text
            )
            bound.info(action="FETCH_IMPRESSUM", status="OK",
                       message=f"{len(body_text)} chars extracted")
            return body_text
        except Exception as exc:
            bound.warning(action="FETCH_IMPRESSUM", status="BODY_ERROR",
                          message=str(exc))
            return ""

    # ──────────────────────────────────────────────────────────────────────
    # Legal field extraction — regex engine
    # ──────────────────────────────────────────────────────────────────────

    def _parse_legal_fields(self, text: str) -> dict:
        """
        Run all regex patterns over *text* and return a dict of extracted fields.
        Always returns all keys (empty string when not found).
        """
        if not text:
            return {
                "owner":        "",
                "legal_entity": "",
                "phone":        "",
                "email":        "",
            }

        fields = {
            "owner":        self._extract_owner(text),
            "legal_entity": self._extract_entity(text),
            "phone":        self._extract_phone(text),
            "email":        self._extract_email(text),
        }

        # Corporate Blacklist Filter
        banned_entities = ["uber portier", "wolt enterprises", "uber eats", "wolt.com", "uber.com"]
        banned_emails = ["support@uber.com", "support@wolt.com", "hilfe@uber.com", "privacy@uber.com"]

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
            return {}

        return fields

    def _extract_owner(self, text: str) -> str:
        """
        Extract the first Owner / Director name using the hardened regex.
        Pattern: Inhaber / Geschäftsführer / Vertreten durch / Representative / Director
        followed by a proper name (1–3 capitalised words).
        """
        m = OWNER_RE.search(text)
        return _clean(m.group(1)) if m else ""

    def _extract_entity(self, text: str) -> str:
        """
        Extract the legal entity name (e.g. 'Pizza Berlin GmbH').
        Prefers names found on their own line (standalone pattern).
        Falls back to inline occurrence.
        """
        # Priority 1: standalone line ending with a legal suffix
        standalone = re.compile(
            r"^([A-ZÄÖÜ0-9][A-Za-z0-9ÄÖÜäöüß\s&,.\-\']{1,60}\s*" + _SUFFIX + r")\s*$",
            re.MULTILINE,
        )
        m = standalone.search(text)
        if m:
            return _clean(m.group(1))

        # Priority 2: inline occurrence anywhere in the text
        m = ENTITY_RE.search(text)
        return _clean(m.group(1)) if m else ""

    def _extract_phone(self, text: str) -> str:
        """
        Extract the first German phone number.
        Normalises by removing excessive interior whitespace.
        """
        m = PHONE_RE.search(text)
        if not m:
            return ""
        raw = m.group(0).strip()
        # Collapse multiple spaces/dots within the number to a single space
        return re.sub(r"[\s.\-]{2,}", " ", raw).strip()

    def _extract_email(self, text: str) -> str:
        """Extract the first e-mail address."""
        m = EMAIL_RE.search(text)
        return m.group(0).lower() if m else ""

    # ──────────────────────────────────────────────────────────────────────
    # DOM helpers
    # ──────────────────────────────────────────────────────────────────────

    def _find_first_element(self, selectors: list[str], timeout: float = 6.0):
        """
        Try each selector until one returns a displayed element or timeout.
        Returns the WebElement or None.
        """
        driver   = self.browser.sb.driver
        deadline = time.time() + timeout

        while time.time() < deadline:
            for sel in selectors:
                try:
                    if _is_xpath(sel):
                        els = driver.find_elements(By.XPATH, sel)
                    else:
                        els = driver.find_elements(By.CSS_SELECTOR, sel)

                    visible = [e for e in els if e.is_displayed()]
                    if visible:
                        return visible[0]
                except Exception:
                    continue
            time.sleep(0.4)

        return None

    # ──────────────────────────────────────────────────────────────────────
    # Persistence
    # ──────────────────────────────────────────────────────────────────────

    def _save_record(self, record: dict):
        """Append one enriched record to the worker's JSONL output file."""
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
            "raw_snippet":     "",
            "error":           error,
        }


# ---------------------------------------------------------------------------
# Signal helpers (module-level so they stay cheap to call)
# ---------------------------------------------------------------------------

_LEGAL_KEYWORDS = frozenset([
    "impressum", "inhaber", "geschäftsführer", "vertreten durch",
    "handelsregister", "ust-id", "umsatzsteuer", "amtsgericht",
    "representative", "director", "legal notice", "rechtliche hinweise",
])

_LEGAL_HREF_FRAGMENTS = frozenset([
    "impressum", "legal", "store-info", "storeinfo",
    "rechtlich", "about", "restaurant-info",
])

_LEGAL_TEXT_FRAGMENTS = frozenset([
    "impressum", "legal", "rechtlich", "store info",
    "über uns", "about", "restaurant info",
])


def _has_legal_signal(text: str) -> bool:
    t = text.lower()
    return any(kw in t for kw in _LEGAL_KEYWORDS)


def _has_owner_signal(text: str) -> bool:
    t = text.lower()
    return any(k in t for k in ("inhaber", "geschäftsführer", "vertreten durch", "director"))


def _is_legal_href(href: str) -> bool:
    h = href.lower()
    return any(f in h for f in _LEGAL_HREF_FRAGMENTS)


def _is_legal_text(text: str) -> bool:
    return any(f in text for f in _LEGAL_TEXT_FRAGMENTS)


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def _load_results(path: Path) -> list[dict]:
    """Read a JSONL file and return list of dicts."""
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


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="WF-04: Extract legal data (Impressum) from restaurant URLs."
    )

    source = p.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--worker-id",
        metavar="ID",
        help="Worker ID — reads .tmp/results_{ID}.jsonl automatically",
    )
    source.add_argument(
        "--url",
        metavar="URL",
        help="Single restaurant URL (debugging only)",
    )

    p.add_argument(
        "--results-file",
        metavar="PATH",
        help="Override the default input file path (use with --worker-id)",
    )
    p.add_argument(
        "--no-headless",
        action="store_true",
        help="Show the browser window (useful for debugging)",
    )
    return p.parse_args()


def main():
    args = parse_args()

    # ── Resolve worker ID and input file ──────────────────────────────────
    if args.url:
        # Single-URL debug mode — synthesise a minimal record
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
            print("INFO: results file is empty or contains no valid records.", file=sys.stderr)
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
