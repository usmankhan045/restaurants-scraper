"""
WAT Framework — Browser Driver
SeleniumBase UC Mode with GDPR handling, randomised fingerprinting,
and stealth fallbacks (proxy rotation → browser restart).

Usage:
    from tools.browser import BrowserDriver

    with BrowserDriver(worker_id="worker-1") as browser:
        ok = browser.open("https://wolt.com/de/deu/berlin/restaurant")
        if ok:
            html = browser.page_source()
"""

import os
import random
import time
from pathlib import Path
from typing import Optional, Any

from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Device fingerprint profiles — (user_agent, width, height)
# Sourced from real-world browser stat distributions for Germany (2024).
# ---------------------------------------------------------------------------

_DEVICE_PROFILES: list[tuple[str, int, int]] = [
    # Windows — Chrome 124
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        1920, 1080,
    ),
    # Windows — Chrome 123 (slightly older, common in corporate environments)
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        1366, 768,
    ),
    # Windows — Edge 124
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 "
        "Edg/124.0.0.0",
        1920, 1080,
    ),
    # macOS — Chrome 124
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        1440, 900,
    ),
    # macOS — Chrome 124 (Retina / larger display)
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        2560, 1440,
    ),
    # Linux — Chrome 124
    (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        1280, 800,
    ),
    # Windows — laptop-size viewport
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        1600, 900,
    ),
]

# ---------------------------------------------------------------------------
# GDPR consent selectors per platform.
# Each entry is a list of CSS / XPath selectors tried in order.
# XPath must start with "//" or "(//".
# ---------------------------------------------------------------------------

_GDPR_MAP: dict[str, list[str]] = {
    "wolt.com": [
        # Wolt-specific data attributes (may vary by locale)
        "[data-localization-key='gdpr-consents.banner.accept-all']",
        "[data-test-id='gdpr-banner-accept-all']",
        "[data-qa='gdpr-accept-all']",
        # Text-based XPath fallbacks (German + English)
        "//button[normalize-space()='Alle akzeptieren']",
        "//button[normalize-space()='Alle Cookies akzeptieren']",
        "//button[normalize-space()='Accept all']",
        "//button[normalize-space()='Accept All']",
    ],
    "ubereats.com": [
        "[data-testid='accept-all-btn']",
        "#accept-all-cookies",
        "button.optanon-allow-all",
        # Text-based XPath fallbacks
        "//button[normalize-space()='Alle akzeptieren']",
        "//button[normalize-space()='Zustimmen']",
        "//button[normalize-space()='Accept All']",
        "//button[normalize-space()='Accept all']",
        # OneTrust (used by Uber)
        "#onetrust-accept-btn-handler",
    ],
    # Generic fallback — applied when no platform key matches
    "_generic": [
        "#onetrust-accept-btn-handler",
        ".cc-accept-all",
        "[aria-label='Accept all cookies']",
        "[aria-label='Alle Cookies akzeptieren']",
        "//button[normalize-space()='Alle akzeptieren']",
        "//button[normalize-space()='Alle Cookies akzeptieren']",
        "//button[normalize-space()='Zustimmen']",
        "//button[normalize-space()='Accept all']",
        "//button[normalize-space()='Accept All']",
    ],
}

# How long to wait (seconds) for a GDPR banner to appear before giving up
_GDPR_WAIT = 5

# How many seconds to pause after clicking the consent button
_GDPR_SETTLE = 1.2


def _load_proxy_list() -> list[str]:
    """
    Read proxies from environment.
    PROXY_LIST  — comma-separated list of proxies (takes priority)
    PROXY_URL   — single proxy (fallback)
    Returns an empty list when no proxies are configured.
    """
    raw = os.getenv("PROXY_LIST", "").strip()
    if raw:
        return [p.strip() for p in raw.split(",") if p.strip()]
    single = os.getenv("PROXY_URL", "").strip()
    return [single] if single else []


# ---------------------------------------------------------------------------


class BrowserDriver:
    """
    Production-grade Selenium driver for the German Restaurant Scraper.

    Parameters
    ----------
    worker_id : str
        Unique name for this worker; used in logs.
    headless : bool
        Run Chrome without a visible window (default True).
    logger : ScraperLogger | None
        Structured logger from tools/logger.py.  If None, a default
        ScraperLogger is created automatically.
    """

    def __init__(
        self,
        worker_id: str = "main",
        headless: bool = True,
        logger=None,
    ):
        self.worker_id = worker_id
        self.headless = headless
        self._proxies = _load_proxy_list()
        self._proxy_index = 0

        # Lazy-import so that projects that don't use browser.py
        # don't need seleniumbase installed just to import tools/.
        if logger is None:
            from tools.logger import ScraperLogger
            logger = ScraperLogger(worker_id=worker_id)
        self._log = logger

        # SeleniumBase context — set during __enter__
        self._sb_ctx: Any = None
        self.sb: Any = None

        # Current fingerprint (set each time the browser starts)
        self._ua: str = ""
        self._win_w: int = 1920
        self._win_h: int = 1080

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "BrowserDriver":
        self._start()
        return self

    def __exit__(self, *args):
        self._stop()

    # ------------------------------------------------------------------
    # Browser lifecycle
    # ------------------------------------------------------------------

    def _pick_profile(self) -> tuple[str, int, int]:
        """Return a random (ua, width, height) device profile."""
        profile = random.choice(_DEVICE_PROFILES)
        # Add a small random jitter to the window size so each session
        # looks different even when the same base profile is selected.
        jitter_w = random.randint(-40, 40)
        jitter_h = random.randint(-20, 20)
        return profile[0], profile[1] + jitter_w, profile[2] + jitter_h

    def _current_proxy(self) -> Optional[str]:
        if not self._proxies:
            return None
        return self._proxies[self._proxy_index % len(self._proxies)]

    def _build_sb_context(self):
        from seleniumbase import SB

        ua, w, h = self._pick_profile()
        self._ua, self._win_w, self._win_h = ua, w, h

        self._log.info(
            action="BROWSER_BUILD",
            message=f"Profile: {w}x{h} | proxy: {self._current_proxy() or 'none'}",
            user_agent=ua[:60] + "…",
        )

        kwargs: dict = dict(
            uc=True,
            headless=self.headless,
            agent=ua,
            # window_size not a SB kwarg — we set it after launch
        )
        if self._current_proxy():
            kwargs["proxy"] = self._current_proxy()

        return SB(**kwargs)

    def _start(self):
        ctx = self._build_sb_context()
        self._sb_ctx = ctx
        self.sb = ctx.__enter__()
        # Apply window size now that Chrome is running
        try:
            self.sb.set_window_size(self._win_w, self._win_h)
        except Exception:
            pass  # non-fatal; best-effort fingerprint

    def _stop(self):
        if self._sb_ctx is not None:
            try:
                self._sb_ctx.__exit__(None, None, None)
            except Exception:
                pass
            self._sb_ctx = None
            self.sb = None

    def _restart(self, new_fingerprint: bool = True):
        """Tear down and rebuild the browser with a fresh fingerprint."""
        self._log.info(action="BROWSER_RESTART", message="Restarting browser")
        self._stop()
        time.sleep(random.uniform(1.5, 3.0))
        self._start()

    def _rotate_proxy(self) -> bool:
        """
        Advance to the next proxy in the list.
        Returns True if a new proxy is available, False if we've exhausted all.
        """
        if len(self._proxies) <= 1:
            return False
        self._proxy_index = (self._proxy_index + 1) % len(self._proxies)
        self._log.warning(
            action="PROXY_ROTATE",
            status="ROTATE",
            message=f"Switched to proxy index {self._proxy_index}: {self._current_proxy()}",
        )
        return True

    # ------------------------------------------------------------------
    # GDPR consent handler
    # ------------------------------------------------------------------

    def _detect_platform(self, url: str) -> str:
        """Return the platform key that matches the URL, or '_generic'."""
        url_lower = url.lower()
        for key in _GDPR_MAP:
            if key != "_generic" and key in url_lower:
                return key
        return "_generic"

    def handle_gdpr(self, url: str = "") -> bool:
        """
        Detect and dismiss GDPR / cookie consent banners.

        Tries platform-specific selectors first, then generic ones.
        Returns True if a button was clicked, False if no banner found.
        """
        platform = self._detect_platform(url)
        selectors = _GDPR_MAP.get(platform, [])

        # Always append generic fallbacks (de-duplicated)
        for s in _GDPR_MAP["_generic"]:
            if s not in selectors:
                selectors.append(s)

        for sel in selectors:
            try:
                # Determine selector type for the right wait/click method
                if sel.startswith("//") or sel.startswith("(//"):
                    self.sb.wait_for_element(sel, by="xpath", timeout=_GDPR_WAIT)
                    self.sb.click(sel, by="xpath")
                else:
                    self.sb.wait_for_element(sel, timeout=_GDPR_WAIT)
                    self.sb.click(sel)

                time.sleep(_GDPR_SETTLE)
                self._log.info(
                    action="GDPR_ACCEPT",
                    status="OK",
                    message=f"Clicked consent button [{sel[:60]}] on {platform}",
                )
                return True

            except Exception:
                # This selector wasn't present — try the next one
                continue

        self._log.debug(
            action="GDPR_ACCEPT",
            status="NOT_FOUND",
            message=f"No consent banner detected on {platform}",
        )
        return False

    # ------------------------------------------------------------------
    # Navigation with stealth fallbacks
    # ------------------------------------------------------------------

    def open(self, url: str, gdpr: bool = True) -> bool:
        """
        Navigate to *url* using UC mode, with automatic GDPR handling
        and stealth fallbacks on failure.

        Fallback order:
            1. uc_open_with_reconnect  (primary)
            2. proxy rotate → restart → uc_open_with_reconnect
            3. browser restart (new fingerprint) → sb.get  (plain navigate)

        Returns True on success, False if all attempts failed.
        """
        # --- Attempt 1: standard UC open ---
        try:
            self._log.info(action="OPEN_URL", status="TRY", message=url, url=url)
            self.sb.uc_open_with_reconnect(url, reconnect_time=4)
            if gdpr:
                self.handle_gdpr(url)
            self._log.info(action="OPEN_URL", status="OK", message=url, url=url)
            return True
        except Exception as exc:
            self._log.warning(
                action="OPEN_URL",
                status="FAIL_UC",
                message=f"UC open failed: {exc}",
                url=url,
            )

        # --- Attempt 2: rotate proxy → restart → UC open ---
        if self._rotate_proxy():
            try:
                self._restart()
                self.sb.uc_open_with_reconnect(url, reconnect_time=4)
                if gdpr:
                    self.handle_gdpr(url)
                self._log.info(
                    action="OPEN_URL",
                    status="OK_PROXY",
                    message=f"Succeeded after proxy rotation",
                    url=url,
                )
                return True
            except Exception as exc:
                self._log.warning(
                    action="OPEN_URL",
                    status="FAIL_PROXY",
                    message=f"UC open failed after proxy rotation: {exc}",
                    url=url,
                )

        # --- Attempt 3: full restart with new fingerprint → plain get ---
        try:
            self._log.warning(
                action="OPEN_URL",
                status="RESTART",
                message="Falling back to plain get after browser restart",
                url=url,
            )
            self._restart()
            self.sb.get(url)
            time.sleep(random.uniform(2.0, 3.5))
            if gdpr:
                self.handle_gdpr(url)
            self._log.info(
                action="OPEN_URL",
                status="OK_RESTART",
                message="Succeeded after browser restart",
                url=url,
            )
            return True
        except Exception as exc:
            self._log.error(
                action="OPEN_URL",
                status="FAIL_ALL",
                message=f"All navigation attempts failed: {exc}",
                url=url,
            )
            return False

    # ------------------------------------------------------------------
    # Convenience wrappers
    # ------------------------------------------------------------------

    def page_source(self) -> str:
        return self.sb.get_page_source()

    def find_elements(self, selector: str):
        return self.sb.find_elements(selector)

    def find_element(self, selector: str):
        return self.sb.find_element(selector)

    def click(self, selector: str):
        self.sb.click(selector)

    def type(self, selector: str, text: str):
        self.sb.type(selector, text)

    def wait_for(self, selector: str, timeout: int = 10):
        self.sb.wait_for_element(selector, timeout=timeout)

    def scroll_to_bottom(self, pause: float = 1.2):
        self.sb.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(pause)

    def scroll_to_element(self, selector: str):
        self.sb.scroll_to(selector)

    def execute_script(self, script: str, *args):
        return self.sb.execute_script(script, *args)

    def sleep(self, seconds: float | None = None):
        """Human-like pause — random by default."""
        t = seconds if seconds is not None else random.uniform(1.5, 3.5)
        time.sleep(t)
