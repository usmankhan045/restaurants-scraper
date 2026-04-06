"""
WAT Framework — Core Utilities
Provides: CustomLogger, StateManager, SeleniumHelper
"""

import json
import os
import time
import logging
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# CustomLogger — structured JSON logs to .tmp/logs/
# ---------------------------------------------------------------------------

LOG_DIR = Path(__file__).parent.parent / ".tmp" / "logs"


class CustomLogger:
    """
    Writes structured JSON log lines to .tmp/logs/<name>_<date>.jsonl
    and also streams human-readable output to the console.
    """

    def __init__(self, name: str):
        self.name = name
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        self.log_path = LOG_DIR / f"{name}_{date_str}.jsonl"

        # Console handler
        self._console = logging.getLogger(name)
        if not self._console.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("[%(levelname)s] %(name)s: %(message)s"))
            self._console.addHandler(handler)
        self._console.setLevel(logging.DEBUG)

    def _write(self, level: str, message: str, **extra):
        record = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "logger": self.name,
            "level": level,
            "message": message,
            **extra,
        }
        with self.log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
        getattr(self._console, level.lower(), self._console.info)(message)

    def info(self, message: str, **extra):
        self._write("INFO", message, **extra)

    def warning(self, message: str, **extra):
        self._write("WARNING", message, **extra)

    def error(self, message: str, **extra):
        self._write("ERROR", message, **extra)

    def debug(self, message: str, **extra):
        self._write("DEBUG", message, **extra)

    def exception(self, message: str, exc: Exception | None = None, **extra):
        tb = traceback.format_exc() if exc is None else "".join(
            traceback.format_exception(type(exc), exc, exc.__traceback__)
        )
        self._write("ERROR", message, traceback=tb, **extra)


# ---------------------------------------------------------------------------
# StateManager — checkpoint-based resume for long scraping runs
# ---------------------------------------------------------------------------

STATE_DIR = Path(__file__).parent.parent / ".tmp"


class StateManager:
    """
    Persists progress to a JSON checkpoint file so interrupted runs can resume.

    Usage:
        sm = StateManager("my_run")
        sm.mark_done("10115")
        if sm.is_done("10115"):
            ...
        sm.set("last_url", "https://...")
        sm.get("last_url")
    """

    def __init__(self, run_id: str):
        self.run_id = run_id
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        self.path = STATE_DIR / f"checkpoint_{run_id}.json"
        self._state: dict[str, Any] = self._load()

    def _load(self) -> dict[str, Any]:
        if self.path.exists():
            with self.path.open("r", encoding="utf-8") as f:
                return json.load(f)
        return {"completed": [], "data": {}, "created_at": datetime.now(timezone.utc).isoformat()}

    def _save(self):
        self._state["updated_at"] = datetime.now(timezone.utc).isoformat()
        with self.path.open("w", encoding="utf-8") as f:
            json.dump(self._state, f, indent=2, ensure_ascii=False)

    def mark_done(self, key: str):
        if key not in self._state["completed"]:
            self._state["completed"].append(key)
            self._save()

    def is_done(self, key: str) -> bool:
        return key in self._state["completed"]

    def set(self, key: str, value: Any):
        self._state["data"][key] = value
        self._save()

    def get(self, key: str, default: Any = None) -> Any:
        return self._state["data"].get(key, default)

    def reset(self):
        self.path.unlink(missing_ok=True)
        self._state = self._load()

    @property
    def completed(self) -> list[str]:
        return list(self._state["completed"])

    def remaining(self, all_keys: list[str]) -> list[str]:
        return [k for k in all_keys if not self.is_done(k)]


# ---------------------------------------------------------------------------
# SeleniumHelper — SeleniumBase UC Mode with sb_cdp fallback
# ---------------------------------------------------------------------------

def _get_proxy() -> str | None:
    from dotenv import load_dotenv
    load_dotenv()
    return os.getenv("PROXY_URL") or None


class SeleniumHelper:
    """
    Wraps SeleniumBase in UC (undetected-chrome) mode.
    Falls back to sb_cdp mode when standard UC is detected/blocked.

    Usage:
        with SeleniumHelper() as helper:
            helper.get("https://example.com")
            html = helper.get_page_source()
    """

    def __init__(
        self,
        headless: bool = True,
        proxy: str | None = None,
        use_cdp_fallback: bool = False,
    ):
        self.headless = headless
        self.proxy = proxy or _get_proxy()
        self.use_cdp_fallback = use_cdp_fallback
        self.sb: Any = None
        self._logger = CustomLogger("SeleniumHelper")

    def _build_sb(self, cdp_mode: bool):
        from seleniumbase import SB

        kwargs = dict(
            uc=not cdp_mode,
            cdp_only=cdp_mode,
            headless=self.headless,
            agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0.0.0 Safari/537.36",
        )
        if self.proxy:
            kwargs["proxy"] = self.proxy

        return SB(**kwargs)

    def __enter__(self):
        try:
            self._logger.info("Starting SeleniumBase in UC mode")
            self._ctx = self._build_sb(cdp_mode=False)
            self.sb = self._ctx.__enter__()
        except Exception as exc:
            if self.use_cdp_fallback:
                self._logger.warning(
                    "UC mode failed — falling back to sb_cdp mode",
                    error=str(exc),
                )
                self._ctx = self._build_sb(cdp_mode=True)
                self.sb = self._ctx.__enter__()
            else:
                raise
        return self

    def __exit__(self, *args):
        if self._ctx:
            self._ctx.__exit__(*args)

    # -- Convenience wrappers ------------------------------------------------

    def get(self, url: str, sleep: float = 1.5):
        self._logger.debug("GET", url=url)
        self.sb.get(url)
        time.sleep(sleep)

    def get_page_source(self) -> str:
        return self.sb.get_page_source()

    def find_elements(self, css_selector: str):
        return self.sb.find_elements(css_selector)

    def find_element(self, css_selector: str):
        return self.sb.find_element(css_selector)

    def click(self, css_selector: str):
        self.sb.click(css_selector)

    def type(self, css_selector: str, text: str):
        self.sb.type(css_selector, text)

    def wait_for_element(self, css_selector: str, timeout: int = 10):
        self.sb.wait_for_element(css_selector, timeout=timeout)

    def scroll_to_bottom(self, pause: float = 1.0):
        self.sb.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(pause)

    def cdp_get(self, url: str):
        """CDP-mode fetch — use when sb_cdp fallback is active."""
        return self.sb.cdp.get(url)
