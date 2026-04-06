"""
WAT Framework — Structured Logger
Emits JSON log lines to .tmp/logs/ with mandatory scraper-context fields.

Every log record includes:
    timestamp   ISO-8601 UTC
    worker_id   Identifies the parallel worker (e.g. "worker-1", "main")
    zip_code    German ZIP being processed (empty string if not applicable)
    action      What the code is doing  (e.g. "OPEN_URL", "PARSE_LISTINGS")
    status      Outcome of that action  (e.g. "OK", "SKIP", "ERROR", "RETRY")
    message     Human-readable detail
    ...extra    Any additional key/value pairs passed at call-site

Usage:
    log = ScraperLogger(worker_id="worker-1")
    log.info(zip_code="10115", action="OPEN_URL", status="OK",
             message="Opened wolt.com", url="https://wolt.com/...")
    log.error(zip_code="10115", action="PARSE_LISTINGS", status="ERROR",
              message="Selector not found", selector=".restaurant-card")
"""

import json
import logging
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

LOG_DIR = Path(__file__).parent.parent / ".tmp" / "logs"

Level = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

# Map our level strings to stdlib logging levels
_STDLIB_LEVELS: dict[Level, int] = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


class ScraperLogger:
    """
    Structured JSON logger scoped to a single worker process/thread.

    Parameters
    ----------
    worker_id : str
        Unique identifier for this worker (e.g. "worker-1", "main", "pid-1234").
    min_level : Level
        Minimum severity to emit (default: "DEBUG" — emit everything).
    """

    def __init__(self, worker_id: str, min_level: Level = "DEBUG"):
        self.worker_id = worker_id
        self._min_level = _STDLIB_LEVELS[min_level]

        LOG_DIR.mkdir(parents=True, exist_ok=True)
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        self._log_path = LOG_DIR / f"scraper_{worker_id}_{date_str}.jsonl"

        # Console mirror so developers see activity in real time
        self._console = logging.getLogger(f"scraper.{worker_id}")
        if not self._console.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(
                logging.Formatter(
                    "[%(levelname)-8s] %(name)s | %(message)s"
                )
            )
            self._console.addHandler(handler)
        self._console.setLevel(self._min_level)
        self._console.propagate = False

    # ------------------------------------------------------------------
    # Core write
    # ------------------------------------------------------------------

    def _write(
        self,
        level: Level,
        *,
        zip_code: str = "",
        action: str,
        status: str,
        message: str,
        **extra,
    ):
        numeric = _STDLIB_LEVELS.get(level, logging.DEBUG)
        if numeric < self._min_level:
            return

        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "worker_id": self.worker_id,
            "zip_code": zip_code,
            "action": action,
            "status": status,
            "message": message,
            **extra,
        }

        # Write JSONL to file
        with self._log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

        # Mirror a compact line to the console
        console_msg = (
            f"[{zip_code or '-':>5}] {action:<22} {status:<8} | {message}"
        )
        self._console.log(numeric, console_msg)

    # ------------------------------------------------------------------
    # Convenience methods
    # ------------------------------------------------------------------

    def debug(self, *, zip_code: str = "", action: str, status: str = "DEBUG",
              message: str, **extra):
        self._write("DEBUG", zip_code=zip_code, action=action,
                    status=status, message=message, **extra)

    def info(self, *, zip_code: str = "", action: str, status: str = "OK",
             message: str, **extra):
        self._write("INFO", zip_code=zip_code, action=action,
                    status=status, message=message, **extra)

    def warning(self, *, zip_code: str = "", action: str, status: str = "WARN",
                message: str, **extra):
        self._write("WARNING", zip_code=zip_code, action=action,
                    status=status, message=message, **extra)

    def error(self, *, zip_code: str = "", action: str, status: str = "ERROR",
              message: str, **extra):
        self._write("ERROR", zip_code=zip_code, action=action,
                    status=status, message=message, **extra)

    def exception(
        self,
        exc: Exception,
        *,
        zip_code: str = "",
        action: str,
        status: str = "EXCEPTION",
        message: str = "",
        **extra,
    ):
        tb = "".join(
            traceback.format_exception(type(exc), exc, exc.__traceback__)
        )
        self._write(
            "ERROR",
            zip_code=zip_code,
            action=action,
            status=status,
            message=message or str(exc),
            traceback=tb,
            **extra,
        )

    def skip(self, *, zip_code: str = "", action: str, message: str = "Already completed", **extra):
        """Convenience for checkpoint-skip events."""
        self._write("INFO", zip_code=zip_code, action=action,
                    status="SKIP", message=message, **extra)

    def retry(self, *, zip_code: str = "", action: str, attempt: int,
              message: str = "", **extra):
        """Convenience for retry events."""
        self._write("WARNING", zip_code=zip_code, action=action,
                    status=f"RETRY_{attempt}", message=message, **extra)

    # ------------------------------------------------------------------
    # Context helper: bind a zip_code so callers don't repeat it
    # ------------------------------------------------------------------

    def bind(self, zip_code: str) -> "BoundLogger":
        """Return a logger pre-bound to a ZIP code."""
        return BoundLogger(self, zip_code)


class BoundLogger:
    """A thin wrapper that pre-fills zip_code on every call."""

    def __init__(self, logger: ScraperLogger, zip_code: str):
        self._log = logger
        self.zip_code = zip_code

    def debug(self, *, action: str, status: str = "DEBUG", message: str, **kw):
        self._log.debug(zip_code=self.zip_code, action=action,
                        status=status, message=message, **kw)

    def info(self, *, action: str, status: str = "OK", message: str, **kw):
        self._log.info(zip_code=self.zip_code, action=action,
                       status=status, message=message, **kw)

    def warning(self, *, action: str, status: str = "WARN", message: str, **kw):
        self._log.warning(zip_code=self.zip_code, action=action,
                          status=status, message=message, **kw)

    def error(self, *, action: str, status: str = "ERROR", message: str, **kw):
        self._log.error(zip_code=self.zip_code, action=action,
                        status=status, message=message, **kw)

    def exception(self, exc: Exception, *, action: str, message: str = "", **kw):
        self._log.exception(exc, zip_code=self.zip_code, action=action,
                            message=message, **kw)

    def skip(self, *, action: str, message: str = "Already completed", **kw):
        self._log.skip(zip_code=self.zip_code, action=action,
                       message=message, **kw)

    def retry(self, *, action: str, attempt: int, message: str = "", **kw):
        self._log.retry(zip_code=self.zip_code, action=action,
                        attempt=attempt, message=message, **kw)
