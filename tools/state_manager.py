"""
WAT Framework — State Manager
JSON-based checkpoint system for resumable scraping runs.

Each task ID (ZIP code or URL) transitions through:
    pending → in_progress → completed | failed

Usage:
    sm = StateManager("wolt_run_001")
    if sm.is_completed("10115"):
        continue
    sm.mark_in_progress("10115")
    ...do work...
    sm.mark_completed("10115", metadata={"restaurants_found": 42})
"""

import json
import os
import tempfile
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

# ---------------------------------------------------------------------------

TMP_DIR = Path(__file__).parent.parent / ".tmp"
Status = Literal["pending", "in_progress", "completed", "failed"]

_LOCK = threading.Lock()  # process-level safety for shared checkpoints


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------


class StateManager:
    """
    Checkpoint-based state persistence for long scraping runs.

    Checkpoint file structure:
    {
      "version": 1,
      "run_id": "...",
      "created_at": "...",
      "updated_at": "...",
      "tasks": {
        "<task_id>": {
          "status": "completed",
          "attempts": 2,
          "first_seen": "...",
          "updated_at": "...",
          "completed_at": "...",   # only when status == completed
          "error": "...",          # only when status == failed
          "metadata": {...}        # caller-supplied payload
        }
      }
    }
    """

    VERSION = 1

    def __init__(self, run_id: str):
        self.run_id = run_id
        TMP_DIR.mkdir(parents=True, exist_ok=True)
        self.path = TMP_DIR / f"checkpoint_{run_id}.json"
        self._state = self._load()

    # ------------------------------------------------------------------
    # Internal I/O
    # ------------------------------------------------------------------

    def _load(self) -> dict:
        if self.path.exists():
            with self.path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            # Migrate v0 checkpoints produced by the old StateManager in utils.py
            if "tasks" not in data:
                completed_list: list = data.get("completed", [])
                tasks = {
                    k: {
                        "status": "completed",
                        "attempts": 1,
                        "first_seen": data.get("created_at", _now()),
                        "updated_at": data.get("updated_at", _now()),
                        "completed_at": data.get("updated_at", _now()),
                        "metadata": {},
                    }
                    for k in completed_list
                }
                data = {
                    "version": self.VERSION,
                    "run_id": self.run_id,
                    "created_at": data.get("created_at", _now()),
                    "updated_at": data.get("updated_at", _now()),
                    "tasks": tasks,
                }
            return data

        return {
            "version": self.VERSION,
            "run_id": self.run_id,
            "created_at": _now(),
            "updated_at": _now(),
            "tasks": {},
        }

    def _save(self):
        """Atomic write: write to a temp file then rename to avoid partial reads."""
        self._state["updated_at"] = _now()
        with _LOCK:
            tmp_fd, tmp_path = tempfile.mkstemp(
                dir=TMP_DIR, prefix=f".ckpt_{self.run_id}_", suffix=".tmp"
            )
            try:
                with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                    json.dump(self._state, f, indent=2, ensure_ascii=False)
                Path(tmp_path).replace(self.path)
            except Exception:
                Path(tmp_path).unlink(missing_ok=True)
                raise

    def _ensure_task(self, task_id: str) -> dict:
        if task_id not in self._state["tasks"]:
            self._state["tasks"][task_id] = {
                "status": "pending",
                "attempts": 0,
                "first_seen": _now(),
                "updated_at": _now(),
                "metadata": {},
            }
        return self._state["tasks"][task_id]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def is_completed(self, task_id: str) -> bool:
        """Return True if this task_id has already been marked completed."""
        return self._state["tasks"].get(task_id, {}).get("status") == "completed"

    def get_status(self, task_id: str) -> Status:
        return self._state["tasks"].get(task_id, {}).get("status", "pending")

    def mark_in_progress(self, task_id: str):
        task = self._ensure_task(task_id)
        task["status"] = "in_progress"
        task["attempts"] += 1
        task["updated_at"] = _now()
        self._save()

    def mark_completed(self, task_id: str, metadata: dict | None = None):
        task = self._ensure_task(task_id)
        task["status"] = "completed"
        task["completed_at"] = _now()
        task["updated_at"] = _now()
        task.pop("error", None)
        if metadata:
            task["metadata"].update(metadata)
        self._save()

    def mark_failed(self, task_id: str, error: str = "", metadata: dict | None = None):
        task = self._ensure_task(task_id)
        task["status"] = "failed"
        task["error"] = error
        task["updated_at"] = _now()
        if metadata:
            task["metadata"].update(metadata)
        self._save()

    def set_metadata(self, task_id: str, **kwargs):
        task = self._ensure_task(task_id)
        task["metadata"].update(kwargs)
        self._save()

    def get_metadata(self, task_id: str) -> dict:
        return self._state["tasks"].get(task_id, {}).get("metadata", {})

    def pending(self, all_ids: list[str]) -> list[str]:
        """Return IDs from all_ids that are NOT completed."""
        return [i for i in all_ids if not self.is_completed(i)]

    def reset_task(self, task_id: str):
        """Force a task back to pending (use to retry failed tasks)."""
        if task_id in self._state["tasks"]:
            del self._state["tasks"][task_id]
            self._save()

    def reset_all(self):
        """Wipe the entire checkpoint."""
        self.path.unlink(missing_ok=True)
        self._state = self._load()

    # ------------------------------------------------------------------
    # Summary / reporting
    # ------------------------------------------------------------------

    def summary(self) -> dict:
        tasks = self._state["tasks"]
        counts: dict[str, int] = {}
        for t in tasks.values():
            s = t.get("status", "pending")
            counts[s] = counts.get(s, 0) + 1
        return {
            "run_id": self.run_id,
            "total": len(tasks),
            **counts,
        }

    def __repr__(self) -> str:
        s = self.summary()
        return (
            f"StateManager(run_id={self.run_id!r}, "
            f"completed={s.get('completed', 0)}, "
            f"failed={s.get('failed', 0)}, "
            f"in_progress={s.get('in_progress', 0)})"
        )
