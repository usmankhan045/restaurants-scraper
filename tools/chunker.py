"""
WF-02: PLZ Chunker
Splits plz_master.json into 20 equal JSON chunks in .tmp/chunks/.

Usage:
    python tools/chunker.py               # full run — uses data/plz_master.json
    python tools/chunker.py --test        # test run — uses 5 ZIPs from config.py
"""

import argparse
import json
import math
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from tools.utils import CustomLogger
from config import VALIDATION_ZIP_CODES

log = CustomLogger("chunker")

# ---------------------------------------------------------------------------
# Paths & settings
# ---------------------------------------------------------------------------

PLZ_MASTER_PATH = ROOT / "data" / "plz_master.json"
CHUNKS_DIR = ROOT / ".tmp" / "chunks"
NUM_CHUNKS = 20


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_codes_full() -> list[str]:
    """Load PLZ list from data/plz_master.json."""
    if not PLZ_MASTER_PATH.exists():
        log.error(
            "plz_master.json not found. Run tools/plz_gen.py first.",
            path=str(PLZ_MASTER_PATH),
        )
        raise FileNotFoundError(
            f"{PLZ_MASTER_PATH} does not exist. "
            "Generate it first with: python tools/plz_gen.py"
        )

    with PLZ_MASTER_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)

    codes = data.get("codes", [])
    if not codes:
        raise ValueError("plz_master.json exists but contains no codes.")

    log.info("Loaded PLZ master list", total=len(codes), path=str(PLZ_MASTER_PATH))
    return codes


def load_codes_test() -> list[str]:
    """Return the 5 hardcoded validation ZIPs from config.py."""
    log.info(
        "TEST MODE: using validation ZIPs from config.py",
        codes=VALIDATION_ZIP_CODES,
    )
    return list(VALIDATION_ZIP_CODES)


def split_into_chunks(codes: list[str], n: int) -> list[list[str]]:
    """Split codes into n roughly-equal chunks."""
    total = len(codes)
    chunk_size = math.ceil(total / n)
    chunks = [codes[i : i + chunk_size] for i in range(0, total, chunk_size)]

    # If codes.count < n, we'll have fewer chunks — that's fine.
    log.info(
        "Split codes into chunks",
        total_codes=total,
        requested_chunks=n,
        actual_chunks=len(chunks),
        chunk_size_approx=chunk_size,
    )
    return chunks


def save_chunks(chunks: list[list[str]], test_mode: bool) -> list[Path]:
    """Write each chunk to .tmp/chunks/chunk_NN.json."""
    CHUNKS_DIR.mkdir(parents=True, exist_ok=True)

    # Clear existing chunks to avoid stale data
    for old in CHUNKS_DIR.glob("chunk_*.json"):
        old.unlink()

    saved: list[Path] = []
    for idx, chunk in enumerate(chunks, start=1):
        filename = CHUNKS_DIR / f"chunk_{idx:02d}.json"
        payload = {
            "chunk_index": idx,
            "total_chunks": len(chunks),
            "test_mode": test_mode,
            "count": len(chunk),
            "codes": chunk,
        }
        with filename.open("w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        saved.append(filename)

    log.info(
        "All chunks written",
        directory=str(CHUNKS_DIR),
        files_written=len(saved),
        test_mode=test_mode,
    )
    return saved


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="WF-02: Split German PLZ list into processing chunks."
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Use only the 5 validation ZIPs from config.py instead of plz_master.json",
    )
    args = parser.parse_args()

    log.info("=== WF-02: Chunker started ===", test_mode=args.test)

    try:
        codes = load_codes_test() if args.test else load_codes_full()

        # In test mode use 5 chunks (one per ZIP) for clarity; otherwise NUM_CHUNKS
        n = len(codes) if args.test else NUM_CHUNKS
        chunks = split_into_chunks(codes, n)
        saved = save_chunks(chunks, test_mode=args.test)

        log.info("=== WF-02 complete ===", chunks_saved=len(saved))
        print(
            f"\nDone. {len(codes):,} codes → {len(saved)} chunk(s) "
            f"in {CHUNKS_DIR}"
            + (" [TEST MODE]" if args.test else "")
        )

        # Print summary table
        print(f"\n{'Chunk':<10} {'Codes':>6}  Path")
        print("-" * 55)
        for path in saved:
            with path.open() as f:
                info = json.load(f)
            print(f"{path.name:<10} {info['count']:>6}  {path}")

    except (FileNotFoundError, ValueError) as exc:
        log.error("WF-02 failed", error=str(exc))
        print(f"\nERROR: {exc}", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        log.exception("WF-02 failed with unhandled exception", exc=exc)
        print(f"\nFATAL: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
