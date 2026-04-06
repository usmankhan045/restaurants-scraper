"""
WAT Framework — WF-05: Dataset Blender
Merges Wolt and Uber Eats scraped datasets via fuzzy matching.

Matching logic:
    Candidate pairs are matched when:
        normalised_name similarity  > 90%  (RapidFuzz token_sort_ratio)
        normalised_address similarity > 95% (RapidFuzz ratio)

    Both thresholds must be met simultaneously.

Merge rules:
    review_count        — summed across both sources
    phone / owner_name  — kept verbatim if identical; otherwise
                          "Source_Wolt: X, Source_Uber: Y"
    All other fields    — Wolt value wins; Uber value appended as
                          *_uber column only when it differs.

Outputs:
    .tmp/merged_restaurants.csv   — final enriched dataset
    .tmp/conflict_report.json     — rows where field values disagreed

Usage:
    python tools/blender.py --wolt-file .tmp/wolt.csv --uber-file .tmp/uber.csv

    # JSONL input (auto-detected by extension)
    python tools/blender.py --wolt-file .tmp/extracted_worker-01.jsonl \\
                            --uber-file .tmp/extracted_worker-02.jsonl

    # Custom output locations
    python tools/blender.py --wolt-file wolt.csv --uber-file uber.csv \\
                            --output-csv results/merged.csv \\
                            --output-json results/conflicts.json

    # Adjust thresholds
    python tools/blender.py --wolt-file wolt.csv --uber-file uber.csv \\
                            --name-threshold 85 --addr-threshold 90
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

# ---------------------------------------------------------------------------
# Fuzzy library — prefer rapidfuzz (10-100× faster); fall back to thefuzz
# ---------------------------------------------------------------------------
try:
    from rapidfuzz import fuzz as _fuzz
    from rapidfuzz import process as _process
    _FUZZY_LIB = "rapidfuzz"
except ImportError:
    try:
        from thefuzz import fuzz as _fuzz          # type: ignore[no-redef]
        from thefuzz import process as _process    # type: ignore[no-redef]
        _FUZZY_LIB = "thefuzz"
    except ImportError:
        sys.exit(
            "ERROR: Neither 'rapidfuzz' nor 'thefuzz' is installed.\n"
            "Install with:  pip install rapidfuzz"
        )

ROOT = Path(__file__).parent.parent

# ---------------------------------------------------------------------------
# Default thresholds
# ---------------------------------------------------------------------------
DEFAULT_NAME_THRESHOLD = 90   # token_sort_ratio  (handles word reordering)
DEFAULT_ADDR_THRESHOLD = 95   # ratio             (strict positional match)

# ---------------------------------------------------------------------------
# Column name aliases
# Maps canonical names → lists of possible column names in the raw data.
# First match wins.
# ---------------------------------------------------------------------------
_ALIAS: dict[str, list[str]] = {
    "name":         ["name", "restaurant_name", "title", "venue_name"],
    "address":      ["address", "addr", "street", "location", "full_address"],
    "zip_code":     ["zip_code", "zip", "plz", "postal_code"],
    "review_count": ["review_count", "reviews", "rating_count", "num_reviews"],
    "phone":        ["phone", "phone_number", "tel", "telefon", "telephone"],
    "owner":        ["owner", "owner_name", "inhaber", "geschäftsführer",
                     "legal_owner", "director"],
    "rating":       ["rating", "score", "stars", "bewertung"],
    "category":     ["category", "cuisine", "food_type", "categories"],
    "url":          ["url", "link", "restaurant_url", "page_url"],
    "legal_entity": ["legal_entity", "entity", "firma", "company"],
    "email":        ["email", "e-mail", "mail"],
    "status":       ["status", "is_open", "open_status"],
}

# Fields where conflicts are tracked in the report
_CONFLICT_FIELDS = ["phone", "owner", "rating", "category", "legal_entity", "email"]

# Fields that are numeric and should be summed
_SUM_FIELDS = ["review_count"]


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------

_STRIP_RE = re.compile(r"[^\w\s]", re.UNICODE)
_SPACE_RE = re.compile(r"\s+")


def _normalise(text: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace."""
    if not isinstance(text, str):
        return ""
    t = text.lower()
    t = _STRIP_RE.sub(" ", t)
    t = _SPACE_RE.sub(" ", t)
    return t.strip()


def _norm_address(row: pd.Series) -> str:
    """
    Build a single normalised address string from available columns.
    Concatenates address + zip_code so partial addresses still match well.
    """
    parts = []
    for col in ("address", "zip_code"):
        val = row.get(col, "")
        if isinstance(val, str) and val.strip():
            parts.append(val.strip())
    combined = " ".join(parts)
    return _normalise(combined)


# ---------------------------------------------------------------------------
# Column resolution
# ---------------------------------------------------------------------------

def _resolve_columns(df: pd.DataFrame) -> dict[str, str | None]:
    """
    Return a mapping {canonical_name: actual_column_in_df} for each alias group.
    Value is None when the canonical field is absent from df.
    """
    cols_lower = {c.lower(): c for c in df.columns}
    resolved: dict[str, str | None] = {}
    for canonical, aliases in _ALIAS.items():
        found = None
        for alias in aliases:
            if alias.lower() in cols_lower:
                found = cols_lower[alias.lower()]
                break
        resolved[canonical] = found
    return resolved


def _get(row: pd.Series, mapping: dict[str, str | None], canonical: str) -> Any:
    """Safe accessor using resolved column mapping."""
    col = mapping.get(canonical)
    if col is None:
        return ""
    val = row.get(col, "")
    return val if val is not None else ""


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def _load(path: Path) -> pd.DataFrame:
    """Load CSV or JSONL into a DataFrame. Raises on unknown extension."""
    suffix = path.suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
    elif suffix in (".jsonl", ".ndjson"):
        records = []
        with path.open("r", encoding="utf-8") as f:
            for lineno, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError as exc:
                    print(f"  WARN: skipping malformed line {lineno} in {path.name}: {exc}",
                          file=sys.stderr)
        df = pd.DataFrame(records).fillna("").astype(str)
    elif suffix == ".json":
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            df = pd.DataFrame(data).fillna("").astype(str)
        else:
            raise ValueError(f"JSON file must contain a top-level list: {path}")
    else:
        raise ValueError(f"Unsupported file format '{suffix}'. Use .csv, .json, or .jsonl")

    print(f"  Loaded {len(df):,} rows from {path.name}  [{', '.join(df.columns[:6])}{'…' if len(df.columns) > 6 else ''}]")
    return df


def _save_csv(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"  CSV  → {path}  ({len(df):,} rows)")


def _save_json(data: Any, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  JSON → {path}")


# ---------------------------------------------------------------------------
# Fuzzy matching engine
# ---------------------------------------------------------------------------

def _build_keys(df: pd.DataFrame, col_map: dict[str, str | None]) -> list[tuple[str, str]]:
    """
    Returns [(norm_name, norm_address), ...] for every row in df.
    Rows with empty name get an empty key so they never match.
    """
    keys = []
    for _, row in df.iterrows():
        name = _normalise(str(_get(row, col_map, "name")))
        addr = _norm_address(row)
        keys.append((name, addr))
    return keys


def _name_score(a: str, b: str) -> float:
    """Token-sort ratio — robust to word-order differences (e.g. 'Burger King' vs 'King Burger')."""
    if not a or not b:
        return 0.0
    return _fuzz.token_sort_ratio(a, b)


def _addr_score(a: str, b: str) -> float:
    """Strict ratio — address order matters."""
    if not a or not b:
        return 0.0
    return _fuzz.ratio(a, b)


def find_matches(
    wolt_keys: list[tuple[str, str]],
    uber_keys: list[tuple[str, str]],
    name_threshold: float,
    addr_threshold: float,
) -> list[tuple[int, int, float, float]]:
    """
    Returns list of (wolt_idx, uber_idx, name_score, addr_score) for every
    pair that exceeds both thresholds.

    Strategy: For each Wolt row, extract candidates whose name score > threshold
    using rapidfuzz process.extract (fast C implementation), then verify
    address score only for those candidates.
    """
    uber_names = [k[0] for k in uber_keys]
    matched: list[tuple[int, int, float, float]] = []
    # Track which Uber indices have already been consumed (greedy 1-to-1)
    consumed_uber: set[int] = set()

    for w_idx, (w_name, w_addr) in enumerate(wolt_keys):
        if not w_name:
            continue

        # Fast name pre-filter — returns top candidates above threshold
        candidates = _process.extract(
            w_name,
            uber_names,
            scorer=_fuzz.token_sort_ratio,
            score_cutoff=name_threshold,
            limit=10,
        )

        best: tuple[int, float, float] | None = None
        for _matched_name, n_score, u_idx in candidates:
            if u_idx in consumed_uber:
                continue
            a_score = _addr_score(w_addr, uber_keys[u_idx][1])
            if a_score >= addr_threshold:
                # Among ties, prefer higher combined score
                combined = n_score + a_score
                if best is None or combined > best[1] + best[2]:
                    best = (u_idx, n_score, a_score)

        if best is not None:
            u_idx, n_score, a_score = best
            matched.append((w_idx, u_idx, n_score, a_score))
            consumed_uber.add(u_idx)

    return matched


# ---------------------------------------------------------------------------
# Merging
# ---------------------------------------------------------------------------

def _coalesce(*values: Any) -> str:
    """Return first non-empty string value."""
    for v in values:
        s = str(v).strip()
        if s and s not in ("nan", "None", ""):
            return s
    return ""


def _merge_conflict(
    canonical: str,
    wolt_val: str,
    uber_val: str,
) -> tuple[str, dict | None]:
    """
    Merge a single field.
    Returns (merged_value, conflict_detail_or_None).
    """
    w = str(wolt_val).strip()
    u = str(uber_val).strip()

    if not w and not u:
        return "", None
    if not u:
        return w, None
    if not w:
        return u, None

    # Normalise for comparison (ignore minor formatting differences)
    if _normalise(w) == _normalise(u):
        return w, None  # same content, pick Wolt value

    # Genuine conflict
    merged = f"Source_Wolt: {w}, Source_Uber: {u}"
    conflict = {"field": canonical, "wolt": w, "uber": u}
    return merged, conflict


def _merge_numeric(wolt_val: Any, uber_val: Any) -> str:
    """Sum two numeric fields. Falls back to concatenation on parse error."""
    try:
        w = float(str(wolt_val).replace(",", "").strip() or "0")
        u = float(str(uber_val).replace(",", "").strip() or "0")
        total = w + u
        return str(int(total)) if total == int(total) else str(total)
    except (ValueError, TypeError):
        w_s = str(wolt_val).strip()
        u_s = str(uber_val).strip()
        if w_s and u_s:
            return f"{w_s} + {u_s}"
        return w_s or u_s


def merge_pair(
    wolt_row: pd.Series,
    uber_row: pd.Series,
    wolt_map: dict[str, str | None],
    uber_map: dict[str, str | None],
    name_score: float,
    addr_score: float,
) -> tuple[dict, list[dict]]:
    """
    Produce one merged record and a (possibly empty) list of conflict dicts.
    """
    merged: dict[str, Any] = {}
    conflicts: list[dict] = []

    # ── Identity fields — take best available value ──────────────────────────
    for canonical in ("name", "address", "zip_code", "url", "status",
                      "category", "legal_entity", "email"):
        w_val = _get(wolt_row, wolt_map, canonical)
        u_val = _get(uber_row, uber_map, canonical)
        merged[canonical] = _coalesce(w_val, u_val)

    # ── Numeric sum fields ────────────────────────────────────────────────────
    for canonical in _SUM_FIELDS:
        w_val = _get(wolt_row, wolt_map, canonical)
        u_val = _get(uber_row, uber_map, canonical)
        merged[canonical] = _merge_numeric(w_val, u_val)
        merged[f"{canonical}_wolt"] = str(w_val).strip()
        merged[f"{canonical}_uber"] = str(u_val).strip()

    # ── Rating — keep higher value ────────────────────────────────────────────
    w_rating = _get(wolt_row, wolt_map, "rating")
    u_rating = _get(uber_row, uber_map, "rating")
    try:
        merged["rating"] = str(max(
            float(str(w_rating) or "0"),
            float(str(u_rating) or "0"),
        ))
    except ValueError:
        merged["rating"] = _coalesce(w_rating, u_rating)

    # ── Conflict fields — merge with source attribution ───────────────────────
    for canonical in ("phone", "owner"):
        w_val = _get(wolt_row, wolt_map, canonical)
        u_val = _get(uber_row, uber_map, canonical)
        val, conflict = _merge_conflict(canonical, str(w_val), str(u_val))
        merged[canonical] = val
        if conflict:
            conflicts.append(conflict)

    # ── Match provenance ──────────────────────────────────────────────────────
    merged["source"]     = "both"
    merged["name_score"] = round(name_score, 1)
    merged["addr_score"] = round(addr_score, 1)

    # ── Carry all remaining original columns (prefixed) ───────────────────────
    for col in wolt_row.index:
        prefixed = f"wolt_{col}"
        if prefixed not in merged:
            merged[prefixed] = wolt_row[col]
    for col in uber_row.index:
        prefixed = f"uber_{col}"
        if prefixed not in merged:
            merged[prefixed] = uber_row[col]

    return merged, conflicts


def build_unmatched(
    df: pd.DataFrame,
    matched_indices: set[int],
    source_label: str,
    col_map: dict[str, str | None],
) -> list[dict]:
    """Build records for rows that had no match, tagged with their source."""
    records = []
    for idx, row in df.iterrows():
        if idx in matched_indices:
            continue
        rec: dict[str, Any] = {"source": source_label}
        for canonical in (*_ALIAS.keys(),):
            val = _get(row, col_map, canonical)
            rec[canonical] = str(val).strip() if val else ""
        # Carry raw columns too
        for col in row.index:
            prefixed = f"{source_label}_{col}"
            if prefixed not in rec:
                rec[prefixed] = row[col]
        rec["name_score"] = ""
        rec["addr_score"] = ""
        records.append(rec)
    return records


# ---------------------------------------------------------------------------
# Main blend routine
# ---------------------------------------------------------------------------

def blend(
    wolt_path: Path,
    uber_path: Path,
    output_csv: Path,
    output_json: Path,
    name_threshold: float = DEFAULT_NAME_THRESHOLD,
    addr_threshold: float = DEFAULT_ADDR_THRESHOLD,
) -> dict:
    """
    Full pipeline. Returns a summary dict.
    """
    print(f"\n{'─'*60}")
    print(f"  Blender  |  fuzzy lib: {_FUZZY_LIB}")
    print(f"  Thresholds — name: {name_threshold}%  addr: {addr_threshold}%")
    print(f"{'─'*60}")

    # 1. Load ─────────────────────────────────────────────────────────────────
    print("\n[1/5] Loading datasets…")
    wolt_df = _load(wolt_path)
    uber_df = _load(uber_path)

    wolt_map = _resolve_columns(wolt_df)
    uber_map = _resolve_columns(uber_df)

    # 2. Build normalised keys ────────────────────────────────────────────────
    print("\n[2/5] Building normalised keys…")
    wolt_keys = _build_keys(wolt_df, wolt_map)
    uber_keys  = _build_keys(uber_df,  uber_map)

    # 3. Fuzzy match ──────────────────────────────────────────────────────────
    print("\n[3/5] Running fuzzy matching…")
    matches = find_matches(wolt_keys, uber_keys, name_threshold, addr_threshold)
    print(f"  Found {len(matches):,} matched pairs")

    matched_wolt: set[int] = set()
    matched_uber: set[int] = set()

    # 4. Merge matched pairs ──────────────────────────────────────────────────
    print("\n[4/5] Merging matched pairs…")
    all_records: list[dict] = []
    conflict_rows: list[dict] = []

    for w_idx, u_idx, n_score, a_score in matches:
        wolt_row = wolt_df.iloc[w_idx]
        uber_row = uber_df.iloc[u_idx]
        merged, conflicts = merge_pair(
            wolt_row, uber_row, wolt_map, uber_map, n_score, a_score
        )
        all_records.append(merged)
        matched_wolt.add(w_idx)
        matched_uber.add(u_idx)

        if conflicts:
            conflict_rows.append({
                "restaurant_name": merged.get("name", ""),
                "address":         merged.get("address", ""),
                "name_score":      round(n_score, 1),
                "addr_score":      round(a_score, 1),
                "conflicts":       conflicts,
            })

    # Unmatched Wolt rows
    unmatched_wolt = build_unmatched(wolt_df, matched_wolt, "wolt", wolt_map)
    all_records.extend(unmatched_wolt)

    # Unmatched Uber rows
    unmatched_uber = build_unmatched(uber_df, matched_uber, "uber", uber_map)
    all_records.extend(unmatched_uber)

    # 5. Save outputs ─────────────────────────────────────────────────────────
    print("\n[5/5] Saving outputs…")
    merged_df = pd.DataFrame(all_records)

    # Reorder: canonical columns first
    canonical_cols = [
        "source", "name", "address", "zip_code", "phone", "owner",
        "rating", "review_count", "review_count_wolt", "review_count_uber",
        "category", "legal_entity", "email", "url", "status",
        "name_score", "addr_score",
    ]
    front = [c for c in canonical_cols if c in merged_df.columns]
    rest  = [c for c in merged_df.columns if c not in front]
    merged_df = merged_df[front + rest]

    _save_csv(merged_df, output_csv)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "fuzzy_lib": _FUZZY_LIB,
        "thresholds": {
            "name_similarity_pct": name_threshold,
            "address_similarity_pct": addr_threshold,
        },
        "summary": {
            "wolt_total":       len(wolt_df),
            "uber_total":       len(uber_df),
            "matched_pairs":    len(matches),
            "unmatched_wolt":   len(unmatched_wolt),
            "unmatched_uber":   len(unmatched_uber),
            "total_output_rows": len(all_records),
            "conflict_rows":    len(conflict_rows),
        },
        "conflicts": conflict_rows,
    }
    _save_json(report, output_json)

    # Print summary
    s = report["summary"]
    print(f"""
{'─'*60}
  BLEND SUMMARY
{'─'*60}
  Wolt input rows      : {s['wolt_total']:>6,}
  Uber Eats input rows : {s['uber_total']:>6,}
  ─────────────────────────────
  Matched pairs        : {s['matched_pairs']:>6,}
  Unmatched Wolt only  : {s['unmatched_wolt']:>6,}
  Unmatched Uber only  : {s['unmatched_uber']:>6,}
  ─────────────────────────────
  Total output rows    : {s['total_output_rows']:>6,}
  Conflict rows        : {s['conflict_rows']:>6,}
{'─'*60}
""")

    return report


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="WF-05: Merge Wolt and Uber Eats datasets via fuzzy matching.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--wolt-file", required=True, metavar="PATH",
                   help="Path to the Wolt dataset (CSV or JSONL)")
    p.add_argument("--uber-file", required=True, metavar="PATH",
                   help="Path to the Uber Eats dataset (CSV or JSONL)")
    p.add_argument("--output-csv", metavar="PATH",
                   default=str(ROOT / ".tmp" / "merged_restaurants.csv"),
                   help="Output path for merged CSV (default: .tmp/merged_restaurants.csv)")
    p.add_argument("--output-json", metavar="PATH",
                   default=str(ROOT / ".tmp" / "conflict_report.json"),
                   help="Output path for conflict JSON report (default: .tmp/conflict_report.json)")
    p.add_argument("--name-threshold", type=float, default=DEFAULT_NAME_THRESHOLD,
                   metavar="N",
                   help=f"Minimum name similarity %% (default: {DEFAULT_NAME_THRESHOLD})")
    p.add_argument("--addr-threshold", type=float, default=DEFAULT_ADDR_THRESHOLD,
                   metavar="N",
                   help=f"Minimum address similarity %% (default: {DEFAULT_ADDR_THRESHOLD})")
    return p.parse_args()


def main():
    args = parse_args()

    wolt_path   = Path(args.wolt_file)
    uber_path   = Path(args.uber_file)
    output_csv  = Path(args.output_csv)
    output_json = Path(args.output_json)

    for p in (wolt_path, uber_path):
        if not p.exists():
            sys.exit(f"ERROR: File not found: {p}")

    blend(
        wolt_path   = wolt_path,
        uber_path   = uber_path,
        output_csv  = output_csv,
        output_json = output_json,
        name_threshold = args.name_threshold,
        addr_threshold = args.addr_threshold,
    )


if __name__ == "__main__":
    main()
