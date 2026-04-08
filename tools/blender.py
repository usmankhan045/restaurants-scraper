"""
WF-05: Blender
Phase 1 – Deduplicate each platform by URL (same restaurant delivers to many ZIPs).
Phase 2 – Cross-platform fuzzy-name match: pull phone/email/owner from Uber into
           the matching Wolt record (Uber sometimes exposes contact info that Wolt hides).
Phase 3 – Write merged CSV for Google Sheets sync.
"""

import argparse
import csv
import json
import difflib
from collections import defaultdict


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def similar(a: str, b: str) -> float:
    return difflib.SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()


def load_jsonl(path: str) -> list[dict]:
    records = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    except FileNotFoundError:
        pass
    return records


def coalesce(*args) -> str:
    """Return the first non-empty, non-placeholder value."""
    for a in args:
        v = str(a).strip() if a is not None else ""
        if v and v not in ("N/A", "None", "", "Hidden by Platform", "null"):
            return v
    return "N/A"


def _is_platform_owned_entity(name: str) -> bool:
    """Detect if owner/legal_entity is the platform company itself."""
    lowered = name.lower()
    banned  = [
        "uber", "wolt", "portier", "platform", "lieferando",
        "delivery hero", "just eat",
    ]
    return any(b in lowered for b in banned)


def get_best_owner(rec: dict) -> str:
    for field in ("owner", "legal_entity"):
        val = rec.get(field, "")
        if val and val not in ("N/A", "", None, "Hidden by Platform"):
            if not _is_platform_owned_entity(val):
                return str(val).strip()
    if any("Hidden" in str(rec.get(f, "")) for f in ("owner", "legal_entity")):
        return "Hidden by Platform"
    return "N/A"


def url_key(url: str) -> str:
    """Normalise URL to a stable dedup key."""
    return url.rstrip("/").lower().split("?")[0]


def _fill_gaps(base: dict, donor: dict) -> None:
    """Copy contact fields from donor into base wherever base has N/A."""
    for field in ("phone", "email", "address"):
        if coalesce(base.get(field)) == "N/A":
            val = coalesce(donor.get(field))
            if val != "N/A":
                base[field] = val

    # Owner: prefer non-platform, non-hidden value
    base_owner  = get_best_owner(base)
    donor_owner = get_best_owner(donor)
    if base_owner in ("N/A", "Hidden by Platform") and donor_owner not in ("N/A", "Hidden by Platform"):
        base["owner"] = donor_owner
    elif base_owner == "N/A" and donor_owner == "Hidden by Platform":
        base["owner"] = "Hidden by Platform"

    # Rating: take whichever has more reviews (more reliable)
    try:
        if int(donor.get("reviews", 0)) > int(base.get("reviews", 0)):
            if coalesce(donor.get("rating")) != "N/A":
                base["rating"]  = donor["rating"]
                base["reviews"] = donor["reviews"]
    except (ValueError, TypeError):
        pass

    # Always track the Uber URL separately
    donor_url = donor.get("url", "")
    if donor_url and donor_url not in base.get("url", ""):
        base["uber_url"] = donor_url


# ---------------------------------------------------------------------------
# Phase 1 – Per-platform URL deduplication
# ---------------------------------------------------------------------------

def dedup_by_url(records: list[dict]) -> list[dict]:
    """
    Collapse records with the same URL into one, keeping the best data.
    A restaurant delivers to many ZIPs so appears many times in raw results.
    """
    seen: dict[str, dict] = {}
    for rec in records:
        url = rec.get("url", "")
        if not url:
            continue
        key = url_key(url)
        if key not in seen:
            seen[key] = dict(rec)
        else:
            _fill_gaps(seen[key], rec)
    return list(seen.values())


# ---------------------------------------------------------------------------
# Phase 2 – Cross-platform fuzzy name merge
# ---------------------------------------------------------------------------

# 0.82 avoids false positives like "Pizza House" vs "Pizza Hut"
SIMILARITY_THRESHOLD = 0.82


def cross_platform_merge(wolt_recs: list[dict], uber_recs: list[dict]) -> list[dict]:
    """
    For every Wolt restaurant that is still missing phone/email,
    find the best-matching Uber Eats record by name similarity and pull
    the contact details across.

    Restaurants that only exist on Uber Eats (no Wolt match) are appended
    to the final list so no data is lost.
    """
    matched_uber_indices: set[int] = set()

    for wolt in wolt_recs:
        wname = wolt.get("name", "").lower().strip()
        if not wname:
            continue

        # Only try to match if we are actually missing something
        needs_contact = (
            coalesce(wolt.get("phone")) == "N/A"
            or coalesce(wolt.get("email")) == "N/A"
            or get_best_owner(wolt) == "N/A"
        )
        if not needs_contact:
            continue

        best_score = 0.0
        best_idx   = -1

        for i, uber in enumerate(uber_recs):
            uname = uber.get("name", "").lower().strip()
            score = similar(wname, uname)
            if score > best_score:
                best_score = score
                best_idx   = i

        if best_score >= SIMILARITY_THRESHOLD and best_idx >= 0:
            donor = uber_recs[best_idx]
            _fill_gaps(wolt, donor)
            matched_uber_indices.add(best_idx)
            # Mark the Wolt record as sourced from both platforms
            wolt["platform"] = "both"

    # Append Uber-only restaurants (those that didn't match any Wolt record)
    uber_only = [
        rec for i, rec in enumerate(uber_recs)
        if i not in matched_uber_indices
    ]

    print(f"Cross-platform merge: {len(matched_uber_indices)} Wolt records enriched from Uber.")
    print(f"Uber-only restaurants appended: {len(uber_only)}")

    return wolt_recs + uber_only


# ---------------------------------------------------------------------------
# Phase 3 – CSV output
# ---------------------------------------------------------------------------

def write_csv(records: list[dict], output_path: str) -> None:
    headers = [
        "Restaurant Name",
        "Address",
        "Email Address",
        "Contact Number",
        "Ratings",
        "Reviews Count",
        "Owner Name",
        "Wolt URL",
        "Uber URL",
        "ZIP Code",
        "Platform",
    ]

    def split_urls(rec: dict) -> tuple[str, str]:
        """Return (wolt_url, uber_url) from record."""
        all_urls = rec.get("url", "")
        parts    = [u.strip() for u in all_urls.split("|") if u.strip()]
        wolt_u   = next((u for u in parts if "wolt.com" in u), "N/A")
        uber_u   = rec.get("uber_url") or next((u for u in parts if "ubereats.com" in u), "N/A")
        return wolt_u, uber_u

    with open(output_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(headers)
        for r in records:
            wolt_url, uber_url = split_urls(r)
            w.writerow([
                r.get("name", "Unknown"),
                coalesce(r.get("address")),
                coalesce(r.get("email")),
                coalesce(r.get("phone")),
                r.get("rating",  "N/A"),
                r.get("reviews", "N/A"),
                get_best_owner(r),
                wolt_url,
                uber_url,
                r.get("zip_code", "UNKNOWN"),
                r.get("platform", "N/A"),
            ])


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(description="WF-05: Merge and deduplicate restaurant records")
    p.add_argument("--wolt-file",  required=True, help="Path to wolt extracted JSONL")
    p.add_argument("--uber-file",  required=True, help="Path to uber extracted JSONL")
    p.add_argument("--output-csv", required=True, help="Output CSV path")
    args = p.parse_args()

    wolt_raw = load_jsonl(args.wolt_file)
    uber_raw = load_jsonl(args.uber_file)
    print(f"Loaded {len(wolt_raw)} Wolt records and {len(uber_raw)} Uber Eats records.")

    if not wolt_raw and not uber_raw:
        print("WARNING: Both input files are empty. Nothing to write.")
        with open(args.output_csv, "w", encoding="utf-8", newline="") as f:
            csv.writer(f).writerow([
                "Restaurant Name", "Address", "Email Address",
                "Contact Number", "Ratings", "Reviews Count",
                "Owner Name", "Wolt URL", "Uber URL", "ZIP Code", "Platform",
            ])
        return

    # Phase 1 — deduplicate each platform individually by URL
    wolt_deduped = dedup_by_url(wolt_raw)
    uber_deduped = dedup_by_url(uber_raw)
    print(f"After URL dedup — Wolt: {len(wolt_deduped)}, Uber: {len(uber_deduped)}")

    # Phase 2 — cross-platform fuzzy name merge
    merged = cross_platform_merge(wolt_deduped, uber_deduped)
    print(f"Final unique restaurants: {len(merged)}")

    # Phase 3 — write CSV
    write_csv(merged, args.output_csv)
    print(f"Successfully wrote {len(merged)} records to {args.output_csv}")


if __name__ == "__main__":
    main()
