"""
WF-05: Blender
Merges Wolt + Uber Eats records, deduplicates by name within each ZIP,
and outputs a clean CSV ready for Google Sheets sync.
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


def _merge_two(base: dict, incoming: dict) -> dict:
    """
    Merge `incoming` into `base`, filling gaps.
    base is mutated in-place and returned.
    """
    for field in ("email", "phone", "address"):
        if coalesce(base.get(field)) == "N/A":
            val = coalesce(incoming.get(field))
            if val != "N/A":
                base[field] = val

    for field in ("rating", "reviews"):
        if coalesce(base.get(field)) == "N/A":
            val = coalesce(incoming.get(field))
            if val != "N/A":
                base[field] = val

    # Owner: prefer non-platform-owned, non-hidden
    base_owner     = get_best_owner(base)
    incoming_owner = get_best_owner(incoming)
    if base_owner in ("N/A", "Hidden by Platform") and incoming_owner not in ("N/A", "Hidden by Platform"):
        base["owner"] = incoming_owner
    elif base_owner == "N/A" and incoming_owner == "Hidden by Platform":
        base["owner"] = "Hidden by Platform"

    # Concatenate URLs for cross-platform reference
    existing_url = base.get("url", "")
    new_url      = incoming.get("url", "")
    if new_url and new_url not in existing_url:
        base["url"] = f"{existing_url} | {new_url}"

    return base


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

# Minimum name similarity to merge two records as the same restaurant.
# 0.82 avoids false positives like "Pizza House" vs "Pizza Hut"
# while still catching "KFC Berlin Mitte" vs "KFC (Berlin)"
SIMILARITY_THRESHOLD = 0.82


def merge_records(records: list[dict]) -> list[dict]:
    """
    Within a list of records for the same ZIP, deduplicate by name similarity.
    """
    merged: list[dict] = []

    for record in records:
        name    = record.get("name", "")
        matched = False

        for existing in merged:
            if similar(name, existing.get("name", "")) >= SIMILARITY_THRESHOLD:
                _merge_two(existing, record)
                matched = True
                break

        if not matched:
            # Deep copy to avoid mutation issues
            merged.append(dict(record))

    return merged


def main():
    p = argparse.ArgumentParser(description="WF-05: Merge and deduplicate restaurant records")
    p.add_argument("--wolt-file",  required=True, help="Path to wolt extracted JSONL")
    p.add_argument("--uber-file",  required=True, help="Path to uber extracted JSONL")
    p.add_argument("--output-csv", required=True, help="Output CSV path")
    args = p.parse_args()

    wolt_recs = load_jsonl(args.wolt_file)
    uber_recs = load_jsonl(args.uber_file)

    print(f"Loaded {len(wolt_recs)} Wolt records and {len(uber_recs)} Uber Eats records.")

    if not wolt_recs and not uber_recs:
        print("WARNING: Both input files are empty. Nothing to write.")
        # Write empty CSV with headers so downstream steps don't crash
        with open(args.output_csv, "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow([
                "Restaurant Name", "Address", "Email Address",
                "Contact Number", "Ratings", "Reviews Count",
                "Owner Name", "Wolt URL", "Uber URL", "ZIP Code", "Platform",
            ])
        return

    # Group by ZIP code
    zip_groups: dict[str, list[dict]] = defaultdict(list)
    for r in wolt_recs + uber_recs:
        zip_code = str(r.get("zip_code", "UNKNOWN")).strip()
        zip_groups[zip_code].append(r)

    # Merge within each ZIP
    clean_data: list[dict] = []
    for zip_code, records in zip_groups.items():
        merged = merge_records(records)
        clean_data.extend(merged)

    print(f"After deduplication: {len(clean_data)} unique restaurants.")

    # Build separate URL columns for clarity
    def split_urls(url_str: str) -> tuple[str, str]:
        """Split concatenated URLs into Wolt and Uber columns."""
        parts   = [u.strip() for u in url_str.split("|") if u.strip()]
        wolt_u  = next((u for u in parts if "wolt.com" in u), "N/A")
        uber_u  = next((u for u in parts if "ubereats.com" in u), "N/A")
        return wolt_u, uber_u

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

    with open(args.output_csv, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(headers)

        for r in clean_data:
            wolt_url, uber_url = split_urls(r.get("url", ""))
            platform = r.get("platform", "N/A")
            # If record has both platforms in URL it was merged
            if "wolt.com" in r.get("url", "") and "ubereats.com" in r.get("url", ""):
                platform = "both"

            w.writerow([
                r.get("name", "Unknown"),
                coalesce(r.get("address")),
                coalesce(r.get("email")),
                coalesce(r.get("phone")),
                r.get("rating", "N/A"),
                r.get("reviews", "N/A"),
                get_best_owner(r),
                wolt_url,
                uber_url,
                r.get("zip_code", "UNKNOWN"),
                platform,
            ])

    print(f"Successfully wrote {len(clean_data)} records to {args.output_csv}")


if __name__ == "__main__":
    main()