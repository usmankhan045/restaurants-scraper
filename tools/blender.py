import argparse
import csv
import json
import difflib
from collections import defaultdict

def similar(a, b):
    return difflib.SequenceMatcher(None, str(a).lower(), str(b).lower()).ratio()

def load_jsonl(path):
    records = []
    try:
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip(): records.append(json.loads(line.strip()))
    except: pass
    return records

def coalesce(*args):
    """Returns the first argument that is not empty or a fallback string."""
    for a in args:
        if a and str(a).strip() not in ("N/A", "None", "", "Hidden by Platform"): return str(a).strip()
    return "N/A"

def get_owner(rec):
    if rec.get("owner") and "Hidden" not in str(rec.get("owner")) and rec.get("owner") != "N/A": return rec.get("owner")
    if rec.get("legal_entity") and "Hidden" not in str(rec.get("legal_entity")) and rec.get("legal_entity") != "N/A": return rec.get("legal_entity")
    if "Hidden" in str(rec.get("owner", "")): return "Hidden by Platform"
    return "N/A"

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--wolt-file", required=True)
    p.add_argument("--uber-file", required=True)
    p.add_argument("--output-csv", required=True)
    args = p.parse_args()

    wolt_recs = load_jsonl(args.wolt_file)
    uber_recs = load_jsonl(args.uber_file)

    print(f"Loaded {len(wolt_recs)} Wolt records and {len(uber_recs)} Uber records.")

    zip_groups = defaultdict(list)
    for r in wolt_recs + uber_recs:
        zip_groups[str(r.get("zip_code", "UNKNOWN"))].append(r)

    clean_data = []
    for zc, records in zip_groups.items():
        merged = []
        for r in records:
            match = False
            for m in merged:
                # 65% similarity to aggressively merge "KFC Berlin" and "KFC (Alexanderplatz)"
                if similar(r.get("name", ""), m.get("name", "")) > 0.65:
                    match = True
                    if m.get("email") in ("N/A", "", None) and r.get("email"): m["email"] = r["email"]
                    if m.get("phone") in ("N/A", "", None) and r.get("phone"): m["phone"] = r["phone"]
                    if m.get("address") in ("N/A", "", None) and r.get("address"): m["address"] = r["address"]
                    if m.get("rating", "N/A") == "N/A" and r.get("rating", "N/A") != "N/A":
                        m["rating"] = r.get("rating")
                        m["reviews"] = r.get("reviews")
                    
                    o1, o2 = get_owner(m), get_owner(r)
                    if o1 in ("N/A", "Hidden by Platform") and o2 not in ("N/A", "Hidden by Platform"): m["owner"] = o2
                    elif o1 == "N/A" and o2 == "Hidden by Platform": m["owner"] = "Hidden by Platform"
                    
                    m["url"] = f"{m.get('url')} | {r.get('url')}"
                    break
            if not match: merged.append(r)
        clean_data.extend(merged)

    # Note the explicit "Address" column mapped below
    headers = ["Restaurant Name", "Address", "Email Address", "Contact Number", "Ratings", "Reviews Count", "Owner Name", "URL", "ZIP Code"]
    with open(args.output_csv, 'w', encoding='utf-8', newline='') as f:
        w = csv.writer(f)
        w.writerow(headers)
        for r in clean_data:
            w.writerow([
                r.get("name", "Unknown"),
                coalesce(r.get("address")),
                coalesce(r.get("email")),
                coalesce(r.get("phone")),
                r.get("rating", "N/A"),
                r.get("reviews", "N/A"),
                get_owner(r),
                r.get("url", "N/A"),
                r.get("zip_code", "UNKNOWN")
            ])
            
    print(f"Successfully wrote {len(clean_data)} records to {args.output_csv}")

if __name__ == "__main__":
    main()