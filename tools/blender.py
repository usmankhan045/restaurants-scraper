import argparse
import csv
import json
import difflib
from collections import defaultdict

def similar(a, b): return difflib.SequenceMatcher(None, str(a).lower(), str(b).lower()).ratio()

def load_jsonl(path):
    records = []
    try:
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip(): records.append(json.loads(line.strip()))
    except: pass
    return records

def coalesce(*args):
    for a in args:
        if a and str(a).strip() not in ("N/A", "None", "", "Hidden by Platform"): return str(a).strip()
    return "Not Found"

def get_owner(rec):
    if rec.get("owner") and "Hidden" not in str(rec.get("owner")): return rec.get("owner")
    if rec.get("legal_entity") and "Hidden" not in str(rec.get("legal_entity")): return rec.get("legal_entity")
    if "Hidden" in str(rec.get("owner", "")): return "Hidden by Platform"
    return "Not Found"

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--wolt-file", required=True)
    p.add_argument("--uber-file", required=True)
    p.add_argument("--output-csv", required=True)
    args = p.parse_args()

    wolt_recs = load_jsonl(args.wolt_file)
    uber_recs = load_jsonl(args.uber_file)

    zip_groups = defaultdict(list)
    for r in wolt_recs + uber_recs:
        zip_groups[str(r.get("zip_code", "UNKNOWN"))].append(r)

    clean_data = []
    for zc, records in zip_groups.items():
        merged = []
        for r in records:
            match = False
            for m in merged:
                if similar(r.get("name", ""), m.get("name", "")) > 0.8:
                    match = True
                    if m.get("email") in ("Not Found", "", None) and r.get("email"): m["email"] = r["email"]
                    if m.get("phone") in ("Not Found", "", None) and r.get("phone"): m["phone"] = r["phone"]
                    if m.get("rating", "N/A") == "N/A" and r.get("rating", "N/A") != "N/A":
                        m["rating"] = r.get("rating")
                        m["reviews"] = r.get("reviews")
                    
                    o1, o2 = get_owner(m), get_owner(r)
                    if o1 in ("Not Found", "Hidden by Platform") and o2 not in ("Not Found", "Hidden by Platform"): m["owner"] = o2
                    elif o1 == "Not Found" and o2 == "Hidden by Platform": m["owner"] = "Hidden by Platform"
                    
                    m["url"] = f"{m.get('url')} | {r.get('url')}"
                    break
            if not match: merged.append(r)
        clean_data.extend(merged)

    headers = ["Restaurant Name", "Email Address", "Contact Number", "Ratings", "Reviews Count", "Owner Name", "URL", "ZIP Code"]
    with open(args.output_csv, 'w', encoding='utf-8', newline='') as f:
        w = csv.writer(f)
        w.writerow(headers)
        for r in clean_data:
            w.writerow([
                r.get("name", "Unknown"),
                coalesce(r.get("email")),
                coalesce(r.get("phone")),
                r.get("rating", "N/A"),
                r.get("reviews", "N/A"),
                get_owner(r),
                r.get("url", "N/A"),
                r.get("zip_code", "UNKNOWN")
            ])

if __name__ == "__main__": main()