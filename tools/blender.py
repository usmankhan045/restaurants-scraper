"""
WAT Framework — WF-05: Dataset Blender
Smart Merger for Wolt and Uber datasets.

Groups all records by zip_code and uses SequenceMatcher for fuzzy matching
restaurant names within the same zip code.
Exports to CSV with exact column headers.
"""

import argparse
import csv
import json
import difflib
from collections import defaultdict


def parse_args():
    parser = argparse.ArgumentParser(description="Smart Merger for Wolt and Uber datasets")
    parser.add_argument("--wolt-file", required=True, help="Path to Wolt JSONL")
    parser.add_argument("--uber-file", required=True, help="Path to Uber JSONL")
    parser.add_argument("--output-csv", required=True, help="Path to final CSV")
    
    # Accept these arguments to prevent argparse failure in existing pipelines
    parser.add_argument("--output-json", default="")
    parser.add_argument("--name-threshold", type=float, default=90)
    parser.add_argument("--addr-threshold", type=float, default=95)
    
    return parser.parse_args()


def load_jsonl(path):
    records = []
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return records


def get_valid_value(val):
    if val is None:
        return ""
    v = str(val).strip()
    if v in ("N/A", "None", ""):
        return ""
    return v


def coalesce(*args):
    for a in args:
        v = get_valid_value(a)
        if v:
            return v
    return ""


def get_owner(rec):
    owner = get_valid_value(rec.get("owner"))
    if owner: 
        return owner
    entity = get_valid_value(rec.get("legal_entity"))
    if entity: 
        return entity
    return ""


def main():
    args = parse_args()
    wolt_recs = load_jsonl(args.wolt_file)
    uber_recs = load_jsonl(args.uber_file)

    wolt_by_zip = defaultdict(list)
    uber_by_zip = defaultdict(list)

    for r in wolt_recs:
        wolt_by_zip[str(r.get("zip_code", ""))].append(r)
    for r in uber_recs:
        uber_by_zip[str(r.get("zip_code", ""))].append(r)

    all_zips = set(wolt_by_zip.keys()) | set(uber_by_zip.keys())
    final_dataset = []

    for zc in all_zips:
        wolt_list = wolt_by_zip.get(zc, [])
        uber_list = uber_by_zip.get(zc, [])
        
        matched_uber = set()
        
        for w_rec in wolt_list:
            best_match = None
            best_score = 0.0
            best_u_idx = -1
            
            w_name = get_valid_value(w_rec.get("name"))
            
            for i, u_rec in enumerate(uber_list):
                if i in matched_uber:
                    continue
                
                u_name = get_valid_value(u_rec.get("name"))
                
                # difflib.SequenceMatcher fuzzy matching threshold > 0.8
                score = difflib.SequenceMatcher(None, w_name.lower(), u_name.lower()).ratio()
                
                if score > 0.8 and score > best_score:
                    best_score = score
                    best_match = u_rec
                    best_u_idx = i
            
            if best_match:
                matched_uber.add(best_u_idx)
                
                u1 = get_valid_value(w_rec.get("url"))
                u2 = get_valid_value(best_match.get("url"))
                merged_url = f"{u1} | {u2}" if u1 and u2 else coalesce(u1, u2)
                
                w_owner = get_owner(w_rec)
                u_owner = get_owner(best_match)
                
                merged = {
                    "Restaurant Name": coalesce(w_rec.get("name"), best_match.get("name")),
                    "Email Address": coalesce(w_rec.get("email"), best_match.get("email")),
                    "Contact Number": coalesce(w_rec.get("phone"), best_match.get("phone")),
                    "Ratings": coalesce(w_rec.get("rating"), best_match.get("rating")),
                    "Reviews Count": coalesce(w_rec.get("reviews"), best_match.get("reviews")),
                    "Owner Name": coalesce(w_owner, u_owner, "Not Found"),
                    "URL": merged_url,
                    "ZIP Code": zc
                }
                final_dataset.append(merged)
            else:
                w_owner = get_owner(w_rec)
                
                merged = {
                    "Restaurant Name": w_rec.get("name", ""),
                    "Email Address": w_rec.get("email", ""),
                    "Contact Number": w_rec.get("phone", ""),
                    "Ratings": w_rec.get("rating", ""),
                    "Reviews Count": w_rec.get("reviews", ""),
                    "Owner Name": coalesce(w_owner, "Not Found"),
                    "URL": w_rec.get("url", ""),
                    "ZIP Code": zc
                }
                final_dataset.append(merged)
        
        for i, u_rec in enumerate(uber_list):
            if i not in matched_uber:
                u_owner = get_owner(u_rec)
                
                merged = {
                    "Restaurant Name": u_rec.get("name", ""),
                    "Email Address": u_rec.get("email", ""),
                    "Contact Number": u_rec.get("phone", ""),
                    "Ratings": u_rec.get("rating", ""),
                    "Reviews Count": u_rec.get("reviews", ""),
                    "Owner Name": coalesce(u_owner, "Not Found"),
                    "URL": u_rec.get("url", ""),
                    "ZIP Code": zc
                }
                final_dataset.append(merged)

    fieldnames = [
        "Restaurant Name", "Email Address", "Contact Number", 
        "Ratings", "Reviews Count", "Owner Name", "URL", "ZIP Code"
    ]

    with open(args.output_csv, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(final_dataset)
    print(f"Merged successfully. Dataset saved to {args.output_csv}")


if __name__ == "__main__":
    main()
