"""
WF-03: API Scout (No-Browser Version)
Discovers all restaurants for given ZIP codes by calling platform APIs directly.
- Wolt:      GET  restaurant-api.wolt.com/v1/pages/delivery?lat=X&lon=Y
- Uber Eats: POST ubereats.com/_p/api/getFeedV1  (requires session cookie)

No Selenium, no XVFB, no browser at all. Pure HTTP → works on GitHub Actions.

Usage:
    python tools/scout_api.py --chunk-file .tmp/chunks/chunk_01.json --worker-id scout-wolt-1 --platform wolt
    python tools/scout_api.py --chunk-file .tmp/chunks/chunk_01.json --worker-id scout-uber-1 --platform uber
    python tools/scout_api.py --zip 10115 --worker-id test-1 --platform wolt
"""

import argparse
import base64
import json
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from config import MAX_RETRIES, REQUEST_DELAY_MIN, REQUEST_DELAY_MAX
from tools.logger import ScraperLogger
from tools.state_manager import StateManager

# ---------------------------------------------------------------------------
# Coordinates cache
# ---------------------------------------------------------------------------

_COORDS_CACHE: dict | None = None

def _load_coords() -> dict:
    global _COORDS_CACHE
    if _COORDS_CACHE is None:
        coords_path = ROOT / "data" / "zip_coords.json"
        if coords_path.exists():
            with coords_path.open("r", encoding="utf-8") as f:
                _COORDS_CACHE = json.load(f)
        else:
            _COORDS_CACHE = {}
    return _COORDS_CACHE


def get_coords(zip_code: str) -> dict | None:
    cache = _load_coords()
    return cache.get(zip_code)


# ---------------------------------------------------------------------------
# Shared HTTP session with browser-like headers
# ---------------------------------------------------------------------------

def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate",
        "Cache-Control":   "no-cache",
        "Pragma":          "no-cache",
    })
    return s


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Wolt API Scout
# ---------------------------------------------------------------------------

WOLT_DELIVERY_URL = "https://restaurant-api.wolt.com/v1/pages/restaurants"
WOLT_VENUE_URL    = "https://restaurant-api.wolt.com/v4/venues/slug/{slug}"
WOLT_CITY_MAP = {
    # country_code_alpha2: wolt country slug in URL
    "DE": "deu",
}


def _wolt_headers() -> dict:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "de-DE,de;q=0.9",
        "Referer":         "https://wolt.com/",
        "Origin":          "https://wolt.com",
        "DNT":             "1",
    }


def wolt_discover(zip_code: str, lat: float, lon: float, log: ScraperLogger) -> list[dict]:
    """
    Call Wolt's delivery page API to list all venues near lat/lon.
    Returns list of partial venue dicts (name, slug, address, rating, etc.)
    """
    session = _make_session()
    bound   = log.bind(zip_code)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(
                WOLT_DELIVERY_URL,
                params={"lat": lat, "lon": lon},
                headers=_wolt_headers(),
                timeout=15,
            )
            if resp.status_code == 429:
                wait = 15 * attempt
                bound.warning(action="WOLT_DISCOVER", message=f"Rate limited, waiting {wait}s")
                time.sleep(wait)
                continue
            if resp.status_code != 200:
                bound.warning(
                    action="WOLT_DISCOVER",
                    message=f"HTTP {resp.status_code} on attempt {attempt}",
                )
                time.sleep(5 * attempt)
                continue

            data     = resp.json()
            sections = data.get("sections", [])
            venues: list[dict] = []
            seen_slugs: set[str] = set()

            for section in sections:
                for item in section.get("items", []):
                    venue = item.get("venue")
                    if not venue:
                        continue
                    slug = venue.get("slug", "")
                    if not slug or slug in seen_slugs:
                        continue
                    seen_slugs.add(slug)
                    venues.append(venue)

            bound.info(
                action="WOLT_DISCOVER",
                message=f"Found {len(venues)} venues",
                venues=len(venues),
                lat=lat,
                lon=lon,
            )
            return venues

        except requests.exceptions.Timeout:
            bound.warning(action="WOLT_DISCOVER", message=f"Timeout attempt {attempt}")
            time.sleep(5 * attempt)
        except Exception as exc:
            bound.error(action="WOLT_DISCOVER", message=f"Error: {exc}")
            time.sleep(5 * attempt)

    return []


def wolt_enrich(slug: str, zip_code: str, log: ScraperLogger) -> dict:
    """
    Call Wolt's v4 venue detail endpoint to get phone, full address, etc.
    Returns enrichment dict (may be empty if request fails).
    """
    session = _make_session()
    url     = WOLT_VENUE_URL.format(slug=slug)

    for attempt in range(1, 3):  # 2 attempts only — this is enrichment, not critical
        try:
            resp = session.get(url, headers=_wolt_headers(), timeout=10)
            if resp.status_code == 200:
                results = resp.json().get("results", [])
                if results:
                    return results[0]
            elif resp.status_code == 429:
                time.sleep(10 * attempt)
                continue
            else:
                break
        except Exception:
            time.sleep(3)

    return {}


def process_wolt_zip(
    zip_code: str,
    worker_id: str,
    log: ScraperLogger,
    out_path: Path,
) -> int:
    """Full Wolt pipeline for one ZIP: discover → enrich → save."""
    bound  = log.bind(zip_code)
    coords = get_coords(zip_code)

    if not coords:
        bound.warning(action="ZIP_SKIP", message="No coordinates found for ZIP, skipping")
        return 0

    lat, lon = coords["lat"], coords["lon"]

    # Step 1: Discovery
    venues = wolt_discover(zip_code, lat, lon, log)
    if not venues:
        bound.info(action="ZIP_DONE", message="Zero venues returned")
        return 0

    records: list[dict] = []
    for v in venues:
        slug = v.get("slug", "")
        if not slug:
            continue

        # Step 2: Enrich with v4 detail (phone, full address, web_page)
        time.sleep(random.uniform(0.6, 1.2))
        detail = wolt_enrich(slug, zip_code, log)

        # Build city slug for URL (e.g. "berlin" from city field)
        city_raw = (
            detail.get("city", {}).get("name", "")
            or v.get("city", "")
            or ""
        )
        city_slug = city_raw.lower().replace(" ", "-").replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")

        # Rating — discovery response has rating as dict: {"score": 9.2, "volume": 340}
        rating_obj = v.get("rating") or detail.get("rating") or {}
        if isinstance(rating_obj, dict):
            rating  = str(rating_obj.get("score", "N/A"))
            reviews = str(rating_obj.get("volume", "N/A"))
        else:
            rating, reviews = "N/A", "N/A"

        # Phone — v4 response field
        phone = (
            detail.get("public_phone")
            or detail.get("phone")
            or "N/A"
        )
        if phone and phone != "N/A":
            phone = str(phone).strip()

        # Address
        address = (
            detail.get("address")
            or v.get("address")
            or "N/A"
        )

        # Build canonical URL
        url = f"https://wolt.com/de/deu/{city_slug}/restaurant/{slug}" if city_slug else f"https://wolt.com/de/restaurant/{slug}"

        records.append({
            "name":       detail.get("name") or v.get("name", "Unknown"),
            "url":        url,
            "platform":   "wolt",
            "status":     "open" if v.get("online", True) else "closed",
            "rating":     rating,
            "reviews":    reviews,
            "address":    address,
            "phone":      phone,
            "email":      "N/A",
            "zip_code":   zip_code,
            "scraped_at": _now_iso(),
            "worker_id":  worker_id,
        })

    if records:
        with out_path.open("a", encoding="utf-8") as f:
            for r in records:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")

    bound.info(action="ZIP_DONE", message=f"Saved {len(records)} Wolt records", count=len(records))
    return len(records)


# ---------------------------------------------------------------------------
# Uber Eats API Scout
# ---------------------------------------------------------------------------

UBER_FEED_URL = "https://www.ubereats.com/_p/api/getFeedV1"


def _encode_uber_pl(lat: float, lon: float) -> str:
    """Encode a lat/lon into Uber Eats' base64 location parameter."""
    payload = {
        "address":       "",
        "reference":     "",
        "referenceType": "google_places",
        "latitude":      lat,
        "longitude":     lon,
        "type":          "google_places",
    }
    return base64.b64encode(json.dumps(payload, separators=(",", ":")).encode()).decode()


def _uber_headers(cookie: str) -> dict:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Content-Type":           "application/json",
        "Accept":                 "*/*",
        "Accept-Language":        "de-DE,de;q=0.9",
        "x-csrf-token":           "x",
        "x-uber-client-gitref":   "x",
        "Referer":                "https://www.ubereats.com/de",
        "Origin":                 "https://www.ubereats.com",
        "Cookie":                 cookie,
    }


def uber_discover_page(
    lat: float,
    lon: float,
    cookie: str,
    offset: int,
    page_size: int = 80,
) -> tuple[list[dict], bool]:
    """
    Call getFeedV1 for one page. Returns (feed_items, has_more).
    """
    pl      = _encode_uber_pl(lat, lon)
    payload = {
        "userQuery":        "",
        "date":             "",
        "startTime":        0,
        "endTime":          0,
        "carouselId":       "",
        "sortAndFilters":   [],
        "feedSessionId":    str(random.randint(10 ** 17, 10 ** 18)),
        "pl":               pl,
        "pageInfo":         {"offset": offset, "pageSize": page_size},
        "marketingFeedType": "DEFAULT_DELIVERY",
        "targetingDetails":  {},
    }

    session = _make_session()
    try:
        resp = session.post(
            UBER_FEED_URL,
            headers=_uber_headers(cookie),
            json=payload,
            timeout=20,
        )
        if resp.status_code == 429:
            return [], False
        if resp.status_code != 200:
            return [], False

        body       = resp.json()
        data       = body.get("data", {})
        feed_items = data.get("feedItems", [])
        meta       = data.get("meta", {})
        has_more   = meta.get("hasMore", False) or (len(feed_items) == page_size)
        return feed_items, has_more

    except Exception:
        return [], False


def uber_discover(zip_code: str, lat: float, lon: float, cookie: str, log: ScraperLogger) -> list[dict]:
    """
    Paginate through getFeedV1 to collect all stores for a location.
    """
    bound    = log.bind(zip_code)
    stores: list[dict] = []
    seen_uuids: set[str] = set()
    offset   = 0
    page_num = 0
    max_pages = 20  # safety cap: 20 pages × 80 = 1,600 restaurants per ZIP

    while page_num < max_pages:
        items, has_more = uber_discover_page(lat, lon, cookie, offset)

        if not items:
            bound.info(action="UBER_DISCOVER", message=f"No more items at offset {offset}")
            break

        for item in items:
            # Items can be stores, carousels, banners — we only want stores
            store = item.get("store") or item.get("storeInfo")
            if not store:
                # Some responses wrap differently
                if item.get("type") == "STORE":
                    store = item
            if not store:
                continue

            uuid = store.get("uuid") or store.get("storeUuid") or store.get("heroImageUuid", "")
            # Fallback: use title as dedup key
            title_obj = store.get("title") or {}
            title_str = title_obj.get("text", "") if isinstance(title_obj, dict) else str(title_obj)
            dedup_key = uuid or title_str
            if not dedup_key or dedup_key in seen_uuids:
                continue
            seen_uuids.add(dedup_key)
            stores.append(store)

        bound.debug(
            action="UBER_DISCOVER",
            message=f"Page {page_num + 1}: got {len(items)} items, cumulative stores={len(stores)}",
        )

        if not has_more:
            break

        offset   += len(items)
        page_num += 1
        time.sleep(random.uniform(1.0, 2.0))  # polite delay between pages

    bound.info(action="UBER_DISCOVER", message=f"Total stores found: {len(stores)}")
    return stores


def _parse_uber_store(store: dict, zip_code: str, worker_id: str) -> dict | None:
    """Parse a raw Uber store object into our standard schema."""
    title_obj = store.get("title") or {}
    name = title_obj.get("text", "") if isinstance(title_obj, dict) else str(title_obj)
    if not name:
        name = store.get("name", "Unknown")

    # URL — Uber provides a slug or relative path
    slug    = store.get("slug") or ""
    url_obj = store.get("url") or {}
    if isinstance(url_obj, dict):
        url = url_obj.get("url") or url_obj.get("path") or ""
    else:
        url = str(url_obj) if url_obj else ""

    if url and url.startswith("/"):
        url = "https://www.ubereats.com" + url
    elif not url and slug:
        url = f"https://www.ubereats.com/de/store/{slug}"
    elif not url:
        return None  # no URL means we can't reference this restaurant

    # Rating
    rating_obj = store.get("rating") or store.get("ratingDetails") or {}
    if isinstance(rating_obj, dict):
        rating  = str(rating_obj.get("rating") or rating_obj.get("ratingValue") or "N/A")
        reviews = str(rating_obj.get("reviewCount") or rating_obj.get("numRatings") or "N/A")
    else:
        rating, reviews = "N/A", "N/A"

    # Address
    location_obj = store.get("location") or store.get("restaurantLocation") or {}
    if isinstance(location_obj, dict):
        address = (
            location_obj.get("address")
            or location_obj.get("streetAddress")
            or location_obj.get("formattedAddress")
            or "N/A"
        )
    else:
        address = "N/A"

    # Phone — Uber almost never exposes this in the feed
    phone = store.get("phoneNumber") or store.get("phone") or "N/A"

    return {
        "name":       name.strip(),
        "url":        url.split("?")[0].rstrip("/"),
        "platform":   "uber",
        "status":     "open",
        "rating":     rating,
        "reviews":    reviews,
        "address":    address,
        "phone":      str(phone).strip() if phone != "N/A" else "N/A",
        "email":      "N/A",
        "zip_code":   zip_code,
        "scraped_at": _now_iso(),
        "worker_id":  worker_id,
    }


def process_uber_zip(
    zip_code: str,
    worker_id: str,
    cookie: str,
    log: ScraperLogger,
    out_path: Path,
) -> int:
    """Full Uber pipeline for one ZIP: discover pages → parse → save."""
    bound  = log.bind(zip_code)
    coords = get_coords(zip_code)

    if not coords:
        bound.warning(action="ZIP_SKIP", message="No coordinates for ZIP")
        return 0

    lat, lon = coords["lat"], coords["lon"]

    stores = uber_discover(zip_code, lat, lon, cookie, log)
    if not stores:
        bound.info(action="ZIP_DONE", message="Zero stores returned")
        return 0

    records = []
    for store in stores:
        rec = _parse_uber_store(store, zip_code, worker_id)
        if rec:
            records.append(rec)

    if records:
        with out_path.open("a", encoding="utf-8") as f:
            for r in records:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")

    bound.info(action="ZIP_DONE", message=f"Saved {len(records)} Uber records", count=len(records))
    return len(records)


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description="WF-03: API-based restaurant scout")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--chunk-file", help="Path to JSON chunk file")
    g.add_argument("--zip",        help="Single ZIP code for testing")
    p.add_argument("--worker-id",  required=True)
    p.add_argument("--platform",   required=True, choices=["wolt", "uber"])
    return p.parse_args()


def main():
    args = parse_args()
    wid  = args.worker_id
    plat = args.platform

    log   = ScraperLogger(worker_id=wid)
    state = StateManager(f"scout_api_{wid}")

    if args.zip:
        zip_codes = [args.zip.strip()]
    else:
        with open(args.chunk_file, "r", encoding="utf-8") as f:
            chunk_data = json.load(f)
        zip_codes = chunk_data.get("codes", [])

    out_path = ROOT / ".tmp" / f"results_{wid}.jsonl"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Uber requires a session cookie stored as env var / GitHub Actions secret
    uber_cookie = os.getenv("UBER_SESSION_COOKIE", "")

    remaining = state.pending(zip_codes)
    log.info(
        action="SCOUT_INIT",
        message=f"Platform={plat.upper()} | {len(remaining)}/{len(zip_codes)} ZIPs to process",
    )

    if plat == "uber" and not uber_cookie:
        log.error(
            action="SCOUT_INIT",
            message=(
                "UBER_SESSION_COOKIE env var is not set. "
                "Capture it from browser DevTools and set as a GitHub secret."
            ),
        )
        # Don't crash the whole job — just skip and log
        sys.exit(0)

    total_found = 0

    for zip_code in remaining:
        state.mark_in_progress(zip_code)
        try:
            if plat == "wolt":
                count = process_wolt_zip(zip_code, wid, log, out_path)
            else:
                count = process_uber_zip(zip_code, wid, uber_cookie, log, out_path)

            total_found += count
            state.mark_completed(zip_code, metadata={"count": count})

        except Exception as exc:
            log.error(action="ZIP_ERROR", message=f"ZIP {zip_code} failed: {exc}")
            state.mark_failed(zip_code, error=str(exc))

        # Polite delay between ZIPs
        time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))

    summary = state.summary()
    log.info(
        action="SCOUT_DONE",
        message=f"Complete. Total restaurants found: {total_found}",
        total_found=total_found,
        **summary,
    )
    print(f"\nDone. Platform={plat} | ZIPs processed={summary.get('completed', 0)} | Restaurants found={total_found}")


if __name__ == "__main__":
    main()