# ─────────────────────────────────────────────────────────────────────────────
# Scraper Configuration
# ─────────────────────────────────────────────────────────────────────────────

# Validation ZIP codes for test runs (5 cities covering different regions)
VALIDATION_ZIP_CODES = [
    "10115",   # Berlin Mitte (large city, many restaurants)
    "80331",   # Munich Altstadt (large city)
    "50667",   # Cologne Altstadt (large city)
    "04109",   # Leipzig Zentrum (medium city)
    "25980",   # Westerland, Sylt (small/rural — tests sparse results handling)
]

# ─────────────────────────────────────────────────────────────────────────────
# Request behaviour
# ─────────────────────────────────────────────────────────────────────────────

# Seconds to wait between ZIP code requests
# (keeps us polite to Wolt/Uber APIs without being excessively slow)
REQUEST_DELAY_MIN = 1.2
REQUEST_DELAY_MAX = 2.8

# Max retries per request before marking as failed
MAX_RETRIES = 3

# ─────────────────────────────────────────────────────────────────────────────
# Browser settings (legacy — only used if running old scout.py)
# ─────────────────────────────────────────────────────────────────────────────
HEADLESS = True

# ─────────────────────────────────────────────────────────────────────────────
# Wolt API settings
# ─────────────────────────────────────────────────────────────────────────────

# Seconds between individual venue detail (v4) calls within one ZIP
WOLT_ENRICH_DELAY_MIN = 0.6
WOLT_ENRICH_DELAY_MAX = 1.2

# ─────────────────────────────────────────────────────────────────────────────
# Uber Eats API settings
# ─────────────────────────────────────────────────────────────────────────────

# Items per getFeedV1 page request
UBER_PAGE_SIZE = 80

# Max pages to fetch per ZIP (safety cap — 20 × 80 = 1,600 restaurants/ZIP)
UBER_MAX_PAGES = 20

# Seconds between paginated feed requests within one ZIP
UBER_PAGE_DELAY_MIN = 1.0
UBER_PAGE_DELAY_MAX = 2.0