# Validation ZIP codes for testing (5 German cities: Berlin, Munich, Cologne, Leipzig, Sylt)
VALIDATION_ZIP_CODES = [
    "10115",  # Berlin Mitte
    "80331",  # Munich Altstadt
    "50667",  # Cologne Altstadt
    "04109",  # Leipzig Zentrum
    "25980",  # Westerland, Sylt
]

# Scraper settings
REQUEST_DELAY_MIN = 1.5   # seconds between requests (min)
REQUEST_DELAY_MAX = 3.5   # seconds between requests (max)
MAX_RETRIES = 3
HEADLESS = True
