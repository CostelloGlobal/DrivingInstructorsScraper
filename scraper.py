import os
import sys
import time
import datetime as dt
from typing import List, Dict, Any

import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# =========================
# 0) CONFIG (via env vars)
# =========================
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
TABLE_NAME = os.environ.get("TABLE_NAME", "instructors")
TEST_MODE = os.environ.get("TEST_MODE", "1")  # "1" = demo insert, "0" = real scraping
UPSERT_ON = os.environ.get("UPSERT_ON", "source_url")

# DVSA find-instructor page (postcode goes in {pc})
SEARCH_URL_TEMPLATE = "https://finddrivinginstructor.dvsa.gov.uk/DSAFindNearestWebApp/findNearest.form?postcode={pc}&lang=en"

# Postcodes to try in TEST_MODE=0 (edit/expand as you like)
POSTCODES = os.environ.get("POSTCODES", "E1,M1,B1").split(",")

# polite delay between requests (seconds)
REQUEST_DELAY = float(os.environ.get("REQUEST_DELAY", "1.0"))

# =========================
# 1) Browser-like Session
# =========================
BROWSER_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/126.0.0.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
    "Referer": "https://www.google.com/",
}

def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5, connect=5, read=5, status=5,
        backoff_factor=1.2,
        status_forcelist=[403,408,425,429,500,502,503,504],
        allowed_methods=["GET","HEAD"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update(BROWSER_HEADERS)
    return s

SESSION = make_session()

# =========================
# 2) Supabase client
# =========================
def fail(msg: str):
    print(f"[SETUP ERROR] {msg}", file=sys.stderr)
    sys.exit(1)

def supabase_client() -> Client:
    if not SUPABASE_URL:
        fail("SUPABASE_URL missing.")
    if not SUPABASE_KEY:
        fail("SUPABASE_KEY missing.")
    try:
        return create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        fail(f"Failed to create Supabase client: {e}")

SB = supabase_client()

def upsert_rows(rows: List[Dict[str, Any]]):
    if not rows:
        print("[INFO] No rows to insert")
        return
    try:
        SB.table(TABLE_NAME).upsert(rows, on_conflict=[UPSERT_ON]).execute()
        print(f"[INFO] Upserted {len(rows)} rows → {TABLE_NAME}")
    except Exception as e:
        print(f"[ERROR] Failed to upsert: {e}", file=sys.stderr)

# =========================
# 3) Helpers
# =========================
def now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def build_url_for_postcode(pc: str) -> str:
    pc = pc.strip().replace(" ", "")
    return SEARCH_URL_TEMPLATE.format(pc=pc)

def extract_title(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    return (soup.title.string or "").strip() if soup.title else ""

def extract_items(html: str, page_url: str) -> List[Dict[str, Any]]:
    """
    Minimal insert that matches your current table (source_url, title, fetched_at).
    Once you decide on extra columns (name/phone/postcode), extend this.
    """
    soup = BeautifulSoup(html, "html.parser")

    # TODO: Replace this with selectors once we pin down DVSA HTML structure.
    # For now, insert one row per page so you can see data arriving.
    title = extract_title(html) or "DVSA results page"
    return [{
        "source_url": page_url,
        "title": title,
        "fetched_at": now_iso(),
    }]

# =========================
# 4) Modes
# =========================
def run_demo():
    url = "https://example.com"
    print(f"[INFO] TEST_MODE=1 → demo insert from {url}")
    r = SESSION.get(url, timeout=25)
    if r.status_code != 200:
        print(f"[ERROR] Demo fetch failed: {r.status_code}")
        return
    row = {
        "source_url": url,
        "title": extract_title(r.text) or "Example Domain",
        "fetched_at": now_iso(),
    }
    upsert_rows([row])

def run_real():
    all_rows: List[Dict[str, Any]] = []
    for pc in POSTCODES:
        url = build_url_for_postcode(pc)
        print(f"[INFO] Fetching {pc.strip()} → {url}")
        r = SESSION.get(url, timeout=25)

        if r.status_code == 403:
            print(f"[BLOCKED] 403 for {url} (site may block cloud IPs).")
            continue
        if r.status_code == 429:
            print("[RATE LIMIT] 429 — sleeping 30s then retrying once.")
            time.sleep(30)
            r = SESSION.get(url, timeout=25)

        if not (200 <= r.status_code < 400):
            print(f"[WARN] HTTP {r.status_code} for {url}")
            continue

        rows = extract_items(r.text, url)
        all_rows.extend(rows)
        time.sleep(REQUEST_DELAY)

    upsert_rows(all_rows)

# =========================
# 5) Entrypoint
# =========================
if __name__ == "__main__":
    print(f"[INFO] TEST_MODE={TEST_MODE}")
    if TEST_MODE == "1":
        run_demo()
    else:
        run_real()
