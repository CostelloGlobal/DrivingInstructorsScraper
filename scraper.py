import os
import sys
import time
import json
import datetime as dt
from typing import List, Dict, Any, Optional

import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# =========================
# 0) CONFIGURATION VIA ENV
# =========================
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
TABLE_NAME = os.environ.get("TABLE_NAME", "instructors")
TEST_MODE = os.environ.get("TEST_MODE", "1")  # "1" = demo row, "0" = real scrape
UPSERT_ON = os.environ.get("UPSERT_ON", "source_url")

# =========================
# Browser-like Session
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
# 1) UTILITIES
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

SUPABASE = supabase_client()

def upsert_rows(rows: List[Dict[str, Any]]):
    if not rows:
        print("[INFO] No rows to insert")
        return
    try:
        resp = SUPABASE.table(TABLE_NAME).upsert(rows, on_conflict=[UPSERT_ON]).execute()
        print(f"[INFO] Upserted {len(rows)} rows → {TABLE_NAME}")
    except Exception as e:
        print(f"[ERROR] Failed to upsert: {e}", file=sys.stderr)

# =========================
# 2) SCRAPING LOGIC
# =========================
def scrape_demo():
    """Insert one demo row for TEST_MODE=1"""
    url = "https://example.com"
    r = SESSION.get(url)
    if r.status_code != 200:
        print(f"[ERROR] Demo fetch failed: {r.status_code}")
        return
    soup = BeautifulSoup(r.text, "html.parser")
    title = soup.title.string if soup.title else "No title"
    row = {
        "source_url": url,
        "title": title,
        "fetched_at": dt.datetime.utcnow().isoformat()
    }
    upsert_rows([row])

def scrape_real():
    """Your real scraping logic goes here"""
    urls = [
        # TODO: replace with your real DVSA or instructor URLs
        "https://example.com/page1",
        "https://example.com/page2"
    ]
    results = []
    for url in urls:
        r = SESSION.get(url)
        if r.status_code == 403:
            print(f"[BLOCKED] 403 at {url}")
            continue
        if not (200 <= r.status_code < 400):
            print(f"[WARN] fetch {url} → {r.status_code}")
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        title = soup.title.string if soup.title else "No title"
        results.append({
            "source_url": url,
            "title": title,
            "fetched_at": dt.datetime.utcnow().isoformat()
        })
        time.sleep(1.0)  # polite delay
    upsert_rows(results)

# =========================
# 3) MAIN
# =========================
if __name__ == "__main__":
    print(f"[INFO] TEST_MODE={TEST_MODE}")
    if TEST_MODE == "1":
        scrape_demo()
    else:
        scrape_real()
