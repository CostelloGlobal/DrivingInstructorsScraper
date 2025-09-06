import os
import sys
import time
import datetime as dt
from typing import List, Dict, Any, Optional

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

# 🔴🔴🔴 IMPORTANT: Set this to the REAL search endpoint that accepts a postcode.
# Example placeholder below — REPLACE with the DVSA / site URL you actually query.
# It must contain "{pc}" where the postcode goes.
SEARCH_URL_TEMPLATE = os.environ.get(
    "SEARCH_URL_TEMPLATE",
    "https://example.com/search?postcode={pc}"
)

# Postcodes to scrape when TEST_MODE="0"
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

# ---- Parsing helpers ----
def extract_title(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    return (soup.title.string or "").strip() if soup.title else ""

def extract_items(html: str, page_url: str) -> List[Dict[str, Any]]:
    """
    This is where you'd parse the *actual* instructor cards on the results page.
    To keep it compatible with your current Supabase schema, we only return
    keys that exist in your table: source_url, title, fetched_at.

    When you're ready to store richer fields (name/phone/postcode), add columns
    in Supabase and extend the dicts below accordingly.
    """
    soup = BeautifulSoup(html, "html.parser")

    results: List[Dict[str, Any]] = []

    # --- Example strategies (uncomment & adapt for the real site) ---
    # for card in soup.select(".instructor-card"):  # CSS selector for each result
    #     name = card.select_one(".name").get_text(strip=True) if card.select_one(".name") else ""
    #     title = name or extract_title(html)
    #     link = card.select_one("a")
    #     href = link["href"] if link and link.has_attr("href") else page_url
    #     results.append({
    #         "source_url": href if href.startswith("http") else page_url,
    #         "title": title,
    #         "fetched_at": now_iso(),
    #     })

    # Fallback: if we don't know the structure yet, insert one row with the page title.
    # (This guarantees you see *something* land in Supabase while we tune selectors.)
    title = extract_title(html)
    results.append({
        "source_url": page_url,
        "title": title or "Results page",
        "fetched_at": now_iso(),
    })

    return results

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
            print(f"[BLOCKED] 403 for {url} (site blocking this IP).")
            # If this persists, use a self-hosted runner or a residential proxy.
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
