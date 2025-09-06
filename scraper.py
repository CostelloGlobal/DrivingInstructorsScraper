import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client
import time
import os

# ✅ Get Supabase credentials from GitHub secrets
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ✅ DVSA base URL (postcode search)
BASE_URL = "https://finddrivinginstructor.dvsa.gov.uk/DSAFindNearestWebApp/findNearest.form"

# ✅ List of UK proxies (IP:PORT) – add more as you find them
PROXIES = [
    "134.209.29.120:80",
    "81.145.247.5:8080",
    "178.62.200.48:8080",
    "51.89.149.67:3128",
    "178.62.193.19:8080"
]

# ✅ Browser headers (pretend to be Chrome UK)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-GB,en;q=0.9",
}


def try_request(postcode):
    """Try scraping DVSA with each proxy until one works"""
    params = {"postcode": postcode}

    for proxy in PROXIES:
        proxy_dict = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
        print(f"🌍 Trying proxy {proxy} for postcode {postcode}...")

        try:
            response = requests.get(BASE_URL, params=params, headers=HEADERS, proxies=proxy_dict, timeout=15)

            if response.status_code == 200:
                print(f"✅ Success with proxy {proxy}")
                return response.text
            else:
                print(f"❌ Proxy {proxy} failed with status {response.status_code}")

        except Exception as e:
            print(f"⚠️ Proxy {proxy} error: {e}")
            continue

    print("❌ All proxies failed for this postcode.")
    return None


def scrape_instructors(postcode="E1"):
    """Scrape DVSA instructor data for a given postcode"""
    html = try_request(postcode)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    instructors = []

    # ⚠️ Selectors need adjusting after inspecting real DVSA HTML
    for card in soup.select(".instructor-card"):
        name = card.select_one(".name").get_text(strip=True) if card.select_one(".name") else None
        phone = card.select_one(".phone").get_text(strip=True) if card.select_one(".phone") else None
        website = card.select_one("a")["href"] if card.select_one("a") else None

        instructors.append({
            "name": name,
            "postcode": postcode,
            "dvsa_number": None,
            "transmission": None,
            "phone": phone,
            "email": None,
            "website": website,
        })
    return instructors


def save_to_supabase(records):
    if not records:
        print("⚠️ No records to insert")
        return
    supabase.table("driving_instructors").insert(records).execute()
    print(f"✅ Inserted {len(records)} instructors into Supabase")


if __name__ == "__main__":
    # Example test postcodes
    postcodes = ["E1", "M1", "B1"]

    for pc in postcodes:
        data = scrape_instructors(pc)
        save_to_supabase(data)
        print(f"⏳ Waiting 5 seconds before next postcode…")
        time.sleep(5)
