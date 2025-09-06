import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client
import os
import time

# ✅ Supabase credentials from GitHub Secrets
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ✅ ScraperAPI key (already added to GitHub Secrets)
SCRAPER_API_KEY = os.getenv("SCRAPER_API_KEY")

BASE_URL = "https://finddrivinginstructor.dvsa.gov.uk/DSAFindNearestWebApp/findNearest.form"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-GB,en;q=0.9",
}

def scrape_instructors(postcode="E1"):
    """Scrape DVSA instructor data for a given postcode via ScraperAPI"""
    scraper_url = f"http://api.scraperapi.com"
    params = {
        "api_key": SCRAPER_API_KEY,
        "url": BASE_URL,
        "render": "false",
        "keep_headers": "true",
        "country_code": "gb",
        "postcode": postcode
    }

    try:
        response = requests.get(scraper_url, headers=HEADERS, params=params, timeout=60)
        response.raise_for_status()
    except Exception as e:
        print(f"❌ Failed to fetch {postcode}: {e}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")

    instructors = []
    for card in soup.select(".instructor-card"):  # ⚠️ Adjust selectors once we inspect actual HTML
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
            "website": website
        })
    return instructors

def save_to_supabase(records):
    if records:
        supabase.table("driving_instructors").insert(records).execute()
        print(f"✅ Inserted {len(records)} instructors into Supabase")
    else:
        print("⚠️ No records to insert")

if __name__ == "__main__":
    postcodes = ["E1", "M1", "B1"]
    for pc in postcodes:
        data = scrape_instructors(pc)
        save_to_supabase(data)
        print(f"⏳ Waiting 5 seconds before next postcode…")
        time.sleep(5)
