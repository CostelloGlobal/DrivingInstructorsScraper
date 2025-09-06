import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client
import os
import time

# ✅ Supabase credentials from GitHub Secrets
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ✅ ScraperAPI credentials
SCRAPERAPI_KEY = os.getenv("SCRAPERAPI_KEY")
SCRAPERAPI_URL = "http://api.scraperapi.com"

# ✅ DVSA base URL
TARGET_URL = "https://finddrivinginstructor.dvsa.gov.uk/DSAFindNearestWebApp/findNearest.form"

def scrape_instructors(postcode="E1"):
    """Scrape DVSA instructor data for a given postcode through ScraperAPI"""
    params = {
        "api_key": SCRAPERAPI_KEY,
        "url": TARGET_URL,
        "render": "false",
        "keep_headers": "true",
    }

    # These are the actual search params that DVSA expects
    search_params = {"postcode": postcode}

    try:
        response = requests.get(SCRAPERAPI_URL, params={**params, **search_params}, timeout=30)
    except Exception as e:
        print(f"❌ Request failed for {postcode}: {e}")
        return []

    if response.status_code != 200:
        print(f"❌ Failed to fetch {postcode}: {response.status_code}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")

    instructors = []
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
    postcodes = ["E1", "M1", "B1"]

    for pc in postcodes:
        data = scrape_instructors(pc)
        save_to_supabase(data)
        print(f"⏳ Waiting 5 seconds before next postcode…")
        time.sleep(5)
