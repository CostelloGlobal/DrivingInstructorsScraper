import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client
import time
import os

# ✅ Supabase credentials (from GitHub secrets)
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://owjiihzdfmvktzltjyia.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "your-anon-key-here")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ✅ DVSA base URL
BASE_URL = "https://finddrivinginstructor.dvsa.gov.uk/DSAFindNearestWebApp/findNearest.form"

# ✅ Fake browser headers (important to bypass 403)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-GB,en;q=0.9",
    "Referer": "https://finddrivinginstructor.dvsa.gov.uk/",
    "Connection": "keep-alive"
}

def scrape_instructors(postcode="E1"):
    """Scrape DVSA instructor data for a given postcode"""
    params = {"postcode": postcode}
    response = requests.get(BASE_URL, params=params, headers=HEADERS)

    if response.status_code != 200:
        print(f"❌ Failed to fetch {postcode}: {response.status_code}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")

    instructors = []
    # ⚠️ Selectors may need adjusting after checking actual DVSA HTML
    for card in soup.select(".instructor-card"):
        name = card.select_one(".name").get_text(strip=True) if card.select_one(".name") else None
        phone = card.select_one(".phone").get_text(strip=True) if card.select_one(".phone") else None
        website = card.select_one("a")["href"] if card.select_one("a") else None

        instructors.append({
            "name": name,
            "postcode": postcode,
            "dvsa_number": None,    # placeholder until inspected
            "transmission": None,   # placeholder
            "phone": phone,
            "email": None,          # placeholder
            "website": website
        })
    return instructors

def save_to_supabase(records):
    if not records:
        return
    supabase.table("driving_instructors").insert(records).execute()
    print(f"✅ Inserted {len(records)} instructors into Supabase")

if __name__ == "__main__":
    # Test a few postcodes
    postcodes = ["E1", "M1", "B1"]

    for pc in postcodes:
        data = scrape_instructors(pc)
        save_to_supabase(data)
        print(f"⏳ Waiting 5 seconds before next postcode…")
        time.sleep(5)
