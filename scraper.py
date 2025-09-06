import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client
import time
import os

# ✅ Supabase credentials (loaded from GitHub secrets / env vars)
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://owjiihzdfmvktzltjyia.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "your-anon-key-here")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ✅ DVSA base URL
BASE_URL = "https://finddrivinginstructor.dvsa.gov.uk/DSAFindNearestWebApp/findNearest.form"

# ✅ Add browser headers so DVSA doesn’t block us
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/115.0 Safari/537.36"
    ),
    "Referer": "https://finddrivinginstructor.dvsa.gov.uk/",
    "Accept-Language": "en-GB,en;q=0.9",
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
    # ⚠️ Adjust selectors after inspecting DVSA HTML structure
    for card in soup.select(".instructor-card"):
        name = card.select_one(".name").get_text(strip=True) if card.select_one(".name") else None
        phone = card.select_one(".phone").get_text(strip=True) if card.select_one(".phone") else None
        website = card.select_one("a")["href"] if card.select_one("a") else None

        instructors.append(
            {
                "name": name,
                "postcode": postcode,
                "dvsa_number": None,  # placeholder
                "transmission": None,  # placeholder
                "phone": phone,
                "email": None,  # placeholder
                "website": website,
            }
        )
    return instructors


def save_to_supabase(records):
    if not records:
        print("⚠️ No records to insert")
        return
    supabase.table("driving_instructors").insert(records).execute()
    print(f"✅ Inserted {len(records)} instructors into Supabase")


if __name__ == "__main__":
    # Example small test set of postcodes
    postcodes = ["E1", "M1", "B1"]

    for pc in postcodes:
        data = scrape_instructors(pc)
        save_to_supabase(data)
        print(f"⏳ Waiting 5 seconds before next postcode…")
        time.sleep(5)  # polite delay
