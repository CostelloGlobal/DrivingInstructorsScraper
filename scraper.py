import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client
import time

# ✅ Your Supabase credentials
SUPABASE_URL = "https://owjiihzdfmvktzltjyia.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im93amlpaHpkZm12a3R6bHRqeWlhIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTU1MjgxNzAsImV4cCI6MjA3MTEwNDE3MH0.wtK8kOhiLx-0UbaU_lp9-st8FTZleOMdmzZeZZ_yAMU"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ✅ DVSA search base URL (postcode based)
BASE_URL = "https://finddrivinginstructor.dvsa.gov.uk/DSAFindNearestWebApp/findNearest.form"

def scrape_instructors(postcode="E1"):
    """Scrape DVSA instructor data for a given postcode"""
    params = {"postcode": postcode}
    response = requests.get(BASE_URL, params=params)

    if response.status_code != 200:
        print(f"❌ Failed to fetch {postcode}: {response.status_code}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")

    instructors = []
    # ⚠️ Selectors will need tuning after inspecting DVSA HTML
    for card in soup.select(".instructor-card"):
        name = card.select_one(".name").get_text(strip=True) if card.select_one(".name") else None
        phone = card.select_one(".phone").get_text(strip=True) if card.select_one(".phone") else None
        website = card.select_one("a")["href"] if card.select_one("a") else None

        instructors.append({
            "name": name,
            "postcode": postcode,
            "dvsa_number": None,    # placeholder until we inspect DVSA HTML
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
    # Example small test set of postcodes
    postcodes = ["E1", "M1", "B1"]

    for pc in postcodes:
        data = scrape_instructors(pc)
        save_to_supabase(data)
        print(f"⏳ Waiting 5 seconds before next postcode…")
        time.sleep(5)   # polite delay
