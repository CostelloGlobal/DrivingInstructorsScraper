import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client
import os
import time

# ✅ Get secrets from GitHub Actions
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SCRAPERAPI_KEY = os.getenv("SCRAPERAPI_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ✅ ScraperAPI wrapper
def fetch_url_with_scraperapi(url):
    api_url = f"http://api.scraperapi.com"
    payload = {
        "api_key": SCRAPERAPI_KEY,
        "url": url,
        "country_code": "gb",   # force UK IP
        "render": "false"
    }
    response = requests.get(api_url, params=payload)
    if response.status_code != 200:
        print(f"❌ Failed to fetch {url}: {response.status_code} {response.text[:200]}")
        return None
    return response.text

# ✅ DVSA base URL
BASE_URL = "https://finddrivinginstructor.dvsa.gov.uk/DSAFindNearestWebApp/findNearest.form"

def scrape_instructors(postcode="E1"):
    """Scrape DVSA instructor data for a given postcode"""
    target_url = f"{BASE_URL}?postcode={postcode}"
    html = fetch_url_with_scraperapi(target_url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    instructors = []

    # ⚠️ Adjust selectors after inspecting DVSA HTML
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
            "website": website
        })

    return instructors

def save_to_supabase(records):
    if not records:
        print("⚠️ No records to insert")
        return
    supabase.table("driving_instructors").insert(records).execute()
    print(f"✅ Inserted {len(records)} instructors into Supabase")

if __name__ == "__main__":
    postcodes = ["E1", "M1", "B1"]  # test run

    for pc in postcodes:
        data = scrape_instructors(pc)
        save_to_supabase(data)
        print(f"⏳ Waiting 5 seconds before next postcode…")
        time.sleep(5)
