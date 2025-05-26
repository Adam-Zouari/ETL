import requests
import json
import time
import os
from dotenv import load_dotenv
from .uk_cities import get_uk_cities_excluding_ni

# Load environment variables from .env file
load_dotenv()

# Get API key from environment variable
API_KEY = os.getenv('IQAIR_API_KEY')

if not API_KEY:
    raise ValueError("IQAIR_API_KEY not found in environment variables. Please check your .env file.")

def extract_iqair_data():
    """Extract air quality data for UK cities and save to JSON file."""
    # Get UK cities excluding Northern Ireland
    uk_cities = get_uk_cities_excluding_ni()

    # Dictionary to store air quality data
    air_quality_data = {}

    # Process each city
    for city, (lat, lon) in uk_cities.items():
        url = f'https://api.airvisual.com/v2/nearest_city?lat={lat}&lon={lon}&key={API_KEY}'

        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            air_quality_data[city] = data
            print(f"[SUCCESS] Data fetched for {city}")
        elif response.status_code == 429:
            print(f"[WARNING] Rate limit hit for {city}, waiting 60 seconds before retrying...")
            time.sleep(60)
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                air_quality_data[city] = data
                print(f"[SUCCESS] Data fetched for {city} after wait")
            else:
                print(f"[ERROR] Failed again for {city}: {response.status_code}")
        else:
            print(f"[ERROR] Failed for {city}: {response.status_code}")

        time.sleep(2)  # wait 2 seconds between all requests

    # Save the data to a JSON file
    with open('iqair_uk_cities.json', 'w', encoding='utf-8') as f:
        json.dump(air_quality_data, f, ensure_ascii=False, indent=4)

    print("[SUCCESS] All data saved to iqair_uk_cities.json")
    return air_quality_data

if __name__ == "__main__":
    extract_iqair_data()
