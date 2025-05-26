import requests
import json

def extract_co2_data():
    """Extract CO2 data for UK regions and save to JSON file."""
    headers = {
        'Accept': 'application/json'
    }

    # Region IDs as given
    region_ids = list(range(1, 18))

    all_regions_data = {}

    for region_id in region_ids:
        url = f'https://api.carbonintensity.org.uk/regional/regionid/{region_id}'
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            all_regions_data[region_id] = data
            print(f"[SUCCESS] Data fetched for region ID {region_id}")
        else:
            print(f"[ERROR] Failed to fetch data for region ID {region_id}: Status code {response.status_code}")

    # Save combined data to a JSON file
    with open('co2.json', 'w', encoding='utf-8') as f:
        json.dump(all_regions_data, f, ensure_ascii=False, indent=4)

    print("[SUCCESS] All region data saved to co2.json")
    return all_regions_data

if __name__ == "__main__":
    extract_co2_data()
