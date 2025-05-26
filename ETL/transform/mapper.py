import json
import csv
import os

# Region ID mapping
REGION_NAME_TO_ID = {
    "North Scotland": 1,
    "South Scotland": 2,
    "North West England": 3,
    "North East England": 4,
    "Yorkshire": 5,
    "North Wales": 6,
    "South Wales": 7,
    "West Midlands": 8,
    "East Midlands": 9,
    "East England": 10,
    "South West England": 11,
    "South England": 12,
    "London": 13,
    "South East England": 14
}

# Define UK regions with their approximate boundaries for coordinate-based mapping
UK_REGIONS = {
    "North Scotland": {"lat_min": 56.5, "lat_max": 60.0, "lon_min": -8.0, "lon_max": -1.0},
    "South Scotland": {"lat_min": 54.8, "lat_max": 56.5, "lon_min": -6.0, "lon_max": -1.5},
    "North West England": {"lat_min": 53.0, "lat_max": 55.0, "lon_min": -4.0, "lon_max": -2.0},
    "North East England": {"lat_min": 53.5, "lat_max": 55.5, "lon_min": -2.0, "lon_max": -0.5},
    "Yorkshire": {"lat_min": 53.0, "lat_max": 54.5, "lon_min": -2.5, "lon_max": -0.5},
    "North Wales": {"lat_min": 52.5, "lat_max": 53.5, "lon_min": -5.0, "lon_max": -2.8},
    "South Wales": {"lat_min": 51.0, "lat_max": 52.5, "lon_min": -5.0, "lon_max": -2.8},
    "West Midlands": {"lat_min": 52.0, "lat_max": 53.0, "lon_min": -3.0, "lon_max": -1.5},
    "East Midlands": {"lat_min": 52.0, "lat_max": 53.5, "lon_min": -1.5, "lon_max": 0.5},
    "East England": {"lat_min": 51.5, "lat_max": 53.0, "lon_min": -0.5, "lon_max": 2.0},
    "South West England": {"lat_min": 49.5, "lat_max": 52.0, "lon_min": -6.5, "lon_max": -2.0},
    "South England": {"lat_min": 50.5, "lat_max": 52.0, "lon_min": -2.0, "lon_max": 0.0},
    "London": {"lat_min": 51.3, "lat_max": 51.7, "lon_min": -0.5, "lon_max": 0.3},
    "South East England": {"lat_min": 50.5, "lat_max": 52.0, "lon_min": 0.0, "lon_max": 1.5}
}

def load_city_to_region_mapping():
    """Load city to region mapping from CSV file."""
    city_to_region = {}
    try:
        # Get the path relative to this file
        csv_path = os.path.join(os.path.dirname(__file__), 'cities_to_regions.csv')
        with open(csv_path, 'r', encoding='utf-8') as f:
            csv_reader = csv.reader(f)
            next(csv_reader)  # Skip header row
            for row in csv_reader:
                if len(row) >= 2:
                    city, region = row[0], row[1]
                    # Skip Northern Ireland cities
                    if region != "Northern Ireland":
                        city_to_region[city] = region
        print(f"✅ Loaded {len(city_to_region)} city-region mappings")
    except Exception as e:
        print(f"❌ Error loading city-region mappings: {e}")

    return city_to_region

def load_iqair_data():
    """Load IQAir data from JSON file."""
    try:
        # Path to root directory where JSON files are stored
        json_path = os.path.join(os.path.dirname(__file__), '..', '..', 'iqair_uk_cities.json')
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"✅ Loaded IQAir data for {len(data)} cities")
        return data
    except Exception as e:
        print(f"❌ Error loading IQAir data: {e}")
        return {}

def get_region_from_coordinates(lat, lon):
    """
    Determine the UK region based on latitude and longitude.

    Args:
        lat (float): Latitude
        lon (float): Longitude

    Returns:
        str: The name of the region or None if no match found
    """
    # Check if coordinates are within any region's boundaries
    for region_name, bounds in UK_REGIONS.items():
        if (bounds["lat_min"] <= lat <= bounds["lat_max"] and
            bounds["lon_min"] <= lon <= bounds["lon_max"]):
            return region_name

    # If no exact match, find the closest region
    min_distance = float('inf')
    closest_region = None

    for region_name, bounds in UK_REGIONS.items():
        # Calculate center of region
        region_lat = (bounds["lat_min"] + bounds["lat_max"]) / 2
        region_lon = (bounds["lon_min"] + bounds["lon_max"]) / 2

        # Calculate distance (simplified)
        distance = ((lat - region_lat) ** 2 + (lon - region_lon) ** 2) ** 0.5

        if distance < min_distance:
            min_distance = distance
            closest_region = region_name

    return closest_region

def map_cities_to_regions(iqair_data, city_to_region):
    """Map cities to their regions and add region information to the data."""
    mapped_data = {}

    for city, data in iqair_data.items():
        region = None

        # Method 1: Try to get region from city_to_region mapping
        if city in city_to_region:
            region = city_to_region[city]

        # Method 2: Try to get region from data if it already has region info
        if not region and "data" in data and "region" in data["data"]:
            region = data["data"]["region"]

        # Method 3: Try to determine region from coordinates
        if not region and "data" in data and "location" in data["data"] and "coordinates" in data["data"]["location"]:
            coordinates = data["data"]["location"]["coordinates"]
            if len(coordinates) >= 2:
                lon, lat = coordinates[0], coordinates[1]  # Note: GeoJSON format is [longitude, latitude]
                region = get_region_from_coordinates(lat, lon)
                if region:
                    print(f"ℹ️ Determined region for {city} based on coordinates: {region}")

        # If still no region, skip this city
        if not region:
            print(f"⚠️ No region found for {city}, skipping")
            continue

        # Get region ID
        region_id = REGION_NAME_TO_ID.get(region)
        if not region_id:
            print(f"⚠️ No region ID found for {region}, skipping {city}")
            continue

        # Add region information to the data
        if "data" in data:
            data["data"]["region"] = region
            data["data"]["region_id"] = region_id

        mapped_data[city] = data

    return mapped_data

def map_and_save():
    """Map cities to regions and save the result."""
    # Load data
    city_to_region = load_city_to_region_mapping()
    iqair_data = load_iqair_data()

    # Map cities to regions
    mapped_data = map_cities_to_regions(iqair_data, city_to_region)

    # Save mapped data to root directory
    output_path = os.path.join(os.path.dirname(__file__), '..', '..', 'mapped.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(mapped_data, f, ensure_ascii=False, indent=4)
    print("✅ Saved mapped data to mapped.json")

    return mapped_data

if __name__ == "__main__":
    map_and_save()
