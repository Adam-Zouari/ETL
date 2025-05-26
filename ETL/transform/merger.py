import json
import os

def load_co2_data():
    """Load CO2 data from JSON file."""
    try:
        # Path to root directory where JSON files are stored
        json_path = os.path.join(os.path.dirname(__file__), '..', '..', 'co2.json')
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"✅ Loaded CO2 data for {len(data)} regions")
        return data
    except Exception as e:
        print(f"❌ Error loading CO2 data: {e}")
        return {}

def merge_data(iqair_aggregated, co2_data):
    """Merge IQAir and CO2 data."""
    merged_data = {}

    # Process each region
    for region_id_str, iqair_region_data in iqair_aggregated.items():
        region_id = int(region_id_str)

        # Check if this region exists in CO2 data
        if str(region_id) in co2_data:
            co2_region_data = co2_data[str(region_id)]

            # Extract CO2 data
            co2_info = None
            if "data" in co2_region_data and len(co2_region_data["data"]) > 0:
                co2_info = co2_region_data["data"][0]

            # Create merged data structure (without shortname and dnoregion)
            merged_data[region_id_str] = {
                "region_id": region_id,
                "region": iqair_region_data["data"]["region"],
                "cities_count": iqair_region_data["data"]["cities_count"],
                "weather": iqair_region_data["data"]["current"]["weather"],
                "pollution": iqair_region_data["data"]["current"]["pollution"]
            }

            # Add CO2 data if available
            if co2_info and "data" in co2_info and len(co2_info["data"]) > 0:
                co2_data_point = co2_info["data"][0]
                if "intensity" in co2_data_point:
                    merged_data[region_id_str]["intensity"] = co2_data_point["intensity"]
                if "generationmix" in co2_data_point:
                    merged_data[region_id_str]["generationmix"] = co2_data_point["generationmix"]
                # Add time elements
                if "from" in co2_data_point:
                    merged_data[region_id_str]["from"] = co2_data_point["from"]
                if "to" in co2_data_point:
                    merged_data[region_id_str]["to"] = co2_data_point["to"]
        else:
            # If no CO2 data, just use IQAir data
            merged_data[region_id_str] = {
                "region_id": region_id,
                "region": iqair_region_data["data"]["region"],
                "cities_count": iqair_region_data["data"]["cities_count"],
                "weather": iqair_region_data["data"]["current"]["weather"],
                "pollution": iqair_region_data["data"]["current"]["pollution"]
            }

    return merged_data

def merge_and_save(aggregated_data):
    """Merge IQAir and CO2 data and save the result."""
    # Load CO2 data
    co2_data = load_co2_data()

    # Merge IQAir and CO2 data
    merged_data = merge_data(aggregated_data, co2_data)

    # Save merged data to root directory
    output_path = os.path.join(os.path.dirname(__file__), '..', '..', 'merged_data.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(merged_data, f, ensure_ascii=False, indent=4)
    print("✅ Saved merged data to merged_data.json")

    return merged_data

if __name__ == "__main__":
    # If run directly, load aggregated data and merge it
    try:
        input_path = os.path.join(os.path.dirname(__file__), '..', '..', 'iqair_mapped.json')
        with open(input_path, 'r', encoding='utf-8') as f:
            aggregated_data = json.load(f)
        print(f"✅ Loaded aggregated data for {len(aggregated_data)} regions")
        merge_and_save(aggregated_data)
    except Exception as e:
        print(f"❌ Error: {e}")
