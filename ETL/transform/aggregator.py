import json
import os
from collections import defaultdict
import statistics

def aggregate_by_region(mapped_data):
    """Aggregate data by region."""
    # Group cities by region
    regions = defaultdict(list)
    for _, data in mapped_data.items():  # Use _ to indicate unused variable
        if "data" in data and "region_id" in data["data"]:
            region_id = data["data"]["region_id"]
            regions[region_id].append(data)

    # Aggregate data for each region
    aggregated_data = {}
    for region_id, cities_data in regions.items():
        # Get region name from the first city
        region_name = cities_data[0]["data"]["region"]

        # Collect weather and pollution data for aggregation
        weather_data = {
            "hu": [],  # humidity
            "pr": [],  # pressure
            "tp": [],  # temperature
            "wd": [],  # wind direction
            "ws": []   # wind speed
        }

        pollution_data = {
            "aqius": [],  # AQI US
            "aqicn": []   # AQI China
        }

        # Collect data from all cities in this region
        for city_data in cities_data:
            if "data" in city_data and "current" in city_data["data"]:
                current = city_data["data"]["current"]

                # Collect weather data
                if "weather" in current:
                    for key in weather_data:
                        if key in current["weather"]:
                            weather_data[key].append(current["weather"][key])

                # Collect pollution data
                if "pollution" in current:
                    for key in pollution_data:
                        if key in current["pollution"]:
                            pollution_data[key].append(current["pollution"][key])

        # Calculate aggregated values (mean)
        aggregated_weather = {}
        for key, values in weather_data.items():
            if values:
                aggregated_weather[key] = round(statistics.mean(values), 2)

        aggregated_pollution = {}
        for key, values in pollution_data.items():
            if values:
                aggregated_pollution[key] = round(statistics.mean(values), 2)

        # Create aggregated data structure
        aggregated_data[str(region_id)] = {
            "status": "success",
            "data": {
                "region_id": region_id,
                "region": region_name,
                "cities_count": len(cities_data),
                "current": {
                    "weather": aggregated_weather,
                    "pollution": aggregated_pollution
                }
            }
        }

    return aggregated_data

def aggregate_and_save(mapped_data):
    """Aggregate data by region and save the result."""
    # Aggregate data by region
    aggregated_data = aggregate_by_region(mapped_data)

    # Save aggregated data to root directory
    output_path = os.path.join(os.path.dirname(__file__), '..', '..', 'iqair_mapped.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(aggregated_data, f, ensure_ascii=False, indent=4)
    print("✅ Saved aggregated data to iqair_mapped.json")

    return aggregated_data

if __name__ == "__main__":
    # If run directly, load mapped data and aggregate it
    try:
        input_path = os.path.join(os.path.dirname(__file__), '..', '..', 'mapped.json')
        with open(input_path, 'r', encoding='utf-8') as f:
            mapped_data = json.load(f)
        print(f"✅ Loaded mapped data for {len(mapped_data)} cities")
        aggregate_and_save(mapped_data)
    except Exception as e:
        print(f"❌ Error: {e}")
