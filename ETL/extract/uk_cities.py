# Function to filter out Northern Ireland cities
def is_northern_ireland_city(city):
    """
    Check if a city is in Northern Ireland.

    Args:
        city (str): The name of the city to check

    Returns:
        bool: True if the city is in Northern Ireland, False otherwise
    """
    northern_ireland_cities = [
        "Belfast", "Londonderry", "Armagh", "Ballyclare", "Holywood",
        "Dundrum", "Londonderry County Borough"
    ]
    return city in northern_ireland_cities

# Cities in the UK with their coordinates
cities_with_coords = {
    
    "Worthing": (50.8200, -0.3700),
    "Wretham": (52.5000, 0.8333),
    "Yalding": (51.2167, 0.4333),
    "Yeadon": (53.8667, -1.6833),
    "York": (53.9599, -1.0873)
}

# Function to get UK cities excluding Northern Ireland
def get_uk_cities_excluding_ni():
    uk_cities = {}
    for city, coords in cities_with_coords.items():
        if not is_northern_ireland_city(city):
            uk_cities[city] = coords
    return uk_cities
