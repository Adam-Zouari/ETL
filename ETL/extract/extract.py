"""
Extract script for collecting air quality and CO2 data.

This script orchestrates the data extraction pipeline:
1. Extracts air quality data using iqair.py
2. Extracts CO2 data using co2.py
"""

def main():
    """Main function to extract all data."""
    # Step 1: Extract IQAir data
    from .iqair import extract_iqair_data
    extract_iqair_data()

    # Step 2: Extract CO2 data
    from .co2 import extract_co2_data
    extract_co2_data()

    print("[SUCCESS] All data extraction completed")

if __name__ == "__main__":
    main()
