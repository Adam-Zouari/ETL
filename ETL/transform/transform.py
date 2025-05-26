"""
Transform script for processing air quality and CO2 data.

This script orchestrates the data processing pipeline:
1. Maps cities to regions
2. Aggregates data by region
3. Merges IQAir and CO2 data
"""

def main():
    """Main function to process data."""
    # Step 1: Map cities to regions
    from .mapper import map_and_save
    mapped_data = map_and_save()

    # Step 2: Aggregate data by region
    from .aggregator import aggregate_and_save
    aggregated_data = aggregate_and_save(mapped_data)

    # Step 3: Merge with CO2 data
    from .merger import merge_and_save
    merge_and_save(aggregated_data)

    print("✅ All processing completed successfully")

if __name__ == "__main__":
    main()
