"""
Loader script for appending processed data to existing files and MongoDB Atlas.

This script loads the final transformed data and saves it to:
1. Local historical JSON files
2. MongoDB Atlas cloud database

This maintains a record of all data over time both locally and in the cloud.
"""

import json
import os
from datetime import datetime
from .mongodb_client import save_to_mongodb, get_mongodb_client

def load_existing_data(filename):
    """Load existing data from a file, return empty list if file doesn't exist."""
    if os.path.exists(filename):
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                data = json.load(f)
            print(f"[SUCCESS] Loaded existing data from {filename}")
            return data if isinstance(data, list) else []
        except Exception as e:
            print(f"[ERROR] Failed to load {filename}: {e}")
            return []
    else:
        print(f"[INFO] {filename} doesn't exist, will create new file")
        return []

def save_data(data, filename):
    """Save data to a file."""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"[SUCCESS] Saved data to {filename}")
        return True
    except Exception as e:
        print(f"[ERROR] Failed to save {filename}: {e}")
        return False

def add_timestamp_to_data(data):
    """Add a timestamp to the data entry."""
    timestamp = datetime.now().isoformat()

    # If data is a dictionary (like merged_data.json), wrap it with timestamp
    if isinstance(data, dict):
        return {
            "timestamp": timestamp,
            "data": data
        }
    # If data is already a list or other format, just add timestamp
    elif isinstance(data, list):
        return {
            "timestamp": timestamp,
            "data": data
        }
    else:
        return {
            "timestamp": timestamp,
            "data": data
        }

def load_and_append_data():
    """Load the transformed data and append it to historical files and MongoDB."""

    # Get MongoDB client
    mongodb_client = get_mongodb_client()
    mongodb_available = mongodb_client.connected

    if mongodb_available:
        print("[INFO] MongoDB Atlas connection available - will save to cloud")
    else:
        print("[WARNING] MongoDB Atlas not available - saving locally only")

    # Files to process
    files_to_load = [
        {
            "source": "mapped.json",
            "target": "historical_mapped.json",
            "description": "mapped city data",
            "mongodb_type": None  # Don't save to MongoDB
        },
        {
            "source": "iqair_mapped.json",
            "target": "historical_iqair_mapped.json",
            "description": "aggregated IQAir data",
            "mongodb_type": None  # Don't save to MongoDB
        },
        {
            "source": "merged_data.json",
            "target": "historical_merged_data.json",
            "description": "final merged data",
            "mongodb_type": "merged"  # Only save final result to MongoDB
        }
    ]

    success_count = 0
    mongodb_success_count = 0

    for file_info in files_to_load:
        source_file = file_info["source"]
        target_file = file_info["target"]
        description = file_info["description"]
        mongodb_type = file_info["mongodb_type"]

        print(f"\nProcessing {description}...")

        # Check if source file exists
        if not os.path.exists(source_file):
            print(f"[WARNING] Source file {source_file} not found, skipping")
            continue

        try:
            # Load new data
            with open(source_file, 'r', encoding='utf-8') as f:
                new_data = json.load(f)

            # Save to MongoDB Atlas only for final merged data
            if mongodb_available and mongodb_type is not None:
                if save_to_mongodb(new_data, mongodb_type):
                    mongodb_success_count += 1
                    print(f"[SUCCESS] Saved {description} to MongoDB Atlas")
                else:
                    print(f"[ERROR] Failed to save {description} to MongoDB Atlas")
            elif mongodb_type is None:
                print(f"[INFO] Skipping MongoDB save for {description} (intermediate data)")

            # Load existing historical data
            historical_data = load_existing_data(target_file)

            # Add timestamp to new data
            timestamped_data = add_timestamp_to_data(new_data)

            # Append to historical data
            historical_data.append(timestamped_data)

            # Save updated historical data locally
            if save_data(historical_data, target_file):
                success_count += 1
                print(f"[SUCCESS] Appended {description} to local {target_file}")
            else:
                print(f"[ERROR] Failed to append {description} to local file")

        except Exception as e:
            print(f"[ERROR] Error processing {source_file}: {e}")

    # Count files that should be saved to MongoDB
    mongodb_files_count = sum(1 for file_info in files_to_load if file_info["mongodb_type"] is not None)

    print(f"\n[SUMMARY] Local files: {success_count}/{len(files_to_load)} successful")
    if mongodb_available:
        print(f"[SUMMARY] MongoDB Atlas: {mongodb_success_count}/{mongodb_files_count} successful (final result only)")

    return success_count == len(files_to_load)

def cleanup_old_entries(filename, max_entries=100):
    """Keep only the most recent entries in historical files to prevent them from growing too large."""
    if not os.path.exists(filename):
        return

    try:
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)

        if isinstance(data, list) and len(data) > max_entries:
            # Keep only the most recent entries
            data = data[-max_entries:]

            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)

            print(f"[INFO] Cleaned up {filename}, kept {max_entries} most recent entries")

    except Exception as e:
        print(f"[ERROR] Failed to cleanup {filename}: {e}")

def main():
    """Main loader function."""
    print("=" * 50)
    print("Starting Data Loading Process")
    print("=" * 50)

    # Load and append new data
    success = load_and_append_data()

    if success:
        print("\n[SUCCESS] All data loaded successfully")

        # Show MongoDB statistics if available
        mongodb_client = get_mongodb_client()
        if mongodb_client.connected:
            print("\n[INFO] MongoDB Atlas Statistics:")
            stats = mongodb_client.get_statistics()
            for key, value in stats.items():
                print(f"  {key}: {value}")

        # Cleanup old entries to prevent files from growing too large
        print("\nCleaning up old local entries...")
        cleanup_old_entries("historical_mapped.json", max_entries=50)
        cleanup_old_entries("historical_iqair_mapped.json", max_entries=50)
        cleanup_old_entries("historical_merged_data.json", max_entries=50)

        print("\n[COMPLETE] Loading process finished")
        return True
    else:
        print("\n[ERROR] Some data loading failed")
        return False

if __name__ == "__main__":
    main()
