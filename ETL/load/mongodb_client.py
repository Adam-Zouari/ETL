"""
MongoDB Atlas client for saving pipeline results to cloud database.

This module handles connections to MongoDB Atlas and provides functions
to save the final pipeline results to the cloud database.
"""

import os
import json
from datetime import datetime
from typing import Dict, Any, Optional

# Try to load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed, use system environment variables

try:
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
    PYMONGO_AVAILABLE = True
except ImportError:
    PYMONGO_AVAILABLE = False
    print("[WARNING] pymongo not installed. Install with: pip install pymongo")

class MongoDBClient:
    """MongoDB Atlas client for pipeline data storage."""

    def __init__(self, connection_string: Optional[str] = None, database_name: str = "Climate_Change"):
        """
        Initialize MongoDB client.

        Args:
            connection_string: MongoDB Atlas connection string
            database_name: Name of the database to use
        """
        self.connection_string = connection_string or os.getenv('MONGODB_CONNECTION_STRING')
        self.database_name = database_name
        self.client = None
        self.db = None
        self.connected = False

        if not PYMONGO_AVAILABLE:
            print("[ERROR] pymongo is required for MongoDB integration")
            return

        if not self.connection_string:
            print("[WARNING] No MongoDB connection string provided")
            print("Set MONGODB_CONNECTION_STRING environment variable or pass connection_string parameter")
            return

        self.connect()

    def connect(self):
        """Establish connection to MongoDB Atlas."""
        if not PYMONGO_AVAILABLE or not self.connection_string:
            return False

        try:
            # Create client with timeout settings
            self.client = MongoClient(
                self.connection_string,
                serverSelectionTimeoutMS=5000,  # 5 second timeout
                connectTimeoutMS=5000,
                socketTimeoutMS=5000
            )

            # Test the connection
            self.client.admin.command('ping')
            self.db = self.client[self.database_name]
            self.connected = True

            print(f"[SUCCESS] Connected to MongoDB Atlas database: {self.database_name}")
            return True

        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            print(f"[ERROR] Failed to connect to MongoDB Atlas: {e}")
            self.connected = False
            return False
        except Exception as e:
            print(f"[ERROR] Unexpected error connecting to MongoDB: {e}")
            self.connected = False
            return False

    def save_pipeline_result(self, data: Dict[str, Any], collection_name: str = "Climate"):
        """
        Save pipeline result to MongoDB Atlas.

        Args:
            data: The data to save
            collection_name: Name of the collection to save to

        Returns:
            bool: True if successful, False otherwise
        """
        if not self.connected:
            print("[WARNING] Not connected to MongoDB, skipping cloud save")
            return False

        try:
            collection = self.db[collection_name]

            # Add metadata
            document = {
                "timestamp": datetime.now(),
                "pipeline_run_id": datetime.now().strftime("%Y%m%d_%H%M%S"),
                "data_type": collection_name,
                "data": data
            }

            # Insert the document
            result = collection.insert_one(document)

            print(f"[SUCCESS] Saved data to MongoDB collection '{collection_name}' with ID: {result.inserted_id}")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to save to MongoDB: {e}")
            return False

    def save_historical_data(self, data: Dict[str, Any], data_type: str):
        """
        Save data to the Climate collection with data type metadata.

        Args:
            data: The data to save
            data_type: Type of data (mapped, aggregated, merged)
        """
        if not self.connected:
            print("[WARNING] Not connected to MongoDB, skipping cloud save")
            return False

        try:
            collection = self.db["Climate"]

            # Add metadata including data type
            document = {
                "timestamp": datetime.now(),
                "pipeline_run_id": datetime.now().strftime("%Y%m%d_%H%M%S"),
                "data_type": data_type,
                "data": data
            }

            # Insert the document
            result = collection.insert_one(document)

            print(f"[SUCCESS] Saved {data_type} data to MongoDB collection 'Climate' with ID: {result.inserted_id}")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to save {data_type} data to MongoDB: {e}")
            return False

    def get_latest_results(self, data_type: str = None, limit: int = 10):
        """
        Get the latest pipeline results from MongoDB Climate collection.

        Args:
            data_type: Type of data to filter by (mapped, aggregated, merged). If None, returns all types.
            limit: Number of results to return

        Returns:
            List of documents or empty list if error
        """
        if not self.connected:
            print("[WARNING] Not connected to MongoDB")
            return []

        try:
            collection = self.db["Climate"]

            # Build query filter
            query = {}
            if data_type:
                query["data_type"] = data_type

            results = list(collection.find(query).sort("timestamp", -1).limit(limit))

            # Convert ObjectId to string for JSON serialization
            for result in results:
                result["_id"] = str(result["_id"])

            return results

        except Exception as e:
            print(f"[ERROR] Failed to query MongoDB: {e}")
            return []

    def get_statistics(self):
        """Get pipeline statistics from MongoDB Climate collection."""
        if not self.connected:
            return {}

        try:
            stats = {}
            collection = self.db["Climate"]

            # Get total count
            total_count = collection.count_documents({})
            stats["total_documents"] = total_count

            # Get counts by data type
            for data_type in ["mapped", "aggregated", "merged"]:
                count = collection.count_documents({"data_type": data_type})
                stats[f"{data_type}_count"] = count

                # Get latest timestamp for this data type
                latest = collection.find_one(
                    {"data_type": data_type},
                    sort=[("timestamp", -1)]
                )
                if latest:
                    stats[f"{data_type}_latest"] = latest["timestamp"]

            return stats

        except Exception as e:
            print(f"[ERROR] Failed to get MongoDB statistics: {e}")
            return {}

    def close(self):
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            self.connected = False
            print("[INFO] MongoDB connection closed")

# Global MongoDB client instance
mongodb_client = None

def get_mongodb_client():
    """Get or create MongoDB client instance."""
    global mongodb_client
    if mongodb_client is None:
        mongodb_client = MongoDBClient()
    return mongodb_client

def save_to_mongodb(data: Dict[str, Any], data_type: str):
    """
    Convenience function to save data to MongoDB.

    Args:
        data: Data to save
        data_type: Type of data (mapped, aggregated, merged)
    """
    client = get_mongodb_client()
    if client.connected:
        return client.save_historical_data(data, data_type)
    return False
