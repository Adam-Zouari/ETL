# UK Air Quality Data Pipeline

An automated ETL (Extract, Transform, Load) pipeline that continuously collects, processes, and stores air quality and CO2 data for cities across the United Kingdom (excluding Northern Ireland).

## 🌟 Features

- **Automated Data Collection**: Extracts air quality data from IQAir API and CO2 data every 30 minutes
- **Comprehensive Coverage**: Monitors 185+ cities across England, Scotland, and Wales
- **Robust Pipeline**: Designed to run continuously with automatic error recovery
- **Dual Storage**: Saves data both locally (JSON files) and to MongoDB Atlas cloud database
- **Real-time Monitoring**: Provides detailed logging and statistics tracking
- **Geographic Organization**: Data organized by regions (London, Yorkshire, Scotland, etc.)

## 🏗️ Architecture

The pipeline follows a modular ETL architecture:

```
ETL/
├── extract/          # Data extraction modules
│   ├── extract.py    # Main extraction orchestrator
│   ├── iqair.py      # IQAir API integration
│   ├── co2.py        # CO2 data collection
│   └── uk_cities.py  # UK cities configuration
├── transform/        # Data transformation modules
│   ├── transform.py  # Main transformation orchestrator
│   ├── mapper.py     # City-to-region mapping
│   ├── aggregator.py # Data aggregation by region
│   └── merger.py     # Data merging and consolidation
└── load/            # Data loading modules
    ├── loader.py     # Main loading orchestrator
    └── mongodb_client.py # MongoDB Atlas integration
```

## 📊 Data Sources

- **IQAir API**: Real-time air quality measurements (PM2.5, PM10, AQI)
- **CO2 Data**: Carbon dioxide concentration levels
- **Geographic Data**: City-to-region mappings from `cities_to_regions.csv`

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- MongoDB Atlas account (optional, for cloud storage)
- IQAir API key (for air quality data)

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd DE_DA
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure API keys and MongoDB connection (see Configuration section)

4. Run the pipeline:
```bash
python pipeline.py
```

## ⚙️ Configuration

### Environment Variables

Create a `.env` file or set environment variables:

```bash
# IQAir API Configuration
IQAIR_API_KEY=your_iqair_api_key_here

# MongoDB Atlas Configuration (optional)
MONGODB_CONNECTION_STRING=mongodb+srv://username:password@cluster.mongodb.net/
MONGODB_DATABASE=Climate_Change
MONGODB_COLLECTION=Climate
```

### Pipeline Settings

Key configuration parameters in `pipeline.py`:

- `PIPELINE_INTERVAL_MINUTES = 30`: Data collection frequency
- `MAX_CONSECUTIVE_FAILURES = 5`: Maximum failures before extended backoff
- `FAILURE_BACKOFF_SECONDS = 300`: Delay after consecutive failures

## 📁 Data Files

### Input Files
- `cities_to_regions.csv`: Maps 185+ UK cities to their regions
- `regions_to_countries.csv`: Maps regions to countries

### Output Files
- `iqair_uk_cities.json`: Latest IQAir data
- `co2.json`: Latest CO2 data
- `mapped.json`: City data mapped to regions
- `iqair_mapped.json`: Aggregated IQAir data by region
- `merged_data.json`: Final consolidated dataset
- `historical_*.json`: Time-series historical data

## 🔄 Pipeline Process

### 1. Extract Phase
- Fetches air quality data for all UK cities (except Northern Ireland)
- Collects CO2 concentration data
- Handles API rate limits and errors gracefully

### 2. Transform Phase
- Maps city data to geographic regions
- Aggregates measurements by region
- Merges air quality and CO2 data
- Preserves temporal information (timestamps)

### 3. Load Phase
- Appends new data to historical JSON files
- Uploads final results to MongoDB Atlas
- Maintains data retention policies (keeps last 50 entries locally)
- Provides comprehensive logging

## 📈 Monitoring & Statistics

The pipeline provides real-time monitoring:

- **Execution Statistics**: Success/failure rates, uptime tracking
- **Error Handling**: Automatic recovery from failures
- **Progress Tracking**: Countdown to next execution
- **MongoDB Statistics**: Cloud storage metrics

Example output:
```
[2024-01-15 14:30:00] [INFO] PIPELINE EXECUTION STARTED
[2024-01-15 14:30:15] [SUCCESS] Data extraction completed
[2024-01-15 14:30:30] [SUCCESS] Data transformation completed
[2024-01-15 14:30:45] [SUCCESS] Data loading completed
[2024-01-15 14:30:45] [SUCCESS] PIPELINE EXECUTION COMPLETED SUCCESSFULLY
[2024-01-15 14:30:45] [INFO] Next pipeline run in: 00:29:15
```

## 🛡️ Error Handling

The pipeline is designed for maximum reliability:

- **Graceful Degradation**: Continues operation even if individual components fail
- **Automatic Recovery**: Retries failed operations with exponential backoff
- **Comprehensive Logging**: Detailed error messages and stack traces
- **Never-Stop Design**: Pipeline continues running despite errors or interruptions

## 🌍 Geographic Coverage

The pipeline covers major UK regions:

- **England**: London, Yorkshire, North West, South West, East Midlands, etc.
- **Scotland**: North Scotland, South Scotland
- **Wales**: North Wales, South Wales
- **Excluded**: Northern Ireland (as per project requirements)

## 📋 Dependencies

Core dependencies (see `requirements.txt`):
- `pymongo>=4.0.0`: MongoDB Atlas integration
- `requests>=2.25.0`: HTTP API calls
- `python-dotenv>=0.19.0`: Environment variable management


## 🆘 Support

For issues or questions:
1. Check the logs for detailed error messages
2. Verify API keys and MongoDB connection
3. Ensure all dependencies are installed
4. Review the pipeline statistics for patterns

## 🔮 Future Enhancements

- Additional data sources integration
- Real-time alerting for air quality thresholds
- Web dashboard for data visualization
- Machine learning predictions
- Extended geographic coverage
