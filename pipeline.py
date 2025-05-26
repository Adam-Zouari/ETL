"""
Pipeline script for automated ETL process.

This script orchestrates the complete data pipeline:
1. Extract: Runs extract.py every 30 minutes to collect fresh data
2. Transform: Processes the extracted data using transform.py
3. Load: Appends the results to historical files using loader.py

The pipeline runs continuously and is designed to NEVER STOP.
It includes robust error handling and automatic recovery mechanisms.
"""

import time
import sys
import traceback
from datetime import datetime, timedelta

# Global flag - pipeline should always run
running = True

# Configuration
PIPELINE_INTERVAL_MINUTES = 30
MAX_CONSECUTIVE_FAILURES = 5
FAILURE_BACKOFF_SECONDS = 300  # 5 minutes

# Statistics
stats = {
    "total_runs": 0,
    "successful_runs": 0,
    "failed_runs": 0,
    "consecutive_failures": 0,
    "start_time": None,
    "last_successful_run": None
}

def log_with_timestamp(message, level="INFO"):
    """Log message with timestamp."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] [{level}] {message}")

def run_extract():
    """Run the data extraction process with robust error handling."""
    log_with_timestamp("Starting Data Extraction")

    try:
        from ETL.extract.extract import main as extract_main
        extract_main()
        log_with_timestamp("Data extraction completed", "SUCCESS")
        return True
    except ImportError as e:
        log_with_timestamp(f"Import error in extraction: {e}", "ERROR")
        return False
    except Exception as e:
        log_with_timestamp(f"Data extraction failed: {e}", "ERROR")
        traceback.print_exc()
        return False

def run_transform():
    """Run the data transformation process with robust error handling."""
    log_with_timestamp("Starting Data Transformation")

    try:
        from ETL.transform.transform import main as transform_main
        transform_main()
        log_with_timestamp("Data transformation completed", "SUCCESS")
        return True
    except ImportError as e:
        log_with_timestamp(f"Import error in transformation: {e}", "ERROR")
        return False
    except Exception as e:
        log_with_timestamp(f"Data transformation failed: {e}", "ERROR")
        traceback.print_exc()
        return False

def run_load():
    """Run the data loading process with robust error handling."""
    log_with_timestamp("Starting Data Loading")

    try:
        from ETL.load.loader import main as loader_main
        loader_main()
        log_with_timestamp("Data loading completed", "SUCCESS")
        return True
    except ImportError as e:
        log_with_timestamp(f"Import error in loading: {e}", "ERROR")
        return False
    except Exception as e:
        log_with_timestamp(f"Data loading failed: {e}", "ERROR")
        traceback.print_exc()
        return False

def update_stats(success):
    """Update pipeline statistics."""
    stats["total_runs"] += 1

    if success:
        stats["successful_runs"] += 1
        stats["consecutive_failures"] = 0
        stats["last_successful_run"] = datetime.now()
    else:
        stats["failed_runs"] += 1
        stats["consecutive_failures"] += 1

def print_stats():
    """Print pipeline statistics."""
    uptime = datetime.now() - stats["start_time"] if stats["start_time"] else timedelta(0)
    success_rate = (stats["successful_runs"] / stats["total_runs"] * 100) if stats["total_runs"] > 0 else 0

    log_with_timestamp("=== PIPELINE STATISTICS ===")
    log_with_timestamp(f"Uptime: {uptime}")
    log_with_timestamp(f"Total runs: {stats['total_runs']}")
    log_with_timestamp(f"Successful runs: {stats['successful_runs']}")
    log_with_timestamp(f"Failed runs: {stats['failed_runs']}")
    log_with_timestamp(f"Success rate: {success_rate:.1f}%")
    log_with_timestamp(f"Consecutive failures: {stats['consecutive_failures']}")
    if stats["last_successful_run"]:
        log_with_timestamp(f"Last successful run: {stats['last_successful_run'].strftime('%Y-%m-%d %H:%M:%S')}")

def run_pipeline():
    """Run the complete ETL pipeline with comprehensive error handling."""
    pipeline_start = datetime.now()
    log_with_timestamp("=" * 60)
    log_with_timestamp("PIPELINE EXECUTION STARTED")
    log_with_timestamp("=" * 60)

    success = True

    # Step 1: Extract
    extract_success = run_extract()
    if not extract_success:
        log_with_timestamp("Pipeline failed at extraction stage", "ERROR")
        success = False

    # Step 2: Transform (only if extract succeeded)
    if extract_success:
        transform_success = run_transform()
        if not transform_success:
            log_with_timestamp("Pipeline failed at transformation stage", "ERROR")
            success = False
    else:
        transform_success = False

    # Step 3: Load (only if transform succeeded)
    if transform_success:
        load_success = run_load()
        if not load_success:
            log_with_timestamp("Pipeline failed at loading stage", "ERROR")
            success = False
    else:
        load_success = False

    # Update statistics
    update_stats(success)

    # Pipeline completed
    pipeline_end = datetime.now()
    duration = pipeline_end - pipeline_start

    log_with_timestamp("=" * 60)
    if success:
        log_with_timestamp("PIPELINE EXECUTION COMPLETED SUCCESSFULLY", "SUCCESS")
    else:
        log_with_timestamp("PIPELINE EXECUTION COMPLETED WITH ERRORS", "ERROR")
    log_with_timestamp(f"Duration: {duration.total_seconds():.2f} seconds")
    log_with_timestamp("=" * 60)

    return success

def run_pipeline_safe():
    """Run pipeline with comprehensive error handling - NEVER let the pipeline stop."""
    try:
        success = run_pipeline()

        # If we have too many consecutive failures, add extra delay
        if stats["consecutive_failures"] >= MAX_CONSECUTIVE_FAILURES:
            log_with_timestamp(f"Too many consecutive failures ({stats['consecutive_failures']}), adding {FAILURE_BACKOFF_SECONDS}s delay", "WARNING")
            time.sleep(FAILURE_BACKOFF_SECONDS)

        return success

    except KeyboardInterrupt:
        log_with_timestamp("Keyboard interrupt received - but pipeline will continue", "WARNING")
        return False
    except SystemExit:
        log_with_timestamp("System exit received - but pipeline will continue", "WARNING")
        return False
    except Exception as e:
        log_with_timestamp(f"Pipeline execution failed with unexpected exception: {e}", "ERROR")
        log_with_timestamp("Full traceback:")
        traceback.print_exc()
        log_with_timestamp("Pipeline will continue and retry at next scheduled time", "INFO")
        return False

def main():
    """Main pipeline orchestrator - NEVER STOPS."""
    global running

    # Initialize statistics
    stats["start_time"] = datetime.now()

    print("*" * 80)
    print("UNSTOPPABLE DATA PIPELINE STARTED")
    print("*" * 80)
    print("Schedule: Every 30 minutes")
    print("This pipeline is designed to NEVER STOP")
    print("It will automatically recover from any errors")
    print("*" * 80)

    # Run the pipeline immediately on startup
    log_with_timestamp("Running initial pipeline execution...")
    run_pipeline_safe()

    # Calculate next run time
    next_run_time = datetime.now() + timedelta(minutes=PIPELINE_INTERVAL_MINUTES)

    # Main scheduler loop - INFINITE LOOP
    while True:
        try:
            current_time = datetime.now()

            # Check if it's time to run the pipeline
            if current_time >= next_run_time:
                log_with_timestamp(f"Scheduled pipeline execution")
                run_pipeline_safe()

                # Print statistics every 10 runs
                if stats["total_runs"] % 10 == 0:
                    print_stats()

                # Schedule next run
                next_run_time = current_time + timedelta(minutes=PIPELINE_INTERVAL_MINUTES)

            # Show countdown to next run (every 60 seconds to reduce spam)
            if int(current_time.timestamp()) % 60 == 0:
                time_until_next = next_run_time - current_time
                if time_until_next.total_seconds() > 0:
                    hours, remainder = divmod(int(time_until_next.total_seconds()), 3600)
                    minutes, seconds = divmod(remainder, 60)
                    log_with_timestamp(f"Next pipeline run in: {hours:02d}:{minutes:02d}:{seconds:02d}")

            # Sleep for 1 second before checking again
            time.sleep(1)

        except KeyboardInterrupt:
            log_with_timestamp("Keyboard interrupt received - but pipeline will continue", "WARNING")
            time.sleep(5)  # Brief pause then continue
        except SystemExit:
            log_with_timestamp("System exit received - but pipeline will continue", "WARNING")
            time.sleep(5)  # Brief pause then continue
        except Exception as e:
            log_with_timestamp(f"Scheduler error: {e} - but pipeline will continue", "ERROR")
            traceback.print_exc()
            time.sleep(10)  # Longer pause for unexpected errors

            # Reset next run time if it got corrupted
            next_run_time = datetime.now() + timedelta(minutes=PIPELINE_INTERVAL_MINUTES)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Even if main() fails, try to restart it
        print(f"CRITICAL ERROR: Main function failed: {e}")
        print("Attempting to restart pipeline in 30 seconds...")
        time.sleep(30)
        main()  # Restart the pipeline
