"""
Databricks Job: Autoloader Blob Storage Ingestion

Reads data from Azure Blob Storage using Spark Autoloader with schema evolution.

Usage in Databricks:
  - Deployed via DAB
  - Triggered by workflow scheduler or GitHub Actions
"""

import sys
from pyspark.sql import SparkSession

# In Databricks, try to import the module
# The bundle deployment puts files in the workspace, and Databricks
# adds them to the Python path automatically
try:
    # First attempt: Direct import (works if package is installed)
    from claude_pyspark_proj.autoloader import create_ingester
except ImportError as e:
    # Fallback: Load the module directly from the workspace
    print(f"[INFO] Direct import failed: {e}")
    print(f"[INFO] Attempting to load from workspace...")

    # In DAB deployment, files are at:
    # /Workspace/Users/.../files/src/jobs/autoloader_job.py
    # We need to add the parent 'src' directory to sys.path:
    # /Workspace/Users/.../files/src
    workspace_paths = [
        "/Workspace/Users/ashutosh_ranghar@epam.com/.bundle/claude-pyspark-proj/dev/files/src",
        "/Workspace/Users/ashutosh_ranghar@epam.com/claude-pyspark-proj/src",
    ]

    for workspace_path in workspace_paths:
        if workspace_path not in sys.path:
            sys.path.insert(0, workspace_path)
            print(f"[INFO] Added to sys.path: {workspace_path}")

    try:
        from claude_pyspark_proj.autoloader import create_ingester
        print("[INFO] Successfully imported from workspace path")
    except ImportError as e2:
        print(f"[ERROR] Failed to import autoloader module: {e2}")
        print(f"[INFO] sys.path: {sys.path}")
        raise


def main():
    """Main entry point for the Databricks Autoloader job."""
    spark = SparkSession.builder \
        .appName("AutoloaderBlobIngestion") \
        .getOrCreate()

    try:
        print("[*] Starting Autoloader Blob Storage ingestion...")

        # Configuration - customize these for your storage account
        storage_account = "YOUR_ACCOUNT.dfs.core.windows.net"  # Update with your account
        container = "raw-data"
        directory = ""
        target_table = "autoloader_ingest"
        file_format = "csv"
        checkpoint_path = "/Workspace/Users/ashutosh_ranghar@epam.com/_checkpoints/"
        schema_location = "/Workspace/Users/ashutosh_ranghar@epam.com/_schemas/"

        # Create ingester using factory function
        ingester = create_ingester(
            spark=spark,
            storage_account=storage_account,
            container=container,
            directory=directory,
            file_format=file_format,
            checkpoint_path=checkpoint_path,
            schema_location=schema_location,
            add_new_columns=True,  # Handle schema evolution
            merge_schema=True,      # Merge schemas safely
            max_files_per_trigger=1000,  # Optimize for batch processing
        )

        # Read stream from blob storage
        print(f"[INFO] Reading from: abfss://{container}@{storage_account}/{directory}")
        df = ingester.read_stream()

        # Write to Delta table with availableNow trigger (process all available files once)
        print(f"[INFO] Writing to table: {target_table}")
        query = ingester.write_to_table(
            df,
            table_name=target_table,
            mode="append",
            trigger_type="availableNow"  # Process all available files once, then exit
        )

        # Wait for the query to complete
        query.awaitTermination()

        # Get final status
        status = ingester.get_stream_status()
        print(f"[INFO] Query status: {status}")
        print("[SUCCESS] Autoloader job completed successfully!")

    except Exception as e:
        print(f"[ERROR] Autoloader job failed: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
