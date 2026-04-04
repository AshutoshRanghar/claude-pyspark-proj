"""
CLI entry point for Autoloader Blob Storage Ingestion.

This module is called as a console script entry point when the wheel is installed.
Used by Databricks jobs to run the autoloader pipeline.
"""

from pyspark.sql import SparkSession
from claude_pyspark_proj.autoloader import create_ingester


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
