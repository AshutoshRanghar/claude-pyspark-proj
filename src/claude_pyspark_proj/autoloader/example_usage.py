"""
Example usage of CloudFileIngester for Autoloader with Azure Blob Storage.

This demonstrates:
- Setting up Autoloader with schema evolution
- Handling new columns with addNewColumns
- Writing to Delta tables
- Monitoring streaming queries
"""

from pyspark.sql import SparkSession
from claude_pyspark_proj.autoloader.cloud_file_ingester import (
    CloudFileIngester,
    AutoloaderConfig,
    create_ingester,
)


def example_1_basic_ingestion():
    """Basic Autoloader ingestion from blob storage."""
    spark = SparkSession.builder.appName("autoloader-basic").getOrCreate()

    # Configure Autoloader for CSV files
    config = AutoloaderConfig(
        storage_account="myaccount.blob.core.windows.net",
        container="raw-data",
        directory="events/",
        file_format="csv",
        checkpoint_path="/Workspace/Users/_checkpoints/",
        schema_location="/Workspace/Users/_schemas/",
    )

    # Create ingester
    ingester = CloudFileIngester(spark, config)

    # Read stream from blob storage
    df = ingester.read_stream()

    # Write to Delta table
    query = ingester.write_to_table(
        df,
        table_name="raw_events",
        mode="append",
        trigger_type="processingTime",
        processing_time="30 seconds"
    )

    # Monitor the stream
    print(f"Query status: {ingester.get_stream_status()}")

    # Keep running
    query.awaitTermination()


def example_2_schema_evolution():
    """Autoloader with schema evolution for handling new columns."""
    spark = SparkSession.builder.appName("autoloader-evolution").getOrCreate()

    # Configure with schema evolution enabled
    config = AutoloaderConfig(
        storage_account="myaccount.blob.core.windows.net",
        container="raw-data",
        directory="events/",
        file_format="csv",
        checkpoint_path="/Workspace/Users/_checkpoints/",
        schema_location="/Workspace/Users/_schemas/",
        # Schema evolution options
        handle_schema_changes=True,
        add_new_columns=True,  # Automatically add new columns
        merge_schema=True,      # Merge schemas across partitions
    )

    ingester = CloudFileIngester(spark, config)
    df = ingester.read_stream()

    # When new columns appear in CSV, they'll be automatically added
    # Old rows will have NULL for the new columns
    query = ingester.write_to_table(
        df,
        table_name="events_with_schema_evolution",
        mode="append"
    )

    query.awaitTermination()


def example_3_factory_function():
    """Using the factory function for simpler setup."""
    spark = SparkSession.builder.appName("autoloader-factory").getOrCreate()

    # Use factory function for convenience
    ingester = create_ingester(
        spark=spark,
        storage_account="myaccount.blob.core.windows.net",
        container="raw-data",
        directory="sales/",
        file_format="csv",
        add_new_columns=True,
        merge_schema=True,
        checkpoint_path="/Workspace/checkpoints/",
        schema_location="/Workspace/schemas/"
    )

    df = ingester.read_stream()
    query = ingester.write_to_table(df, table_name="sales_data")

    query.awaitTermination()


def example_4_multiple_formats():
    """Autoloader with different file formats."""
    spark = SparkSession.builder.appName("autoloader-formats").getOrCreate()

    # Example 1: JSON files
    json_ingester = create_ingester(
        spark=spark,
        storage_account="account.blob.core.windows.net",
        container="raw",
        directory="json-logs/",
        file_format="json",
    )

    json_df = json_ingester.read_stream()
    json_query = json_ingester.write_to_table(json_df, table_name="json_logs")

    # Example 2: Parquet files
    parquet_ingester = create_ingester(
        spark=spark,
        storage_account="account.blob.core.windows.net",
        container="raw",
        directory="parquet-data/",
        file_format="parquet",
    )

    parquet_df = parquet_ingester.read_stream()
    parquet_query = parquet_ingester.write_to_table(parquet_df, table_name="parquet_data")

    # Both queries run in parallel
    json_query.awaitTermination()


def example_5_performance_tuning():
    """Autoloader with performance optimization options."""
    spark = SparkSession.builder.appName("autoloader-perf").getOrCreate()

    config = AutoloaderConfig(
        storage_account="account.blob.core.windows.net",
        container="raw-data",
        file_format="csv",
        # Performance options
        max_files_per_trigger=1000,  # Limit files processed per trigger
        mode="PERMISSIVE",           # Allow malformed records (FAILFAST = strict)
        # Schema options
        add_new_columns=True,
        merge_schema=True,
    )

    ingester = CloudFileIngester(spark, config)
    df = ingester.read_stream()

    # Use availableNow for bounded processing (one-time read)
    # Or use processingTime for continuous streaming
    query = ingester.write_to_table(
        df,
        table_name="optimized_data",
        trigger_type="availableNow"  # Process all available files once
    )

    query.awaitTermination()


if __name__ == "__main__":
    print("Autoloader Examples:")
    print("1. Basic ingestion")
    print("2. Schema evolution with addNewColumns")
    print("3. Factory function usage")
    print("4. Multiple file formats")
    print("5. Performance tuning")
    print("\nImport and call examples as needed")
