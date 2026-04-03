"""
Autoloader ingester for Azure Blob Storage using Cloud Files.

Handles schema changes, data format detection, and efficient streaming
from Azure Blob Storage with Spark Structured Streaming.
"""

from dataclasses import dataclass
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery


@dataclass
class AutoloaderConfig:
    """Configuration for Autoloader Cloud Files ingestion."""

    # Required: Storage account details
    storage_account: str  # e.g., "myaccount.blob.core.windows.net"
    container: str  # e.g., "raw-data"
    directory: str = ""  # Optional subdirectory in container

    # Checkpoint and schema location
    checkpoint_path: str = "/Workspace/Users/_checkpoints/"
    schema_location: str = "/Workspace/Users/_schemas/"

    # File format
    file_format: str = "csv"  # csv, json, parquet, etc.

    # Schema evolution
    handle_schema_changes: bool = True
    merge_schema: bool = True
    add_new_columns: bool = True

    # Autoloader behavior
    detect_schema: bool = True
    infer_schema: bool = True
    recursive: bool = True

    # Performance tuning
    max_files_per_trigger: Optional[int] = None  # Limit files per micro-batch
    mode: str = "FAILFAST"  # FAILFAST, PERMISSIVE, DROPMALFORMED

    def get_cloud_path(self) -> str:
        """Build the full cloud path for the storage account."""
        path = f"abfss://{self.container}@{self.storage_account}/{self.directory}"
        return path.rstrip("/")


class CloudFileIngester:
    """
    Autoloader-based ingester for Azure Blob Storage.

    Features:
    - Automatic schema detection and evolution
    - Handles new columns with addNewColumns option
    - Recursive directory scanning
    - Efficient micro-batching
    - Checkpointing for fault tolerance
    """

    def __init__(self, spark: SparkSession, config: AutoloaderConfig):
        """
        Initialize the CloudFileIngester.

        Args:
            spark: SparkSession instance
            config: AutoloaderConfig with storage and ingestion settings
        """
        self.spark = spark
        self.config = config
        self.query: Optional[StreamingQuery] = None

    def read_stream(
        self,
        output_mode: str = "append",
        processing_time: str = "10 seconds",
        trigger_type: str = "processingTime",
        **kwargs: Any
    ) -> StreamingQuery:
        """
        Read data from blob storage using Autoloader with schema evolution.

        Args:
            output_mode: append, update, or complete
            processing_time: Trigger interval (e.g., "10 seconds", "1 minute")
            trigger_type: processingTime or availableNow
            **kwargs: Additional options to pass to writeStream

        Returns:
            StreamingQuery object for monitoring the stream
        """

        # Build the readStream with Autoloader (Cloud Files)
        reader = self.spark.readStream \
            .format("cloudFiles") \
            .option("cloudFiles.format", self.config.file_format) \
            .option("cloudFiles.schemaLocation", self.config.schema_location) \
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns" if self.config.add_new_columns else "rescue") \
            .option("cloudFiles.maxFilesPerTrigger", self.config.max_files_per_trigger or 5000) \
            .option("recursiveFileLookup", "true" if self.config.recursive else "false")

        # Format-specific options
        if self.config.file_format.lower() == "csv":
            reader = reader \
                .option("header", "true") \
                .option("inferSchema", "true" if self.config.infer_schema else "false") \
                .option("mode", self.config.mode)

        elif self.config.file_format.lower() == "json":
            reader = reader \
                .option("inferSchema", "true" if self.config.infer_schema else "false") \
                .option("mode", self.config.mode)

        elif self.config.file_format.lower() == "parquet":
            pass  # Parquet handles schema well natively

        # Load the stream
        cloud_path = self.config.get_cloud_path()
        df = reader.load(cloud_path)

        return df

    def write_to_table(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "append",
        trigger_type: str = "processingTime",
        processing_time: str = "10 seconds"
    ) -> StreamingQuery:
        """
        Write streaming data to a Delta table.

        Args:
            df: Input DataFrame from read_stream()
            table_name: Delta table name to write to
            mode: append, update, or complete
            trigger_type: processingTime or availableNow
            processing_time: Trigger interval

        Returns:
            StreamingQuery object for monitoring writes
        """

        checkpoint_path = f"{self.config.checkpoint_path}{table_name}"

        self.query = df.writeStream \
            .format("delta") \
            .mode(mode) \
            .option("checkpointLocation", checkpoint_path) \
            .option("mergeSchema", "true" if self.config.merge_schema else "false") \
            .trigger(**{trigger_type: processing_time} if trigger_type == "processingTime" else {trigger_type: True}) \
            .toTable(table_name)

        return self.query

    def write_to_path(
        self,
        df: DataFrame,
        output_path: str,
        mode: str = "append",
        trigger_type: str = "processingTime",
        processing_time: str = "10 seconds"
    ) -> StreamingQuery:
        """
        Write streaming data to a file path.

        Args:
            df: Input DataFrame from read_stream()
            output_path: Path to write data to
            mode: append, update, or complete
            trigger_type: processingTime or availableNow
            processing_time: Trigger interval

        Returns:
            StreamingQuery object
        """

        checkpoint_path = f"{self.config.checkpoint_path}write_{output_path.replace('/', '_')}"

        self.query = df.writeStream \
            .format("delta") \
            .mode(mode) \
            .option("checkpointLocation", checkpoint_path) \
            .option("mergeSchema", "true" if self.config.merge_schema else "false") \
            .trigger(**{trigger_type: processing_time} if trigger_type == "processingTime" else {trigger_type: True}) \
            .start(output_path)

        return self.query

    def get_stream_status(self) -> Dict[str, Any]:
        """Get the status of the current streaming query."""
        if not self.query:
            return {"status": "No active query"}

        return {
            "id": self.query.id,
            "status": self.query.status,
            "is_active": self.query.isActive,
            "recent_progress": self.query.recentProgress[-1] if self.query.recentProgress else None
        }

    def stop_stream(self, await_termination: bool = True) -> None:
        """Stop the streaming query."""
        if self.query:
            self.query.stop()
            if await_termination:
                self.query.awaitTermination()

    def await_termination(self, timeout_seconds: Optional[int] = None) -> None:
        """Wait for the query to terminate."""
        if self.query:
            self.query.awaitTermination(timeout_seconds)


def create_ingester(
    spark: SparkSession,
    storage_account: str,
    container: str,
    directory: str = "",
    file_format: str = "csv",
    **config_kwargs: Any
) -> CloudFileIngester:
    """
    Factory function to create a CloudFileIngester.

    Args:
        spark: SparkSession instance
        storage_account: Azure storage account (e.g., "account.blob.core.windows.net")
        container: Blob container name
        directory: Optional subdirectory
        file_format: File format (csv, json, parquet)
        **config_kwargs: Additional config options

    Returns:
        CloudFileIngester instance
    """
    config = AutoloaderConfig(
        storage_account=storage_account,
        container=container,
        directory=directory,
        file_format=file_format,
        **config_kwargs
    )
    return CloudFileIngester(spark, config)
