"""
Test suite for Autoloader Cloud Files ingestion.

Tests cover:
- AutoloaderConfig initialization and path building
- CloudFileIngester creation and configuration
- Schema evolution options (addNewColumns)
- Multi-format support (CSV, JSON, Parquet)
- Edge cases and error handling

Run with: pytest tests/test_autoloader.py -v
"""

import pytest
from pyspark.sql import SparkSession

from claude_pyspark_proj.autoloader import (
    AutoloaderConfig,
    CloudFileIngester,
    create_ingester,
)


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture(scope="session")
def spark_session():
    """Create a SparkSession for testing."""
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("pytest-autoloader")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def default_config():
    """Default Autoloader configuration."""
    return AutoloaderConfig(
        storage_account="testaccount.dfs.core.windows.net",
        container="raw-data",
        directory="events/",
        file_format="csv",
    )


# ============================================================================
# Tests: AutoloaderConfig
# ============================================================================

class TestAutoloaderConfig:
    """Tests for AutoloaderConfig dataclass."""

    def test_config_initialization(self, default_config):
        """Verify AutoloaderConfig initializes with required fields."""
        assert default_config.storage_account == "testaccount.dfs.core.windows.net"
        assert default_config.container == "raw-data"
        assert default_config.directory == "events/"
        assert default_config.file_format == "csv"

    def test_config_default_values(self):
        """Verify AutoloaderConfig has sensible defaults."""
        config = AutoloaderConfig(
            storage_account="account.dfs.core.windows.net",
            container="container",
        )

        assert config.add_new_columns is True  # Schema evolution enabled by default
        assert config.merge_schema is True
        assert config.recursive is True
        assert config.file_format == "csv"
        assert config.max_files_per_trigger is None

    def test_cloud_path_building(self, default_config):
        """Verify cloud path is built correctly in abfss:// format."""
        path = default_config.get_cloud_path()

        # Should follow: abfss://container@account/directory
        assert path.startswith("abfss://")
        assert "raw-data@testaccount.dfs.core.windows.net" in path
        assert path.endswith("events")  # rstrip("/") removes trailing slash

    def test_cloud_path_without_trailing_slash(self):
        """Verify trailing slashes are removed from cloud path."""
        config = AutoloaderConfig(
            storage_account="account.dfs.core.windows.net",
            container="container",
            directory="data/",
        )

        path = config.get_cloud_path()
        assert not path.endswith("/")

    def test_cloud_path_empty_directory(self):
        """Verify cloud path works with empty directory."""
        config = AutoloaderConfig(
            storage_account="account.dfs.core.windows.net",
            container="container",
            directory="",
        )

        path = config.get_cloud_path()
        assert path == "abfss://container@account.dfs.core.windows.net"

    def test_config_schema_evolution_enabled(self):
        """Verify schema evolution options can be enabled."""
        config = AutoloaderConfig(
            storage_account="account.dfs.core.windows.net",
            container="container",
            add_new_columns=True,
            merge_schema=True,
        )

        assert config.add_new_columns is True
        assert config.merge_schema is True

    def test_config_schema_evolution_disabled(self):
        """Verify schema evolution can be disabled."""
        config = AutoloaderConfig(
            storage_account="account.dfs.core.windows.net",
            container="container",
            add_new_columns=False,
            merge_schema=False,
        )

        assert config.add_new_columns is False
        assert config.merge_schema is False

    def test_config_file_formats(self):
        """Verify config supports multiple file formats."""
        formats = ["csv", "json", "parquet"]

        for fmt in formats:
            config = AutoloaderConfig(
                storage_account="account.dfs.core.windows.net",
                container="container",
                file_format=fmt,
            )
            assert config.file_format == fmt


# ============================================================================
# Tests: CloudFileIngester
# ============================================================================

class TestCloudFileIngester:
    """Tests for CloudFileIngester class."""

    def test_ingester_initialization(self, spark_session, default_config):
        """Verify CloudFileIngester initializes correctly."""
        ingester = CloudFileIngester(spark_session, default_config)

        assert ingester.spark is spark_session
        assert ingester.config == default_config
        assert ingester.query is None  # No query started yet

    def test_ingester_with_different_formats(self, spark_session):
        """Verify ingester works with different file formats."""
        formats = ["csv", "json", "parquet"]

        for fmt in formats:
            config = AutoloaderConfig(
                storage_account="account.dfs.core.windows.net",
                container="container",
                file_format=fmt,
            )
            ingester = CloudFileIngester(spark_session, config)
            assert ingester.config.file_format == fmt

    def test_ingester_schema_evolution_options(self, spark_session):
        """Verify ingester respects schema evolution configuration."""
        config_with_evolution = AutoloaderConfig(
            storage_account="account.dfs.core.windows.net",
            container="container",
            add_new_columns=True,
            merge_schema=True,
        )

        ingester = CloudFileIngester(spark_session, config_with_evolution)
        assert ingester.config.add_new_columns is True
        assert ingester.config.merge_schema is True

    def test_get_stream_status_no_query(self, spark_session, default_config):
        """Verify get_stream_status returns sensible value when no query."""
        ingester = CloudFileIngester(spark_session, default_config)
        status = ingester.get_stream_status()

        assert "status" in status
        assert status["status"] == "No active query"


# ============================================================================
# Tests: Factory Function
# ============================================================================

class TestCreateIngesterFactory:
    """Tests for create_ingester() factory function."""

    def test_factory_basic_usage(self, spark_session):
        """Verify factory function creates ingester correctly."""
        ingester = create_ingester(
            spark=spark_session,
            storage_account="account.dfs.core.windows.net",
            container="raw",
            directory="data/",
            file_format="csv",
        )

        assert isinstance(ingester, CloudFileIngester)
        assert ingester.config.storage_account == "account.dfs.core.windows.net"
        assert ingester.config.container == "raw"
        assert ingester.config.file_format == "csv"

    def test_factory_with_schema_evolution_options(self, spark_session):
        """Verify factory passes schema evolution options to config."""
        ingester = create_ingester(
            spark=spark_session,
            storage_account="account.dfs.core.windows.net",
            container="raw",
            add_new_columns=True,
            merge_schema=True,
        )

        assert ingester.config.add_new_columns is True
        assert ingester.config.merge_schema is True

    def test_factory_with_performance_tuning(self, spark_session):
        """Verify factory accepts performance tuning parameters."""
        ingester = create_ingester(
            spark=spark_session,
            storage_account="account.dfs.core.windows.net",
            container="raw",
            max_files_per_trigger=1000,
        )

        assert ingester.config.max_files_per_trigger == 1000

    def test_factory_with_custom_paths(self, spark_session):
        """Verify factory accepts custom checkpoint and schema paths."""
        checkpoint_path = "/mnt/custom/checkpoints/"
        schema_path = "/mnt/custom/schemas/"

        ingester = create_ingester(
            spark=spark_session,
            storage_account="account.dfs.core.windows.net",
            container="raw",
            checkpoint_path=checkpoint_path,
            schema_location=schema_path,
        )

        assert ingester.config.checkpoint_path == checkpoint_path
        assert ingester.config.schema_location == schema_path


# ============================================================================
# Tests: Configuration Combinations
# ============================================================================

class TestConfigurationCombinations:
    """Tests for realistic configuration scenarios."""

    def test_csv_with_schema_evolution(self, spark_session):
        """Verify CSV ingestion with automatic schema evolution."""
        config = AutoloaderConfig(
            storage_account="account.dfs.core.windows.net",
            container="raw-data",
            directory="csv-files/",
            file_format="csv",
            add_new_columns=True,  # Auto-add new columns
            merge_schema=True,      # Merge schemas
        )

        ingester = CloudFileIngester(spark_session, config)
        assert ingester.config.file_format == "csv"
        assert ingester.config.add_new_columns is True

    def test_json_with_infer_schema(self, spark_session):
        """Verify JSON ingestion with schema inference."""
        config = AutoloaderConfig(
            storage_account="account.dfs.core.windows.net",
            container="raw-data",
            directory="json-logs/",
            file_format="json",
            infer_schema=True,
        )

        ingester = CloudFileIngester(spark_session, config)
        assert ingester.config.file_format == "json"
        assert ingester.config.infer_schema is True

    def test_parquet_with_recursive_lookup(self, spark_session):
        """Verify Parquet ingestion with recursive directory scanning."""
        config = AutoloaderConfig(
            storage_account="account.dfs.core.windows.net",
            container="raw-data",
            directory="parquet-data/",
            file_format="parquet",
            recursive=True,  # Scan subdirectories
        )

        ingester = CloudFileIngester(spark_session, config)
        assert ingester.config.file_format == "parquet"
        assert ingester.config.recursive is True

    def test_production_config(self, spark_session):
        """Verify production-ready configuration with all best practices."""
        config = AutoloaderConfig(
            storage_account="account.dfs.core.windows.net",
            container="raw-data",
            directory="production/",
            file_format="csv",
            checkpoint_path="/Workspace/checkpoints/",
            schema_location="/Workspace/schemas/",
            add_new_columns=True,    # Schema evolution
            merge_schema=True,        # Merge schemas
            max_files_per_trigger=1000,  # Batch size
            recursive=True,            # Scan subdirectories
        )

        ingester = CloudFileIngester(spark_session, config)

        # Verify all production settings
        assert ingester.config.add_new_columns is True
        assert ingester.config.merge_schema is True
        assert ingester.config.max_files_per_trigger == 1000
        assert ingester.config.recursive is True
        assert ingester.config.checkpoint_path == "/Workspace/checkpoints/"


# ============================================================================
# Run Tests
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
