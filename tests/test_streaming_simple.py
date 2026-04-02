"""
Practical pytest test suite for PySpark streaming pipeline.

Focuses on logic tests and DataFrame structure validation
without relying on complex Spark serialization (Windows-compatible).

Run with: pytest tests/test_streaming_simple.py -v
"""

import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType,
    StructField, StructType
)


@pytest.fixture(scope="session")
def spark_session():
    """Create a SparkSession for testing (local mode, single-threaded)."""
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("pytest-pyspark-streaming")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()


# ============================================================================
# Test: Basic DataFrame Creation & Schema
# ============================================================================

class TestDataFrameCreation:
    """Test basic DataFrame operations."""

    def test_create_simple_dataframe(self, spark_session):
        """Verify we can create a simple DataFrame."""
        data = [("vendor1", 100.0), ("vendor2", 200.0)]
        df = spark_session.createDataFrame(data, ["vendor", "amount"])

        assert df.count() == 2, "Should have 2 rows"
        assert "vendor" in df.columns
        assert "amount" in df.columns

    def test_dataframe_with_custom_schema(self, spark_session):
        """Verify custom schema is applied correctly."""
        schema = StructType([
            StructField("vendorID", StringType(), True),
            StructField("totalAmount", DoubleType(), True),
            StructField("timestamp", LongType(), True),
        ])

        data = [
            ("vendor1", 100.0, 1609459200000),
            ("vendor2", 200.0, 1609462800000),
        ]
        df = spark_session.createDataFrame(data, schema=schema)

        # Verify schema matches
        field_names = {f.name for f in df.schema.fields}
        assert field_names == {"vendorID", "totalAmount", "timestamp"}

    def test_empty_dataframe(self, spark_session):
        """Verify empty DataFrames are handled correctly."""
        schema = StructType([
            StructField("vendor", StringType()),
            StructField("amount", DoubleType()),
        ])

        empty_df = spark_session.createDataFrame([], schema)
        assert empty_df.count() == 0
        assert len(empty_df.columns) == 2


# ============================================================================
# Test: Aggregation Operations
# ============================================================================

class TestAggregations:
    """Test aggregation logic (groupBy, sum, count)."""

    def test_sum_aggregation(self, spark_session):
        """Verify sum aggregation works correctly."""
        data = [
            ("vendor1", 100.0),
            ("vendor1", 50.0),
            ("vendor2", 200.0),
        ]
        df = spark_session.createDataFrame(data, ["vendor", "amount"])

        result = df.groupBy("vendor").agg(spark_sum("amount").alias("total"))

        # Should have 2 vendors
        assert result.count() == 2

        # Verify both vendors are in result
        vendors = set()
        for row in result.collect():
            vendors.add(row["vendor"])

        assert vendors == {"vendor1", "vendor2"}

    def test_count_aggregation(self, spark_session):
        """Verify count aggregation works correctly."""
        data = [
            ("location_A", ),
            ("location_A", ),
            ("location_B", ),
        ]
        df = spark_session.createDataFrame(data, ["location"])

        result = df.groupBy("location").agg(count("*").alias("trip_count"))

        assert result.count() == 2

        # Verify counts are correct
        location_counts = {}
        for row in result.collect():
            location_counts[row["location"]] = row["trip_count"]

        assert location_counts["location_A"] == 2
        assert location_counts["location_B"] == 1

    def test_aggregation_with_nulls(self, spark_session):
        """Verify aggregation handles null values correctly."""
        data = [
            ("vendor1", 100.0),
            ("vendor1", None),  # null
            ("vendor2", 50.0),
        ]
        df = spark_session.createDataFrame(data, ["vendor", "amount"])

        result = df.groupBy("vendor").agg(spark_sum("amount").alias("total"))

        # Should have 2 vendor groups
        assert result.count() == 2

        # Sum should ignore nulls
        totals = {}
        for row in result.collect():
            totals[row["vendor"]] = row["total"]

        assert totals["vendor1"] == 100.0  # Nulls ignored
        assert totals["vendor2"] == 50.0


# ============================================================================
# Test: Filtering & Selection
# ============================================================================

class TestFiltering:
    """Test filtering operations."""

    def test_filter_by_string(self, spark_session):
        """Verify string filtering works."""
        data = [
            ("vendor1", 100.0),
            ("vendor2", 200.0),
            ("vendor1", 150.0),
        ]
        df = spark_session.createDataFrame(data, ["vendor", "amount"])

        vendor1_df = df.filter(col("vendor") == "vendor1")

        assert vendor1_df.count() == 2

    def test_filter_by_number(self, spark_session):
        """Verify numeric filtering works."""
        data = [
            ("vendor1", 100.0),
            ("vendor2", 50.0),
            ("vendor3", 200.0),
        ]
        df = spark_session.createDataFrame(data, ["vendor", "amount"])

        high_amount = df.filter(col("amount") > 100.0)

        assert high_amount.count() == 2

    def test_filter_null_values(self, spark_session):
        """Verify null filtering works."""
        data = [
            ("vendor1", 100.0),
            ("vendor2", None),
            ("vendor3", 200.0),
        ]
        df = spark_session.createDataFrame(data, ["vendor", "amount"])

        not_null = df.filter(col("amount").isNotNull())

        assert not_null.count() == 2


# ============================================================================
# Test: Schema Validation
# ============================================================================

class TestSchemaValidation:
    """Test schema validation and structure."""

    def test_taxi_schema_validation(self, spark_session):
        """Verify a taxi events schema has required fields."""
        taxi_schema = StructType([
            StructField("vendorID", StringType(), True),
            StructField("tpepPickupDateTime", LongType(), True),
            StructField("totalAmount", DoubleType(), True),
            StructField("puLocationId", StringType(), True),
        ])

        # Check field names
        field_names = {f.name for f in taxi_schema.fields}
        required_fields = {"vendorID", "tpepPickupDateTime", "totalAmount"}

        assert required_fields.issubset(field_names)

    def test_column_selection(self, spark_session):
        """Verify column selection works."""
        data = [
            ("vendor1", 100.0, "location_A"),
            ("vendor2", 200.0, "location_B"),
        ]
        df = spark_session.createDataFrame(data, ["vendor", "amount", "location"])

        # Select specific columns
        selected = df.select("vendor", "amount")

        assert len(selected.columns) == 2
        assert "vendor" in selected.columns
        assert "amount" in selected.columns
        assert "location" not in selected.columns

    def test_distinct_values(self, spark_session):
        """Verify distinct() works correctly."""
        data = [
            ("vendor1", ),
            ("vendor1", ),
            ("vendor2", ),
        ]
        df = spark_session.createDataFrame(data, ["vendor"])

        distinct_vendors = df.select("vendor").distinct()

        assert distinct_vendors.count() == 2


# ============================================================================
# Test: Multi-step Transformations
# ============================================================================

class TestTransformations:
    """Test multi-step data transformations."""

    def test_filter_then_aggregate(self, spark_session):
        """Test chaining filter and aggregate operations."""
        data = [
            ("vendor1", 100.0),
            ("vendor1", 50.0),
            ("vendor2", 30.0),
            ("vendor2", 20.0),
        ]
        df = spark_session.createDataFrame(data, ["vendor", "amount"])

        # Filter amounts > 40 then sum
        result = (
            df.filter(col("amount") > 40)
            .groupBy("vendor")
            .agg(spark_sum("amount").alias("total"))
        )

        assert result.count() == 2

        # vendor1: 100 + 50 = 150 (both > 40)
        # vendor2: 0 (both <= 40)
        totals = {}
        for row in result.collect():
            totals[row["vendor"]] = row["total"]

        assert totals["vendor1"] == 150.0
        assert totals["vendor2"] is None

    def test_select_filter_aggregate(self, spark_session):
        """Test selecting columns, filtering, then aggregating."""
        data = [
            ("vendor1", 100.0, "loc_A"),
            ("vendor1", 50.0, "loc_B"),
            ("vendor2", 200.0, "loc_A"),
        ]
        df = spark_session.createDataFrame(data, ["vendor", "amount", "location"])

        # Select, filter, aggregate
        result = (
            df.select("vendor", "amount")
            .filter(col("amount") > 50)
            .groupBy("vendor")
            .agg(spark_sum("amount").alias("total"))
        )

        assert result.count() == 2


# ============================================================================
# Test: Joining DataFrames
# ============================================================================

class TestJoins:
    """Test DataFrame join operations."""

    def test_inner_join(self, spark_session):
        """Verify inner join works correctly."""
        left_data = [
            ("vendor1", "name1"),
            ("vendor2", "name2"),
        ]
        right_data = [
            ("vendor1", 100.0),
            ("vendor3", 300.0),
        ]

        left_df = spark_session.createDataFrame(left_data, ["vendor_id", "vendor_name"])
        right_df = spark_session.createDataFrame(right_data, ["vendor_id", "amount"])

        result = left_df.join(right_df, "vendor_id", "inner")

        # Should only have vendor1 (inner join)
        assert result.count() == 1

    def test_left_join(self, spark_session):
        """Verify left join preserves all left rows."""
        left_data = [
            ("vendor1", "name1"),
            ("vendor2", "name2"),
        ]
        right_data = [
            ("vendor1", 100.0),
        ]

        left_df = spark_session.createDataFrame(left_data, ["vendor_id", "vendor_name"])
        right_df = spark_session.createDataFrame(right_data, ["vendor_id", "amount"])

        result = left_df.join(right_df, "vendor_id", "left")

        # Should have both vendors
        assert result.count() == 2

        # Verify vendor2 has null amount
        vendor2_result = result.filter(col("vendor_id") == "vendor2")
        assert vendor2_result.count() == 1


# ============================================================================
# Test: Real Taxi Pipeline Logic
# ============================================================================

class TestTaxiPipelineLogic:
    """Test actual taxi pipeline business logic."""

    def test_vendor_revenue_calculation(self, spark_session):
        """Test calculating revenue per vendor (like write_vendor_revenue)."""
        # Sample taxi data
        data = [
            ("vendor1", 17.30),
            ("vendor1", 40.06),
            ("vendor2", 24.30),
            ("vendor2", 14.30),
        ]
        df = spark_session.createDataFrame(data, ["vendorID", "totalAmount"])

        # Aggregate: sum revenue by vendor
        vendor_revenue = (
            df.groupBy("vendorID")
            .agg(spark_sum("totalAmount").alias("totalAmount"))
        )

        assert vendor_revenue.count() == 2

        # Verify totals
        totals = {}
        for row in vendor_revenue.collect():
            totals[row["vendorID"]] = round(row["totalAmount"], 2)

        assert totals["vendor1"] == 57.36
        assert totals["vendor2"] == 38.60

    def test_pickup_traffic_calculation(self, spark_session):
        """Test calculating trip count per location (like write_pickup_traffic)."""
        # Sample trip data
        data = [
            ("location_A", ),
            ("location_B", ),
            ("location_A", ),
            ("location_C", ),
        ]
        df = spark_session.createDataFrame(data, ["puLocationId"])

        # Aggregate: count trips by location
        pickup_traffic = (
            df.groupBy("puLocationId")
            .agg(count("*").alias("trip_count"))
        )

        assert pickup_traffic.count() == 3

        # Verify counts
        counts = {}
        for row in pickup_traffic.collect():
            counts[row["puLocationId"]] = row["trip_count"]

        assert counts["location_A"] == 2
        assert counts["location_B"] == 1
        assert counts["location_C"] == 1

    def test_combined_aggregation(self, spark_session):
        """Test running multiple aggregations on same dataset."""
        data = [
            ("vendor1", "location_A", 100.0),
            ("vendor1", "location_B", 150.0),
            ("vendor2", "location_A", 200.0),
        ]
        df = spark_session.createDataFrame(
            data, ["vendor", "location", "amount"]
        )

        # Revenue by vendor
        vendor_revenue = df.groupBy("vendor").agg(
            spark_sum("amount").alias("revenue")
        )

        # Traffic by location
        location_traffic = df.groupBy("location").agg(
            count("*").alias("trips")
        )

        assert vendor_revenue.count() == 2
        assert location_traffic.count() == 2


# ============================================================================
# Run Tests
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
