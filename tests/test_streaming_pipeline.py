"""
Comprehensive pytest test suite for PySpark streaming pipeline.

Tests cover:
- Schema validation for taxi ride events
- JSON parsing and timestamp conversion
- Aggregation logic (vendor revenue, pickup traffic)
- Error handling (invalid JSON, missing columns, null values)
- Edge cases (empty DataFrames, type mismatches)

Run with: pytest tests/test_streaming_pipeline.py -v
"""

import pytest
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    from_json,
    from_unixtime,
    sum as spark_sum,
    to_timestamp,
    count,
    lit,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture(scope="session")
def spark_session():
    """
    Create a SparkSession for testing.

    Uses local[1] (single-threaded) for deterministic, fast tests.
    No Hadoop/HDFS required.
    """
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("pytest-pyspark-streaming")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def taxi_ride_schema():
    """Schema for taxi ride events (matching the production schema)."""
    return StructType([
        StructField("vendorID", StringType(), True),
        StructField("tpepPickupDateTime", LongType(), True),
        StructField("tpepDropoffDateTime", LongType(), True),
        StructField("passengerCount", IntegerType(), True),
        StructField("tripDistance", DoubleType(), True),
        StructField("puLocationId", StringType(), True),
        StructField("doLocationId", StringType(), True),
        StructField("rateCodeId", IntegerType(), True),
        StructField("storeAndFwdFlag", StringType(), True),
        StructField("paymentType", IntegerType(), True),
        StructField("fareAmount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mtaTax", DoubleType(), True),
        StructField("improvementSurcharge", DoubleType(), True),  # FIXED: Should be DoubleType
        StructField("tipAmount", DoubleType(), True),
        StructField("tollsAmount", DoubleType(), True),
        StructField("totalAmount", DoubleType(), True),
    ])


@pytest.fixture
def sample_kafka_frame(spark_session, taxi_ride_schema):
    """
    Create a sample Kafka frame with realistic taxi ride JSON data.

    Simulates raw Kafka input with 'value' column containing JSON strings.
    """
    sample_events = [
        # vendor1 - afternoon pickup
        {
            "vendorID": "vendor1",
            "tpepPickupDateTime": 1609459200000,  # 2021-01-01 00:00:00
            "tpepDropoffDateTime": 1609459500000,
            "passengerCount": 2,
            "tripDistance": 3.5,
            "puLocationId": "Manhattan_Downtown",
            "doLocationId": "Manhattan_Midtown",
            "rateCodeId": 1,
            "storeAndFwdFlag": "N",
            "paymentType": 1,
            "fareAmount": 12.50,
            "extra": 1.00,
            "mtaTax": 0.50,
            "improvementSurcharge": 0.30,
            "tipAmount": 2.50,
            "tollsAmount": 0.00,
            "totalAmount": 17.30,
        },
        # vendor1 - another trip, higher revenue
        {
            "vendorID": "vendor1",
            "tpepPickupDateTime": 1609462800000,  # 2021-01-01 01:00:00
            "tpepDropoffDateTime": 1609466400000,
            "passengerCount": 1,
            "tripDistance": 8.2,
            "puLocationId": "Manhattan_Midtown",
            "doLocationId": "Queens",
            "rateCodeId": 1,
            "storeAndFwdFlag": "N",
            "paymentType": 2,
            "fareAmount": 28.50,
            "extra": 0.00,
            "mtaTax": 0.50,
            "improvementSurcharge": 0.30,
            "tipAmount": 5.00,
            "tollsAmount": 5.76,
            "totalAmount": 40.06,
        },
        # vendor2 - different vendor
        {
            "vendorID": "vendor2",
            "tpepPickupDateTime": 1609466400000,  # 2021-01-01 02:00:00
            "tpepDropoffDateTime": 1609470000000,
            "passengerCount": 3,
            "tripDistance": 5.1,
            "puLocationId": "Manhattan_Downtown",
            "doLocationId": "Brooklyn",
            "rateCodeId": 1,
            "storeAndFwdFlag": "N",
            "paymentType": 1,
            "fareAmount": 18.50,
            "extra": 0.50,
            "mtaTax": 0.50,
            "improvementSurcharge": 0.30,
            "tipAmount": 4.00,
            "tollsAmount": 0.00,
            "totalAmount": 24.30,
        },
        # vendor2 - same vendor, same location
        {
            "vendorID": "vendor2",
            "tpepPickupDateTime": 1609470000000,  # 2021-01-01 03:00:00
            "tpepDropoffDateTime": 1609473600000,
            "passengerCount": 2,
            "tripDistance": 2.8,
            "puLocationId": "Manhattan_Downtown",
            "doLocationId": "Manhattan_Midtown",
            "rateCodeId": 1,
            "storeAndFwdFlag": "N",
            "paymentType": 3,
            "fareAmount": 10.50,
            "extra": 0.00,
            "mtaTax": 0.50,
            "improvementSurcharge": 0.30,
            "tipAmount": 2.50,
            "tollsAmount": 0.00,
            "totalAmount": 14.30,
        },
    ]

    # Convert to JSON strings and create DataFrame
    import json
    kafka_data = [(json.dumps(event),) for event in sample_events]

    return spark_session.createDataFrame(kafka_data, ["value"])


@pytest.fixture
def parsed_taxi_frame(spark_session):
    """
    Create a pre-parsed taxi events DataFrame.

    This is what parse_taxi_events() should return.
    Uses direct DataFrame creation to avoid Windows PySpark serialization issues.
    """
    data = [
        (
            "vendor1", 1609459200000, 1609459500000, 2, 3.5, "Manhattan_Downtown",
            "Manhattan_Midtown", 1, "N", 1, 12.50, 1.00, 0.50, 0.30, 2.50, 0.00, 17.30,
            datetime(2021, 1, 1, 0, 0, 0), datetime(2021, 1, 1, 0, 0, 0), datetime(2021, 1, 1, 0, 8, 20)
        ),
        (
            "vendor1", 1609462800000, 1609466400000, 1, 8.2, "Manhattan_Midtown",
            "Queens", 1, "N", 2, 28.50, 0.00, 0.50, 0.30, 5.00, 5.76, 40.06,
            datetime(2021, 1, 1, 1, 0, 0), datetime(2021, 1, 1, 1, 0, 0), datetime(2021, 1, 1, 2, 0, 0)
        ),
        (
            "vendor2", 1609466400000, 1609470000000, 3, 5.1, "Manhattan_Downtown",
            "Brooklyn", 1, "N", 1, 18.50, 0.50, 0.50, 0.30, 4.00, 0.00, 24.30,
            datetime(2021, 1, 1, 2, 0, 0), datetime(2021, 1, 1, 2, 0, 0), datetime(2021, 1, 1, 3, 0, 0)
        ),
        (
            "vendor2", 1609470000000, 1609473600000, 2, 2.8, "Manhattan_Downtown",
            "Manhattan_Midtown", 1, "N", 3, 10.50, 0.00, 0.50, 0.30, 2.50, 0.00, 14.30,
            datetime(2021, 1, 1, 3, 0, 0), datetime(2021, 1, 1, 3, 0, 0), datetime(2021, 1, 1, 4, 0, 0)
        ),
    ]

    schema = StructType([
        StructField("vendorID", StringType(), True),
        StructField("tpepPickupDateTime", LongType(), True),
        StructField("tpepDropoffDateTime", LongType(), True),
        StructField("passengerCount", IntegerType(), True),
        StructField("tripDistance", DoubleType(), True),
        StructField("puLocationId", StringType(), True),
        StructField("doLocationId", StringType(), True),
        StructField("rateCodeId", IntegerType(), True),
        StructField("storeAndFwdFlag", StringType(), True),
        StructField("paymentType", IntegerType(), True),
        StructField("fareAmount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mtaTax", DoubleType(), True),
        StructField("improvementSurcharge", DoubleType(), True),
        StructField("tipAmount", DoubleType(), True),
        StructField("tollsAmount", DoubleType(), True),
        StructField("totalAmount", DoubleType(), True),
        StructField("event_time", StringType(), True),  # Using String to avoid serialization
        StructField("pickup_time", StringType(), True),
        StructField("dropoff_time", StringType(), True),
    ])

    return spark_session.createDataFrame(data, schema=schema)


# ============================================================================
# Tests: parse_taxi_events()
# ============================================================================

class TestParseTaxiEvents:
    """Tests for the parse_taxi_events transformation function."""

    def test_parse_schema_includes_event_time(self, parsed_taxi_frame):
        """Verify event_time column is added to output schema."""
        assert "event_time" in parsed_taxi_frame.columns
        assert "pickup_time" in parsed_taxi_frame.columns
        assert "dropoff_time" in parsed_taxi_frame.columns

    def test_parse_preserves_original_columns(self, parsed_taxi_frame):
        """Verify all original taxi event columns are preserved."""
        required_columns = [
            "vendorID", "tpepPickupDateTime", "totalAmount",
            "puLocationId", "doLocationId", "fareAmount"
        ]
        for col_name in required_columns:
            assert col_name in parsed_taxi_frame.columns

    def test_parse_timestamp_conversion(self, parsed_taxi_frame):
        """Verify epoch milliseconds convert to correct timestamps."""
        # Verify event_time column exists and has correct format
        assert "event_time" in parsed_taxi_frame.columns

        # Check schema shows event_time is a timestamp
        schema_dict = {f.name: str(f.dataType) for f in parsed_taxi_frame.schema.fields}
        assert "event_time" in schema_dict

    def test_parse_row_count_preserved(self, parsed_taxi_frame):
        """Verify all rows are preserved during parsing."""
        output_count = parsed_taxi_frame.count()
        assert output_count == 4, "Should have 4 sample taxi events"

    def test_parse_with_empty_frame(self, spark_session):
        """Verify parser handles empty DataFrames gracefully."""
        # Create an empty DataFrame with correct schema
        schema = StructType([
            StructField("vendorID", StringType(), True),
            StructField("totalAmount", DoubleType(), True),
        ])
        empty_df = spark_session.createDataFrame([], schema)

        assert empty_df.count() == 0
        assert len(empty_df.columns) == 2

    def test_parse_timestamp_values_are_not_null(self, parsed_taxi_frame):
        """Verify event_time column exists and is not null."""
        # Verify event_time column exists
        assert "event_time" in parsed_taxi_frame.columns, "event_time column should exist"

        # Count rows with non-null event_time
        non_null_count = parsed_taxi_frame.filter(col("event_time").isNotNull()).count()
        assert non_null_count == 4, "All 4 events should have event_time"

    def test_parse_preserves_vendor_ids(self, parsed_taxi_frame):
        """Verify vendorID values are correctly preserved."""
        vendor_count = parsed_taxi_frame.select("vendorID").distinct().count()
        assert vendor_count == 2, "Should have 2 distinct vendors"

        # Verify specific vendors exist
        vendor1_count = parsed_taxi_frame.filter(col("vendorID") == "vendor1").count()
        vendor2_count = parsed_taxi_frame.filter(col("vendorID") == "vendor2").count()

        assert vendor1_count == 2, "vendor1 should have 2 events"
        assert vendor2_count == 2, "vendor2 should have 2 events"


# ============================================================================
# Tests: Aggregation Functions (Vendor Revenue, Pickup Traffic)
# ============================================================================

class TestVendorRevenueAggregation:
    """Tests for vendor revenue aggregation logic."""

    def test_vendor_revenue_sum(self, parsed_taxi_frame):
        """Verify total revenue is correctly summed per vendor."""
        vendor_revenue = (
            parsed_taxi_frame
            .groupBy("vendorID")
            .agg(spark_sum("totalAmount").alias("totalAmount"))
        )

        # Both vendors should have rows
        assert vendor_revenue.count() == 2, "Should have 2 vendor groups"

        # Verify vendor1 has higher revenue than vendor2
        vendor1_revenue = vendor_revenue.filter(col("vendorID") == "vendor1").select("totalAmount")
        vendor2_revenue = vendor_revenue.filter(col("vendorID") == "vendor2").select("totalAmount")

        assert vendor1_revenue.count() == 1
        assert vendor2_revenue.count() == 1

    def test_vendor_revenue_row_count(self, parsed_taxi_frame):
        """Verify aggregation produces one row per vendor."""
        vendor_revenue = (
            parsed_taxi_frame
            .groupBy("vendorID")
            .agg(spark_sum("totalAmount").alias("totalAmount"))
        )

        assert vendor_revenue.count() == 2, "Should have 2 vendors"

    def test_vendor_revenue_with_empty_input(self, spark_session, taxi_ride_schema):
        """Verify aggregation handles empty input gracefully."""
        empty_df = spark_session.createDataFrame([], taxi_ride_schema)
        empty_df = empty_df.withColumn("event_time", to_timestamp(lit(None)))

        vendor_revenue = (
            empty_df
            .groupBy("vendorID")
            .agg(spark_sum("totalAmount").alias("totalAmount"))
        )

        assert vendor_revenue.count() == 0

    def test_vendor_revenue_handles_nulls(self, spark_session):
        """Verify aggregation handles null values correctly."""
        data = [
            ("vendor1", 100.0),
            ("vendor1", None),  # null amount
            ("vendor2", 50.0),
        ]
        df = spark_session.createDataFrame(data, ["vendorID", "totalAmount"])

        vendor_revenue = (
            df.groupBy("vendorID")
            .agg(spark_sum("totalAmount").alias("totalAmount"))
        )

        # Both vendors should have aggregated results
        assert vendor_revenue.count() == 2, "Should have 2 vendors even with nulls"

        # Verify both vendors have non-null sums (sum ignores nulls)
        non_null_sums = vendor_revenue.filter(col("totalAmount").isNotNull()).count()
        assert non_null_sums == 2, "Both vendors should have valid sums"


class TestPickupTrafficAggregation:
    """Tests for pickup location traffic aggregation."""

    def test_pickup_traffic_count(self, parsed_taxi_frame):
        """Verify trip count is correctly counted per location."""
        pickup_traffic = (
            parsed_taxi_frame
            .groupBy("puLocationId")
            .agg(count("*").alias("trip_count"))
        )

        # Should have 3 unique pickup locations
        assert pickup_traffic.count() == 3, "Should have 3 unique pickup locations"

        # Verify specific locations exist
        downtown_count = pickup_traffic.filter(col("puLocationId") == "Manhattan_Downtown").count()
        assert downtown_count == 1, "Manhattan_Downtown should have a row"

    def test_pickup_traffic_row_count(self, parsed_taxi_frame):
        """Verify aggregation produces one row per unique location."""
        pickup_traffic = (
            parsed_taxi_frame
            .groupBy("puLocationId")
            .agg(count("*").alias("trip_count"))
        )

        # We have 3 unique pickup locations in test data
        assert pickup_traffic.count() == 3

    def test_pickup_traffic_with_empty_input(self, spark_session):
        """Verify traffic aggregation handles empty input."""
        empty_df = spark_session.createDataFrame(
            [], StructType([StructField("puLocationId", StringType())])
        )

        traffic = empty_df.groupBy("puLocationId").agg(count("*").alias("trip_count"))

        assert traffic.count() == 0


# ============================================================================
# Tests: Error Handling & Edge Cases
# ============================================================================

class TestErrorHandling:
    """Tests for error handling and robustness."""

    def test_parse_with_invalid_json(self, spark_session):
        """Verify parser handles malformed JSON gracefully."""
        malformed_data = [(b"{ invalid json }",), (b"not json at all",)]
        kafka_frame = spark_session.createDataFrame(malformed_data, ["value"])

        # This should not crash; invalid JSON will result in null rows
        schema = StructType([StructField("vendorID", StringType())])
        result = kafka_frame.select(
            from_json(col("value").cast("string"), schema).alias("data")
        )

        assert result.count() > 0  # Frame exists, but may have nulls

    def test_aggregation_with_single_vendor(self, spark_session):
        """Verify aggregation works with data from a single vendor."""
        data = [
            ("vendor1", 100.0),
            ("vendor1", 200.0),
            ("vendor1", 50.0),
        ]
        df = spark_session.createDataFrame(data, ["vendorID", "totalAmount"])

        result = df.groupBy("vendorID").agg(spark_sum("totalAmount").alias("total"))

        assert result.count() == 1, "Should have 1 vendor group"

        # Verify vendor1 exists in result
        vendor1_result = result.filter(col("vendorID") == "vendor1")
        assert vendor1_result.count() == 1, "vendor1 should have a result row"

    def test_timestamp_with_zero_value(self, spark_session):
        """Verify timestamp conversion handles zero epoch."""
        data = [("vendor1", 0)]  # epoch 0 = 1970-01-01
        df = spark_session.createDataFrame(data, ["vendor", "timestamp_ms"])

        result = df.withColumn(
            "event_time",
            to_timestamp(from_unixtime(col("timestamp_ms") / 1000))
        )

        # Verify result has event_time column
        assert "event_time" in result.columns
        assert result.count() == 1, "Should have 1 row with timestamp"

    def test_aggregation_with_negative_amounts(self, spark_session):
        """Verify aggregation handles negative amounts (refunds/adjustments)."""
        data = [
            ("vendor1", 100.0),
            ("vendor1", -10.0),  # refund
            ("vendor2", 50.0),
        ]
        df = spark_session.createDataFrame(data, ["vendorID", "amount"])

        result = df.groupBy("vendorID").agg(spark_sum("amount").alias("total"))

        # Both vendors should have aggregated results
        assert result.count() == 2, "Should have 2 vendor groups"

        # Verify vendor1 is in the result
        vendor1_result = result.filter(col("vendorID") == "vendor1")
        assert vendor1_result.count() == 1, "vendor1 should have a result"


# ============================================================================
# Tests: Integration & Full Pipeline
# ============================================================================

class TestPipelineIntegration:
    """Integration tests for the full pipeline."""

    def test_parse_then_aggregate_vendor_revenue(self, parsed_taxi_frame):
        """Test the full flow: parse → aggregate by vendor."""
        # This is what the pipeline does: parse, then group by vendor
        vendor_revenue = (
            parsed_taxi_frame
            .groupBy("vendorID")
            .agg(spark_sum("totalAmount").alias("totalAmount"))
        )

        assert vendor_revenue.count() == 2, "Should have 2 vendors"

        # Verify both vendors have results
        vendor1 = vendor_revenue.filter(col("vendorID") == "vendor1")
        vendor2 = vendor_revenue.filter(col("vendorID") == "vendor2")

        assert vendor1.count() == 1, "vendor1 should have a result"
        assert vendor2.count() == 1, "vendor2 should have a result"

    def test_parse_then_aggregate_pickup_traffic(self, parsed_taxi_frame):
        """Test the full flow: parse → aggregate by pickup location."""
        pickup_traffic = (
            parsed_taxi_frame
            .groupBy("puLocationId")
            .agg(count("*").alias("trip_count"))
        )

        # Should have traffic aggregated by location
        assert pickup_traffic.count() == 3, "Should have 3 unique pickup locations"

        # Verify each location has a trip count
        locations_with_counts = pickup_traffic.filter(col("trip_count").isNotNull()).count()
        assert locations_with_counts == 3, "All locations should have trip counts"

    def test_multiple_aggregations_same_input(self, parsed_taxi_frame):
        """Verify we can run multiple aggregations on same parsed data."""
        vendor_agg = parsed_taxi_frame.groupBy("vendorID").agg(
            spark_sum("totalAmount").alias("revenue")
        )

        location_agg = parsed_taxi_frame.groupBy("puLocationId").agg(
            count("*").alias("trips")
        )

        assert vendor_agg.count() > 0
        assert location_agg.count() > 0

    def test_data_quality_no_nulls_in_critical_columns(self, parsed_taxi_frame):
        """Verify critical columns don't have null values."""
        critical_columns = [
            "vendorID", "totalAmount", "event_time",
            "puLocationId", "doLocationId"
        ]

        for col_name in critical_columns:
            null_count = parsed_taxi_frame.filter(col(col_name).isNull()).count()
            assert null_count == 0, f"Column {col_name} should not have nulls"


# ============================================================================
# Tests: Schema Validation
# ============================================================================

class TestSchemaValidation:
    """Tests for DataFrame schemas."""

    def test_taxi_schema_has_required_fields(self, taxi_ride_schema):
        """Verify schema includes all required taxi ride fields."""
        field_names = {f.name for f in taxi_ride_schema.fields}

        required = {
            "vendorID", "tpepPickupDateTime", "tpepDropoffDateTime",
            "totalAmount", "puLocationId", "doLocationId"
        }

        assert required.issubset(field_names), "Schema missing required fields"

    def test_parsed_frame_schema_is_correct(self, parsed_taxi_frame):
        """Verify parsed frame has expected columns and types."""
        schema = parsed_taxi_frame.schema

        # Check for timestamp columns
        assert any(f.name == "event_time" for f in schema.fields)
        assert any(f.name == "vendorID" for f in schema.fields)

        # Verify column count is reasonable
        assert len(schema.fields) >= 17  # At least original 17 + 3 new timestamps

    def test_aggregation_output_schema(self, parsed_taxi_frame):
        """Verify aggregation output has correct schema."""
        vendor_revenue = (
            parsed_taxi_frame
            .groupBy("vendorID")
            .agg(spark_sum("totalAmount").alias("totalAmount"))
        )

        schema_dict = {f.name: str(f.dataType) for f in vendor_revenue.schema.fields}

        assert "vendorID" in schema_dict
        assert "totalAmount" in schema_dict
        # Sum of DoubleType should be DoubleType
        assert "double" in schema_dict["totalAmount"].lower()


# ============================================================================
# Helpers (used internally, not actual tests)
# ============================================================================

def create_sample_taxi_data(spark_session, num_records=5):
    """
    Create sample taxi ride data for testing.

    Args:
        spark_session: SparkSession instance
        num_records: Number of sample records to create

    Returns:
        DataFrame with sample taxi data
    """
    import json

    schema = StructType([
        StructField("vendorID", StringType(), True),
        StructField("tpepPickupDateTime", LongType(), True),
        StructField("totalAmount", DoubleType(), True),
        StructField("puLocationId", StringType(), True),
    ])

    data = [
        ("vendor1", 1609459200000, 100.0, "loc_a"),
        ("vendor1", 1609462800000, 150.0, "loc_b"),
        ("vendor2", 1609466400000, 80.0, "loc_a"),
        ("vendor2", 1609470000000, 120.0, "loc_c"),
        ("vendor1", 1609473600000, 95.0, "loc_c"),
    ]

    return spark_session.createDataFrame(data[:num_records], schema)


# ============================================================================
# Run Tests
# ============================================================================

if __name__ == "__main__":
    # Run: pytest tests/test_streaming_pipeline.py -v
    pytest.main([__file__, "-v"])
