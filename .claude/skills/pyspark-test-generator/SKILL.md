---
name: pyspark-test-generator
description: Generate comprehensive pytest test suites for PySpark streaming and batch jobs. Use this whenever the user mentions testing PySpark code, writing tests for Spark DataFrames, testing streaming pipelines, adding test coverage to batch jobs, or validating Spark transformations. This includes testing data transformations, aggregations, joins, watermarking behavior, schema validation, and error handling for both streaming and batch workloads.
compatibility: pytest, PySpark 3.0+, Python 3.8+
---

# PySpark Test Generator

This skill generates production-ready pytest test suites for PySpark streaming and batch workloads. It handles the unique testing challenges of Spark: fixture setup, DataFrame creation, assertions on distributed data, and streaming-specific patterns.

## Overview

When you have PySpark code that needs testing, this skill will:

1. **Analyze your code** — Understand the structure (UDFs, transformations, aggregations, joins, streaming logic)
2. **Generate fixtures** — Create reusable SparkSession fixtures with appropriate configs
3. **Create unit tests** — Test individual functions with sample data
4. **Cover streaming specifics** — Add tests for watermarks, checkpoints, trigger modes
5. **Add error cases** — Test invalid inputs, malformed data, missing columns
6. **Provide best practices** — Follow Spark testing patterns (in-memory mode, no Hadoop, proper cleanup)

## How to Use

### Provide your PySpark code

Share the code you want tested. This can be:
- A single function (e.g., a UDF or transformation)
- An entire module (e.g., your streaming pipeline)
- A notebook cell (copy/paste the code)

The skill works best with:
- Functions that take/return DataFrames
- Aggregations, joins, windowed operations
- Streaming sources and sinks
- Custom transformations or business logic

### What you'll get

The skill generates a `test_<module>.py` file with:

```
test_<module>.py
├── Imports and setup
├── Fixtures
│   ├── spark_session
│   ├── sample_data (DataFrames for testing)
├── Unit tests
│   ├── test_schema_validation
│   ├── test_transformations
│   ├── test_aggregations
│   ├── test_error_handling
├── Streaming tests (if applicable)
│   ├── test_watermarking
│   ├── test_windowed_operations
├── Helper functions
└── Cleanup
```

## Testing Patterns for PySpark

### Unit Tests (Most Common)

Test individual functions in isolation with small sample DataFrames:

```python
def test_my_transformation(spark_session):
    # Create small input
    input_data = [("vendor1", 100.0), ("vendor2", 200.0)]
    input_df = spark_session.createDataFrame(input_data, ["vendor", "amount"])
    
    # Call function
    result = my_transform(input_df)
    
    # Assert on result
    assert result.count() == 2
    assert result.filter(col("vendor") == "vendor1").select("amount").collect()[0][0] == 100.0
```

**When to use**: Testing transformations, UDFs, schema changes, data quality rules.

### Aggregation Tests

Verify groupBy, window, and agg operations:

```python
def test_vendor_revenue_aggregation(spark_session):
    """Verify revenue is correctly summed per vendor."""
    input_data = [
        ("vendor1", 100.0), ("vendor1", 50.0),
        ("vendor2", 200.0)
    ]
    df = spark_session.createDataFrame(input_data, ["vendor", "revenue"])
    
    result = df.groupBy("vendor").agg(spark_sum("revenue").alias("total"))
    
    rows = {row["vendor"]: row["total"] for row in result.collect()}
    assert rows["vendor1"] == 150.0
    assert rows["vendor2"] == 200.0
```

**When to use**: Testing KPIs, groupBy operations, windowed aggregations.

### Streaming Tests

For streaming pipelines (Kafka, Kinesis, Event Hubs):

```python
def test_streaming_aggregation(spark_session):
    """Verify streaming aggregation produces correct results."""
    input_stream = spark_session.readStream.format("rate").load()
    
    windowed = (
        input_stream
        .withWatermark("timestamp", "10 minutes")
        .groupBy(window("timestamp", "5 minutes"))
        .count()
    )
    
    query = windowed.writeStream.format("memory").start()
    query.awaitTermination(timeout=5)
    
    # Verify window count is reasonable
    assert spark_session.sql("SELECT COUNT(*) FROM windowed_results").collect()[0][0] > 0
```

**When to use**: Testing streaming aggregations, watermarks, session windows.

### Error Handling Tests

Verify your code handles bad data gracefully:

```python
def test_parse_events_with_invalid_json(spark_session):
    """Verify invalid JSON is skipped or handled."""
    # Create data with malformed JSON
    malformed_data = [("{invalid json",)]
    df = spark_session.createDataFrame(malformed_data, ["value"])
    
    result = parse_events(df)
    
    # Should either drop bad rows or have them in error column
    assert result.count() == 0  # Or check error column if your function does that
```

**When to use**: Testing robustness of transformations, null handling, type mismatches.

### Join Tests

Verify join logic, handling of missing keys, etc.:

```python
def test_join_preserves_all_records(spark_session):
    """Verify outer join includes unmatched records."""
    left = spark_session.createDataFrame(
        [("a", 1), ("b", 2)], ["key", "left_val"]
    )
    right = spark_session.createDataFrame(
        [("a", 10), ("c", 30)], ["key", "right_val"]
    )
    
    result = left.join(right, "key", "outer")
    
    assert result.count() == 3
    assert result.filter(col("key") == "b").select("right_val").collect()[0][0] is None
```

**When to use**: Testing logic that combines multiple sources.

## Fixtures You'll Get

### SparkSession Fixture

```python
@pytest.fixture(scope="session")
def spark_session():
    """Create a SparkSession for testing."""
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("pytest-pyspark")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()
```

**Note**: Uses `local[1]` (single-threaded) for predictable, fast tests. No Hadoop/HDFS needed.

### Sample Data Helpers

```python
def create_sample_taxi_data(spark_session, num_records=10):
    """Create realistic sample taxi ride data."""
    data = [
        ("vendor1", 1609459200000, 100.0, "location_A"),
        ("vendor2", 1609459260000, 150.0, "location_B"),
    ]
    return spark_session.createDataFrame(
        data, ["vendorID", "tpepPickupDateTime", "totalAmount", "puLocationId"]
    )
```

## Best Practices for PySpark Testing

1. **Use local[1] mode** — Single-threaded, deterministic, fast. No distributed overhead.
2. **Create minimal DataFrames** — 5-10 rows is enough. Larger DataFrames slow tests down.
3. **Avoid actual Kafka/HDFS in unit tests** — Use in-memory or local filesystem for integration tests only.
4. **Test schema separately** — Don't rely on schema inference; explicitly test schema changes.
5. **Collect() sparingly** — Use it to assert on small results, not large datasets.
6. **Name assertions clearly** — Include what you're testing: `assert result.count() == 2, "Should have exactly 2 vendor records"`
7. **Clean up resources** — Spark creates JVM objects; use fixtures with teardown.

## Common Patterns

### Testing a UDF

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

def my_udf(value):
    return value * 2

spark_udf = udf(my_udf, DoubleType())

def test_udf(spark_session):
    df = spark_session.createDataFrame([(1.0,), (2.0,)], ["value"])
    result = df.withColumn("doubled", spark_udf(col("value")))
    rows = result.collect()
    assert rows[0]["doubled"] == 2.0
```

### Testing a Pipeline (Multiple Transformations)

```python
def test_full_pipeline(spark_session):
    input_df = create_sample_data(spark_session)
    
    # Step 1: Parse
    parsed = parse_events(input_df)
    assert "event_time" in parsed.columns
    
    # Step 2: Filter
    filtered = parsed.filter(col("amount") > 100)
    assert filtered.count() <= parsed.count()
    
    # Step 3: Aggregate
    result = filtered.groupBy("vendor").agg(sum("amount"))
    assert result.count() > 0
```

### Testing Data Quality Rules

```python
def test_no_nulls_in_critical_columns(spark_session):
    """Verify required fields are never null."""
    df = create_sample_data(spark_session)
    
    for col_name in ["vendorID", "totalAmount", "event_time"]:
        null_count = df.filter(col(col_name).isNull()).count()
        assert null_count == 0, f"Column {col_name} should not have nulls"
```

## Running Your Tests

Once generated, run with pytest:

```bash
# Run all tests
pytest test_my_module.py -v

# Run a specific test
pytest test_my_module.py::test_schema_validation -v

# Run with coverage
pytest test_my_module.py --cov=src.my_module

# Run in parallel (faster)
pytest test_my_module.py -n auto
```

## Edge Cases Covered

The skill will generate tests for:
- Empty DataFrames (no rows)
- Null/missing values in columns
- Type mismatches (string instead of int)
- Duplicate keys in joins
- Late-arriving data in streaming (watermark behavior)
- Very large values (overflow scenarios)
- Special characters in strings

## What NOT to Test

- Spark's internal behavior (it's already tested)
- Hadoop/HDFS functionality (mock it or use integration tests)
- Network I/O in unit tests (too slow; use integration tests instead)
- Exact row ordering (Spark doesn't guarantee it; use sorted assertions)

## Next Steps

1. Share your PySpark code
2. The skill will generate a comprehensive test file
3. Review the tests and customize as needed
4. Run with `pytest` and integrate into your CI/CD pipeline
5. Iterate on test data and assertions based on your requirements
