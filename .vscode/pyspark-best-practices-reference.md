# PySpark & Python Best Practices Reference

## Table of Contents
1. [PySpark Performance Optimization](#pyspark-performance)
2. [Code Structure & Architecture](#code-structure)
3. [Security Best Practices](#security)
4. [Testing Patterns](#testing)
5. [Common Refactoring Patterns](#patterns)

---

## PySpark Performance Optimization

### Shuffle Operations

**What Creates Shuffles:**
- `join()` without a broadcast
- `groupBy()` operations
- `distinct()`
- `repartition()`
- `orderBy()` / `sort()`

**Minimize Shuffles:**
- Use `broadcast()` for small tables (< 2GB)
- Push filters down before joins (`filter()` before `join()`)
- Partition data appropriately based on query patterns
- Use `coalesce()` instead of `repartition()` when combining partitions

### Caching Strategy

**Use `.cache()` when:**
- DataFrame is reused multiple times (2+ downstream operations)
- After expensive transformations (joins, aggregations)
- Before multiple actions (multiple `write()` operations)

**Don't cache when:**
- The DataFrame is only used once
- Memory is constrained
- Streaming DataFrames (use `checkpoint()` instead)

```python
# ✓ Good: Reused multiple times
df_cached = df.filter(col("year") == 2023).cache()
agg1 = df_cached.groupBy("category").agg(...)
agg2 = df_cached.groupBy("region").agg(...)

# ✗ Bad: Used only once
df.filter(...).cache().groupBy(...).agg(...)
```

### Partition Strategy

**Partitioning by:**
- Most commonly filtered columns
- Join keys
- Aggregation keys
- Time-based columns for time-series data

```python
# Store data partitioned for efficient querying
df.write \
    .partitionBy("year", "month") \
    .mode("overwrite") \
    .parquet(path)
```

### Avoiding Expensive Operations

**Avoid:**
- `collect()` – pulls entire dataset to driver memory
- `take()` on large datasets – wasteful
- UDFs for simple operations – use SQL functions instead
- Window functions without partition – can be memory-intensive

**Instead:**
- Keep data distributed as long as possible
- Aggregate before collecting small results
- Use native Catalyst-optimized functions
- Partition window operations

---

## Code Structure & Architecture

### Configuration Management

```python
from dataclasses import dataclass
from typing import Optional
import os
from abc import ABC, abstractmethod

@dataclass(frozen=True)
class AppConfig:
    """Application configuration from environment."""
    
    kafka_brokers: str
    kafka_topic: str
    checkpoint_path: str
    output_path: str
    
    @classmethod
    def from_env(cls) -> "AppConfig":
        """Load configuration from environment variables."""
        return cls(
            kafka_brokers=os.getenv("KAFKA_BROKERS", "localhost:9092"),
            kafka_topic=os.getenv("KAFKA_TOPIC", "events"),
            checkpoint_path=os.getenv("CHECKPOINT_PATH", "/tmp/checkpoints"),
            output_path=os.getenv("OUTPUT_PATH", "/tmp/output")
        )
```

### Separation of Concerns

```python
# ✓ Good: Separated responsibilities
class KafkaSource:
    """Handles Kafka connectivity."""
    def read(self, spark: SparkSession, config: AppConfig) -> DataFrame:
        return spark.readStream.format("kafka")...

class EventParser:
    """Handles event parsing logic."""
    def parse(self, df: DataFrame) -> DataFrame:
        return df.select(from_json(...))

class Pipeline:
    """Orchestrates the pipeline."""
    def __init__(self, source: KafkaSource, parser: EventParser):
        self.source = source
        self.parser = parser
    
    def run(self, spark: SparkSession, config: AppConfig):
        raw = self.source.read(spark, config)
        parsed = self.parser.parse(raw)
        return parsed
```

### Type Hints Best Practices

```python
from typing import List, Dict, Optional, Callable, Union
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

def process_events(
    spark: SparkSession,
    df: DataFrame,
    schema: StructType,
    output_path: str,
    filters: Optional[Dict[str, str]] = None,
    timeout_ms: int = 60000
) -> DataFrame:
    """Process events with optional filtering.
    
    Args:
        spark: SparkSession instance
        df: Input DataFrame with raw events
        schema: Expected schema for JSON parsing
        output_path: Where to write results
        filters: Optional dict of column:value filters
        timeout_ms: Query timeout in milliseconds
        
    Returns:
        Processed DataFrame
        
    Raises:
        ValueError: If schema doesn't match data
        TimeoutError: If operation exceeds timeout_ms
    """
    pass
```

### Docstring Patterns

```python
def calculate_revenue_by_vendor(
    transactions: DataFrame,
    date_range: tuple[str, str]
) -> DataFrame:
    """Calculate total revenue per vendor for a date range.
    
    This function aggregates transaction amounts by vendor ID,
    filtering to the specified date range. Results are cached
    for efficient reuse in downstream operations.
    
    Args:
        transactions: DataFrame with columns: vendor_id, amount, 
                     transaction_date
        date_range: Tuple of (start_date, end_date) in ISO format
        
    Returns:
        DataFrame with columns: vendor_id, total_revenue
        
    Example:
        >>> revenue = calculate_revenue_by_vendor(
        ...     df, 
        ...     ("2023-01-01", "2023-12-31")
        ... )
        >>> revenue.show()
        vendor_id | total_revenue
        1         | 50000.00
        2         | 75000.00
    """
    pass
```

---

## Security Best Practices

### Credential Management

```python
# ✗ Bad: Hardcoded credentials
kafka_servers = "kafka.prod.local:9092"
connection_string = "DefaultEndpointsProtocol=https;AccountName=..."

# ✓ Good: Environment variables
import os
from pathlib import Path

kafka_servers = os.getenv("KAFKA_BROKERS")
connection_string = os.getenv("STORAGE_CONNECTION_STRING")

# ✓ Better: Secrets manager integration
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

credential = DefaultAzureCredential()
client = SecretClient(vault_url="https://my-vault.vault.azure.net", credential=credential)
connection_string = client.get_secret("storage-connection-string").value
```

### SQL Injection Prevention

```python
# ✗ Bad: String concatenation
user_id = "123; DROP TABLE users--"
query = f"SELECT * FROM users WHERE id = {user_id}"

# ✓ Good: Parameterized queries with Spark
spark.sql(
    "SELECT * FROM users WHERE id = ?",
    args=[user_id]
)

# ✓ Good: Use col() for dynamic column references
query_df = df.filter(col("id") == user_id)
```

### Data Privacy

```python
from pyspark.sql.functions import md5, concat_ws

# Mask sensitive columns
def mask_pii(df: DataFrame) -> DataFrame:
    """Mask personally identifiable information."""
    return df.select(
        col("id"),
        md5(concat_ws("", col("email"))).alias("email_hash"),  # Hash email
        col("age"),
        col("created_at")
        # Don't select ssn, address, phone, etc.
    )
```

### Error Handling Without Exposure

```python
# ✗ Bad: Exposes sensitive information
try:
    result = spark.sql("SELECT * FROM customers WHERE api_key = ?", args=[key])
except Exception as e:
    print(f"Database error: {e}")  # Might expose schema, credentials

# ✓ Good: Generic error message, detailed logging
import logging

logger = logging.getLogger(__name__)

try:
    result = spark.sql("SELECT * FROM customers WHERE api_key = ?", args=[key])
except Exception as e:
    logger.error(f"Customer lookup failed", exc_info=True)  # Log details securely
    raise RuntimeError("Customer lookup failed") from e  # Generic user message
```

---

## Testing Patterns

### Unit Testing PySpark Code

```python
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession for testing."""
    return SparkSession.builder \
        .appName("test") \
        .master("local[2]") \
        .getOrCreate()

def test_parse_events(spark):
    """Test event parsing logic."""
    schema = StructType([
        StructField("id", StringType()),
        StructField("amount", IntegerType())
    ])
    
    input_df = spark.createDataFrame([
        ("1", 100),
        ("2", 200)
    ], schema=schema)
    
    result = parse_events(input_df)
    
    assert result.count() == 2
    assert "processed_at" in result.columns
```

### Dependency Injection

```python
# ✓ Good: Dependency injection for testability
class DataProcessor:
    def __init__(self, source, sink, spark_session=None):
        self.source = source
        self.sink = sink
        self.spark = spark_session
    
    def process(self):
        df = self.source.read(self.spark)
        transformed = self._transform(df)
        self.sink.write(transformed)
        return transformed
    
    def _transform(self, df):
        # Pure function – testable without I/O
        return df.filter(col("valid") == True)

# In tests:
mock_source = MockSource(test_df)
mock_sink = MockSink()
processor = DataProcessor(mock_source, mock_sink, test_spark)
result = processor.process()
```

---

## Common Refactoring Patterns

### 1. Extract Magic Numbers

```python
# ✗ Before
df = df.filter(col("age") > 18)
df = df.filter(col("balance") > 10000)
df = df.repartition(200)

# ✓ After
MIN_AGE = 18
MIN_BALANCE = 10000
PARTITION_COUNT = 200

df = df.filter(col("age") > MIN_AGE)
df = df.filter(col("balance") > MIN_BALANCE)
df = df.repartition(PARTITION_COUNT)
```

### 2. Consolidate Filters

```python
# ✗ Before: Multiple filter operations
df = df.filter(col("status") == "active")
df = df.filter(col("region") == "US")
df = df.filter(col("amount") > 1000)

# ✓ After: Single filter with conditions
df = df.filter(
    (col("status") == "active") &
    (col("region") == "US") &
    (col("amount") > 1000)
)
```

### 3. Extract Repeated Logic

```python
# ✗ Before: Repeated transformation
vendor_revenue = (
    df.withColumn("year", year(col("date")))
    .withColumn("month", month(col("date")))
    .groupBy("vendor", "year", "month")
    .agg(sum("amount"))
)

region_revenue = (
    df.withColumn("year", year(col("date")))
    .withColumn("month", month(col("date")))
    .groupBy("region", "year", "month")
    .agg(sum("amount"))
)

# ✓ After: Extract common transformation
def add_date_parts(df: DataFrame) -> DataFrame:
    """Add year and month columns from date."""
    return df.select(
        col("*"),
        year(col("date")).alias("year"),
        month(col("date")).alias("month")
    )

vendor_revenue = add_date_parts(df).groupBy("vendor", "year", "month").agg(sum("amount"))
region_revenue = add_date_parts(df).groupBy("region", "year", "month").agg(sum("amount"))
```

### 4. Use Intermediate Variables for Clarity

```python
# ✗ Before: Hard to follow
result = df.join(broadcast(lookup), "id").select(
    col("df.id"), col("lookup.category")
).filter(col("lookup.category").isin(["A", "B"])).groupBy("category").agg(count("*"))

# ✓ After: Clear steps
enriched_df = df.join(broadcast(lookup), "id")
selected_df = enriched_df.select(col("df.id"), col("lookup.category"))
filtered_df = selected_df.filter(col("lookup.category").isin(["A", "B"]))
result = filtered_df.groupBy("category").agg(count("*").alias("count"))
```

---

## Performance Checklist

- [ ] All joins use `broadcast()` for tables < 2GB
- [ ] Filters are pushed down before expensive operations
- [ ] `collect()` is only called on small result sets
- [ ] UDFs are replaced with native functions where possible
- [ ] Data is partitioned appropriately for access patterns
- [ ] Caching is used for reused DataFrames
- [ ] Configuration is externalized
- [ ] Error handling logs details without exposing sensitive data
- [ ] Type hints are present for all public functions
- [ ] Tests exist for business logic without I/O dependencies
- [ ] Credentials come from environment/secrets manager
- [ ] Code follows SOLID principles and DRY
