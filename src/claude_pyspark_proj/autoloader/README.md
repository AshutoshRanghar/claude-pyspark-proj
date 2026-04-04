# Autoloader Package: Azure Blob Storage Ingestion

Production-ready Autoloader implementation for streaming data ingestion from Azure Blob Storage with automatic schema evolution support.

## Features

### ✅ Core Capabilities
- **Cloud Files Integration**: Uses Spark's Autoloader (cloudFiles format)
- **Azure Blob Storage**: Full support for `abfss://` protocol
- **Schema Evolution**: Automatically handles new columns with `addNewColumns`
- **Multi-Format**: CSV, JSON, Parquet, and other formats
- **Fault Tolerance**: Checkpointing for recovery from failures
- **Streaming**: Continuous or bounded micro-batch processing

### ✅ Schema Evolution (Critical Feature)
When data structure changes in source files:
- New columns are automatically added to the target table
- Existing rows get NULL for new columns
- No pipeline failures due to schema mismatches
- Backward compatible with existing data

### ✅ Important Autoloader Options
| Option | Purpose | Default |
|--------|---------|---------|
| `cloudFiles.format` | File format | Auto-detect |
| `cloudFiles.schemaLocation` | Where to store schemas | Required |
| `cloudFiles.schemaEvolutionMode` | How to handle changes | `addNewColumns` |
| `cloudFiles.maxFilesPerTrigger` | Files per micro-batch | 5000 |
| `recursiveFileLookup` | Scan subdirectories | true |
| `mergeSchema` | Merge schemas safely | true |

## Installation

The package is part of `claude-pyspark-proj`. Import directly:

```python
from claude_pyspark_proj.autoloader import (
    CloudFileIngester,
    AutoloaderConfig,
    create_ingester,
)
```

## Quick Start

### Basic Usage
```python
from pyspark.sql import SparkSession
from claude_pyspark_proj.autoloader import create_ingester

spark = SparkSession.builder.appName("autoloader").getOrCreate()

# Create ingester
ingester = create_ingester(
    spark=spark,
    storage_account="myaccount.blob.core.windows.net",
    container="raw-data",
    directory="events/",
    file_format="csv",
    add_new_columns=True,  # Handle schema changes
)

# Read stream from blob storage
df = ingester.read_stream()

# Write to Delta table
query = ingester.write_to_table(
    df,
    table_name="events",
    processing_time="30 seconds"
)

# Monitor
query.awaitTermination()
```

### With Schema Evolution
```python
from claude_pyspark_proj.autoloader import AutoloaderConfig, CloudFileIngester

config = AutoloaderConfig(
    storage_account="account.blob.core.windows.net",
    container="raw-data",
    directory="events/",
    file_format="csv",
    checkpoint_path="/Workspace/checkpoints/",
    schema_location="/Workspace/schemas/",
    # Schema evolution
    handle_schema_changes=True,
    add_new_columns=True,  # Auto-add new columns
    merge_schema=True,     # Merge schemas safely
)

ingester = CloudFileIngester(spark, config)
df = ingester.read_stream()

# When CSV gains new columns, they're automatically added
query = ingester.write_to_table(df, table_name="raw_events")
query.awaitTermination()
```

## API Reference

### CloudFileIngester

Main class for ingestion from blob storage.

#### `__init__(spark, config)`
Initialize with SparkSession and AutoloaderConfig.

#### `read_stream() -> DataFrame`
Read stream from blob storage using Autoloader.

```python
df = ingester.read_stream()
```

#### `write_to_table(df, table_name, mode='append', trigger_type='processingTime', processing_time='10 seconds') -> StreamingQuery`
Write stream to Delta table.

```python
query = ingester.write_to_table(
    df,
    table_name="events",
    mode="append",
    trigger_type="processingTime",
    processing_time="10 seconds"
)
```

#### `write_to_path(df, output_path, mode='append', ...) -> StreamingQuery`
Write stream to file path.

```python
query = ingester.write_to_path(
    df,
    output_path="/mnt/delta/events",
    mode="append"
)
```

#### `get_stream_status() -> Dict`
Get current query status.

```python
status = ingester.get_stream_status()
print(f"Active: {status['is_active']}")
print(f"Records processed: {status['recent_progress']['numInputRows']}")
```

#### `stop_stream(await_termination=True) -> None`
Stop the streaming query.

```python
ingester.stop_stream()
```

#### `await_termination(timeout_seconds=None) -> None`
Wait for query to terminate.

```python
ingester.await_termination(timeout_seconds=3600)
```

### AutoloaderConfig

Dataclass for configuration.

```python
config = AutoloaderConfig(
    storage_account="account.blob.core.windows.net",
    container="raw-data",
    directory="events/",
    file_format="csv",
    checkpoint_path="/Workspace/checkpoints/",
    schema_location="/Workspace/schemas/",
    handle_schema_changes=True,
    add_new_columns=True,
    merge_schema=True,
    max_files_per_trigger=1000,
)
```

### create_ingester() Factory

Simplified creation without dataclass:

```python
ingester = create_ingester(
    spark=spark,
    storage_account="account.blob.core.windows.net",
    container="raw-data",
    directory="events/",
    file_format="csv",
    add_new_columns=True,
)
```

## Examples

See `example_usage.py` for:
1. **Basic ingestion** - Simple read and write
2. **Schema evolution** - Handling new columns
3. **Factory function** - Simplified setup
4. **Multiple formats** - CSV, JSON, Parquet in parallel
5. **Performance tuning** - Optimized for large datasets

## Schema Evolution Behavior

### addNewColumns Mode (Recommended)
When a new column appears:
- **New data**: Includes the new column
- **Old data**: NULL for the new column
- **Pipeline**: Continues without failure
- **Use case**: Evolving data sources

Example:
```
Before: customer_id, name, email
After:  customer_id, name, email, phone

Result: phone column added, old rows have NULL for phone
```

### Rescue Mode
Alternative: Keep problematic rows in `_rescued_data` column.
```python
# In cloudFiles.schemaEvolutionMode
"rescue"  # Instead of "addNewColumns"
```

## Performance Tuning

### Micro-batch Configuration
```python
config = AutoloaderConfig(
    max_files_per_trigger=1000,  # Limit files per trigger
    # Lower = smaller batches, higher latency
    # Higher = larger batches, higher latency
)
```

### Checkpoint Location
Use high-performance storage:
```python
checkpoint_path="/mnt/fast-storage/checkpoints/"  # High IOPS storage
schema_location="/mnt/fast-storage/schemas/"
```

### Processing Mode
```python
# Continuous (recommended)
ingester.write_to_table(
    df,
    table_name="events",
    trigger_type="processingTime",
    processing_time="10 seconds"
)

# Bounded (one-time)
ingester.write_to_table(
    df,
    table_name="events",
    trigger_type="availableNow"
)
```

## Troubleshooting

### Schema Mismatch Errors
Enable schema evolution:
```python
config.add_new_columns = True
config.merge_schema = True
```

### Slow Processing
- Increase `max_files_per_trigger`
- Use faster storage for checkpoints
- Reduce `processing_time` interval

### Missing Files
- Enable `recursive=True` for subdirectories
- Check `cloudFiles.schemaLocation` is accessible
- Verify Azure credentials

### Memory Issues
- Reduce `max_files_per_trigger`
- Increase cluster size
- Use `mode="PERMISSIVE"` to skip bad records

## Best Practices

1. **Always enable schema evolution** for production
   ```python
   add_new_columns=True
   merge_schema=True
   ```

2. **Use Delta Lake** for target tables
   - ACID transactions
   - Time travel
   - Schema enforcement

3. **Monitor stream health**
   ```python
   status = ingester.get_stream_status()
   if not status['is_active']:
       # Alert and restart
   ```

4. **Checkpoint in reliable storage**
   - ADLS Gen2 or S3-compatible
   - Avoid ephemeral storage

5. **Test schema changes** before production
   - Validate new columns
   - Test NULL handling

## Related Documentation

- [Spark Autoloader](https://docs.databricks.com/ingestion/auto-loader/)
- [Cloud Files Format](https://docs.databricks.com/ingestion/cloud-object-storage/index.html)
- [Delta Lake Schema Evolution](https://docs.databricks.com/en/delta/schema-evolution.html)
- [Azure Blob Storage with Spark](https://learn.microsoft.com/en-us/azure/databricks/connect/storage/azure-storage)
