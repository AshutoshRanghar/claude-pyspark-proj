"""
Autoloader package for Azure Blob Storage ingestion.

Provides Cloud Files-based streaming ingestion with:
- Schema evolution and addNewColumns support
- Automatic format detection
- Fault-tolerant checkpointing
- Multi-format support (CSV, JSON, Parquet)
"""

from .cloud_file_ingester import (
    AutoloaderConfig,
    CloudFileIngester,
    create_ingester,
)

__all__ = [
    "AutoloaderConfig",
    "CloudFileIngester",
    "create_ingester",
]
