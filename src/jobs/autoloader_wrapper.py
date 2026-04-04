"""
Wrapper script for Autoloader Blob Storage Ingestion job.

This script is minimal and only imports from the installed wheel package.
It runs via Databricks job pointing to this file.

The actual logic is in claude_pyspark_proj.cli_autoloader (from wheel).
"""

from claude_pyspark_proj.cli_autoloader import main

if __name__ == "__main__":
    main()
