"""
Databricks Job: Event Hub Kafka Streaming Pipeline

This is the main entry point for the DAB job.
It imports and runs the streaming pipeline.

Usage in Databricks:
  - Deployed via DAB
  - Triggered by workflow scheduler or GitHub Actions
"""

import sys
from pyspark.sql import SparkSession

# In Databricks, try to import the module
# The bundle deployment puts files in the workspace, and Databricks
# adds them to the Python path automatically
try:
    # First attempt: Direct import (works if package is installed)
    from claude_pyspark_proj.streaming.get_data_from_event_hubs import run_pipeline
except ImportError as e:
    # Fallback: Load the module directly from the workspace
    print(f"[INFO] Direct import failed: {e}")
    print(f"[INFO] Attempting to load from workspace...")

    # In DAB deployment, files are at:
    # /Workspace/Users/.../files/src/jobs/event_hub_streaming.py
    # We need to add the parent 'src' directory to sys.path:
    # /Workspace/Users/.../files/src
    workspace_paths = [
        "/Workspace/Users/ashutosh_ranghar@epam.com/.bundle/claude-pyspark-proj/dev/files/src",
        "/Workspace/Users/ashutosh_ranghar@epam.com/claude-pyspark-proj/src",
    ]

    for workspace_path in workspace_paths:
        if workspace_path not in sys.path:
            sys.path.insert(0, workspace_path)
            print(f"[INFO] Added to sys.path: {workspace_path}")

    try:
        from claude_pyspark_proj.streaming.get_data_from_event_hubs import run_pipeline
        print("[INFO] Successfully imported from workspace path")
    except ImportError as e2:
        print(f"[ERROR] Failed to import streaming pipeline module: {e2}")
        print(f"[INFO] sys.path: {sys.path}")
        raise


def main():
    """Main entry point for the Databricks job."""
    spark = SparkSession.builder \
        .appName("EventHubStreamingPipeline") \
        .getOrCreate()

    try:
        print("[*] Starting Event Hub streaming pipeline...")
        run_pipeline(spark)
        print("[✓] Pipeline completed successfully!")
    except Exception as e:
        print(f"[✗] Pipeline failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
