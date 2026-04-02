"""
Databricks Job: Event Hub Kafka Streaming Pipeline

This is the main entry point for the DAB job.
It imports and runs the streaming pipeline.

Usage in Databricks:
  - Deployed via DAB
  - Triggered by workflow scheduler or GitHub Actions
"""

import sys
from pathlib import Path
from pyspark.sql import SparkSession

# Add src directory to Python path for imports
# In Databricks, the job file is in /Workspace/Users/.../files/src/jobs/event_hub_streaming.py
# So we need to add the parent src directory to the path
src_path = str(Path(__file__).parent.parent)
if src_path not in sys.path:
    sys.path.insert(0, src_path)

# Import the streaming pipeline
from claude_pyspark_proj.streaming.get_data_from_event_hubs import run_pipeline


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
