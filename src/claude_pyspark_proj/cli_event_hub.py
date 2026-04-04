"""
CLI entry point for Event Hub Kafka Streaming Pipeline.

This module is called as a console script entry point when the wheel is installed.
Used by Databricks jobs to run the streaming pipeline.
"""

from pyspark.sql import SparkSession
from claude_pyspark_proj.streaming.get_data_from_event_hubs import run_pipeline


def main():
    """Main entry point for the Databricks Event Hub Streaming job."""
    spark = SparkSession.builder \
        .appName("EventHubStreamingPipeline") \
        .getOrCreate()

    try:
        print("[*] Starting Event Hub streaming pipeline...")
        run_pipeline(spark)
        print("[SUCCESS] Pipeline completed successfully!")
    except Exception as e:
        print(f"[ERROR] Pipeline failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
