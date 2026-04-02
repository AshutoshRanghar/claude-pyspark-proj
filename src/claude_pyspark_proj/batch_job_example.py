"""
Simple Spark Batch Job — demonstrates DataFrame operations locally.

Run with:
    python src/claude_pyspark_proj/batch_job_example.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, avg, round as spark_round


def run_batch_job():
    """Run a simple batch job with in-memory data."""
    spark = SparkSession.builder \
        .appName("BatchJobExample") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    # Create sample taxi ride data
    taxi_data = [
        ("vendor_1", "location_A", 25.50, 2),
        ("vendor_2", "location_B", 18.75, 1),
        ("vendor_1", "location_A", 30.00, 3),
        ("vendor_1", "location_C", 22.25, 2),
        ("vendor_2", "location_A", 28.50, 2),
        ("vendor_2", "location_B", 16.00, 1),
        ("vendor_1", "location_B", 35.00, 4),
        ("vendor_2", "location_C", 20.00, 1),
    ]

    columns = ["vendor", "location", "fare", "passenger_count"]
    df = spark.createDataFrame(taxi_data, columns)

    print("\n" + "="*60)
    print("ORIGINAL DATA")
    print("="*60)
    df.show()

    # Aggregation 1: Revenue per vendor
    vendor_revenue = df.groupBy("vendor") \
        .agg(
            spark_sum("fare").alias("total_revenue"),
            count("*").alias("trip_count"),
            spark_round(avg("fare"), 2).alias("avg_fare")
        ) \
        .orderBy(col("total_revenue").desc())

    print("\n" + "="*60)
    print("VENDOR REVENUE ANALYSIS")
    print("="*60)
    vendor_revenue.show()

    # Aggregation 2: Traffic per location
    location_traffic = df.groupBy("location") \
        .agg(
            count("*").alias("trip_count"),
            spark_sum("passenger_count").alias("total_passengers"),
            spark_round(avg("fare"), 2).alias("avg_fare")
        ) \
        .orderBy(col("trip_count").desc())

    print("\n" + "="*60)
    print("LOCATION TRAFFIC ANALYSIS")
    print("="*60)
    location_traffic.show()

    # Write results to CSV (local file system)
    output_dir = "target/results"
    try:
        vendor_revenue.coalesce(1) \
            .write.mode("overwrite") \
            .csv(output_dir + "/vendor_revenue", header=True)
        print(f"\n✓ Results written to {output_dir}/vendor_revenue/")
    except Exception as e:
        print(f"\n⚠ Could not write to file: {e}")

    spark.stop()
    print("\n✓ Batch job completed successfully!")


if __name__ == "__main__":
    run_batch_job()
