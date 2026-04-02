"""
Simple PySpark test to verify Spark environment.
Run with: pytest src/claude_pyspark_proj/test_spark_simple.py -v -s
"""

from pyspark.sql import SparkSession


def test_spark_session_creation():
    """Test that we can create a Spark session."""
    spark = SparkSession.builder \
        .appName("test_simple") \
        .master("local[1]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()

    try:
        assert spark is not None
        assert spark.sparkContext.appName == "test_simple"
        print("\n[PASS] Spark session created successfully!")
    finally:
        spark.stop()


def test_spark_version():
    """Test that Spark is installed and accessible."""
    spark = SparkSession.builder \
        .appName("test_version") \
        .master("local[1]") \
        .getOrCreate()

    try:
        version = spark.version
        print(f"\n[PASS] PySpark version: {version}")
        assert version is not None
    finally:
        spark.stop()
