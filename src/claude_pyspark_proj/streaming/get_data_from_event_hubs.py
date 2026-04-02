"""Kafka Streaming Pipeline — Azure Event Hubs → Delta Lake.

Reads taxi-ride events from Azure Event Hubs (Kafka interface),
parses JSON payloads, and writes two streaming KPI tables:
  • kpi_vendor_revenue  – total revenue per vendor
  • kpi_pickup_traffic  – trip count per pickup location

Usage (Databricks notebook or job):
    from kafka_streaming_pipeline import run_pipeline
    run_pipeline(spark)
"""

from dataclasses import dataclass

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    from_json,
    from_unixtime,
    sum as spark_sum,
    to_timestamp,
)
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class KafkaConfig:
    """Connection settings for Azure Event Hubs (Kafka endpoint)."""

    bootstrap_server: str = "ash-kafka.servicebus.windows.net:9093"
    topic: str = "ash-basic-kafka-topic-1"
    connection_string: str = (
        "Endpoint=sb://ash-kafka.servicebus.windows.net/;"
        "SharedAccessKeyName=RootManageSharedAccessKey;"
        "SharedAccessKey=QVVL4kfnfH9ow6K9WTxbgS1HBsAPYyf3k+AEhOhOMcU="
    )

    @property
    def sasl_config(self) -> str:
        """Build the JAAS config string for SASL_SSL authentication."""
        return (
            "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
            f'required username="$ConnectionString" '
            f'password="{self.connection_string}";'
        )


@dataclass(frozen=True)
class CheckpointConfig:
    """DBFS checkpoint paths for each streaming sink."""

    kafka_read: str = "/FileStore/checkpoints/ash-kafka-test"
    vendor_revenue: str = "/FileStore/checkpoints/kpi_vendor_rev"
    pickup_traffic: str = "/FileStore/checkpoints/kpi_traffic"


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
TAXI_RIDE_SCHEMA = StructType(
    [
        StructField("vendorID", StringType(), True),
        StructField("tpepPickupDateTime", LongType(), True),
        StructField("tpepDropoffDateTime", LongType(), True),
        StructField("passengerCount", IntegerType(), True),
        StructField("tripDistance", DoubleType(), True),
        StructField("puLocationId", StringType(), True),
        StructField("doLocationId", StringType(), True),
        StructField("rateCodeId", IntegerType(), True),
        StructField("storeAndFwdFlag", StringType(), True),
        StructField("paymentType", IntegerType(), True),
        StructField("fareAmount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mtaTax", DoubleType(), True),
        StructField("improvementSurcharge", StringType(), True),
        StructField("tipAmount", DoubleType(), True),
        StructField("tollsAmount", DoubleType(), True),
        StructField("totalAmount", DoubleType(), True),
    ]
)


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------
def read_kafka_stream(
    spark: SparkSession,
    kafka_cfg: KafkaConfig,
    checkpoint_path: str,
) -> DataFrame:
    """Create a streaming DataFrame from the Azure Event Hubs Kafka topic."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_cfg.bootstrap_server)
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.jaas.config", kafka_cfg.sasl_config)
        .option("startingOffsets", "earliest")
        .option("subscribe", kafka_cfg.topic)
        .option("checkpointLocation", checkpoint_path)
        .load()
    )


# ---------------------------------------------------------------------------
# Transformation
# ---------------------------------------------------------------------------
def parse_taxi_events(raw_df: DataFrame) -> DataFrame:
    """Parse Kafka value JSON and convert epoch-ms timestamps."""
    parsed_df = (
        raw_df.select(
            from_json(col("value").cast("string"), TAXI_RIDE_SCHEMA).alias("data")
        )
        .select("data.*")
        .withColumn(
            "event_time",
            to_timestamp(from_unixtime(col("tpepPickupDateTime") / 1000)),
        )
        .withColumn(
            "pickup_time",
            to_timestamp(from_unixtime(col("tpepPickupDateTime") / 1000)),
        )
        .withColumn(
            "dropoff_time",
            to_timestamp(from_unixtime(col("tpepDropoffDateTime") / 1000)),
        )
    )
    return parsed_df


# ---------------------------------------------------------------------------
# Streaming Sinks
# ---------------------------------------------------------------------------
def write_vendor_revenue(
    df: DataFrame,
    checkpoint_path: str,
) -> StreamingQuery:
    """Aggregate total revenue per vendor and write to a Delta table."""
    vendor_revenue_df = (
        df.withWatermark("event_time", "10 minutes")
        .groupBy("vendorID")
        .agg(spark_sum("totalAmount").alias("totalAmount"))
    )

    return (
        vendor_revenue_df.writeStream.format("delta")
        .outputMode("complete")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable("kpi_vendor_revenue")
    )


def write_pickup_traffic(
    df: DataFrame,
    checkpoint_path: str,
) -> StreamingQuery:
    """Count trips per pickup location and write to a Delta table."""
    pickup_traffic_df = (
        df.withWatermark("event_time", "10 minutes")
        .groupBy("puLocationId")
        .count()
    )

    return (
        pickup_traffic_df.writeStream.format("delta")
        .outputMode("complete")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable("kpi_pickup_traffic")
    )


# ---------------------------------------------------------------------------
# Pipeline Orchestration
# ---------------------------------------------------------------------------
def run_pipeline(spark: SparkSession) -> None:
    """End-to-end pipeline: ingest → parse → write KPI tables."""
    spark.conf.set(
        "spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false"
    )

    kafka_cfg = KafkaConfig()
    ckpt_cfg = CheckpointConfig()

    raw_df = read_kafka_stream(spark, kafka_cfg, ckpt_cfg.kafka_read)
    parsed_df = parse_taxi_events(raw_df)

    vendor_query = write_vendor_revenue(parsed_df, ckpt_cfg.vendor_revenue)
    traffic_query = write_pickup_traffic(parsed_df, ckpt_cfg.pickup_traffic)

    # Block until both micro-batch triggers finish
    vendor_query.awaitTermination()
    traffic_query.awaitTermination()

    print("Pipeline complete — both KPI tables updated.")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    run_pipeline(spark)
