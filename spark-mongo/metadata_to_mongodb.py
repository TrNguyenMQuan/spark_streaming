"""Kafka metadata consumer for Lab04 Task 5.

Reads JSON messages from ``code.events.metadata`` with Spark Structured Streaming
and replaces the corresponding MongoDB document by ``file_path``.  The stable
key and persistent checkpoint make restarts/replays safe.
"""

import os
from typing import Optional

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, from_json, row_number, to_timestamp
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


METADATA_SCHEMA = StructType(
    [
        StructField("schema_version", StringType(), nullable=False),
        StructField("event_timestamp", StringType(), nullable=False),
        StructField("file_path", StringType(), nullable=False),
        StructField("file_hash", StringType(), nullable=False),
        StructField("language", StringType(), nullable=False),
        StructField("loc", IntegerType(), nullable=False),
        StructField("num_nodes", IntegerType(), nullable=False),
        StructField("num_edges", IntegerType(), nullable=False),
        StructField("repo", StringType(), nullable=True),
        StructField("repo_commit", StringType(), nullable=False),
    ]
)


def setting(name: str, default: Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


KAFKA_BOOTSTRAP_SERVERS = setting("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = setting("KAFKA_TOPIC", "code.events.metadata")
MONGODB_URI = setting("MONGODB_URI")
MONGODB_DATABASE = setting("MONGODB_DATABASE", "cpg")
MONGODB_COLLECTION = setting("MONGODB_COLLECTION", "source_metadata")
CHECKPOINT_LOCATION = setting("CHECKPOINT_LOCATION")
STARTING_OFFSETS = setting("STARTING_OFFSETS", "earliest")


def write_metadata(batch_df, batch_id: int) -> None:
    """Upsert one latest metadata document per file in each micro-batch.

    Kafka's offset is used as a deterministic tiebreaker for repeat events in a
    batch. Mongo's connector performs a replace+upsert using ``file_path``.
    """
    latest_per_file = (
        batch_df.withColumn(
            "row_number",
            row_number().over(
                Window.partitionBy("file_path").orderBy(
                    col("event_timestamp").desc(), col("kafka_offset").desc()
                )
            ),
        )
        .where(col("row_number") == 1)
        .drop("row_number", "kafka_offset")
    )

    # The MongoDB Spark Connector's replace operation uses idFieldList as its
    # match filter and upserts when no matching document exists.
    (
        latest_per_file.write.format("mongodb")
        .option("connection.uri", MONGODB_URI)
        .option("database", MONGODB_DATABASE)
        .option("collection", MONGODB_COLLECTION)
        .option("operationType", "replace")
        .option("idFieldList", "file_path")
        .option("upsertDocument", "true")
        .mode("append")
        .save()
    )
    print(f"MongoDB upsert completed for micro-batch {batch_id}")


spark = SparkSession.builder.appName("lab04-metadata-to-mongodb").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

kafka_messages = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", STARTING_OFFSETS)
    .load()
)

metadata_events = (
    kafka_messages.select(
        from_json(col("value").cast("string"), METADATA_SCHEMA).alias("event"),
        col("key").cast("string").alias("kafka_key"),
        col("offset").alias("kafka_offset"),
    )
    .select("event.*", "kafka_key", "kafka_offset")
    # StructType describes the contract, but from_json represents missing fields
    # as null. Validate the v1 constraints explicitly before writing to MongoDB.
    .withColumn("parsed_event_timestamp", to_timestamp(col("event_timestamp")))
    .where(
        (col("schema_version") == "v1")
        & (col("kafka_key") == col("file_path"))
        & col("file_path").isNotNull()
        & col("event_timestamp").isNotNull()
        & col("parsed_event_timestamp").isNotNull()
        & col("file_hash").rlike("^[0-9a-f]{64}$")
        & (col("language") == "python")
        & col("loc").isNotNull()
        & (col("loc") >= 0)
        & col("num_nodes").isNotNull()
        & (col("num_nodes") >= 0)
        & col("num_edges").isNotNull()
        & (col("num_edges") >= 0)
        & col("repo_commit").isNotNull()
    )
    .drop("kafka_key", "parsed_event_timestamp")
)

query = (
    metadata_events.writeStream.foreachBatch(write_metadata)
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .start()
)

query.awaitTermination()
