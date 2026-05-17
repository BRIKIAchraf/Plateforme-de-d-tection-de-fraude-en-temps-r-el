import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
DELTA_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "delta_transactions")

# Configuration de Spark avec Delta Lake
spark = SparkSession.builder \
    .appName("KafkaToDeltaLake") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Schéma du message (adapté du producer Flink qui renvoie transaction, score, decision)
schema = StructType([
    StructField("transaction", StructType([
        StructField("transactionId", StringType()),
        StructField("userId", StringType()),
        StructField("cardId", StringType()),
        StructField("amount", DoubleType()),
        StructField("merchant", StringType()),
        StructField("country", StringType()),
        StructField("timestamp", StringType())
    ])),
    StructField("score", DoubleType()),
    StructField("decision", StringType()),
    StructField("latency_ms", DoubleType())
])

# Lecture depuis Kafka en Streaming
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", KAFKA_BROKER) \
  .option("subscribe", "fraud-scores") \
  .option("startingOffsets", "earliest") \
  .load()

# Désérialisation
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Aplatissement et ajout d'une colonne de partition
final_df = parsed_df \
    .select("transaction.*", "score", "decision", "latency_ms") \
    .withColumn("ingest_date", expr("to_date(timestamp)")) \
    .withColumn("ingest_timestamp", current_timestamp())

# Écriture en streaming dans Delta Lake avec partitionnement par ingest_date
query = final_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .partitionBy("ingest_date") \
    .option("checkpointLocation", os.path.join(DELTA_PATH, "_checkpoints")) \
    .start(DELTA_PATH)

print(f"Démarrage de l'écriture en continu vers Delta Lake: {DELTA_PATH}")
query.awaitTermination()
