# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, from_json, expr, to_timestamp, when
# from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, ArrayType
# import os

# # Get configuration from environment variables
# kafka_host = os.environ.get("KAFKA_HOST", "kafka")
# kafka_port = os.environ.get("KAFKA_PORT", "9092")
# cassandra_host = os.environ.get("CASSANDRA_HOST", "cassandra")
# cassandra_port = os.environ.get("CASSANDRA_PORT", "9042")
# kafka_servers = f"{kafka_host}:{kafka_port}"

# print(f"Kafka connected successfully: {kafka_servers}")
# print(f"Cassandra connected successfully: {cassandra_host}:{cassandra_port}")

# # Create SparkSession
# spark = SparkSession.builder \
#     .appName("CoinbaseCandles") \
#     .config("spark.cassandra.connection.host", cassandra_host) \
#     .config("spark.cassandra.connection.port", cassandra_port) \
#     .getOrCreate()

# # Define schema for individual candle object from Advanced Trade API
# # This schema directly matches the structure of each candle object in the events.candles array
# candle_schema = StructType([
#     StructField("start", StringType(), True),
#     StructField("high", StringType(), True),
#     StructField("low", StringType(), True),
#     StructField("open", StringType(), True),
#     StructField("close", StringType(), True),
#     StructField("volume", StringType(), True),
#     StructField("product_id", StringType(), True)
# ])

# # Read stream from Kafka
# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", kafka_servers) \
#     .option("subscribe", "coin-data-model") \
#     .option("startingOffsets", "earliest") \
#     .option("kafka.security.protocol", "PLAINTEXT") \
#     .load()

# # Parse the JSON message which is already a candle object
# parsed_df = df.select(
#     from_json(col("value").cast("string"), candle_schema).alias("candle")
# ).select("candle.*")

# # Process the data types
# processed_df = parsed_df.select(
#     col("product_id"),
#     # Convert start timestamp (Unix timestamp in seconds)
#     when(col("start").cast("long").isNotNull(), 
#          to_timestamp(col("start").cast("long"))).otherwise(
#          to_timestamp(col("start"))).alias("time"),
#     col("open").cast("double").alias("open"),
#     col("high").cast("double").alias("high"),
#     col("low").cast("double").alias("low"),
#     col("close").cast("double").alias("close"),
#     col("volume").cast("double").alias("volume")
# )

# # Checkpoint path
# checkpoint_path = "/tmp/spark-candles-checkpoint"

# # Save to Cassandra
# cassandra_query = processed_df.writeStream \
#     .foreachBatch(lambda batch_df, batch_id: 
#         batch_df.write \
#             .format("org.apache.spark.sql.cassandra") \
#             .option("keyspace", "coinbase") \
#             .option("table", "candles") \
#             .mode("append") \
#             .save()
#     ) \
#     .option("checkpointLocation", checkpoint_path) \
#     .start()

# print("Writing candles data to Cassandra...")

# # Wait for queries to complete
# spark.streams.awaitAnyTermination()