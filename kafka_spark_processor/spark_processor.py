from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
import os

# Lấy thông tin cấu hình từ biến môi trường
kafka_host = os.environ.get("KAFKA_HOST", "kafka")
kafka_port = os.environ.get("KAFKA_PORT", "9092")
cassandra_host = os.environ.get("CASSANDRA_HOST", "cassandra")
cassandra_port = os.environ.get("CASSANDRA_PORT", "9042")
kafka_servers = f"{kafka_host}:{kafka_port}"

print(f"Kết nối đến Kafka: {kafka_servers}")
print(f"Kết nối đến Cassandra: {cassandra_host}:{cassandra_port}")

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("CoinbasePipeline") \
    .config("spark.cassandra.connection.host", cassandra_host) \
    .config("spark.cassandra.connection.port", cassandra_port) \
    .getOrCreate()

# Schema cho dữ liệu từ Kafka
schema = StructType([
    StructField("type", StringType(), True),
    StructField("sequence", LongType(), True),
    StructField("product_id", StringType(), True),
    StructField("price", StringType(), True),
    StructField("open_24h", StringType(), True),
    StructField("volume_24h", StringType(), True),
    StructField("low_24h", StringType(), True),
    StructField("high_24h", StringType(), True),
    StructField("volume_30d", StringType(), True),
    StructField("best_bid", StringType(), True),
    StructField("best_ask", StringType(), True),
    StructField("side", StringType(), True),
    StructField("time", StringType(), True),
    StructField("trade_id", LongType(), True),
    StructField("last_size", StringType(), True)
])

# Đọc stream từ Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_servers) \
    .option("subscribe", "coin-data") \
    .option("startingOffsets", "earliest") \
    .option("kafka.security.protocol", "PLAINTEXT") \
    .load()

# Parse JSON từ Kafka
json_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Xử lý dữ liệu
processed_df = json_df.select(
    "product_id",
    col("price").cast("double").alias("price"),
    to_timestamp(col("time")).alias("time")
).where(col("type") == "ticker")

# Đường dẫn checkpoint tương đối
checkpoint_path = "/tmp/spark-checkpoint"

# Lưu vào Cassandra
cassandra_query = processed_df.writeStream \
    .foreachBatch(lambda batch_df, batch_id: 
        batch_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", "coinbase") \
            .option("table", "prices") \
            .mode("append") \
            .save()
    ) \
    .option("checkpointLocation", checkpoint_path) \
    .start()

print("Đã bắt đầu ghi dữ liệu vào Cassandra...")

# Chờ các query hoàn thành
spark.streams.awaitAnyTermination()