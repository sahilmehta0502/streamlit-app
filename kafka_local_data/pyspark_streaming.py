from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# ✅ Initialize Spark Session with Kafka Support
spark = SparkSession.builder \
    .appName("ImageMetadataStreaming") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

# ✅ Define Schema for Incoming Data
schema = StructType([
    StructField("file_name", StringType(), True),
    StructField("image_format", StringType(), True),
    StructField("resolution", StringType(), True),
    StructField("size_kb", FloatType(), True)
])

# ✅ Read Stream from Kafka
streaming_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "image_metadata") \
    .option("startingOffsets", "earliest") \
    .load()

# ✅ Convert Kafka Value (Binary) to JSON
json_df = streaming_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# ✅ Streaming Queries

# 🔹 Query 1: Count Images by Format (Real-time)
query1 = json_df.groupBy("image_format").count()

# 🔹 Query 2: Find Largest Image (Real-time)
query2 = json_df.orderBy("size_kb", ascending=False).limit(1)

# 🔹 Query 3: Find Smallest Image (Real-time)
query3 = json_df.orderBy("size_kb", ascending=True).limit(1)

# ✅ Output Streaming Results to Console
query1.writeStream.format("console").outputMode("complete").start()
query2.writeStream.format("console").outputMode("complete").start()
query3.writeStream.format("console").outputMode("complete").start()

# ✅ Wait for Stream to Run
spark.streams.awaitAnyTermination()
