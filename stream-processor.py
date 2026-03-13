from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json

# Define the schema
schema = StructType([
    StructField("slug", StringType()),
    StructField("outcomes", StringType()),
    StructField("outcomePrices", StringType()),
])

# Create SparkSession with Kafka packages
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,io.delta:delta-spark_2.12:3.3.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Read from Kafka
kafkaDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "topicBTCpm") \
    .load()

# Cast the value column to a string
parsedJSON = kafkaDF.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumnRenamed("slug", "name") \
    .withColumn("timestamp", current_timestamp())

# Check if query already running
for s in spark.streams.active:
    if s.name == "data":
        print(f"Stopping existing query: data")
        s.stop()

# Create the "Write" stream to display data in your terminal
query = parsedJSON.writeStream \
    .format("delta") \
    .option("checkpointLocation", "./checkpoints") \
    .start("./data/pm_data")

query.awaitTermination()