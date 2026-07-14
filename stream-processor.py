from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import from_json

polymarketSchema = StructType([
    StructField("slug", StringType()),
    StructField("startDate", StringType()),
    StructField("endDate", StringType()),
    StructField("outcomes", StringType()),
    StructField("outcomePrices", StringType()),
])

coingeckoSchema = StructType([
    StructField("slug", StringType()),
    StructField("usd", IntegerType()),
])

# Create SparkSession with Kafka packages
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,io.delta:delta-spark_2.12:3.3.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

polymarketKafkaDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "topicBTCpm") \
    .option("failOnDataLoss", "false") \
    .load()

# Cast the value column to a string
polymarketParsedJSON = polymarketKafkaDF.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), polymarketSchema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", current_timestamp())

coingeckoKafkaDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "topicBTCcg") \
    .option("failOnDataLoss", "false") \
    .load()

coingeckoParsedJSON = coingeckoKafkaDF.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), coingeckoSchema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", current_timestamp())

# Check if query already running
for s in spark.streams.active:
    if s.name == "data":
        print(f"Stopping existing query: data")
        s.stop()

polymarketQuery = polymarketParsedJSON.writeStream \
    .format("delta") \
    .option("checkpointLocation", "./checkpoints/pm_checkpoints") \
    .start("./data/pm_data")

coingeckoQuery = coingeckoParsedJSON.writeStream \
    .format("delta") \
    .option("checkpointLocation", "./checkpoints/cg_checkpoints") \
    .start("./data/cg_data")

spark.streams.awaitAnyTermination()