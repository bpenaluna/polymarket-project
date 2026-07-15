from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,io.delta:delta-spark_2.12:3.3.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read the folder as a single Delta table
pm_df = spark.read.format("delta").load("./data/pm_data")
cg_df = spark.read.format("delta").load("./data/cg_data")

print("Polymarket Data\n===============")
print(pm_df.show())
print(pm_df.count())
print("\n")
print("Coingecko Data\n==============")
print(cg_df.show())
print(cg_df.count())