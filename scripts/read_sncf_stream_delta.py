from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StringType, ArrayType, StructField, LongType

# Initialisation Spark + Delta
spark = SparkSession.builder \
    .appName("SNCFTripUpdatesDelta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Schéma
update_schema = StructType([
    StructField("stop_id", StringType()),
    StructField("arrival", LongType()),
    StructField("departure", LongType())
])

schema = StructType([
    StructField("trip_id", StringType()),
    StructField("start_date", StringType()),
    StructField("updates", ArrayType(update_schema))
])

# Lecture Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "sncf-trip-updates") \
    .option("startingOffsets", "latest") \
    .load()

# Conversion JSON
json_df = df.selectExpr("CAST(value AS STRING) as json_str")
parsed_df = json_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# Explosion des données
exploded_df = parsed_df.select(
    col("trip_id"),
    col("start_date"),
    explode(col("updates")).alias("stop")
).select(
    col("trip_id"),
    col("start_date"),
    col("stop.stop_id").alias("stop_id"),
    col("stop.arrival").alias("arrival"),
    col("stop.departure").alias("departure")
)

# Sauvegarde en Delta Lake
query = exploded_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/home/jovyan/data/checkpoint_delta") \
    .start("/home/jovyan/data/delta_sncf")

query.awaitTermination()
